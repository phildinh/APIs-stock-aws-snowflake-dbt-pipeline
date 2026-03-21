# ============================================================
# fetch_stock_prices.py
# Purpose: Pull daily stock prices from yfinance → S3 → Snowflake
# Run:     python -m ingestion.fetch_stock_prices
# ============================================================

import os
from datetime import date, timedelta
from dotenv import load_dotenv
from ingestion.utils.logger import get_logger
from ingestion.utils.snowflake_client import execute_query, execute_many, fetch_results
from ingestion.utils.s3_client import upload_json
from ingestion.utils.yfinance_client import get_stock_prices

load_dotenv()
logger = get_logger(__name__)

TICKERS = {
    "Technology":       ["AAPL", "MSFT", "NVDA"],
    "Financials":       ["JPM"],
    "Healthcare":       ["JNJ"],
    "Consumer":         ["AMZN"],
    "Energy":           ["XOM"],
    "Industrials":      ["CAT"],
    "Consumer Staples": ["KO"],
    "Communications":   ["GOOGL"],
}

ALL_TICKERS = [t for tickers in TICKERS.values() for t in tickers]

RAW_TABLE = "STOCK_DB_DEV.RAW.RAW_STOCK_PRICES"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
    ticker        VARCHAR(10),
    trade_date    DATE,
    open_price    FLOAT,
    high_price    FLOAT,
    low_price     FLOAT,
    close_price   FLOAT,
    volume        BIGINT,
    _extracted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    _source       VARCHAR(50)   DEFAULT 'yfinance'
)
"""

INSERT_SQL = f"""
INSERT INTO {RAW_TABLE} (
    ticker, trade_date, open_price, high_price,
    low_price, close_price, volume, _source
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


def ensure_raw_table_exists():
    """Create RAW_STOCK_PRICES if it doesn't exist yet."""
    logger.info("Ensuring RAW_STOCK_PRICES table exists...")
    execute_query(CREATE_TABLE_SQL)


def get_load_window() -> tuple[str, str]:
    """
    Determine the date range to load.
    - First run (table empty): last 2 years
    - Subsequent runs: yesterday only (incremental)
    """
    results = fetch_results(f"SELECT MAX(trade_date) AS max_date FROM {RAW_TABLE}")
    max_date = results[0]["MAX_DATE"] if results else None

    if max_date is None:
        # Full load — 2 years of history
        start = str(date.today() - timedelta(days=730))
        end   = str(date.today())
        logger.info(f"Full load: {start} to {end}")
    else:
        # Incremental — pick up from last loaded date
        start = str(max_date + timedelta(days=1))
        end   = str(date.today())
        logger.info(f"Incremental load: {start} to {end}")

    return start, end


def ingest_prices():
    """Main ingestion function — yfinance → S3 → Snowflake."""
    ensure_raw_table_exists()
    start_date, end_date = get_load_window()

    for ticker in ALL_TICKERS:
        logger.info(f"Processing {ticker}...")

        # 1. Fetch from yfinance
        records = get_stock_prices(ticker, start_date, end_date)
        if not records:
            logger.warning(f"Skipping {ticker} — no data returned")
            continue

        # 2. Upload raw JSON to S3 as backup
        s3_key = f"stock_prices/{ticker}/{end_date}.json"
        upload_json(records, s3_key)

        # 3. Idempotency — delete today's rows before inserting
        # Prevents duplicates if script is re-run on the same day
        execute_query(
            f"DELETE FROM {RAW_TABLE} WHERE ticker = %s AND trade_date >= %s",
            params=(ticker, start_date)
        )

        # 4. Insert into Snowflake
        rows = [
            (
                r["ticker"],
                r["trade_date"],
                r["open_price"],
                r["high_price"],
                r["low_price"],
                r["close_price"],
                r["volume"],
                "yfinance"
            )
            for r in records
        ]
        execute_many(INSERT_SQL, rows)
        logger.info(f"Loaded {len(rows)} rows for {ticker}")

    logger.info("fetch_stock_prices.py complete")


if __name__ == "__main__":
    ingest_prices()