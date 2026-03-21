# ============================================================
# fetch_financials.py
# Purpose: Pull income statements from FMP → S3 → Snowflake
# Run:     python -m ingestion.fetch_financials
# ============================================================

import os
from datetime import date
from dotenv import load_dotenv
from ingestion.utils.logger import get_logger
from ingestion.utils.snowflake_client import execute_query, execute_many
from ingestion.utils.s3_client import upload_json
from ingestion.utils.fmp_client import get_financial_statements

load_dotenv()
logger = get_logger(__name__)

TICKERS = ["AAPL", "MSFT", "NVDA", "JPM", "JNJ", "AMZN", "XOM", "CAT", "KO", "GOOGL"]

RAW_TABLE = "STOCK_DB_DEV.RAW.RAW_FINANCIAL_STATEMENTS"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
    ticker           VARCHAR(10),
    fiscal_date      DATE,
    fiscal_year      VARCHAR(4),
    period           VARCHAR(10),
    revenue          FLOAT,
    gross_profit     FLOAT,
    operating_income FLOAT,
    net_income       FLOAT,
    eps              FLOAT,
    ebitda           FLOAT,
    _extracted_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    _source          VARCHAR(50)   DEFAULT 'fmp'
)
"""

INSERT_SQL = f"""
INSERT INTO {RAW_TABLE} (
    ticker, fiscal_date, fiscal_year, period,
    revenue, gross_profit, operating_income,
    net_income, eps, ebitda, _source
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def ensure_raw_table_exists():
    logger.info("Ensuring RAW_FINANCIAL_STATEMENTS table exists...")
    execute_query(CREATE_TABLE_SQL)


def ingest_financials():
    """Main ingestion function — FMP → S3 → Snowflake."""
    ensure_raw_table_exists()

    for ticker in TICKERS:
        logger.info(f"Processing {ticker}...")

        # 1. Fetch from FMP
        statements = get_financial_statements(ticker, limit=5)
        if not statements:
            logger.warning(f"Skipping {ticker} — no statements returned")
            continue

        # 2. Upload raw JSON to S3 as backup
        s3_key = f"financial_statements/{ticker}/{date.today()}.json"
        upload_json(statements, s3_key)

        # 3. Idempotency — delete all existing rows for this ticker
        # We always fetch all 5 years so a full replace is safe and simpler
        execute_query(
            f"DELETE FROM {RAW_TABLE} WHERE ticker = %s",
            params=(ticker,)
        )

        # 4. Bulk insert all statements in one batch — one connection, 5 rows
        rows = [
            (
                stmt["ticker"],
                stmt["fiscal_date"],
                stmt["fiscal_year"],
                stmt["period"],
                stmt["revenue"],
                stmt["gross_profit"],
                stmt["operating_income"],
                stmt["net_income"],
                stmt["eps"],
                stmt["ebitda"],
                "fmp",
            )
            for stmt in statements
        ]
        execute_many(INSERT_SQL, rows)
        logger.info(f"Loaded {len(rows)} statements for {ticker}")

    logger.info("fetch_financials.py complete")


if __name__ == "__main__":
    ingest_financials()