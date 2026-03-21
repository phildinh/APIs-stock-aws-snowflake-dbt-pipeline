# ============================================================
# fetch_company_profiles.py
# Purpose: Pull company profiles from Finnhub → S3 → Snowflake
# Run:     python -m ingestion.fetch_company_profiles
# ============================================================

import os
from datetime import date
from dotenv import load_dotenv
from ingestion.utils.logger import get_logger
from ingestion.utils.snowflake_client import execute_query, fetch_results
from ingestion.utils.s3_client import upload_json
from ingestion.utils.finnhub_client import get_company_profile

load_dotenv()
logger = get_logger(__name__)

TICKERS = ["AAPL", "MSFT", "NVDA", "JPM", "JNJ", "AMZN", "XOM", "CAT", "KO", "GOOGL"]

RAW_TABLE = "STOCK_DB_DEV.RAW.RAW_COMPANY_PROFILES"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
    ticker        VARCHAR(10),
    company_name  VARCHAR(200),
    sector        VARCHAR(100),
    exchange      VARCHAR(50),
    market_cap    FLOAT,
    country       VARCHAR(50),
    currency      VARCHAR(10),
    logo_url      VARCHAR(500),
    ipo_date      DATE,
    weburl        VARCHAR(500),
    _extracted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    _source       VARCHAR(50)   DEFAULT 'finnhub'
)
"""

MERGE_SQL = f"""
MERGE INTO {RAW_TABLE} AS target
USING (SELECT %s AS ticker) AS source
ON target.ticker = source.ticker
WHEN MATCHED THEN UPDATE SET
    company_name  = %s,
    sector        = %s,
    exchange      = %s,
    market_cap    = %s,
    country       = %s,
    currency      = %s,
    logo_url      = %s,
    ipo_date      = %s,
    weburl        = %s,
    _loaded_at    = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    ticker, company_name, sector, exchange,
    market_cap, country, currency, logo_url, ipo_date, weburl
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def ensure_raw_table_exists():
    logger.info("Ensuring RAW_COMPANY_PROFILES table exists...")
    execute_query(CREATE_TABLE_SQL)


def ingest_profiles():
    """Main ingestion function — Finnhub → S3 → Snowflake."""
    ensure_raw_table_exists()

    for ticker in TICKERS:
        logger.info(f"Processing {ticker}...")

        # 1. Fetch from Finnhub
        profile = get_company_profile(ticker)
        if not profile:
            logger.warning(f"Skipping {ticker} — no profile returned")
            continue

        # 2. Upload raw JSON to S3
        s3_key = f"company_profiles/{ticker}/{date.today()}.json"
        upload_json(profile, s3_key)

        # 3. MERGE into Snowflake — upsert on ticker
        execute_query(MERGE_SQL, params=(
            # USING clause
            ticker,
            # WHEN MATCHED — update values
            profile["company_name"],
            profile["sector"],
            profile["exchange"],
            profile["market_cap"],
            profile["country"],
            profile["currency"],
            profile["logo_url"],
            profile["ipo_date"],
            profile["weburl"],
            # WHEN NOT MATCHED — insert values
            ticker,
            profile["company_name"],
            profile["sector"],
            profile["exchange"],
            profile["market_cap"],
            profile["country"],
            profile["currency"],
            profile["logo_url"],
            profile["ipo_date"],
            profile["weburl"],
        ))
        logger.info(f"Upserted profile for {ticker}")

    logger.info("fetch_company_profiles.py complete")


if __name__ == "__main__":
    ingest_profiles()