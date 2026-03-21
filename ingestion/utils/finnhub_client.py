# ============================================================
# finnhub_client.py
# Purpose: Fetch company profiles from Finnhub API
# Usage:   from utils.finnhub_client import get_company_profile
# ============================================================

import os
import time
import finnhub
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
from ingestion.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)


def get_finnhub_client():
    """Create and return a Finnhub client."""
    return finnhub.Client(api_key=os.getenv("FINNHUB_API_KEY"))


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_company_profile(ticker: str) -> dict | None:
    """
    Fetch company profile for a ticker from Finnhub.

    Args:
        ticker: Stock symbol e.g. 'AAPL'

    Returns:
        Dict ready for Snowflake insert, or None if not found
    """
    logger.info(f"Fetching company profile for {ticker}")
    client  = get_finnhub_client()
    profile = client.company_profile2(symbol=ticker)

    if not profile:
        logger.warning(f"No profile returned for {ticker}")
        return None

    record = {
        "ticker":       ticker,
        "company_name": profile.get("name"),
        "sector":       profile.get("finnhubIndustry"),
        "exchange":     profile.get("exchange"),
        "market_cap":   profile.get("marketCapitalization"),
        "country":      profile.get("country"),
        "currency":     profile.get("currency"),
        "logo_url":     profile.get("logo"),
        "ipo_date":     profile.get("ipo"),
        "weburl":       profile.get("weburl"),
    }

    logger.info(f"Fetched profile for {ticker}: {record['company_name']}")

    # Rate limit — Finnhub free tier is 60 calls/minute
    time.sleep(10)

    return record