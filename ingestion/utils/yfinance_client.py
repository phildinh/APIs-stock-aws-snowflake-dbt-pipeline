# ============================================================
# yfinance_client.py
# Purpose: Fetch historical stock prices from yfinance
# Usage:   from utils.yfinance_client import get_stock_prices
# ============================================================

import time
import yfinance as yf
from ingestion.utils.logger import get_logger

logger = get_logger(__name__)


def get_stock_prices(ticker: str, start_date: str, end_date: str) -> list[dict]:
    """
    Fetch daily OHLCV prices for a ticker from yfinance.

    Args:
        ticker:     Stock symbol e.g. 'AAPL'
        start_date: 'YYYY-MM-DD'
        end_date:   'YYYY-MM-DD'

    Returns:
        List of dicts, one per trading day, ready for Snowflake insert
    """
    logger.info(f"Fetching prices for {ticker} from {start_date} to {end_date}")

    df = yf.download(
        tickers          = ticker,
        start            = start_date,
        end              = end_date,
        auto_adjust      = True,
        multi_level_index = False   # prevents multi-level column headers
    )

    if df.empty:
        logger.warning(f"No data returned for {ticker}")
        return []

    df = df.reset_index()
    df.columns = [col.lower() for col in df.columns]

    records = []
    for _, row in df.iterrows():
        records.append({
            "ticker":     ticker,
            "trade_date": str(row["date"].date()),
            "open_price":  round(float(row["open"]),  6),
            "high_price":  round(float(row["high"]),  6),
            "low_price":   round(float(row["low"]),   6),
            "close_price": round(float(row["close"]), 6),
            "volume":      int(row["volume"]),
        })

    logger.info(f"Fetched {len(records)} rows for {ticker}")

    # Rate limit — be respectful to yfinance
    time.sleep(10)

    return records