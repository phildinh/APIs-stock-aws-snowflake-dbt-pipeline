import os
import time
import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from ingestion.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

BASE_URL = "https://financialmodelingprep.com/api/v3"


def is_retryable_error(exception):
    """Only retry on temporary errors — never on 403 or 404."""
    if isinstance(exception, requests.exceptions.HTTPError):
        status_code = exception.response.status_code
        return status_code not in [403, 404, 401]
    return True


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception(is_retryable_error)
)
def get_financial_statements(ticker: str, limit: int = 5) -> list[dict]:
    logger.info(f"Fetching financial statements for {ticker}")

    url      = f"{BASE_URL}/income-statement/{ticker}"
    params   = {"limit": limit, "apikey": os.getenv("FMP_API_KEY")}
    response = requests.get(url, params=params, timeout=30)

    if response.status_code == 403:
        logger.error(f"FMP 403 Forbidden for {ticker} — endpoint not available on free tier")
        return []

    response.raise_for_status()
    data = response.json()

    if not data:
        logger.warning(f"No financial statements returned for {ticker}")
        return []

    records = []
    for item in data:
        records.append({
            "ticker":            ticker,
            "fiscal_date":       item.get("date"),
            "fiscal_year":       item.get("calendarYear"),
            "period":            item.get("period"),
            "revenue":           item.get("revenue"),
            "gross_profit":      item.get("grossProfit"),
            "operating_income":  item.get("operatingIncome"),
            "net_income":        item.get("netIncome"),
            "eps":               item.get("eps"),
            "ebitda":            item.get("ebitda"),
        })

    logger.info(f"Fetched {len(records)} statements for {ticker}")
    time.sleep(10)
    return records