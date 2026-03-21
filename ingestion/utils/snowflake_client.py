# ============================================================
# snowflake_client.py
# Purpose: Snowflake connection and query execution
# Usage:   from utils.snowflake_client import get_snowflake_connection
# ============================================================

import os
import snowflake.connector
from dotenv import load_dotenv
from ingestion.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)


def get_snowflake_connection():
    """Create and return a Snowflake connection using env vars."""
    logger.info("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        role      = os.getenv("SNOWFLAKE_ROLE"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        database  = os.getenv("SNOWFLAKE_DATABASE"),
        schema    = os.getenv("SNOWFLAKE_SCHEMA"),
    )
    logger.info("Snowflake connection established")
    return conn


def execute_query(query: str, params: tuple = None):
    """Execute a single query — for DDL and DELETE statements."""
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            logger.debug(f"Query executed: {query[:80]}...")
    finally:
        conn.close()


def execute_many(query: str, data: list):
    """Execute a query for multiple rows — for bulk inserts."""
    conn = get_snowflake_connection()
    try:
        with conn.cursor() as cur:
            cur.executemany(query, data)
            logger.info(f"Inserted {len(data)} rows")
    finally:
        conn.close()


def fetch_results(query: str, params: tuple = None) -> list:
    """Execute a query and return results as a list of dicts."""
    conn = get_snowflake_connection()
    try:
        with conn.cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(query, params)
            results = cur.fetchall()
            logger.debug(f"Fetched {len(results)} rows")
            return results
    finally:
        conn.close()