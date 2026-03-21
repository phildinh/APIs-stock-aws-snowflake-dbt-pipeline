# ============================================================
# s3_client.py
# Purpose: AWS S3 upload, read, and list operations
# Usage:   from utils.s3_client import upload_json, read_json
# ============================================================

import os
import json
import boto3
from dotenv import load_dotenv
from ingestion.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)


def get_s3_client():
    """Create and return an S3 client using env vars."""
    return boto3.client(
        "s3",
        aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name           = os.getenv("AWS_REGION"),
    )


def upload_json(data: dict | list, s3_key: str):
    """
    Upload a Python dict or list as a JSON file to S3.
    s3_key example: 'stock_prices/AAPL/2024-01-15.json'
    """
    client = get_s3_client()
    bucket = os.getenv("S3_BUCKET_NAME")
    body   = json.dumps(data, indent=2, default=str)

    client.put_object(
        Bucket      = bucket,
        Key         = s3_key,
        Body        = body,
        ContentType = "application/json"
    )
    logger.info(f"Uploaded to s3://{bucket}/{s3_key}")


def read_json(s3_key: str) -> dict | list:
    """Download and parse a JSON file from S3."""
    client = get_s3_client()
    bucket = os.getenv("S3_BUCKET_NAME")

    response = client.get_object(Bucket=bucket, Key=s3_key)
    data     = json.loads(response["Body"].read().decode("utf-8"))
    logger.info(f"Read from s3://{bucket}/{s3_key}")
    return data


def list_files(prefix: str) -> list:
    """List all files in S3 under a given prefix/folder."""
    client  = get_s3_client()
    bucket  = os.getenv("S3_BUCKET_NAME")

    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files    = [obj["Key"] for obj in response.get("Contents", [])]
    logger.info(f"Found {len(files)} files under s3://{bucket}/{prefix}")
    return files