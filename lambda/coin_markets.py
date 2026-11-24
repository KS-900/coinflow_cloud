"""
Lambda: CoinGecko /coins/markets -> single upserted JSON file in S3
S3 layout: raw/coins_markets/latest.json
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BASE_URL = "https://api.coingecko.com/api/v3"
RAW_PREFIX = "raw/coins_markets"
MAX_RETRIES = 5

DEFAULT_MARKETS_PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": "100",
    "page": "1",
}

s3 = boto3.client("s3")


def _optional_demo_api_key() -> str | None:
    return (
        os.getenv("COINGECKO_API_KEY")
        or os.getenv("CG_DEMO_API_KEY")
        or os.getenv("API_KEY")
        or None
    )


def _load_existing_s3_json(bucket: str, key: str) -> dict:
    """Fetches the existing JSON object from S3 if it exists; returns {} otherwise."""
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        content = resp["Body"].read().decode("utf-8")
        data = json.loads(content)
        return data if isinstance(data, dict) else {}
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("NoSuchKey", "AccessDenied"):
            logger.info(
                "No readable existing file at s3://%s/%s (%s); starting fresh.",
                bucket, key, error_code,
            )
            return {}
        raise
    except ValueError:
        logger.warning("Existing file at s3://%s/%s was invalid JSON; overwriting.", bucket, key)
        return {}


def fetch_coin_markets(session: requests.Session, api_key: str | None) -> list[dict]:
    """
    GET /coins/markets. Returns parsed list of coin market dicts.
    Retries on 429 using Retry-After.
    Raises on any failure, so the Lambda invocation is marked as failed.
    """
    headers = {"x-cg-demo-api-key": api_key} if api_key else {}
    url = f"{BASE_URL}/coins/markets"

    for attempt in range(MAX_RETRIES):
        r = session.get(url, params=DEFAULT_MARKETS_PARAMS, headers=headers, timeout=30)

        if r.status_code == 429:
            wait_raw = r.headers.get("Retry-After", "60")
            try:
                wait_s = int(wait_raw)
            except ValueError:
                wait_s = 60
            wait_s = min(max(wait_s, 1), 120)
            logger.warning(
                "Rate limited on /coins/markets (attempt %d/%d); sleeping %ds",
                attempt + 1, MAX_RETRIES, wait_s,
            )
            time.sleep(wait_s)
            continue

        r.raise_for_status()

        payload = r.json()
        if not isinstance(payload, list) or not payload:
            raise RuntimeError(
                f"Unexpected markets payload: {type(payload).__name__}, "
                f"{len(payload) if hasattr(payload, '__len__') else 'n/a'} item(s)"
            )
        return payload

    raise RuntimeError(f"Giving up on /coins/markets after {MAX_RETRIES} attempts (rate limit)")


def lambda_handler(event, context):
    bucket = os.environ["S3_BUCKET"]
    api_key = _optional_demo_api_key()

    logger.info("Starting coin markets ingestion")

    with requests.Session() as session:
        markets_list = fetch_coin_markets(session, api_key)

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    key = f"{RAW_PREFIX}/latest.json"

    # Key each coin record by its 'id' so updates overwrite existing coins without duplicates
    fetched_by_id: dict[str, dict] = {}
    for item in markets_list:
        coin_id = item.get("id")
        if coin_id:
            fetched_by_id[coin_id] = {
                "updated_at": now_iso,
                "data": item,
            }

    # Merge into existing S3 data so older coins outside the top 100 are preserved,
    # while any returned coin is updated with its most recent data
    combined_data = _load_existing_s3_json(bucket, key)
    combined_data.update(fetched_by_id)

    body_bytes = json.dumps(combined_data, ensure_ascii=False).encode("utf-8")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body_bytes,
        ContentType="application/json",
        Metadata={
            "ingested_at": now_iso,
            "record_count": str(len(combined_data)),
        },
    )

    logger.info(
        "Upserted %d records (total %d in file, %d bytes) to s3://%s/%s",
        len(fetched_by_id), len(combined_data), len(body_bytes), bucket, key,
    )
    return {
        "bucket": bucket,
        "key": key,
        "upserted": len(fetched_by_id),
        "total_in_file": len(combined_data),
    }


if __name__ == "__main__":
    try:
        result = lambda_handler({}, None)
        logger.info("Completed successfully: %s", json.dumps(result))
    finally:
        s3.close()
        logging.shutdown()
    sys.exit(0)
