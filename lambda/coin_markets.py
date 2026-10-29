import logging
import os
import time
from datetime import datetime, timezone

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BASE_URL = "https://api.coingecko.com/api/v3"
RAW_PREFIX = "raw/coins_markets"
MAX_RETRIES = 5

# Same endpoint params as ingest_coin_markets.py
DEFAULT_MARKETS_PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": "100",
    "page": "1",
}

s3 = boto3.client("s3")


def _optional_demo_api_key() -> str | None:
    # Same env var discipline as the script
    return (
        os.getenv("COINGECKO_API_KEY")
        or os.getenv("CG_DEMO_API_KEY")
        or os.getenv("API_KEY")
        or None
    )


def fetch_coin_markets(session: requests.Session, api_key: str | None) -> tuple[bytes, int]:
    """
    GET /coins/markets. Returns (raw response bytes, record count).
    Retries on 429 using Retry-After (same pattern as the script).
    Raises on any failure, so the Lambda invocation is marked as failed.
    """
    # Key goes in a header, NOT the query string, so it can never
    # appear in an exception message or a CloudWatch log line.
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

        # Parse ONLY to validate. What we store is r.content, untouched.
        payload = r.json()
        if not isinstance(payload, list) or not payload:
            raise RuntimeError(
                f"Unexpected markets payload: {type(payload).__name__}, "
                f"{len(payload) if hasattr(payload, '__len__') else 'n/a'} item(s)"
            )
        return r.content, len(payload)

    raise RuntimeError(f"Giving up on /coins/markets after {MAX_RETRIES} attempts (rate limit)")


def lambda_handler(event, context):
    bucket = os.environ["S3_BUCKET"]
    api_key = _optional_demo_api_key()

    logger.info("Starting coin markets ingestion")
    body, record_count = fetch_coin_markets(requests.Session(), api_key)

    now = datetime.now(timezone.utc)
    key = (
        f"{RAW_PREFIX}/dt={now:%Y-%m-%d}/"
        f"coins_markets_{now:%Y%m%dT%H%M%SZ}.json"
    )

    # Written only after validation, so a failed run leaves nothing in S3.
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        Metadata={"ingested_at": now.isoformat(), "record_count": str(record_count)},
    )

    logger.info("Wrote %d records (%d bytes) to s3://%s/%s", record_count, len(body), bucket, key)
    return {"bucket": bucket, "key": key, "records": record_count}