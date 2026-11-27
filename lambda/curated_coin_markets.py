from __future__ import annotations

import io
import json
import logging
import os
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

RAW_PREFIX = "raw/coins_markets"
CURATED_PREFIX = "curated/coins_markets"
BUCKET = "aws-coinflow-s3-bucket"
KEY = "raw/coins_markets/latest.json"

# 1. DEFINE YOUR EXPLICIT SCHEMA
# Add or adjust fields to match the exact CoinGecko /coins/markets keys you want to keep.
COINS_MARKETS_SCHEMA = pa.schema([
    pa.field("id", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("name", pa.string(), nullable=False),
    pa.field("current_price", pa.float64(), nullable=True),
    pa.field("market_cap", pa.float64(), nullable=True),
    pa.field("market_cap_rank", pa.int64(), nullable=True),
    pa.field("total_volume", pa.float64(), nullable=True),
    pa.field("high_24h", pa.float64(), nullable=True),
    pa.field("low_24h", pa.float64(), nullable=True),
    pa.field("price_change_percentage_24h", pa.float64(), nullable=True),
    pa.field("circulating_supply", pa.float64(), nullable=True),
    pa.field("ath", pa.float64(), nullable=True),
    pa.field("atl", pa.float64(), nullable=True),
    pa.field("last_updated", pa.timestamp("us", tz="UTC"), nullable=True),
    # TODO: Add any remaining fields you care about (e.g., ath, atl, total_supply)
])


def _read_raw_records(bucket: str, key: str) -> list[dict]:
    """
    TODO: Implement reading raw JSON from S3 for the given date `dt`.
    - Option A (if reading daily prefix): Use s3.list_objects_v2(Bucket=bucket, Prefix=f"{RAW_PREFIX}/dt={dt}/")
      to find all JSON keys for that day, fetch each via s3.get_object(), and combine the coin dicts.
    - Option B (if reading latest.json): Fetch f"{RAW_PREFIX}/latest.json" and extract [v["data"] for v in raw.values()].
    """
    response = s3.get_object(Bucket=bucket, Key=key)
    raw = json.load(response["Body"])
    # Extracting the list of coin records from the raw JSON
    raw = [coin for coin in raw.values()]
    return raw


def _normalize_record(raw: dict) -> dict:
    """
    TODO: Map one raw CoinGecko JSON object to match COINS_MARKETS_SCHEMA field-for-field.
    - Safely cast numeric fields to float or int (handling None values).
    - Parse ISO timestamp strings (e.g., '2026-11-27T08:00:00.000Z') with datetime.fromisoformat().
    """
    raw = {
        "id": raw.get("id"),
        "symbol": raw.get("symbol"),
        "name": raw.get("name"),
        "current_price": float(raw.get("current_price")) if raw.get("current_price") is not None else None,
        "market_cap": float(raw.get("market_cap")) if raw.get("market_cap") is not None else None,
        "market_cap_rank": int(raw.get("market_cap_rank")) if raw.get("market_cap_rank") is not None else None,
        "total_volume": float(raw.get("total_volume")) if raw.get("total_volume") is not None else None,
        "high_24h": float(raw.get("high_24h")) if raw.get("high_24h") is not None else None,
        "low_24h": float(raw.get("low_24h")) if raw.get("low_24h") is not None else None,
        "price_change_percentage_24h": float(raw.get("price_change_percentage_24h")) if raw.get("price_change_percentage_24h") is not None else None,
        "circulating_supply": float(raw.get("circulating_supply")) if raw.get("circulating_supply") is not None else None,
        "ath": float(raw.get("ath")) if raw.get("ath") is not None else None,
        "atl": float(raw.get("atl")) if raw.get("atl") is not None  else None,
        "last_updated": datetime.fromisoformat(raw.get("last_updated").replace("Z", "+00:00")) if raw.get("last_updated") is not None else None,
    }
    raise NotImplementedError("Implement field normalization and type casting")
    return raw


def _write_parquet_to_s3(records: list[dict], bucket: str, key: str) -> int:
    """Converts normalized records into a Snappy-compressed Parquet file in S3."""
    # Enforce the explicit schema here—PyArrow will raise ArrowTypeError if types mismatch
    table = pa.Table.from_pylist(records, schema=COINS_MARKETS_SCHEMA)

    buffer = io.BytesIO()
    pq.write_table(
        table,
        buffer,
        compression="snappy",
    )

    parquet_bytes = buffer.getvalue()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=parquet_bytes,
        ContentType="application/octet-stream",
    )
    return len(parquet_bytes)


def lambda_handler(event, context):
    bucket = os.environ["S3_BUCKET"]

    # Allow passing {"dt": "2026-11-27"} in the event for manual backfills; default to today (UTC)
    dt = (event.get("dt") if isinstance(event, dict) else None) or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    raw_records = _read_raw_records(bucket, dt)
    if not raw_records:
        logger.warning("No raw records found for dt=%s; skipping Parquet write.", dt)
        return {"status": "empty", "dt": dt}

    # TODO: Deduplicate records by 'id' if multiple raw files exist for the same day
    normalized = [_normalize_record(r) for r in raw_records]

    output_key = f"{CURATED_PREFIX}/dt={dt}/coins_markets.parquet"
    bytes_written = _write_parquet_to_s3(normalized, bucket, output_key)

    logger.info("Wrote %d rows (%d bytes) to s3://%s/%s", len(normalized), bytes_written, bucket, output_key)
    return {
        "bucket": bucket,
        "key": output_key,
        "rows": len(normalized),
        "bytes": bytes_written,
    }
