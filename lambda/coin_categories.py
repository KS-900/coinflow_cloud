"""
Lambda: CoinGecko /coins/{id} -> single upserted JSON file in S3
S3 layout: raw/coin_categories/latest.json
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

s3 = boto3.client("s3")

BASE_URL = "https://api.coingecko.com/api/v3"
RAW_PREFIX = "raw/coin_categories"
MAX_RETRIES = 5
DEFAULT_REQUEST_DELAY_SEC = 2.5
STOP_BEFORE_TIMEOUT_MS = 30_000  # Stop 30s before Lambda timeout so partial progress saves

DEFAULT_COINS = (
    "bitcoin", "ethereum", "tether", "binancecoin", "usd-coin", "ripple",
    "solana", "tron", "figure-heloc", "hyperliquid", "dogecoin", "usds",
    "leo-token", "rain", "zcash", "stellar", "canton-network", "cardano",
    "monero", "chainlink", "whitebit-coin", "the-open-network", "usd1-wlfi",
    "ethena-usd", "dai", "bitcoin-cash", "memecore", "hedera-hashgraph",
    "labra-finance", "litecoin", "sui", "hashnote-usd", "avalanche-2",
    "near-protocol", "paypal-usd", "shiba-inu", "crypto-com-chain",
    "tether-gold", "global-dollar-stablecoin-gds",
    "blackrock-usd-institutional-digital-liquidity-fund",
    "ondo-us-dollar-yield", "bittensor", "pax-gold", "ondo-finance", "mantle",
    "world-liberty-financial", "worldcoin", "ripple-usd", "polkadot",
    "aster-2", "htx-dao", "uniswap", "okb", "falcon-financial", "pi-network",
    "usdd", "sky", "bfusd", "internet-computer", "bitget-token", "audiera",
    "pepe", "morpho", "ethereum-classic", "dexe", "usdtb", "eutbl",
    "united-states-dollar", "quant-network", "blockchain-capital", "aave",
    "superstate", "cosmos", "siren-2", "kaspa",
    "janus-henderson-anemoy-treasury-fund", "kucoin-shares", "render-token",
    "algorand", "polygon-ecosystem", "nexo", "ethena", "stable-2",
    "venice-token", "just", "bianrensheng", "gatechain-token", "xdce-crowd",
    "beldex", "flare-network", "filecoin", "gho", "injective-protocol",
    "usual-usd", "aptos", "pump-fun", "ylds", "hash-2", "midnight-3",
    "jupiter-exchange",
)


def _parse_coin_ids() -> list[str]:
    raw = os.getenv("COIN_IDS", "").strip()
    if not raw:
        return list(DEFAULT_COINS)
    return [c.strip().lower() for c in raw.split(",") if c.strip()]


def _dedupe_preserve_order(ids: list[str]) -> list[str]:
    return list(dict.fromkeys(ids))


def _request_delay_sec() -> float:
    raw = os.getenv("API_DELAY_SECONDS", "").strip()
    if not raw:
        return DEFAULT_REQUEST_DELAY_SEC
    return max(0.0, float(raw))


def _base_url() -> str:
    raw = os.getenv("URL_COINGECKO", "").strip()
    return raw if raw else BASE_URL


def _optional_demo_api_key() -> str | None:
    return (
        os.getenv("COINGECKO_API_KEY")
        or os.getenv("CG_DEMO_API_KEY")
        or os.getenv("API_KEY")
        or None
    )


def _time_is_low(context) -> bool:
    if context is None or not hasattr(context, "get_remaining_time_in_millis"):
        return False
    return context.get_remaining_time_in_millis() < STOP_BEFORE_TIMEOUT_MS


def _load_existing_s3_json(bucket: str, key: str) -> dict:
    """Fetches the existing JSON object from S3 if it exists; returns {} otherwise."""
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        content = resp["Body"].read().decode("utf-8")
        data = json.loads(content)
        return data if isinstance(data, dict) else {}
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logger.info("No existing file at s3://%s/%s; starting fresh.", bucket, key)
            return {}
        raise
    except ValueError:
        logger.warning("Existing file at s3://%s/%s was invalid JSON; overwriting.", bucket, key)
        return {}


def _fetch_coin_and_category_data(
    session: requests.Session,
    coin_id: str,
    base_url: str,
) -> dict | None:
    """GET /coins/{id} — returns coin data including its categories."""
    url = f"{base_url}/coins/{coin_id}"
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, timeout=30)
            if response.status_code == 429:
                wait_raw = response.headers.get("Retry-After", "60")
                try:
                    wait_s = int(wait_raw)
                except ValueError:
                    wait_s = 60
                wait_s = min(max(wait_s, 1), 122)
                logger.warning(
                    "Rate limit exceeded on %s (attempt %d/%d). Waiting %ds...",
                    coin_id, attempt + 1, MAX_RETRIES, wait_s,
                )
                time.sleep(wait_s)
                continue

            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as e:
            logger.error("Error fetching coin data for %s: %s", coin_id, e)
            return None

    logger.info("Giving up on fetching %s after %d attempts.", coin_id, MAX_RETRIES)
    return None


def _fetch_all_coins_and_categories(
    coin_ids: list[str],
    base_url: str,
    api_key: str | None = None,
    delay_sec: float = 2.5,
    context=None,
) -> tuple[dict[str, dict], list[str], list[str]]:
    fetched_data: dict[str, dict] = {}
    failed: list[str] = []
    not_attempted: list[str] = []

    # Context manager ensures HTTP keep-alive sockets are closed immediately upon exit
    with requests.Session() as session:
        if api_key:
            session.headers.update({"x-cg-demo-api-key": api_key})

        for i, coin_id in enumerate(coin_ids):
            if _time_is_low(context):
                not_attempted = coin_ids[i:]
                logger.warning("Running low on time; %d coin(s) not attempted.", len(not_attempted))
                break

            logger.info("Fetching coin %d/%d: %s", i + 1, len(coin_ids), coin_id)
            coin_data = _fetch_coin_and_category_data(session, coin_id=coin_id, base_url=base_url)

            if coin_data is not None:
                fetched_data[coin_id] = {
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "data": coin_data,
                }
            else:
                failed.append(coin_id)

            if i < len(coin_ids) - 1 and delay_sec > 0:
                time.sleep(delay_sec)

    return fetched_data, failed, not_attempted


def lambda_handler(event, context):
    bucket = os.environ["S3_BUCKET"]
    override = event.get("coin_ids") if isinstance(event, dict) else None

    if override:
        coin_ids = _dedupe_preserve_order([c.strip().lower() for c in override])
    else:
        coin_ids = _dedupe_preserve_order(_parse_coin_ids())

    delay_sec = _request_delay_sec()
    base_url = _base_url()
    api_key = _optional_demo_api_key()

    run_ts = datetime.now(timezone.utc)
    # Fixed key ensures updates overwrite the single file without creating duplicates
    key = f"{RAW_PREFIX}/latest.json"

    logger.info("Starting ingestion: %d coins, delay=%.1fs", len(coin_ids), delay_sec)

    fetched_data, failed, not_attempted = _fetch_all_coins_and_categories(
        coin_ids,
        base_url=base_url,
        api_key=api_key,
        delay_sec=delay_sec,
        context=context,
    )

    if not fetched_data:
        raise RuntimeError("No data fetched for any coins; aborting S3 upload.")

    # Load existing S3 file and upsert new data (replaces old entries, keeps untouched ones)
    combined_data = _load_existing_s3_json(bucket, key)
    combined_data.update(fetched_data)

    json_bytes = json.dumps(combined_data, ensure_ascii=False).encode("utf-8")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json_bytes,
        ContentType="application/json",
        Metadata={
            "ingested_at": run_ts.isoformat(),
            "record_count": str(len(combined_data)),
        },
    )

    logger.info(
        "Upserted %d coin(s) (total %d in file, %d bytes) to s3://%s/%s",
        len(fetched_data), len(combined_data), len(json_bytes), bucket, key,
    )

    if failed or not_attempted:
        logger.warning(
            "Incomplete run: saved=%d, failed=%s, not_attempted=%s",
            len(fetched_data), failed, not_attempted,
        )

    return {
        "bucket": bucket,
        "key": key,
        "upserted_count": len(fetched_data),
        "total_in_file": len(combined_data),
        "failed_count": len(failed),
    }


if __name__ == "__main__":
    # Cleanly run and terminate when executed directly in a local terminal
    try:
        result = lambda_handler({}, None)
        logger.info("Completed successfully: %s", json.dumps(result))
    finally:
        s3.close()
        logging.shutdown()
    sys.exit(0)
