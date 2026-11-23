"""
Lambda: CoinGecko/coins/{id}/categories -> raw JSON to s3 in one file

s3 layout:
raw/coin_categories/dt=YYYY-MM-DD/<run timestamp>.json
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BASE_URL = "https://api.coingecko.com/api/v3"
RAW_PREFIX = "raw/coin_categories"
MAX_RETRIES = 5
DEFAULT_REQUEST_DELAY_SEC = 2.5
DEFAULT_COINS = (
    "bitcoin", "ethereum", "tether", "binancecoin", "usd-coin", "ripple", "solana", "tron",
    "figure-heloc", "hyperliquid", "dogecoin", "usds", "leo-token", "rain", "zcash", "stellar",
    "canton-network", "cardano", "monero", "chainlink", "whitebit-coin", "the-open-network",
    "usd1-wlfi", "ethena-usd", "dai", "bitcoin-cash", "memecore", "hedera-hashgraph",
    "labra-finance", "litecoin", "sui", "hashnote-usd", "avalanche-2", "near-protocol",
    "paypal-usd", "shiba-inu", "crypto-com-chain", "tether-gold", "global-dollar-stablecoin-gds",
    "blackrock-usd-institutional-digital-liquidity-fund", "ondo-us-dollar-yield", "bittensor",
    "pax-gold", "ondo-finance", "mantle", "world-liberty-financial", "worldcoin", "ripple-usd",
    "polkadot", "aster-2", "htx-dao", "uniswap", "okb", "falcon-financial", "pi-network", "usdd",
    "sky", "bfusd", "internet-computer", "bitget-token", "audiera", "pepe", "morpho",
    "ethereum-classic", "dexe", "usdtb", "eutbl", "united-states-dollar", "quant-network",
    "blockchain-capital", "aave", "superstate", "cosmos", "siren-2", "kaspa",
    "janus-henderson-anemoy-treasury-fund", "kucoin-shares", "render-token", "algorand",
    "polygon-ecosystem", "nexo", "ethena", "stable-2", "venice-token", "just", "bianrensheng",
    "gatechain-token", "xdce-crowd", "beldex", "flare-network", "filecoin", "gho", "injective-protocol",
    "usual-usd", "aptos", "pump-fun", "ylds", "hash-2", "midnight-3", "jupiter-exchange",
)
# Function collects coin ids from environment variable COIN_IDS, or defaults to DEFAULT_COINS if not set. It returns a list of coin ids in lowercase, stripped of whitespace.
def _parse_coin_ids() -> list[str]:
        """Comma-separated COINGECKO_COIN_IDS overrides DEFAULT_COINS."""
        raw = os.getenv("COIN_IDS", "").strip()
        if not raw:
            return list(DEFAULT_COINS)
        return [c.strip() for c in raw.split(",") if c.strip()]

# Function removes duplicate coin ids while preserving the order of their first occurrence. It takes a list of coin ids as input and returns a new list with duplicates removed.
def _dedupe_preserve_order(ids: list[str]) -> list[str]:
    return list(dict.fromkeys(ids))

# Function to delay API requests to avoid rate limiting
def _request_delay_sec() -> float:
    raw = os.getenv("API_DELAY_SECONDS", "").strip()
    if not raw:
        return DEFAULT_REQUEST_DELAY_SEC
    return max(0.0, float(raw))

# Function to get the base URL for the CoinGecko API, allowing for an override via an environment variable. If the environment variable is not set, it defaults to the predefined BASE_URL.
def _base_url() -> str:
    raw = os.getenv("URL_COINGECKO", "").strip()
    if not raw:
        return BASE_URL
    return raw

# Function to retrieve an optional API key for the CoinGecko API from environment variables. It checks multiple environment variable names and returns the first one found, or None if none are set.
def _optional_demo_api_key() -> str | None:
    return (
        os.getenv("COINGECKO_API_KEY")
        or os.getenv("CG_DEMO_API_KEY")
        or os.getenv("API_KEY")
        or None
    )

# Function to fetch coin data and its category from the CoinGecko API for a given coin ID. It handles rate limiting by checking for a 429 status code and waiting for the specified time before retrying. The function attempts to fetch the data up to a maximum number of retries and returns the JSON response if successful, or None if all attempts fail.
def _fetch_coin_and_category_data(
    session: requests.Session,
    id: str,
) -> list[dict]:
    """
    GET /coins/{id} — returns coin data including its category.
    """


    url = f"{BASE_URL}/coins/{id}"
    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=30)
            print(f"Fetched data for coin: {id} (attempt {attempt + 1}/{max_retries})")
            if response.status_code == 429:
                # Rate limit exceeded; wait and retry
                wait_raw = response.headers.get("Retry-After", "60")
                try:
                    wait_s = int(wait_raw)
                except ValueError:
                    wait_s = 60
                wait_s = min(max(wait_s, 1), 122)
                logger.warning("Rate limit exceeded on coin: %s (attempt %d/%d). Waiting for %d seconds before retrying...",
                               id, attempt, max_retries, wait_s
                )
                time.sleep(wait_s)
                continue
            response.raise_for_status()  # Raise an exception for other HTTP errors
            payload = response.json()
            return payload  # Return the JSON response if successful
        except (requests.RequestException, ValueError) as e:
            logger.error("Error fetching coin data for %s: %s", id, e)
            return None  # Return None if all retries failed
    logger.info("Giving up on fetching coin data for %s after %d attempts.", id, max_retries)
    return None  # Return None if all retries failed


# Function to fetch data for all specified coin IDs from the CoinGecko API. It uses a requests session for connection pooling and iterates through the list of coin IDs, fetching data for each one. If a request fails, it logs the failure and continues with the next coin ID. The function also implements a delay between requests to avoid hitting rate limits. Finally, it returns a dictionary containing the fetched data for each coin ID.
def _fetch_all_coins_and_categories(
    coin_ids: list[str],
    delay_sec: float = 2.5,
) -> dict[str, dict]:
    data = {} # Store coin data keyed by coin ID
    session = requests.Session()  # Use a session for connection pooling
    for i, coin_id in enumerate(coin_ids):
        logger.info("Fetching data for coin %d/%d: %s", i , len(coin_ids), coin_id)
        coin_data = _fetch_coin_and_category_data(session, id=coin_id)
        if coin_data is not None:
            data[coin_id] = coin_data
        else:
            logger.error("Failed to fetch data for coin: %s", coin_id)
        if i < len(coin_ids) - 1 and delay_sec > 0:
            time.sleep(delay_sec)
    return data

# Lambda handler function that serves as the entry point for the AWS Lambda function. It retrieves the list of coin IDs, fetches their data and categories from the CoinGecko API, and uploads the results to an S3 bucket. The function also handles logging and error reporting.
def lambda_handler(event, context):
    bucket = os.environ["S3_BUCKET"]

    override = event.get("coin_ids") if isinstance(event, dict) else None
    if override:
        coin_ids = _dedupe_preserve_order([c.strip().lower() for c in override])
    else:
        coin_ids = _dedupe_preserve_order(_parse_coin_ids())
    
    delay_sec = _request_delay_sec()
    api_key = _optional_demo_api_key()

    run_ts = datetime.now(timezone.utc)
    dt = run_ts.strftime("%Y-%m-%d")
    stamp = run_ts.strftime("%Y%m%dT%H%M%SZ")

    logger.info("Starting coin categories ingestion: %d coins, delay=%.1fs", len(coin_ids), delay_sec)

    session = requests.Session()
    saved: list[str] = []
    failed: list[str] = []
    
    body = _fetch_all_coins_and_categories(coin_ids, delay_sec=delay_sec)
    if body is None:
        logger.error("No data fetched for any coins; exiting.")
        failed.append(c for c in coin_ids)
        return
    else:
        key = f"{RAW_PREFIX}/dt={dt}/coin_categories_{stamp}.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
            Metadata={"ingested_at": run_ts.isoformat(), "record_count": str(len(body))},
        )
        saved.append(c for c in coin_ids)
        logger.info("Saved %s (%d bytes) to s3://%s/%s", coin_id, len(body), bucket, key)
    
    if failed:
        raise RuntimeError(f"Failed to fetch data for {len(failed)} coin(s): {', '.join(failed)}")
    if saved:
        raise RuntimeError(f"Successfully fetched data for {len(saved)} coin(s): {', '.join(saved)}")
    
    return {"bucket": bucket, "prefix": f"{RAW_PREFIX}/dt={dt}/", "record_count": len(body),
            "saved": len(saved)}