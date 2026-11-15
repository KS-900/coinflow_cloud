import logging
import os
import time
from datetime import datetime, timezone

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#------------------------------------------------------------------------------------------------------------------
# Defaults of script to use incase of no env file or secrets folder
#------------------------------------------------------------------------------------------------------------------
BASE_URL = "https://api.coingecko.com/api/v3"
RAW_PREFIX = "raw/coin_history"
MAX_RETRIES = 5
DEFAULT_CHART_DAYS = 90
DEFAULT_REQUEST_DELAY_SEC = 2.5
DEFAULT_COINS = ("bitcoin", "ethereum", "tether", "binancecoin", "usd-coin", "ripple", "solana", "tron", 
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
                 "usual-usd", "aptos", "pump-fun", "ylds", "hash-2", "midnight-3", "jupiter-exchange")

s3 = boto3.client("s3")


def _optional_demo_api_key() -> str | None:
    # Same env var discipline as the script
    return (
        os.getenv("COINGECKO_API_KEY")
        or os.getenv("CG_DEMO_API_KEY")
        or os.getenv("API_KEY")
        or None
    )

def _parse_coin_ids() -> list[str]:
    raw = os.getenv("COINGECKO_COIN_IDS").strip()
    if not raw:
        return list(DEFAULT_COINS)
    return [c.strip().lower() for c in raw.split(",") if c.strip()]

def _dedupe_preserve_order(ids: list[str]) -> list[str]:
    # Duplicate IDs (e.g. copy-paste lists) waste API quota; keep first occurrence only.
    return list(dict.fromkeys(ids))

def _chart_days() -> int:
    raw = os.getenv("COINGECKO_CHART_DAYS", "").strip()
    if not raw:
        return DEFAULT_CHART_DAYS
    return max(1, int(raw))

def _request_delay_sec() -> float:
    raw = os.getenv("COINGECKO_REQUEST_DELAY_SEC", "").strip()
    if not raw:
        return DEFAULT_REQUEST_DELAY_SEC
    return max(0.0, float(raw))

def _optional_demo_api_key() -> str | None:
    """
    TASK.md: demo key is optional but raises rate limits.
    We only append it when set so free-tier/no-key setups still work.
    """
    return os.getenv("COINGECKO_API_KEY") or os.getenv("CG_DEMO_API_KEY") or None

def fetch_market_chart(
    session: requests.Session,
    coin_id: str,
    chart_days: int,
    api_key: str | None,
) -> dict | None:
    """
    GET /coins/{id}/market_chart — returns prices, market_caps, total_volumes arrays.

    Returns None on hard failure so callers can continue other coins; raises only
    for caller-controlled fatal cases if we extend this later.
    """
    params: dict[str, str] = {"vs_currency": "usd", "days": str(chart_days)}
    if api_key:
        params["x_cg_demo_api_key"] = api_key

    url = f"{BASE_URL}/coins/{coin_id}/market_chart"
    max_retries = 5
    for attempt in range(max_retries):
        try:
            # Bounded timeout: hung connections should not block orchestration forever.
            r = session.get(url, params=params, timeout=30)
            if r.status_code == 429:
                # Respect Retry-After when present; backoff avoids hammering the API.
                wait_raw = r.headers.get("Retry-After", "60")
                try:
                    wait_s = int(wait_raw)
                except ValueError:
                    wait_s = 60
                wait_s = min(max(wait_s, 1), 120)
                print(
                    f"Rate limited on {coin_id} (attempt {attempt + 1}/{max_retries}); "
                    f"sleeping {wait_s}s"
                )
                time.sleep(wait_s)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            print(f"Error fetching market_chart for {coin_id}: {e}")
            return None
    print(f"Giving up on {coin_id} after {max_retries} attempts (rate limit).")
    return None


def fetch_all_coin_charts(
    coin_ids: list[str],
    chart_days: int,
    delay_sec: float,
    api_key: str | None,
) -> dict[str, dict]:
    """Fetch each coin sequentially with delay — respects CoinGecko per-minute caps."""
    out: dict[str, dict] = {}
    session = requests.Session()
    for i, cid in enumerate(coin_ids):
        payload = fetch_market_chart(session, cid, chart_days, api_key)
        if payload is not None:
            out[cid] = payload
            print(f"Fetched chart for {cid}")
        # Do not sleep after the last coin, saves one unnecessary pause.
        if i < len(coin_ids) - 1 and delay_sec > 0:
            time.sleep(delay_sec)
    return out
attempts (rate limit)")


def lambda_handler(event, context):
    bucket = os.environ["S3_BUCKET"]
    api_key = _optional_demo_api_key()

    logger.info("Starting coin markets ingestion")
    body, record_count = fetch_coin_markets(requests.Session(), api_key)

    now = datetime.now(timezone.utc)
    key = (
        f"{RAW_PREFIX}/dt={now:%Y-%m-%d}/"
        f"coins_history_{now:%Y%m%dT%H%M%SZ}.json"
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