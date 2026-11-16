"""
Lambda: CoinGecko /coins/{id}/market_chart -> raw JSON in S3, one object per coin.

S3 layout:
  raw/coin_price_history/dt=YYYY-MM-DD/chart_days=90/<coin_id>_<run timestamp>.json
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
RAW_PREFIX = "raw/coin_price_history"
MAX_RETRIES = 5
DEFAULT_CHART_DAYS = 90
DEFAULT_REQUEST_DELAY_SEC = 2.5
STOP_BEFORE_TIMEOUT_MS = 150_000
EXPECTED_KEYS = ("prices", "market_caps", "total_volumes")

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

s3 = boto3.client("s3")


def _parse_coin_ids() -> list[str]:
    raw = os.getenv("COINGECKO_COIN_IDS", "").strip()
    if not raw:
        return list(DEFAULT_COINS)
    return [c.strip().lower() for c in raw.split(",") if c.strip()]


def _dedupe_preserve_order(ids: list[str]) -> list[str]:
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
    return (
        os.getenv("COINGECKO_API_KEY")
        or os.getenv("CG_DEMO_API_KEY")
        or os.getenv("API_KEY")
        or None
    )


def _time_is_low(context) -> bool:
    if context is None:
        return False
    return context.get_remaining_time_in_millis() < STOP_BEFORE_TIMEOUT_MS


def fetch_market_chart(
    session: requests.Session,
    coin_id: str,
    chart_days: int,
    api_key: str | None,
) -> bytes | None:
    params = {"vs_currency": "usd", "days": str(chart_days)}
    headers = {"x-cg-demo-api-key": api_key} if api_key else {}
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"

    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, params=params, headers=headers, timeout=30)
            if r.status_code == 429:
                wait_raw = r.headers.get("Retry-After", "60")
                try:
                    wait_s = int(wait_raw)
                except ValueError:
                    wait_s = 60
                wait_s = min(max(wait_s, 1), 120)
                logger.warning(
                    "Rate limited on %s (attempt %d/%d); sleeping %ds",
                    coin_id, attempt + 1, MAX_RETRIES, wait_s,
                )
                time.sleep(wait_s)
                continue
            r.raise_for_status()
            payload = r.json()
        except (requests.RequestException, ValueError) as e:
            logger.error("Error fetching market_chart for %s: %s", coin_id, e)
            return None

        if not isinstance(payload, dict):
            logger.error("Unexpected payload type for %s: %s", coin_id, type(payload).__name__)
            return None
        missing = [k for k in EXPECTED_KEYS if k not in payload]
        if missing:
            logger.warning("%s: response is missing %s (saving it anyway as evidence)", coin_id, missing)
        return r.content

    logger.error("Giving up on %s after %d attempts (rate limit)", coin_id, MAX_RETRIES)
    return None


def lambda_handler(event, context):
    bucket = os.environ["S3_BUCKET"]

    override = event.get("coin_ids") if isinstance(event, dict) else None
    if override:
        coin_ids = _dedupe_preserve_order([c.strip().lower() for c in override])
    else:
        coin_ids = _dedupe_preserve_order(_parse_coin_ids())

    chart_days = _chart_days()
    delay_sec = _request_delay_sec()
    api_key = _optional_demo_api_key()

    run_ts = datetime.now(timezone.utc)
    dt = run_ts.strftime("%Y-%m-%d")
    stamp = run_ts.strftime("%Y%m%dT%H%M%SZ")

    logger.info("Starting history ingestion: %d coins, chart_days=%d, delay=%.1fs",
                len(coin_ids), chart_days, delay_sec)

    session = requests.Session()
    saved: list[str] = []
    failed: list[str] = []
    not_attempted: list[str] = []

    for i, coin_id in enumerate(coin_ids):
        if _time_is_low(context):
            not_attempted = coin_ids[i:]
            logger.error("Running low on time; %d coin(s) not attempted", len(not_attempted))
            break

        body = fetch_market_chart(session, coin_id, chart_days, api_key)
        if body is None:
            failed.append(coin_id)
        else:
            key = f"{RAW_PREFIX}/dt={dt}/chart_days={chart_days}/{coin_id}_{stamp}.json"
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
                Metadata={
                    "ingested_at": run_ts.isoformat(),
                    "coin_id": coin_id,
                    "chart_days": str(chart_days),
                },
            )
            saved.append(coin_id)
            logger.info("Saved %s (%d bytes) to s3://%s/%s", coin_id, len(body), bucket, key)

        if i < len(coin_ids) - 1 and delay_sec > 0:
            time.sleep(delay_sec)

    logger.info("Run finished: requested=%d saved=%d failed=%d not_attempted=%d",
                len(coin_ids), len(saved), len(failed), len(not_attempted))

    if failed or not_attempted:
        raise RuntimeError(
            f"Incomplete run: saved={len(saved)}, failed={failed}, not_attempted={not_attempted}"
        )

    return {"bucket": bucket, "prefix": f"{RAW_PREFIX}/dt={dt}/chart_days={chart_days}/",
            "saved": len(saved)}