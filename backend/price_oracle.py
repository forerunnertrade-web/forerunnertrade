"""
Chainlink price oracle (EVM).

Fetches ETH/USD from the canonical Chainlink aggregator on mainnet.
Result is cached for `CACHE_TTL` seconds to avoid an RPC call per pool event.

Why Chainlink instead of an off-chain API: we already have an RPC connection
open. Adding a CoinGecko or Coinbase API key would mean one more credential
to manage and one more thing that can fail. Chainlink data is on-chain,
sub-minute fresh, and free at the marginal call.

Failure mode: if the RPC is slow or the call reverts, we fall back to the
LAST known price (or a hardcoded sensible default if we never got one).
This way enrichment never blocks on price discovery.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

import httpx
from eth_abi import decode as abi_decode
from eth_utils import keccak

log = logging.getLogger(__name__)

# Chainlink ETH/USD aggregator on Ethereum mainnet.
# Returns 8-decimal answer (e.g. 300000000000 = $3000.00).
ETH_USD_FEED = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"
SEL_LATEST_ANSWER = "0x" + keccak(text="latestAnswer()").hex()[:8]
FEED_DECIMALS = 8

# Cache settings
CACHE_TTL = 60.0  # seconds
DEFAULT_PRICE = 3000.0  # used only if we never got a real price

# Module-level cache. One process = one cache. Fine for our scale.
_cached_price: Optional[float] = None
_cached_at: float = 0.0


async def get_eth_usd(rpc_url: str, client: Optional[httpx.AsyncClient] = None) -> float:
    """Return current ETH price in USD. Uses cache if fresh, else fetches.

    Always returns a number — falls back to last-known or DEFAULT_PRICE
    on failure rather than raising. Pricing is for sizing/display, not
    for trading decisions, so a stale value is preferable to an error."""
    global _cached_price, _cached_at

    now = time.time()
    if _cached_price is not None and (now - _cached_at) < CACHE_TTL:
        return _cached_price

    try:
        own_client = client is None
        client = client or httpx.AsyncClient()
        try:
            resp = await client.post(
                rpc_url,
                json={
                    "jsonrpc": "2.0", "id": 1, "method": "eth_call",
                    "params": [
                        {"to": ETH_USD_FEED, "data": SEL_LATEST_ANSWER},
                        "latest",
                    ],
                },
                timeout=5.0,
            )
            resp.raise_for_status()
            body = resp.json()
            if "error" in body:
                raise RuntimeError(body["error"])

            raw = body.get("result")
            if not raw or len(raw) < 66:
                raise ValueError("empty answer")

            (raw_int,) = abi_decode(["int256"], bytes.fromhex(raw[2:]))
            price = raw_int / (10 ** FEED_DECIMALS)

            # Sanity check — Chainlink can briefly return 0 if a node is down.
            # We require at least $100 (well below any plausible ETH price)
            # to accept the answer.
            if price < 100:
                raise ValueError(f"implausible price: {price}")

            _cached_price = price
            _cached_at = now
            log.info("ETH/USD = $%.2f (chainlink)", price)
            return price
        finally:
            if own_client:
                await client.aclose()

    except Exception as e:
        log.debug("ETH price fetch failed (%s); using fallback", e)
        if _cached_price is not None:
            return _cached_price
        return DEFAULT_PRICE


def reset_cache() -> None:
    """For tests."""
    global _cached_price, _cached_at
    _cached_price = None
    _cached_at = 0.0
