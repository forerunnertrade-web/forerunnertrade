"""
DEXScreener integration.

What this is for:
  - Cross-check signal source. The on-chain scanners detect new pools
    instantly; DEXScreener tells us what the market thinks is trending.
    A new pool that ALSO appears on DEXScreener boosted lists within
    minutes is a stronger signal than either source alone.
  - Coverage extension. We wire scanners for ETH/SOL/SUI directly, but
    DEXScreener gives us free coverage of Base, BSC, Polygon, etc.
    Latency is poll-based (~30s+) so this is for opportunistic catches,
    not sniping.

What this is NOT:
  - A replacement for the on-chain scanners. DEXScreener's "new pairs"
    page is web-only; their API doesn't have a "new pairs since X"
    endpoint. We use boosts/profiles as a proxy for "freshly active
    tokens" — these are tokens where someone has paid to promote
    visibility, which is a strong correlation with active launches.
  - Real-time. Their indexer adds visible delay.

Endpoints used (all free, no API key):
  /token-boosts/latest/v1 — most recently boosted tokens (60 req/min cap)
  /token-boosts/top/v1    — most-boosted tokens (60 req/min cap)
  /tokens/v1/{chain}/{addrs} — enrich up to 30 token addresses (300/min)

Rate limit budget: we poll latest boosts every 60s = 60 req/hour.
Profile lookups use the 300/min endpoint with batching.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

import httpx

log = logging.getLogger(__name__)

BASE_URL = "https://api.dexscreener.com"
POLL_INTERVAL_SECONDS = 60

# Map DEXScreener chain ids to our internal chain codes (uppercase 3-char).
# These are the chains we route through the dashboard. Other chains are
# silently skipped (DEXScreener returns many we don't handle).
CHAIN_MAP = {
    "ethereum": "ETH",
    "solana":   "SOL",
    "sui":      "SUI",
    "base":     "BASE",
    "bsc":      "BSC",
    "polygon":  "POLY",
    "arbitrum": "ARB",
}


@dataclass
class TrendingToken:
    """Normalized shape for a token surfaced by DEXScreener."""
    chain: str           # uppercase code (ETH, SOL, BASE, ...)
    chain_id_raw: str    # original DEXScreener chain id
    address: str
    symbol: Optional[str] = None
    name: Optional[str] = None
    icon_url: Optional[str] = None
    price_usd: Optional[float] = None
    liq_usd: Optional[float] = None
    volume_24h: Optional[float] = None
    price_change_24h: Optional[float] = None
    market_cap: Optional[float] = None
    pair_address: Optional[str] = None
    pair_url: Optional[str] = None
    boost_amount: Optional[int] = None  # # of boosts active
    first_seen_at: float = 0.0          # epoch seconds when WE first saw it


# Module-level dedup cache. Map of (chain, address) -> first_seen epoch.
# Old entries are pruned hourly to keep memory bounded.
_seen: dict[tuple[str, str], float] = {}
_PRUNE_AFTER_SECONDS = 6 * 60 * 60  # 6 hours


def _prune_seen():
    cutoff = time.time() - _PRUNE_AFTER_SECONDS
    stale = [k for k, ts in _seen.items() if ts < cutoff]
    for k in stale:
        _seen.pop(k, None)


# ─────────────────────────────────────────────────────────────────────────────
# Low-level fetches
# ─────────────────────────────────────────────────────────────────────────────
async def _get_json(client: httpx.AsyncClient, path: str, params: dict | None = None):
    url = BASE_URL + path
    try:
        resp = await client.get(url, params=params, timeout=8.0)
        if resp.status_code == 429:
            log.warning("DEXScreener rate-limited on %s", path)
            return None
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPError as e:
        log.warning("DEXScreener fetch failed (%s): %s", path, e)
        return None


async def fetch_latest_boosts(client: httpx.AsyncClient) -> list[dict]:
    data = await _get_json(client, "/token-boosts/latest/v1")
    if not data:
        return []
    return data if isinstance(data, list) else data.get("tokens", []) or []


async def fetch_token_pairs(
    client: httpx.AsyncClient, chain_id_raw: str, addresses: list[str]
) -> list[dict]:
    """Returns the pair-level enrichment for up to 30 addresses on one chain."""
    if not addresses:
        return []
    path = f"/tokens/v1/{chain_id_raw}/{','.join(addresses[:30])}"
    data = await _get_json(client, path)
    if not data:
        return []
    if isinstance(data, dict):
        return data.get("pairs", []) or []
    return data


# ─────────────────────────────────────────────────────────────────────────────
# Normalization
# ─────────────────────────────────────────────────────────────────────────────
def _safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _enrich_with_pair(token: TrendingToken, pair: dict) -> TrendingToken:
    """Merge pair-level fields (price, liquidity, volume) into a TrendingToken."""
    base = pair.get("baseToken") or {}
    if base.get("address", "").lower() == token.address.lower():
        token.symbol = token.symbol or base.get("symbol")
        token.name = token.name or base.get("name")

    info = pair.get("info") or {}
    token.icon_url = token.icon_url or info.get("imageUrl")

    token.price_usd = _safe_float(pair.get("priceUsd")) or token.price_usd
    liq = pair.get("liquidity") or {}
    token.liq_usd = _safe_float(liq.get("usd")) or token.liq_usd
    vol = pair.get("volume") or {}
    token.volume_24h = _safe_float(vol.get("h24")) or token.volume_24h
    pc = pair.get("priceChange") or {}
    token.price_change_24h = _safe_float(pc.get("h24")) or token.price_change_24h
    token.market_cap = _safe_float(pair.get("marketCap")) or token.market_cap
    token.pair_address = token.pair_address or pair.get("pairAddress")
    token.pair_url = token.pair_url or pair.get("url")
    return token


def _boost_to_token(item: dict) -> Optional[TrendingToken]:
    """Convert a /token-boosts/latest record to a TrendingToken."""
    chain_id_raw = item.get("chainId")
    addr = item.get("tokenAddress")
    if not chain_id_raw or not addr:
        return None
    chain = CHAIN_MAP.get(chain_id_raw)
    if not chain:
        return None
    return TrendingToken(
        chain=chain,
        chain_id_raw=chain_id_raw,
        address=addr,
        icon_url=item.get("icon"),
        boost_amount=int(item.get("amount") or 0),
        first_seen_at=time.time(),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────
async def fetch_trending(
    enabled_chains: list[str] | None = None,
) -> list[TrendingToken]:
    """One pass: fetch latest boosts, enrich pair data, return only tokens
    we haven't seen before (dedup) and only on enabled chains.

    enabled_chains: list of uppercase codes (["ETH", "SOL", "BASE"]).
                    None means all CHAIN_MAP entries.
    """
    chains_filter = set(enabled_chains) if enabled_chains else set(CHAIN_MAP.values())
    _prune_seen()

    async with httpx.AsyncClient(headers={"User-Agent": "forerunner/0.2"}) as client:
        boosts = await fetch_latest_boosts(client)
        if not boosts:
            return []

        # Convert + filter
        candidates: list[TrendingToken] = []
        for raw in boosts:
            t = _boost_to_token(raw)
            if t is None or t.chain not in chains_filter:
                continue
            key = (t.chain, t.address.lower())
            if key in _seen:
                continue
            _seen[key] = time.time()
            candidates.append(t)

        if not candidates:
            return []

        # Group by chain, fetch pair enrichment in batches of 30
        by_chain: dict[str, list[TrendingToken]] = {}
        for t in candidates:
            by_chain.setdefault(t.chain_id_raw, []).append(t)

        for chain_raw, tokens in by_chain.items():
            for i in range(0, len(tokens), 30):
                batch = tokens[i:i + 30]
                pairs = await fetch_token_pairs(
                    client, chain_raw, [t.address for t in batch]
                )
                # Build address->pair map (a token may have multiple pairs;
                # take the highest-liquidity one).
                by_addr: dict[str, dict] = {}
                for p in pairs:
                    base_addr = (p.get("baseToken") or {}).get("address", "").lower()
                    if not base_addr:
                        continue
                    existing = by_addr.get(base_addr)
                    cur_liq = (p.get("liquidity") or {}).get("usd") or 0
                    ex_liq = (existing or {}).get("liquidity", {}).get("usd") or 0
                    if existing is None or cur_liq > ex_liq:
                        by_addr[base_addr] = p
                for t in batch:
                    pair = by_addr.get(t.address.lower())
                    if pair:
                        _enrich_with_pair(t, pair)

        return candidates


async def run_polling_loop(
    on_trending,
    enabled_chains_provider,
    interval: int = POLL_INTERVAL_SECONDS,
):
    """Long-running task. Calls on_trending(token) for each newly-seen
    trending token. enabled_chains_provider is a callable returning the
    current list of enabled chains (lets us respond to user toggles
    without restart)."""
    log.info("DEXScreener trending poller started (interval=%ds)", interval)
    while True:
        try:
            chains = enabled_chains_provider() if callable(enabled_chains_provider) else enabled_chains_provider
            tokens = await fetch_trending(chains)
            if tokens:
                log.info("DEXScreener: %d new trending token(s)", len(tokens))
            for t in tokens:
                try:
                    await on_trending(t)
                except Exception:
                    log.exception("on_trending handler raised")
        except Exception:
            log.exception("DEXScreener poll failed")
        await asyncio.sleep(interval)
