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


# ─────────────────────────────────────────────────────────────────────────────
# Pool spot prices for mark-to-market
# ─────────────────────────────────────────────────────────────────────────────
# Beyond the ETH/USD feed above, the engine needs per-position spot prices
# so its TP/SL fires on real market movement rather than the previous
# random-walk drift. These functions take an open position and return its
# current USD spot.
#
# Failure mode: every path returns None on any error. Caller (engine) falls
# back to the previous mark, which prevents simulation hangs when an RPC
# rate-limits or a pool gets removed.
# ─────────────────────────────────────────────────────────────────────────────

import os as _os
from dataclasses import dataclass as _dataclass

SEL_GET_RESERVES = "0x0902f1ac"  # getReserves()
SEL_TOKEN0       = "0x0dfe1681"  # token0()
SEL_DECIMALS     = "0x313ce567"  # decimals()

# Known quote-side tokens we can convert to USD without an oracle call.
# For paper-trading marks, "USD ≈ 1.0 for stables" is sufficient — what
# matters for TP/SL is RELATIVE price change since entry, and using the
# same simplification at entry and exit cancels out the approximation.
_USD_HINTS = {
    "So11111111111111111111111111111111111111112": None,   # WSOL → uses fallback below
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": 1.0,    # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": 1.0,    # USDT
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 1.0,     # USDC mainnet (lowercase)
    "0xdac17f958d2ee523a2206206994597c13d831ec7": 1.0,     # USDT mainnet
    "0x6b175474e89094c44da98b954eedeac495271d0f": 1.0,     # DAI mainnet
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": None,    # WETH → uses fallback
}
_WETH_LC = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
_WSOL    = "So11111111111111111111111111111111111111112"
SOL_USD_FALLBACK = 180.0  # rough recent; engine TP/SL is %-based so absolute error is bounded


@_dataclass
class _MarkResult:
    pool_address: str
    price_usd: Optional[float]
    error: Optional[str] = None


async def _eth_call_simple(client, rpc_url, to_addr, data) -> Optional[str]:
    try:
        resp = await client.post(rpc_url, json={
            "jsonrpc": "2.0", "id": 1, "method": "eth_call",
            "params": [{"to": to_addr, "data": data}, "latest"],
        }, timeout=6.0)
        body = resp.json()
        return body.get("result")
    except Exception:
        return None


async def _fetch_evm_price(client, rpc_url, pool, token, quote) -> _MarkResult:
    """Read reserves + decimals + token0 ordering, compute price."""
    tok0_raw = await _eth_call_simple(client, rpc_url, pool, SEL_TOKEN0)
    if not tok0_raw or len(tok0_raw) < 66:
        return _MarkResult(pool, None, "token0 read failed")
    token0 = "0x" + tok0_raw[-40:].lower()
    our_is_token0 = token0 == token.lower()

    res_raw = await _eth_call_simple(client, rpc_url, pool, SEL_GET_RESERVES)
    if not res_raw or len(res_raw) < 2 + 64 * 3:
        return _MarkResult(pool, None, "reserves read failed")
    try:
        data = bytes.fromhex(res_raw[2:])
        r0 = int.from_bytes(data[:32], "big")
        r1 = int.from_bytes(data[32:64], "big")
    except Exception as e:
        return _MarkResult(pool, None, f"reserves decode: {e}")

    # Decimals for both sides
    dec_raw = [await _eth_call_simple(client, rpc_url, a, SEL_DECIMALS) for a in (token, quote)]
    if not all(dec_raw) or any(len(r) < 4 for r in dec_raw):
        return _MarkResult(pool, None, "decimals read failed")
    try:
        tok_dec = int(dec_raw[0], 16)
        quote_dec = int(dec_raw[1], 16)
    except Exception:
        return _MarkResult(pool, None, "decimals parse failed")
    if not (0 <= tok_dec <= 36 and 0 <= quote_dec <= 36):
        return _MarkResult(pool, None, "implausible decimals")

    our_r, quote_r = (r0, r1) if our_is_token0 else (r1, r0)
    if our_r == 0:
        return _MarkResult(pool, None, "zero reserve")

    our_h = our_r / (10 ** tok_dec)
    quote_h = quote_r / (10 ** quote_dec)
    price_in_quote = quote_h / our_h

    # Quote → USD
    qlc = (quote or "").lower()
    hint = _USD_HINTS.get(qlc)
    if hint is not None:
        quote_usd = hint
    elif qlc == _WETH_LC:
        # Use the cached Chainlink ETH/USD price; fall back to DEFAULT_PRICE
        # in get_eth_usd's failure path.
        quote_usd = await get_eth_usd(rpc_url, client)
    else:
        return _MarkResult(pool, None, f"unknown quote: {quote}")

    return _MarkResult(pool, price_in_quote * quote_usd)


async def _fetch_solana_price(client, rpc_url, pool, token) -> _MarkResult:
    """Read pool state + vault balances + compute price."""
    try:
        from enricher_solana import _get_account_data, _decode_pool_state, _get_token_account_balance
        data = await _get_account_data(client, rpc_url, pool)
        if not data:
            return _MarkResult(pool, None, "pool not found")
        state = _decode_pool_state(data)
        if not state:
            return _MarkResult(pool, None, "pool decode failed")

        import asyncio
        base_bal, quote_bal = await asyncio.gather(
            _get_token_account_balance(client, rpc_url, state["base_vault"]),
            _get_token_account_balance(client, rpc_url, state["quote_vault"]),
        )
        if base_bal is None or quote_bal is None:
            return _MarkResult(pool, None, "vault read failed")

        base_mint = state.get("base_mint", "")
        quote_mint = state.get("quote_mint", "")
        our_is_base = base_mint.lower() == token.lower()
        our_r = base_bal if our_is_base else quote_bal
        quote_r = quote_bal if our_is_base else base_bal
        quote_mint_addr = quote_mint if our_is_base else base_mint

        if our_r == 0:
            return _MarkResult(pool, None, "zero reserve")

        price_in_quote = quote_r / our_r
        hint = _USD_HINTS.get(quote_mint_addr)
        if hint is not None:
            quote_usd = hint
        elif quote_mint_addr == _WSOL:
            quote_usd = SOL_USD_FALLBACK  # see TODO above; oracle integration deferred
        else:
            return _MarkResult(pool, None, f"unknown quote: {quote_mint_addr[:12]}")

        return _MarkResult(pool, price_in_quote * quote_usd)
    except Exception as e:
        return _MarkResult(pool, None, f"{type(e).__name__}: {e}")


async def fetch_position_prices(positions: list[dict]) -> dict[int, float]:
    """Given a list of open position dicts with id/chain/pool_address/address,
    return {position_id: current_price_usd}. Positions whose price couldn't
    be fetched (RPC error, missing config, decoder fail) are omitted from
    the result — caller should hold the previous mark for those."""
    if not positions:
        return {}

    eth_rpc = _os.getenv("ETH_HTTP_URL", "")
    sol_rpc = _os.getenv("SOL_HTTP_URL", "")
    if not eth_rpc and not sol_rpc:
        return {}

    import asyncio
    async with httpx.AsyncClient(timeout=6.0) as client:
        tasks = []
        meta = []
        for p in positions:
            chain = (p.get("chain") or "").upper()
            pool = p.get("pool_address") or p.get("poolAddress")
            tok = p.get("address")
            if not pool or not tok:
                continue
            if chain == "ETH" and eth_rpc:
                quote = p.get("quote_address") or _WETH_LC
                tasks.append(_fetch_evm_price(client, eth_rpc, pool, tok, quote))
                meta.append(p["id"])
            elif chain == "SOL" and sol_rpc:
                tasks.append(_fetch_solana_price(client, sol_rpc, pool, tok))
                meta.append(p["id"])

        if not tasks:
            return {}

        results = await asyncio.gather(*tasks, return_exceptions=True)

    out: dict[int, float] = {}
    for pos_id, r in zip(meta, results):
        if isinstance(r, _MarkResult) and r.price_usd is not None and r.price_usd > 0:
            out[pos_id] = r.price_usd
        elif isinstance(r, _MarkResult):
            log.debug("price fetch failed pos=%d: %s", pos_id, r.error)
    return out
