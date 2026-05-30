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
SOL_USD_FALLBACK = 85.0  # rough recent (May 2026); Jupiter is the live source. Update periodically.


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
            quote_usd = await _get_sol_usd(client, rpc_url)
        else:
            return _MarkResult(pool, None, f"unknown quote: {quote_mint_addr[:12]}")

        return _MarkResult(pool, price_in_quote * quote_usd)
    except Exception as e:
        return _MarkResult(pool, None, f"{type(e).__name__}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# SOL/USD oracle (Jupiter lite-api + DEXScreener fallback, cached)
# ─────────────────────────────────────────────────────────────────────────────
# Previously we read Pyth's old v1 SOL/USD account directly. Pyth has since
# migrated to a "receiver" architecture where the price isn't at a fixed
# address — it's at a PDA derived from a feed ID + shard ID. Implementing
# that derivation correctly is non-trivial.
#
# Simpler approach: Jupiter's public lite-api. They deprecated the old
# `price.jup.ag` domain (DNS for it is now broken from many networks
# including Railway); the current endpoint is `lite-api.jup.ag/price/v3`.
#
# Belt-and-suspenders: if Jupiter fails, fall back to DEXScreener's
# WSOL price (we already know DEXScreener is reachable from Railway —
# our trending poller uses it successfully). Only fall through to the
# hardcoded constant if BOTH sources fail.
#
# Cache 60s — SOL/USD doesn't move enough to need sub-minute precision.

_WSOL_MINT = "So11111111111111111111111111111111111111112"
JUPITER_PRICE_URL = f"https://lite-api.jup.ag/price/v3?ids={_WSOL_MINT}"
DEXSCREENER_WSOL_URL = f"https://api.dexscreener.com/latest/dex/tokens/{_WSOL_MINT}"
SOL_USD_CACHE_TTL = 60.0  # seconds
_SOL_USD_CACHE = {"value": None, "fetched_at": 0.0, "logged_fail": False}


async def _fetch_sol_usd_jupiter(client) -> float:
    """Try Jupiter lite-api. Returns price or raises."""
    r = await client.get(JUPITER_PRICE_URL)
    if r.status_code != 200:
        raise ValueError(f"HTTP {r.status_code}")
    body = r.json()
    # Jupiter v3 shape: { "So11...": { "usdPrice": "164.83", ... } }
    entry = body.get(_WSOL_MINT)
    if not entry:
        raise ValueError("missing mint key in response")
    price_raw = entry.get("usdPrice")
    if price_raw is None:
        raise ValueError("missing usdPrice field")
    price = float(price_raw)
    if not (10 < price < 10000):
        raise ValueError(f"price {price} outside plausible range")
    return price


async def _fetch_sol_usd_dexscreener(client) -> float:
    """DEXScreener fallback: read WSOL across all pairs, pick highest-liquidity priceUsd."""
    r = await client.get(DEXSCREENER_WSOL_URL)
    if r.status_code != 200:
        raise ValueError(f"HTTP {r.status_code}")
    body = r.json()
    pairs = body.get("pairs") or []
    if not pairs:
        raise ValueError("no pairs returned")
    # Pick highest-liquidity pair to avoid manipulated micropool prices
    pairs.sort(
        key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0),
        reverse=True,
    )
    price_raw = pairs[0].get("priceUsd")
    if price_raw is None:
        raise ValueError("missing priceUsd")
    price = float(price_raw)
    if not (10 < price < 10000):
        raise ValueError(f"price {price} outside plausible range")
    return price


async def _get_sol_usd(client, rpc_url) -> float:
    """Return current SOL/USD price.

    Tries Jupiter (lite-api) first, falls back to DEXScreener, then to the
    hardcoded constant. Caches the result for SOL_USD_CACHE_TTL seconds.
    Logs WARNING only on the first failure so production logs aren't
    flooded — subsequent failures within the TTL serve cached fallback
    silently.
    """
    import time
    now = time.time()
    cached = _SOL_USD_CACHE["value"]
    if cached is not None and (now - _SOL_USD_CACHE["fetched_at"]) < SOL_USD_CACHE_TTL:
        return cached

    errors = []
    for name, fn in (("Jupiter", _fetch_sol_usd_jupiter),
                     ("DEXScreener", _fetch_sol_usd_dexscreener)):
        try:
            price = await fn(client)
            _SOL_USD_CACHE["value"] = price
            _SOL_USD_CACHE["fetched_at"] = now
            _SOL_USD_CACHE["logged_fail"] = False
            log.debug("SOL/USD updated from %s: $%.2f", name, price)
            return price
        except Exception as e:
            errors.append(f"{name}: {type(e).__name__}: {e}")

    # Both sources failed
    if not _SOL_USD_CACHE["logged_fail"]:
        log.warning("SOL/USD: all sources failed (%s) — using fallback $%.0f",
                    "; ".join(errors), SOL_USD_FALLBACK)
        _SOL_USD_CACHE["logged_fail"] = True
    # Cache fallback briefly so we don't hammer external APIs on outages
    _SOL_USD_CACHE["value"] = SOL_USD_FALLBACK
    _SOL_USD_CACHE["fetched_at"] = now - (SOL_USD_CACHE_TTL - 30.0)
    return SOL_USD_FALLBACK


# ─────────────────────────────────────────────────────────────────────────────
# pump.fun bonding-curve price reader
# ─────────────────────────────────────────────────────────────────────────────
# Pump.fun tokens trade against a constant-product virtual reserve, not a
# traditional AMM with two vault accounts. The bonding-curve PDA holds the
# state. PumpPortal already gives us this address as `bondingCurveKey`, so
# we just need to decode the account data.
#
# Layout (after the 8-byte Anchor discriminator, pump.fun is NOT padded):
#   offset  size  field
#      0    8    discriminator
#      8    8    virtualTokenReserves (u64 LE) — in token base units (10^6)
#     16    8    virtualSolReserves   (u64 LE) — in lamports (10^9)
#     24    8    realTokenReserves    (u64 LE)
#     32    8    realSolReserves      (u64 LE)
#     40    8    tokenTotalSupply     (u64 LE)
#     48    1    complete             (bool) — true once token graduated to Raydium
#
# Price formula:
#   sol_per_token = (virtualSolReserves / 1e9) / (virtualTokenReserves / 1e6)
#                 = virtualSolReserves / virtualTokenReserves * 1e-3
#   usd_per_token = sol_per_token * sol_usd
#
# When `complete` is true, the bonding curve is frozen and price discovery
# must move to Raydium. We return None in that case; caller (engine) holds
# the last-known mark until either we re-route through Raydium or the
# position closes.

PUMPFUN_TOKEN_DECIMALS = 6     # All pump.fun tokens use 6 decimals
LAMPORTS_PER_SOL = 1_000_000_000


async def _fetch_pumpfun_price(client, rpc_url, bonding_curve_pda) -> _MarkResult:
    """Read pump.fun bonding-curve PDA and compute current spot price USD."""
    try:
        import base64
        resp = await client.post(rpc_url, json={
            "jsonrpc": "2.0", "id": 1,
            "method": "getAccountInfo",
            "params": [bonding_curve_pda, {"encoding": "base64"}],
        })
        body = resp.json()
        if "error" in body:
            return _MarkResult(bonding_curve_pda, None,
                               f"rpc error: {body['error'].get('message', '?')[:60]}")

        value = (body.get("result") or {}).get("value")
        if not value:
            return _MarkResult(bonding_curve_pda, None, "bonding curve not found")

        data_b64 = value.get("data", [None])[0]
        if not data_b64:
            return _MarkResult(bonding_curve_pda, None, "empty curve data")

        data = base64.b64decode(data_b64)
        if len(data) < 49:
            return _MarkResult(bonding_curve_pda, None,
                               f"curve too short: {len(data)} bytes")

        # Skip 8-byte discriminator
        v_tokens = int.from_bytes(data[8:16], "little")
        v_sol = int.from_bytes(data[16:24], "little")
        complete = bool(data[48])

        if complete:
            # Token has graduated. Caller should route to Raydium for this
            # token from now on. We don't know its Raydium pool address
            # from here, so return None and let the position hold its mark.
            return _MarkResult(bonding_curve_pda, None,
                               "bonding curve complete (graduated)")

        if v_tokens == 0:
            return _MarkResult(bonding_curve_pda, None, "zero token reserves")

        # Spot price in SOL per token, with decimal correction:
        #   v_sol is in lamports (1e9 per SOL)
        #   v_tokens is in token base units (1e6 per token)
        sol_per_token = (v_sol / LAMPORTS_PER_SOL) / (v_tokens / (10 ** PUMPFUN_TOKEN_DECIMALS))

        sol_usd = await _get_sol_usd(client, rpc_url)
        price_usd = sol_per_token * sol_usd

        if price_usd <= 0:
            return _MarkResult(bonding_curve_pda, None, "zero price computed")

        return _MarkResult(bonding_curve_pda, price_usd)
    except Exception as e:
        return _MarkResult(bonding_curve_pda, None, f"{type(e).__name__}: {e}")


async def _fetch_pumpfun_prices_batched(
    client, rpc_url: str, positions: list[dict]
) -> dict[int, "_MarkResult"]:
    """Batch-read N pump.fun bonding curves in a single RPC call.

    Why this matters: previously we made one getAccountInfo call per open
    position, every 5 seconds. With 5 positions that's 60 RPCs/minute just
    for mark refreshes — enough to blow free-tier quotas on a Solana RPC.
    `getMultipleAccountsInfo` lets us pack all N into one HTTP request,
    cutting RPC volume 5x. Same wire format as getAccountInfo, just a list.

    Returns {position_id: _MarkResult}. Each position gets its own result —
    if the batched call fails entirely, all positions get the same error;
    if individual entries in the response are null/malformed, only those
    positions get errors. This preserves the per-position error surfacing
    in the dispatcher.

    Limit: Solana RPC caps getMultipleAccountsInfo at ~100 accounts per
    request. We're nowhere near that with `maxConcurrent` of 5-10, so no
    chunking needed yet. If/when we lift that limit, chunk this here.
    """
    import base64
    if not positions:
        return {}

    pdas = [p["pool_address"] for p in positions]
    pos_ids = [p["id"] for p in positions]

    try:
        resp = await client.post(rpc_url, json={
            "jsonrpc": "2.0", "id": 1,
            "method": "getMultipleAccounts",
            "params": [pdas, {"encoding": "base64"}],
        })
        body = resp.json()
        if "error" in body:
            # Whole-batch failure — propagate same error to every position
            err = f"rpc error: {body['error'].get('message', '?')[:80]}"
            return {pid: _MarkResult(pdas[i], None, err) for i, pid in enumerate(pos_ids)}

        value = (body.get("result") or {}).get("value") or []
        if len(value) != len(positions):
            # Malformed: response should have exactly N entries
            err = f"batch response had {len(value)} entries, expected {len(positions)}"
            return {pid: _MarkResult(pdas[i], None, err) for i, pid in enumerate(pos_ids)}
    except Exception as e:
        err = f"batch fetch failed: {type(e).__name__}: {e}"
        return {pid: _MarkResult(pdas[i], None, err) for i, pid in enumerate(pos_ids)}

    # SOL/USD once per batch (cached anyway, but no point calling it N times)
    sol_usd = await _get_sol_usd(client, rpc_url)

    out: dict[int, _MarkResult] = {}
    for i, (pos_id, pda, entry) in enumerate(zip(pos_ids, pdas, value)):
        if entry is None:
            out[pos_id] = _MarkResult(pda, None, "bonding curve not found")
            continue
        try:
            data_b64 = entry.get("data", [None])[0]
            if not data_b64:
                out[pos_id] = _MarkResult(pda, None, "empty curve data")
                continue
            data = base64.b64decode(data_b64)
            if len(data) < 49:
                out[pos_id] = _MarkResult(pda, None, f"curve too short: {len(data)} bytes")
                continue

            v_tokens = int.from_bytes(data[8:16], "little")
            v_sol = int.from_bytes(data[16:24], "little")
            complete = bool(data[48])

            if complete:
                out[pos_id] = _MarkResult(pda, None, "bonding curve complete (graduated)")
                continue
            if v_tokens == 0:
                out[pos_id] = _MarkResult(pda, None, "zero token reserves")
                continue

            sol_per_token = (v_sol / LAMPORTS_PER_SOL) / (v_tokens / (10 ** PUMPFUN_TOKEN_DECIMALS))
            price_usd = sol_per_token * sol_usd

            if price_usd <= 0:
                out[pos_id] = _MarkResult(pda, None, "zero price computed")
            else:
                out[pos_id] = _MarkResult(pda, price_usd)
        except Exception as e:
            out[pos_id] = _MarkResult(pda, None, f"{type(e).__name__}: {e}")

    return out


async def fetch_position_prices(positions: list[dict]) -> dict[int, float]:
    """Given a list of open position dicts with id/chain/pool_address/address,
    return {position_id: current_price_usd}. Positions whose price couldn't
    be fetched (RPC error, missing config, decoder fail) are omitted from
    the result — caller should hold the previous mark for those.

    RPC routing: pump.fun bonding-curve reads are the highest-volume
    sustained traffic in this system. To avoid blowing free-tier quotas
    on the scanner's RPC (Helius), the price oracle can use a separate
    RPC via SOL_PRICE_RPC_URL. Falls back to SOL_HTTP_URL if unset, and
    finally to the public Solana RPC if neither is configured. EVM and
    Raydium reads still use their dedicated RPC env vars.
    """
    if not positions:
        return {}

    eth_rpc = _os.getenv("ETH_HTTP_URL", "")
    sol_rpc = _os.getenv("SOL_HTTP_URL", "")
    # Price-oracle traffic goes here. If unset, fall back to the scanner
    # RPC; if that's also unset, try Solana's free public RPC. The latter
    # is heavily throttled by IP, but better than nothing for paper trading.
    sol_price_rpc = (
        _os.getenv("SOL_PRICE_RPC_URL")
        or sol_rpc
        or "https://api.mainnet-beta.solana.com"
    )
    if not eth_rpc and not sol_rpc and not _os.getenv("SOL_PRICE_RPC_URL"):
        return {}

    import asyncio

    # Partition positions by routing path so the batched pump.fun call can
    # be made independently of the per-position EVM/Raydium fetchers.
    pumpfun_positions = []
    other_tasks = []
    other_meta = []

    async with httpx.AsyncClient(timeout=6.0) as client:
        for p in positions:
            chain = (p.get("chain") or "").upper()
            pool = p.get("pool_address") or p.get("poolAddress")
            tok = p.get("address")
            dex = (p.get("dex") or "").lower()
            if not pool or not tok:
                continue
            if chain == "ETH" and eth_rpc:
                quote = p.get("quote_address") or _WETH_LC
                other_tasks.append(_fetch_evm_price(client, eth_rpc, pool, tok, quote))
                other_meta.append(p["id"])
            elif chain == "SOL":
                if dex == "pumpfun":
                    pumpfun_positions.append(p)
                elif sol_rpc:
                    # Raydium path: keep on scanner RPC (lower volume,
                    # only fires for trending/non-pumpfun Solana positions)
                    other_tasks.append(_fetch_solana_price(client, sol_rpc, pool, tok))
                    other_meta.append(p["id"])

        if not pumpfun_positions and not other_tasks:
            return {}

        # Fire both paths concurrently
        pumpfun_task = (
            _fetch_pumpfun_prices_batched(client, sol_price_rpc, pumpfun_positions)
            if pumpfun_positions else asyncio.sleep(0, result={})
        )
        if other_tasks:
            other_results, pumpfun_results = await asyncio.gather(
                asyncio.gather(*other_tasks, return_exceptions=True),
                pumpfun_task,
            )
        else:
            pumpfun_results = await pumpfun_task
            other_results = []

    # Stitch together
    out: dict[int, float] = {}
    errors: list[tuple[int, str]] = []
    exceptions = 0

    # pump.fun batched results
    for pos_id, r in pumpfun_results.items():
        if r.price_usd is not None and r.price_usd > 0:
            out[pos_id] = r.price_usd
        else:
            errors.append((pos_id, r.error or "no reason"))

    # EVM/Raydium per-position results
    for pos_id, r in zip(other_meta, other_results):
        if isinstance(r, _MarkResult) and r.price_usd is not None and r.price_usd > 0:
            out[pos_id] = r.price_usd
        elif isinstance(r, _MarkResult):
            errors.append((pos_id, r.error or "no reason"))
        else:
            exceptions += 1
            errors.append((pos_id, f"exception: {type(r).__name__}: {r}"))

    n_total = len(pumpfun_positions) + len(other_tasks)
    if n_total == 0:
        pass
    elif len(out) == n_total:
        log.info("price oracle: refreshed %d/%d positions", len(out), n_total)
    else:
        log.warning(
            "price oracle: refreshed %d/%d positions (%d failures%s)",
            len(out), n_total, len(errors),
            f", {exceptions} exceptions" if exceptions else "",
        )
        for pos_id, reason in errors[:3]:
            log.warning("  pos=%d: %s", pos_id, reason)
        if len(errors) > 3:
            log.warning("  ... and %d more failures with similar reasons",
                        len(errors) - 3)
    return out
