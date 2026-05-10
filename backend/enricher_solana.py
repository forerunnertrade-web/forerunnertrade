"""
Launch metrics enricher for Solana / Raydium AMM v4.

The on-chain shape:

  Pool state account (LIQUIDITY_STATE_LAYOUT_V4, 752 bytes total) holds:
    - decimals for both sides (base/quote)
    - PublicKeys of the two SPL Token vaults (the actual liquidity)
    - PublicKeys of the two SPL Token mints

  Each vault is a standard SPL Token account whose `amount` field tells us
  how many raw units of the token the pool currently holds.

To measure launch dynamics:
  1. Fetch + decode the pool account → get vault pubkeys + decimals + mints.
  2. Fetch both vault accounts (jsonParsed encoding gives uiAmount directly).
  3. Compute initial liquidity in USD using SOL/USD oracle.
  4. Sleep `sample_seconds`.
  5. Fetch vaults again → final liquidity, price change, final price.

Buyer counting is NOT implemented here — see notes in main module. This
function leaves buy_count, sell_count, unique_buyers as None. The dashboard
already handles null metrics gracefully.

Layout reference: raydium-io/raydium-sdk-v1, src/liquidity/layout.ts.
Field offsets I rely on (all little-endian):
  coinDecimals   @ 32   (u64, but always fits in u8)
  pcDecimals     @ 40   (u64)
  baseVault      @ 336  (PublicKey, 32 bytes)
  quoteVault     @ 368  (PublicKey, 32 bytes)
  baseMint       @ 400  (PublicKey, 32 bytes)
  quoteMint      @ 432  (PublicKey, 32 bytes)

These are derived by summing the layout: 16 u64 (128) + 8 u64 (64) +
4 u64 (32) + 4 u64 (32) + 2 u128 (32) + 1 u64 (8) + 2 u128 (32) + 1 u64 (8)
= 336 bytes preceding baseVault.
"""
from __future__ import annotations

import asyncio
import base64
import logging
from typing import Optional

import base58
import httpx

from scanners.base import LaunchMetrics, PoolEvent

log = logging.getLogger(__name__)

# Common quote tokens on Solana. Their mint addresses identify the "USD-priced"
# side of a pair so we know how to denominate the other side.
WSOL_MINT  = "So11111111111111111111111111111111111111112"
USDC_MINT  = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT  = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"

QUOTE_MINTS = {
    WSOL_MINT: {"is_sol": True},
    USDC_MINT: {"is_sol": False},
    USDT_MINT: {"is_sol": False},
}

# Field offsets in LIQUIDITY_STATE_LAYOUT_V4 (see module docstring)
OFF_COIN_DECIMALS = 32
OFF_PC_DECIMALS   = 40
OFF_BASE_VAULT    = 336
OFF_QUOTE_VAULT   = 368
OFF_BASE_MINT     = 400
OFF_QUOTE_MINT    = 432
POOL_DATA_MIN_LEN = OFF_QUOTE_MINT + 32  # need at least this many bytes


# ─────────────────────────────────────────────────────────────────────────────
# RPC helpers
# ─────────────────────────────────────────────────────────────────────────────
async def _rpc(client: httpx.AsyncClient, url: str, method: str, params: list, timeout=8.0):
    resp = await client.post(
        url,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=timeout,
    )
    resp.raise_for_status()
    body = resp.json()
    if "error" in body:
        raise RuntimeError(f"RPC {method}: {body['error']}")
    return body.get("result")


async def _get_account_data_b64(client, url, pubkey: str) -> Optional[bytes]:
    """Returns raw account data as bytes, or None if account missing."""
    result = await _rpc(client, url, "getAccountInfo", [
        pubkey,
        {"encoding": "base64", "commitment": "confirmed"},
    ])
    if not result or not result.get("value"):
        return None
    data_b64 = result["value"]["data"][0]
    return base64.b64decode(data_b64)


async def _get_token_account_balance(client, url, pubkey: str) -> Optional[float]:
    """Returns the token account balance as a uiAmount (float, scaled by decimals).
    None if account missing or decode fails."""
    result = await _rpc(client, url, "getTokenAccountBalance", [
        pubkey,
        {"commitment": "confirmed"},
    ])
    if not result or not result.get("value"):
        return None
    try:
        ui = result["value"].get("uiAmount")
        return float(ui) if ui is not None else 0.0
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Pool state decoder
# ─────────────────────────────────────────────────────────────────────────────
def _decode_pool_state(data: bytes) -> Optional[dict]:
    """Pulls out only the fields we need. Returns None if data is too short
    (defensive — Raydium pool data is fixed-size 752 bytes but other accounts
    on the same program could be different)."""
    if len(data) < POOL_DATA_MIN_LEN:
        return None
    try:
        coin_decimals  = int.from_bytes(data[OFF_COIN_DECIMALS:OFF_COIN_DECIMALS + 8], "little")
        pc_decimals    = int.from_bytes(data[OFF_PC_DECIMALS:OFF_PC_DECIMALS + 8], "little")
        base_vault     = base58.b58encode(data[OFF_BASE_VAULT:OFF_BASE_VAULT + 32]).decode()
        quote_vault    = base58.b58encode(data[OFF_QUOTE_VAULT:OFF_QUOTE_VAULT + 32]).decode()
        base_mint      = base58.b58encode(data[OFF_BASE_MINT:OFF_BASE_MINT + 32]).decode()
        quote_mint     = base58.b58encode(data[OFF_QUOTE_MINT:OFF_QUOTE_MINT + 32]).decode()
    except Exception as e:
        log.debug("pool decode error: %s", e)
        return None
    return {
        "coin_decimals": coin_decimals,
        "pc_decimals": pc_decimals,
        "base_vault": base_vault,
        "quote_vault": quote_vault,
        "base_mint": base_mint,
        "quote_mint": quote_mint,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Pricing
# ─────────────────────────────────────────────────────────────────────────────
async def _get_sol_price_usd(client: httpx.AsyncClient) -> float:
    """Fetch SOL price in USD from a public price endpoint.

    Solana doesn't have an on-chain Chainlink-style oracle as a standard
    primitive, and querying Pyth requires extra layout decoding. For a
    paper-trading harness, an HTTPS price feed is fine. Cached for 60s.
    """
    return await _sol_price_cached(client)


# Module-level cache
import time
_sol_price: Optional[float] = None
_sol_price_at: float = 0.0
SOL_FALLBACK_USD = 200.0  # used only if every source fails


async def _sol_price_cached(client: httpx.AsyncClient) -> float:
    global _sol_price, _sol_price_at
    if _sol_price is not None and (time.time() - _sol_price_at) < 60:
        return _sol_price

    # Try CoinGecko's free spot endpoint first
    try:
        resp = await client.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "solana", "vs_currencies": "usd"},
            timeout=4.0,
        )
        resp.raise_for_status()
        price = float(resp.json()["solana"]["usd"])
        if price > 1:
            _sol_price = price
            _sol_price_at = time.time()
            log.info("SOL/USD = $%.2f (coingecko)", price)
            return price
    except Exception as e:
        log.debug("CoinGecko SOL price fetch failed: %s", e)

    if _sol_price is not None:
        return _sol_price
    return SOL_FALLBACK_USD


def _quote_to_usd(quote_amount: float, quote_mint: str, sol_usd: float) -> Optional[float]:
    """Convert a uiAmount of the quote side into USD."""
    info = QUOTE_MINTS.get(quote_mint)
    if not info:
        return None
    if info["is_sol"]:
        return quote_amount * sol_usd
    return quote_amount  # USDC, USDT


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────
async def enrich_solana_pool(
    rpc_url: str,
    event: PoolEvent,
    sample_seconds: int = 30,
) -> LaunchMetrics:
    """Measure liquidity + price change for a Raydium AMM v4 pool over
    sample_seconds. Buy/sell counts not implemented (left None)."""
    metrics = LaunchMetrics(sample_seconds=sample_seconds)

    pool_address = event.pool_address
    if not pool_address:
        metrics.error = "missing pool address"
        return metrics

    try:
        async with httpx.AsyncClient() as client:
            # 1. Decode pool state to find vaults and mints
            pool_data = await _get_account_data_b64(client, rpc_url, pool_address)
            if pool_data is None:
                metrics.error = "pool account not found"
                return metrics

            pool = _decode_pool_state(pool_data)
            if pool is None:
                metrics.error = "pool state decode failed"
                return metrics

            # Resolve the quote side. Solana convention: pc = quote, coin = base.
            # But we double-check by looking at which mint matches a known quote.
            quote_mint = pool["quote_mint"]
            base_mint  = pool["base_mint"]
            if quote_mint not in QUOTE_MINTS and base_mint in QUOTE_MINTS:
                # Pool is "swapped" — base is actually the quote side.
                # Swap our local references so the math below works uniformly.
                pool["base_vault"], pool["quote_vault"] = pool["quote_vault"], pool["base_vault"]
                pool["base_mint"], pool["quote_mint"] = pool["quote_mint"], pool["base_mint"]
                pool["coin_decimals"], pool["pc_decimals"] = pool["pc_decimals"], pool["coin_decimals"]
                quote_mint = pool["quote_mint"]
                base_mint  = pool["base_mint"]

            if quote_mint not in QUOTE_MINTS:
                metrics.error = f"unknown quote mint {quote_mint[:8]}…"
                return metrics

            sol_usd = await _get_sol_price_usd(client)

            # Multi-sample loop: ~10 evenly-spaced vault snapshots
            NUM_SAMPLES = 10
            interval = max(1.0, sample_seconds / (NUM_SAMPLES - 1))

            init_base, init_quote = await asyncio.gather(
                _get_token_account_balance(client, rpc_url, pool["base_vault"]),
                _get_token_account_balance(client, rpc_url, pool["quote_vault"]),
            )

            if init_quote is not None:
                quote_usd = _quote_to_usd(init_quote, quote_mint, sol_usd)
                if quote_usd is not None:
                    metrics.initial_liq_usd = quote_usd * 2

            samples = [(init_base, init_quote)]
            for _ in range(NUM_SAMPLES - 1):
                await asyncio.sleep(interval)
                b, q = await asyncio.gather(
                    _get_token_account_balance(client, rpc_url, pool["base_vault"]),
                    _get_token_account_balance(client, rpc_url, pool["quote_vault"]),
                )
                samples.append((b, q))

            fin_base, fin_quote = samples[-1]

            if fin_quote is not None:
                quote_usd_fin = _quote_to_usd(fin_quote, quote_mint, sol_usd)
                if quote_usd_fin is not None:
                    metrics.final_liq_usd = quote_usd_fin * 2

            # Build price series in USD per unit of base token
            price_series = []
            for b, q in samples:
                if b and q and b > 0:
                    price_in_quote = q / b
                    usd = _quote_to_usd(price_in_quote, quote_mint, sol_usd)
                    if usd is not None and usd > 0:
                        price_series.append(usd)

            metrics.price_samples = price_series

            if price_series:
                metrics.final_price_usd = price_series[-1]
                if len(price_series) >= 2 and price_series[0] > 0:
                    metrics.price_change_pct = (
                        (price_series[-1] - price_series[0]) / price_series[0]
                    ) * 100

            # Breakout detection
            if len(price_series) >= 6:
                from breakout import detect_breakout
                bo = detect_breakout(price_series)
                metrics.breakout_triggered = bo.triggered
                metrics.breakout_score = bo.score
                metrics.breakout_reason = bo.reason

    except httpx.HTTPError as e:
        metrics.error = f"http: {type(e).__name__}"
    except Exception as e:
        log.exception("enrich_solana_pool failed for %s", pool_address[:10])
        metrics.error = f"{type(e).__name__}: {str(e)[:80]}"

    return metrics
