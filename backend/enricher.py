"""
Launch metrics enricher (EVM).

Given a freshly created pool, measures real launch dynamics over a fixed
sample window (default 30s). Handles both Uniswap V2 and V3 swap events.

For each pool:
  1. Read reserves at creation block — initial liquidity baseline.
  2. Sleep `sample_seconds`.
  3. Read reserves at "latest" — final liquidity + price.
  4. eth_getLogs Swap events on the pool from creation block to latest.
  5. Decode each swap as buy/sell using V2 or V3 ABI per the pool's DEX.
  6. Price the quote-side reserve in USD via Chainlink ETH/USD feed.
  7. Compute price-in-USD at sample time for the live entry price.

V2 vs V3 swap events:

  V2: Swap(sender, amount0In, amount1In, amount0Out, amount1Out, to)
      Direction: which `amount?In` is non-zero tells you which token came in.

  V3: Swap(sender, recipient, amount0, amount1, sqrtPriceX96, liquidity, tick)
      Direction: amount0/amount1 are SIGNED. Positive = into pool.

V3 has no LP token, so the LP-lock check in the auditor reads as "no LP
token (likely V3)" — that's expected, not a bug.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import httpx
from eth_abi import decode as abi_decode
from eth_utils import keccak, to_checksum_address

from price_oracle import get_eth_usd
from scanners.base import LaunchMetrics, PoolEvent

log = logging.getLogger(__name__)

# Common quote tokens (the "other side" of a NEW/QUOTE pair).
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"

QUOTE_TOKENS = {
    WETH.lower(): {"decimals": 18, "is_eth_priced": True},
    USDC.lower(): {"decimals": 6,  "is_eth_priced": False},  # USD-pegged
    USDT.lower(): {"decimals": 6,  "is_eth_priced": False},
}

# Selectors / topic hashes
SEL_GET_RESERVES = "0x" + keccak(text="getReserves()").hex()[:8]
SEL_TOKEN0       = "0x" + keccak(text="token0()").hex()[:8]
SEL_SLOT0        = "0x" + keccak(text="slot0()").hex()[:8]
SEL_LIQUIDITY    = "0x" + keccak(text="liquidity()").hex()[:8]

TOPIC_SWAP_V2 = "0x" + keccak(
    text="Swap(address,uint256,uint256,uint256,uint256,address)"
).hex()
TOPIC_SWAP_V3 = "0x" + keccak(
    text="Swap(address,address,int256,int256,uint160,uint128,int24)"
).hex()


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
        raise RuntimeError(f"RPC {method} error: {body['error']}")
    return body.get("result")


async def _get_reserves_v2(client, url, pool_address: str, block: str = "latest"):
    raw = await _rpc(client, url, "eth_call", [
        {"to": pool_address, "data": SEL_GET_RESERVES},
        block,
    ])
    if not raw or len(raw) < 2 + 64 * 3:
        return None
    try:
        data = bytes.fromhex(raw[2:])
        r0, r1, ts = abi_decode(["uint112", "uint112", "uint32"], data)
        return int(r0), int(r1), int(ts)
    except Exception as e:
        log.debug("reserves decode failed: %s", e)
        return None


async def _get_token0(client, url, pool_address: str) -> Optional[str]:
    raw = await _rpc(client, url, "eth_call", [
        {"to": pool_address, "data": SEL_TOKEN0},
        "latest",
    ])
    if not raw or len(raw) < 66:
        return None
    try:
        (addr,) = abi_decode(["address"], bytes.fromhex(raw[2:]))
        return to_checksum_address(addr)
    except Exception:
        return None


async def _get_swap_logs(client, url, pool_address: str, from_block: int, topic: str):
    """eth_getLogs for Swap events on this pool from `from_block` to latest."""
    raw = await _rpc(client, url, "eth_getLogs", [{
        "fromBlock": hex(from_block),
        "toBlock": "latest",
        "address": pool_address,
        "topics": [topic],
    }])
    return raw or []


# ─────────────────────────────────────────────────────────────────────────────
# Decoders
# ─────────────────────────────────────────────────────────────────────────────
def _decode_swap_v2(log_obj: dict):
    """Returns (amount0_in, amount1_in, amount0_out, amount1_out, to_addr)."""
    data = bytes.fromhex(log_obj["data"][2:])
    a0in, a1in, a0out, a1out = abi_decode(
        ["uint256", "uint256", "uint256", "uint256"], data
    )
    to_topic = log_obj["topics"][2]
    to_addr = to_checksum_address("0x" + to_topic[-40:])
    return int(a0in), int(a1in), int(a0out), int(a1out), to_addr


def _decode_swap_v3(log_obj: dict):
    """Returns (amount0_signed, amount1_signed, recipient).
    Positive amounts = INTO pool, negative = OUT of pool."""
    data = bytes.fromhex(log_obj["data"][2:])
    a0, a1, _sqrtPriceX96, _liquidity, _tick = abi_decode(
        ["int256", "int256", "uint160", "uint128", "int24"], data
    )
    # recipient is indexed (topic 2)
    recip_topic = log_obj["topics"][2]
    recipient = to_checksum_address("0x" + recip_topic[-40:])
    return int(a0), int(a1), recipient


# ─────────────────────────────────────────────────────────────────────────────
# Pricing
# ─────────────────────────────────────────────────────────────────────────────
async def _quote_usd_per_unit(rpc_url: str, client: httpx.AsyncClient, quote_addr: str) -> float:
    """How many USD is one whole-unit (10^decimals raw) of the quote token worth?"""
    info = QUOTE_TOKENS.get(quote_addr.lower())
    if not info:
        return 0.0
    if info["is_eth_priced"]:
        return await get_eth_usd(rpc_url, client)
    return 1.0  # USDC, USDT


def _liq_to_usd(reserve_quote_raw: int, quote_addr: str, usd_per_unit: float) -> Optional[float]:
    info = QUOTE_TOKENS.get(quote_addr.lower())
    if not info or usd_per_unit <= 0:
        return None
    quote_usd = (reserve_quote_raw / (10 ** info["decimals"])) * usd_per_unit
    return quote_usd * 2  # both sides combined, V2 invariant assumption


def _price_in_usd(
    base_reserve_raw: int,
    quote_reserve_raw: int,
    base_decimals: int,
    quote_addr: str,
    usd_per_unit: float,
) -> Optional[float]:
    """Compute the marginal price of base token in USD.
    base_decimals defaults to 18 since we don't fetch it (cost). For
    accurate USD pricing on tokens with non-18 decimals we'd need an
    extra eth_call — accepted as a known approximation."""
    if base_reserve_raw <= 0 or usd_per_unit <= 0:
        return None
    info = QUOTE_TOKENS.get(quote_addr.lower())
    if not info:
        return None
    quote_human = quote_reserve_raw / (10 ** info["decimals"])
    base_human = base_reserve_raw / (10 ** base_decimals)
    if base_human <= 0:
        return None
    price_in_quote = quote_human / base_human
    return price_in_quote * usd_per_unit


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────
async def enrich_pool(
    rpc_url: str,
    event: PoolEvent,
    sample_seconds: int = 30,
) -> LaunchMetrics:
    """Measure launch dynamics for a pool. Always returns LaunchMetrics —
    on any error, the `error` field is populated and other fields stay None."""
    metrics = LaunchMetrics(sample_seconds=sample_seconds)

    if event.chain != "ethereum":
        # SOL/SUI handled elsewhere. Caller should branch on chain.
        metrics.error = "non-EVM enrichment routed wrong"
        return metrics

    is_v3 = (event.dex or "").lower().endswith("v3")
    pool = to_checksum_address(event.pool_address)
    start_block = event.block_or_slot

    try:
        async with httpx.AsyncClient() as client:
            token0 = await _get_token0(client, rpc_url, pool)
            if token0 is None:
                metrics.error = "token0() unreadable"
                return metrics

            # Resolve which side is the quote token
            t0_l = token0.lower()
            other = event.token1 if event.token0.lower() == t0_l else event.token0
            other_l = other.lower()

            quote_addr = None
            quote_index = None  # 0 if token0 is the quote, 1 if token1
            if t0_l in QUOTE_TOKENS:
                quote_addr, quote_index = token0, 0
            elif other_l in QUOTE_TOKENS:
                quote_addr, quote_index = other, 1

            usd_per_unit = 0.0
            if quote_addr:
                usd_per_unit = await _quote_usd_per_unit(rpc_url, client, quote_addr)

            # ── V2 path: reserves are readable ───────────────────────────────
            if not is_v3:
                # Multi-sample loop: take ~10 evenly-spaced reserve snapshots
                # over the window so we have enough data points for the
                # breakout detector. First snapshot is at the creation block.
                NUM_SAMPLES = 10
                interval = max(1.0, sample_seconds / (NUM_SAMPLES - 1))

                init = await _get_reserves_v2(client, rpc_url, pool, hex(start_block))
                if init is None:
                    metrics.error = "initial reserves unreadable"
                    return metrics
                r0_init, r1_init, _ = init

                if quote_addr is not None:
                    qres_init = r0_init if quote_index == 0 else r1_init
                    metrics.initial_liq_usd = _liq_to_usd(qres_init, quote_addr, usd_per_unit)

                # Build the price series. Each sample is (base_reserve_raw,
                # quote_reserve_raw) — we'll compute USD prices from them at
                # the end so we don't multiply error sources mid-loop.
                samples_raw = [(r0_init, r1_init)]
                for _ in range(NUM_SAMPLES - 1):
                    await asyncio.sleep(interval)
                    snap = await _get_reserves_v2(client, rpc_url, pool, "latest")
                    if snap is not None:
                        samples_raw.append((snap[0], snap[1]))

                if len(samples_raw) < 2:
                    metrics.error = "no reserve snapshots collected"
                    return metrics

                r0_fin, r1_fin = samples_raw[-1]

                if quote_addr is not None:
                    qres_fin = r0_fin if quote_index == 0 else r1_fin
                    metrics.final_liq_usd = _liq_to_usd(qres_fin, quote_addr, usd_per_unit)

                # Convert samples to a USD price series. Skipping samples
                # where the base side is zero (pre-funding edge case).
                price_series = []
                if quote_addr is not None:
                    base_idx = 1 - quote_index
                    for r0, r1 in samples_raw:
                        base_res = r0 if base_idx == 0 else r1
                        quote_res = r0 if quote_index == 0 else r1
                        if base_res > 0:
                            usd = _price_in_usd(base_res, quote_res, 18, quote_addr, usd_per_unit)
                            if usd is not None and usd > 0:
                                price_series.append(usd)

                metrics.price_samples = price_series

                if price_series:
                    metrics.final_price_usd = price_series[-1]

                # Price change %: still simple first-vs-last for the strategy filter
                try:
                    if quote_index == 0:
                        p_init = r0_init / max(r1_init, 1)
                        p_fin  = r0_fin  / max(r1_fin,  1)
                    else:
                        p_init = r1_init / max(r0_init, 1)
                        p_fin  = r1_fin  / max(r0_fin,  1)
                    if p_init > 0:
                        metrics.price_change_pct = ((p_fin - p_init) / p_init) * 100
                except Exception:
                    pass

                # Breakout detection on the price series
                if len(price_series) >= 6:
                    from breakout import detect_breakout
                    bo = detect_breakout(price_series)
                    metrics.breakout_triggered = bo.triggered
                    metrics.breakout_score = bo.score
                    metrics.breakout_reason = bo.reason

                # Swap counts
                metrics.buy_count, metrics.sell_count, metrics.unique_buyers = \
                    await _count_v2_swaps(client, rpc_url, pool, start_block, quote_index)

            # ── V3 path: no reserves; only swap-event flow ───────────────────
            else:
                # V3 doesn't expose reserves the same way. We could read
                # liquidity() and slot0(), but for buyer-count purposes we
                # only need the swap logs. Liquidity in USD is left None
                # for V3 (a known limitation; would need slot0 + liquidity
                # decode + tick math to reconstruct).
                await asyncio.sleep(sample_seconds)

                metrics.buy_count, metrics.sell_count, metrics.unique_buyers = \
                    await _count_v3_swaps(client, rpc_url, pool, start_block, quote_index)

                # We can still compute price change from sqrtPriceX96 in the
                # first vs last swap event if there are at least two.
                # Skipping for now — TODO if V3 launches become a focus.

    except httpx.HTTPError as e:
        metrics.error = f"http: {type(e).__name__}"
    except Exception as e:
        log.exception("enrich_pool failed for %s", event.pool_address[:10])
        metrics.error = f"{type(e).__name__}: {str(e)[:80]}"

    return metrics


async def _count_v2_swaps(client, rpc_url, pool, start_block, quote_index):
    """Returns (buys, sells, unique_buyers). quote_index can be None — in that
    case we count any swap as a 'transaction' but don't classify direction."""
    try:
        logs = await _get_swap_logs(client, rpc_url, pool, start_block, TOPIC_SWAP_V2)
    except Exception as e:
        log.debug("getLogs V2 failed for %s: %s", pool[:10], e)
        return 0, 0, 0

    buys = 0
    sells = 0
    buyers = set()
    for lg in logs:
        try:
            a0in, a1in, a0out, a1out, to_addr = _decode_swap_v2(lg)
        except Exception:
            continue
        if quote_index is None:
            # No reference token — just count uniques
            buyers.add(to_addr)
            continue
        # Buy of base token = quote-in & base-out
        if quote_index == 1:
            is_buy  = a1in > 0 and a0out > 0
            is_sell = a0in > 0 and a1out > 0
        else:
            is_buy  = a0in > 0 and a1out > 0
            is_sell = a1in > 0 and a0out > 0
        if is_buy:
            buys += 1
            buyers.add(to_addr)
        elif is_sell:
            sells += 1
    return buys, sells, len(buyers)


async def _count_v3_swaps(client, rpc_url, pool, start_block, quote_index):
    """Returns (buys, sells, unique_buyers) for V3.

    V3 amounts are signed:
      amount0 > 0 means token0 was sent INTO the pool by the caller
      amount0 < 0 means token0 was sent OUT of the pool to the recipient
      (token1 mirrors)

    A buy of the BASE token = base goes OUT, quote goes IN."""
    try:
        logs = await _get_swap_logs(client, rpc_url, pool, start_block, TOPIC_SWAP_V3)
    except Exception as e:
        log.debug("getLogs V3 failed for %s: %s", pool[:10], e)
        return 0, 0, 0

    buys = 0
    sells = 0
    buyers = set()
    for lg in logs:
        try:
            a0, a1, recipient = _decode_swap_v3(lg)
        except Exception:
            continue
        if quote_index is None:
            buyers.add(recipient)
            continue
        # Buy: quote (positive into pool) on the quote side, base (negative
        # out of pool) on the base side.
        if quote_index == 1:
            # quote=token1, base=token0
            is_buy  = a1 > 0 and a0 < 0
            is_sell = a0 > 0 and a1 < 0
        else:
            # quote=token0, base=token1
            is_buy  = a0 > 0 and a1 < 0
            is_sell = a1 > 0 and a0 < 0
        if is_buy:
            buys += 1
            buyers.add(recipient)
        elif is_sell:
            sells += 1
    return buys, sells, len(buyers)
