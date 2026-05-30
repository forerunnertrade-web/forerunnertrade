"""
Slippage model for pump.fun bonding-curve trades.

Paper trading without slippage produces wildly optimistic P&L. Pump.fun is
a constant-product AMM (k = v_sol × v_tokens), which means:

  - Your buy moves the price up (you get fewer tokens than the pre-trade
    spot would suggest)
  - Your sell moves the price down (you get less USD than the pre-trade
    spot would suggest)
  - Pump.fun takes 1% protocol fee on each side

This module exposes two functions, simulate_buy and simulate_sell, that
take the pre-trade spot price + the order size and return the actual fill
price after slippage and fees.

Reserves are reconstructed from the spot price using the known initial
state of every pump.fun curve (30 SOL × 1.073B tokens) and the constant-
product invariant. This means we don't need to read the bonding curve PDA
at fill time — we trust the price oracle's spot reading and back out the
implied reserves.

This is a LOWER BOUND on real slippage. It models:
  - AMM impact (correct for current curve state)
  - 1% protocol fee
It does NOT model:
  - Transaction confirmation latency (price moves during the 400-2000ms
    your tx is propagating; for fresh launches this is significant)
  - Front-running / MEV (other bots filling at better prices than you)
  - Failed transactions (your buy reverts because the curve moved past
    your slippage limit)

For paper trading, modeling the AMM + fee component alone is enough to
expose the realistic shape of P&L. Real-money execution will be worse
than this model predicts, often by 30-50% on fast-moving tokens.
"""
from __future__ import annotations

import math
from typing import NamedTuple


# Pump.fun protocol constants
LAMPORTS_PER_SOL = 1_000_000_000
PUMPFUN_TOKEN_DECIMALS = 6

# Standard pump.fun initial bonding curve reserves (every launch starts here)
PUMPFUN_INITIAL_V_SOL = 30 * LAMPORTS_PER_SOL                       # 30 SOL in lamports
PUMPFUN_INITIAL_V_TOKENS = 1_073_000_000 * (10 ** PUMPFUN_TOKEN_DECIMALS)  # 1.073B tokens in base units
PUMPFUN_K = PUMPFUN_INITIAL_V_SOL * PUMPFUN_INITIAL_V_TOKENS         # constant product

# Pump.fun's per-side protocol fee
PUMPFUN_FEE_RATE = 0.01  # 1%


class FillResult(NamedTuple):
    """Result of a simulated AMM fill."""
    quantity: float          # tokens received (buy) or USD received (sell)
    effective_price: float   # USD per token, including slippage and fees
    slippage_pct: float      # % away from pre-trade spot (positive = worse for trader)


def _reserves_from_spot(spot_price_usd: float, sol_usd: float) -> tuple[int, int]:
    """Given current spot price USD/token, back out (v_sol_lamports, v_tokens_base).
    
    spot_price = (v_sol/LAMPORTS_PER_SOL) / (v_tokens/10^decimals) * sol_usd
               = (v_sol/v_tokens) * sol_usd * (10^decimals / LAMPORTS_PER_SOL)
               = (v_sol/v_tokens) * sol_usd * 10^(decimals - 9)
    
    For pump.fun decimals=6, that scaling is 10^-3.
    
    Combined with k = v_sol × v_tokens:
      ratio = v_sol/v_tokens = spot_price / (sol_usd × 10^-3)
      v_sol = sqrt(k × ratio)
      v_tokens = sqrt(k / ratio)
    """
    decimal_scale = 10 ** (PUMPFUN_TOKEN_DECIMALS - 9)  # = 1e-3 for pump.fun
    ratio = spot_price_usd / (sol_usd * decimal_scale)
    v_sol = int(math.sqrt(PUMPFUN_K * ratio))
    v_tokens = int(math.sqrt(PUMPFUN_K / ratio))
    return (v_sol, v_tokens)


def simulate_buy(spot_price_usd: float, usd_in: float, sol_usd: float) -> FillResult:
    """Simulate buying with usd_in USD at given spot.
    
    Returns FillResult with:
      quantity        - tokens received (after fee and price impact)
      effective_price - actual USD/token paid (includes fee and impact)
      slippage_pct    - how much worse than spot you got (positive %)
    
    On any degenerate input (zero spot, zero size, negative ratios), returns
    a zero FillResult — caller can handle that as "trade not viable".
    """
    if spot_price_usd <= 0 or usd_in <= 0 or sol_usd <= 0:
        return FillResult(0.0, 0.0, 0.0)
    
    try:
        v_sol, v_tokens = _reserves_from_spot(spot_price_usd, sol_usd)
    except (ValueError, ZeroDivisionError):
        return FillResult(0.0, 0.0, 0.0)
    if v_sol <= 0 or v_tokens <= 0:
        return FillResult(0.0, 0.0, 0.0)
    
    # 1% fee removed before swap math
    usd_after_fee = usd_in * (1 - PUMPFUN_FEE_RATE)
    sol_in = usd_after_fee / sol_usd
    lamports_in = int(sol_in * LAMPORTS_PER_SOL)
    
    # Constant-product swap: new_v_tokens = k / new_v_sol
    new_v_sol = v_sol + lamports_in
    new_v_tokens = (v_sol * v_tokens) // new_v_sol
    tokens_received_base = v_tokens - new_v_tokens
    
    if tokens_received_base <= 0:
        return FillResult(0.0, 0.0, 0.0)
    
    quantity = tokens_received_base / (10 ** PUMPFUN_TOKEN_DECIMALS)
    # Effective price = total $ you spent (including fee) divided by tokens received
    effective_price = usd_in / quantity
    slippage_pct = (effective_price / spot_price_usd - 1) * 100
    
    return FillResult(quantity, effective_price, slippage_pct)


def simulate_sell(spot_price_usd: float, tokens_out: float, sol_usd: float) -> FillResult:
    """Simulate selling tokens_out tokens at given spot.
    
    Returns FillResult with:
      quantity        - USD received (after fee and price impact)
      effective_price - actual USD/token received
      slippage_pct    - how much worse than spot you got (positive % = bigger loss)
    """
    if spot_price_usd <= 0 or tokens_out <= 0 or sol_usd <= 0:
        return FillResult(0.0, 0.0, 0.0)
    
    try:
        v_sol, v_tokens = _reserves_from_spot(spot_price_usd, sol_usd)
    except (ValueError, ZeroDivisionError):
        return FillResult(0.0, 0.0, 0.0)
    if v_sol <= 0 or v_tokens <= 0:
        return FillResult(0.0, 0.0, 0.0)
    
    tokens_out_base = int(tokens_out * (10 ** PUMPFUN_TOKEN_DECIMALS))
    
    # Constant-product swap going the other way
    new_v_tokens = v_tokens + tokens_out_base
    new_v_sol = (v_sol * v_tokens) // new_v_tokens
    lamports_out = v_sol - new_v_sol
    
    if lamports_out <= 0:
        return FillResult(0.0, 0.0, 0.0)
    
    sol_out = lamports_out / LAMPORTS_PER_SOL
    usd_gross = sol_out * sol_usd
    usd_net = usd_gross * (1 - PUMPFUN_FEE_RATE)
    
    effective_price = usd_net / tokens_out
    slippage_pct = (1 - effective_price / spot_price_usd) * 100  # positive = bad
    
    return FillResult(usd_net, effective_price, slippage_pct)
