"""
Local breakout detector — replaces the TradingView/Pine-Script signal path.

Takes a series of price samples from the enrichment window (typically 10
samples at 3s intervals over 30 seconds) and answers one question: did this
pool exhibit the breakout pattern we want to act on?

Pattern, in plain English:
  - Price in the second half of the window must be higher than the first.
  - The latest sample must exceed the running max of all prior samples by at
    least `breakout_pct` percent — this is the "first candle to break out".
  - At least `min_samples` valid (positive, finite) samples must exist.
  - Optional: momentum slope must be positive.

This is conceptually equivalent to the Pine Script's "RSI > threshold AND
breakout above prior N-bar high" but adapted for the very-short-window case
where classical RSI(14) is undefined. It's a *shape* detector, not a moving
average crossover, because there isn't enough history for crossovers.

What this is NOT:
  - A general-purpose technical indicator. It only works on the first 30s
    of a pool's life because that's when the samples come from.
  - A substitute for the Pine Script if you trade established tokens — the
    Pine Script + TradingView path remains supported for that use case.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass
class BreakoutResult:
    triggered: bool
    score: int            # 0-100, monotonic in confidence
    high: float | None
    low: float | None
    last: float | None
    momentum_pct: float | None  # last-half avg vs first-half avg, %
    breakout_pct: float | None  # latest vs prior-max, %
    sample_count: int
    reason: str


def detect_breakout(
    samples: Iterable[float],
    breakout_pct_threshold: float = 5.0,
    momentum_pct_threshold: float = 3.0,
    min_samples: int = 6,
) -> BreakoutResult:
    """Run breakout detection over a series of price samples.

    samples: iterable of floats, ordered oldest-first. Zero or None values
             are filtered (treated as missing samples).
    Returns BreakoutResult with `triggered=True` only if all conditions pass.
    """
    cleaned = [s for s in samples if s is not None and s > 0 and s == s]  # NaN check

    if len(cleaned) < min_samples:
        return BreakoutResult(
            triggered=False, score=0,
            high=None, low=None, last=None,
            momentum_pct=None, breakout_pct=None,
            sample_count=len(cleaned),
            reason=f"only {len(cleaned)} valid samples (need {min_samples})",
        )

    high = max(cleaned)
    low = min(cleaned)
    last = cleaned[-1]

    # Momentum: how does the second half compare to the first?
    mid = len(cleaned) // 2
    first_half = cleaned[:mid]
    last_half = cleaned[mid:]
    avg_first = sum(first_half) / len(first_half)
    avg_last = sum(last_half) / len(last_half)
    momentum_pct = ((avg_last - avg_first) / avg_first) * 100 if avg_first > 0 else 0.0

    # Breakout: latest vs prior max (excluding the latest sample itself)
    prior_max = max(cleaned[:-1]) if len(cleaned) > 1 else cleaned[0]
    breakout_pct = ((last - prior_max) / prior_max) * 100 if prior_max > 0 else 0.0

    momentum_ok = momentum_pct >= momentum_pct_threshold
    breakout_ok = breakout_pct >= breakout_pct_threshold

    triggered = momentum_ok and breakout_ok

    # Score: blend of how strongly each threshold was beaten.
    # Caps at 100; floor 0. A trigger always scores at least 50.
    if triggered:
        mom_share = min(50, int(50 * momentum_pct / max(momentum_pct_threshold * 3, 1)))
        bo_share  = min(50, int(50 * breakout_pct / max(breakout_pct_threshold * 3, 1)))
        score = mom_share + bo_share
    else:
        # Partial credit so the dashboard can show "almost-triggered" signals
        score = 0
        if momentum_ok: score += 20
        if breakout_ok: score += 20
        if last > avg_first: score += 10  # weak positive movement

    if triggered:
        reason = f"breakout +{breakout_pct:.1f}% momentum +{momentum_pct:.1f}%"
    elif not breakout_ok and not momentum_ok:
        reason = "no breakout (flat or down)"
    elif not breakout_ok:
        reason = f"momentum +{momentum_pct:.1f}% but no breakout above prior max"
    else:
        reason = f"breakout +{breakout_pct:.1f}% but weak momentum"

    return BreakoutResult(
        triggered=triggered,
        score=score,
        high=high,
        low=low,
        last=last,
        momentum_pct=momentum_pct,
        breakout_pct=breakout_pct,
        sample_count=len(cleaned),
        reason=reason,
    )
