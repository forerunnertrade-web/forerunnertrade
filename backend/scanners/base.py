"""Shared types: a chain-agnostic PoolEvent and the BaseScanner contract.

Each per-chain scanner subclasses BaseScanner, runs its own WebSocket loop, and
calls the supplied async `handler(event)` whenever it detects a pool creation.

The `metrics` field is populated *after* initial detection, by the enricher.
It's None until the enricher has run a sample window. Strategy filters on the
dashboard side should treat None as "not yet ready" and skip those signals.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional


@dataclass
class LaunchMetrics:
    """Real on-chain launch dynamics, measured over a fixed sample window
    after pool creation. None = the enricher couldn't measure it (e.g. RPC
    error, non-EVM chain, pool already gone)."""
    sample_seconds: int = 30
    initial_liq_usd: Optional[float] = None
    final_liq_usd: Optional[float] = None
    buy_count: Optional[int] = None
    sell_count: Optional[int] = None
    unique_buyers: Optional[int] = None
    price_change_pct: Optional[float] = None
    final_price_usd: Optional[float] = None  # used as the live entry price
    price_samples: Optional[list] = None     # ordered oldest-first; for sparkline + breakout
    breakout_triggered: Optional[bool] = None
    breakout_score: Optional[int] = None
    breakout_reason: Optional[str] = None
    error: Optional[str] = None


@dataclass
class PoolEvent:
    chain: str
    dex: str
    pool_address: str
    token0: str
    token1: str
    block_or_slot: int
    tx_hash: str
    deployer: Optional[str] = None
    initial_liquidity_raw: Optional[int] = None
    raw: Optional[Any] = None
    metrics: Optional[LaunchMetrics] = None


PoolHandler = Callable[[PoolEvent], Awaitable[None]]


class BaseScanner(ABC):
    def __init__(self, name: str, handler: PoolHandler):
        self.name = name
        self.handler = handler

    @abstractmethod
    async def run(self) -> None:
        """Run forever; reconnect on transient failures."""
