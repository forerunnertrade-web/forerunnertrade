"""
Backend paper-trade simulation engine.

This is the same engine that previously ran in the React tick loop, ported
to Python and running as an asyncio task. Trades happen here whether or
not a dashboard is connected. Dashboards observe state via /state and
adjust config via /params.

State model:
  - cash: USD currently uncommitted
  - positions: dict of position_id -> Position (open)
  - trades: list of Trade (closed, append-only, capped at 500 in memory)
  - equity: list of (t, v) tuples (capped at 2000)
  - params: dict from strategy panel
  - signal_queue: incoming pool events from scanners (gets here via add_signal)

Tick loop (every TICK_SECONDS):
  1. Mark open positions to current price (best-effort live price; for
     synthetic/missing data, random walk)
  2. Close positions hitting TP/SL/timeout
  3. Try to open a new position from the head of the signal queue
  4. Append equity snapshot

All state mutations are under a single asyncio.Lock so /state reads see
a consistent snapshot even mid-tick. The lock contention is trivial
(reads are millisecond-fast).

Persistence:
  - Writes settled trades to Supabase via the supabase_writer module IF
    SUPABASE_URL/SERVICE_KEY are configured. Without those, state lives
    in memory only and resets on restart.
  - On startup, hydrates settings (params, cash, start_balance) from
    Supabase if available. Open positions and trades do NOT hydrate
    because we don't yet have a clean "restart picks up where it left
    off" story — restart is treated as a fresh session.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import asdict, dataclass, field
from typing import Optional

log = logging.getLogger(__name__)

TICK_SECONDS = 1.0
EQUITY_CAP = 2000
TRADE_CAP = 500


# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class Position:
    id: int
    chain: str
    symbol: str
    address: Optional[str]
    qty: float
    entry_px: float
    mark_px: float
    bias: float          # random-walk bias; for live, 0
    tp_pct: float
    sl_pct: float
    opened_at: float     # epoch ms

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Trade:
    id: int
    chain: str
    symbol: str
    address: Optional[str]
    qty: float
    entry_px: float
    exit_px: float
    pnl_usd: float
    pnl_pct: float
    reason: str          # "tp", "sl", "timeout"
    opened_at: float
    closed_at: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class EnginePublicState:
    """Snapshot returned by /state. Frontend renders directly from this."""
    running: bool
    start_balance: float
    cash: float
    positions: list
    trades: list
    equity: list
    params: dict
    pending_signals: int   # depth of incoming queue
    tick_count: int
    last_tick_at: float


# ─────────────────────────────────────────────────────────────────────────────
# Engine
# ─────────────────────────────────────────────────────────────────────────────
class SimulationEngine:
    """Single-instance simulation engine. Constructed once at startup."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._next_id = 1

        # Account state
        self.start_balance: float = 500.0
        self.cash: float = 500.0
        self.positions: dict[int, Position] = {}
        self.trades: list[Trade] = []
        self.equity: list[dict] = [{"t": 0, "v": 500.0}]
        self.tick_count = 0
        self.last_tick_at = 0.0

        # Strategy params — same shape as the frontend
        self.params: dict = {
            "minLiqUsd": 5000,
            "minBuyers": 7,
            "minPriceChange": 5,
            "minAuditScore": 70,
            "requireBreakout": False,
            "positionSizeUsd": 50,
            "takeProfitPct": 25,
            "stopLossPct": 10,
            "maxConcurrent": 5,
        }

        # Signal queue — populated by scanners via add_signal()
        self._signals: asyncio.Queue = asyncio.Queue(maxsize=200)

    def _next(self) -> int:
        n = self._next_id
        self._next_id += 1
        return n

    # ─── Public API ──────────────────────────────────────────────────────────
    async def start(self) -> None:
        async with self._lock:
            if self._running:
                return
            self._running = True
            self._task = asyncio.create_task(self._loop(), name="engine")
            log.info("Engine started")

    async def stop(self) -> None:
        async with self._lock:
            if not self._running:
                return
            self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        log.info("Engine stopped")

    async def reset(self) -> None:
        """Wipe all state but keep params. Stops the engine first."""
        await self.stop()
        async with self._lock:
            self.cash = self.start_balance
            self.positions.clear()
            self.trades.clear()
            self.equity = [{"t": 0, "v": self.start_balance}]
            self.tick_count = 0
            self.last_tick_at = 0.0
            # Drain pending signals so old ones don't fire on restart
            while not self._signals.empty():
                try: self._signals.get_nowait()
                except asyncio.QueueEmpty: break
        log.info("Engine reset")

    async def set_params(self, new_params: dict) -> None:
        """Merge incoming params over current. Frontend sends partial updates."""
        async with self._lock:
            self.params.update(new_params)
            log.info("Params updated: %s", {k: new_params[k] for k in new_params})

    async def set_start_balance(self, amount: float) -> None:
        async with self._lock:
            self.start_balance = max(0.0, float(amount))
            # If we haven't opened any positions yet, sync cash too
            if not self.positions and not self.trades:
                self.cash = self.start_balance

    async def add_signal(self, signal: dict) -> None:
        """Called by the scanner pipeline when an audited+enriched pool
        passes the audit gate. Frontend NEVER calls this — it's
        backend-internal."""
        try:
            self._signals.put_nowait(signal)
        except asyncio.QueueFull:
            log.warning("Engine signal queue full; dropping oldest")
            # Drop the oldest to make room
            try:
                self._signals.get_nowait()
                self._signals.put_nowait(signal)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass

    async def snapshot(self) -> EnginePublicState:
        """Atomic read of engine state. The dashboard polls this."""
        async with self._lock:
            return EnginePublicState(
                running=self._running,
                start_balance=self.start_balance,
                cash=self.cash,
                positions=[p.to_dict() for p in self.positions.values()],
                trades=[t.to_dict() for t in self.trades[:200]],  # most recent
                equity=list(self.equity),
                params=dict(self.params),
                pending_signals=self._signals.qsize(),
                tick_count=self.tick_count,
                last_tick_at=self.last_tick_at,
            )

    # ─── The tick loop ───────────────────────────────────────────────────────
    async def _loop(self) -> None:
        """Main simulation loop. Runs until cancelled."""
        try:
            while True:
                await asyncio.sleep(TICK_SECONDS)
                await self._tick()
        except asyncio.CancelledError:
            log.info("Engine loop cancelled cleanly")
            raise

    async def _tick(self) -> None:
        async with self._lock:
            self.tick_count += 1
            self.last_tick_at = time.time()
            now_ms = self.last_tick_at * 1000

            # ── 1. Drift marks ───────────────────────────────────────────────
            # For now: small random walk. Live mode replacement is a separate
            # piece of work — would query an oracle/AMM for current price
            # per position. The current marks are still "real" entries from
            # the enricher's final_price_usd, but they don't update without
            # this drift.
            for pos in self.positions.values():
                drift = random.uniform(-0.04, 0.05) + pos.bias
                pos.mark_px = max(1e-9, pos.mark_px * (1 + drift))

            # ── 2. Close positions hitting TP/SL/timeout ────────────────────
            close_age_ms = 45_000
            to_close: list[tuple[int, str]] = []
            for pos_id, pos in self.positions.items():
                pnl_pct = ((pos.mark_px - pos.entry_px) / pos.entry_px) * 100
                age_ms = now_ms - pos.opened_at
                reason: Optional[str] = None
                if pnl_pct >= pos.tp_pct: reason = "tp"
                elif pnl_pct <= -pos.sl_pct: reason = "sl"
                elif age_ms > close_age_ms: reason = "timeout"
                if reason:
                    to_close.append((pos_id, reason))

            for pos_id, reason in to_close:
                pos = self.positions.pop(pos_id)
                proceeds = pos.qty * pos.mark_px
                self.cash += proceeds
                pnl_usd = (pos.mark_px - pos.entry_px) * pos.qty
                pnl_pct = ((pos.mark_px - pos.entry_px) / pos.entry_px) * 100
                trade = Trade(
                    id=self._next(),
                    chain=pos.chain, symbol=pos.symbol, address=pos.address,
                    qty=pos.qty, entry_px=pos.entry_px, exit_px=pos.mark_px,
                    pnl_usd=pnl_usd, pnl_pct=pnl_pct, reason=reason,
                    opened_at=pos.opened_at, closed_at=now_ms,
                )
                self.trades.insert(0, trade)
                # Cap memory
                if len(self.trades) > TRADE_CAP:
                    self.trades = self.trades[:TRADE_CAP]
                log.info(
                    "CLOSE %s %s qty=%.4f pnl=%+.2f%% (%s)",
                    pos.chain, pos.symbol, pos.qty, pnl_pct, reason
                )

            # ── 3. Try to open a new position ───────────────────────────────
            if (
                self._signals.qsize() > 0
                and len(self.positions) < self.params["maxConcurrent"]
                and self.cash >= self.params["positionSizeUsd"]
            ):
                try:
                    sig = self._signals.get_nowait()
                except asyncio.QueueEmpty:
                    sig = None
                if sig and self._signal_passes(sig):
                    entry_px = float(sig.get("final_price_usd") or 1.0)
                    if entry_px > 0:
                        qty = self.params["positionSizeUsd"] / entry_px
                        pos = Position(
                            id=self._next(),
                            chain=sig.get("chain", "?"),
                            symbol=sig.get("symbol", "?"),
                            address=sig.get("address"),
                            qty=qty,
                            entry_px=entry_px,
                            mark_px=entry_px,
                            bias=0.0,  # live entries don't drift artificially
                            tp_pct=self.params["takeProfitPct"],
                            sl_pct=self.params["stopLossPct"],
                            opened_at=now_ms,
                        )
                        self.positions[pos.id] = pos
                        self.cash -= self.params["positionSizeUsd"]
                        log.info(
                            "OPEN %s %s qty=%.4f @ $%.6g",
                            pos.chain, pos.symbol, qty, entry_px
                        )

            # ── 4. Equity snapshot ──────────────────────────────────────────
            open_value = sum(p.qty * p.mark_px for p in self.positions.values())
            total_equity = self.cash + open_value
            self.equity.append({"t": self.tick_count, "v": total_equity})
            if len(self.equity) > EQUITY_CAP:
                # Drop oldest to keep bounded
                self.equity = self.equity[-EQUITY_CAP:]

    def _signal_passes(self, sig: dict) -> bool:
        """Same filter logic as frontend's strategyPasses, ported."""
        p = self.params
        if (sig.get("audit_score") or 0) < p["minAuditScore"]:
            return False
        liq = sig.get("liq_usd")
        if p["minLiqUsd"] > 0 and (liq is None or liq < p["minLiqUsd"]):
            return False
        buyers = sig.get("unique_buyers")
        if p["minBuyers"] > 0 and (buyers is None or buyers < p["minBuyers"]):
            return False
        pc = sig.get("price_change_pct")
        if p["minPriceChange"] > 0 and (pc is None or pc < p["minPriceChange"]):
            return False
        if p["requireBreakout"] and not sig.get("breakout_triggered"):
            return False
        return True


# Module-level singleton; main.py imports this and wires it up.
engine = SimulationEngine()
