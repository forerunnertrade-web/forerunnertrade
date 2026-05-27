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

# Price refresh: how often to fetch real spot prices for open positions.
# Every PRICE_REFRESH_TICKS ticks we hit the price oracle; in between we
# hold the last-known mark. 5s is conservative — gives TP/SL ~5s latency
# but keeps RPC calls bounded.
PRICE_REFRESH_TICKS = 5


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
    # Optional fields for live mark-to-market via price_oracle. Default to
    # None for backwards compat with existing rows.
    pool_address: Optional[str] = None
    quote_address: Optional[str] = None

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

        # Live-price cache: filled by an oracle task every PRICE_REFRESH_TICKS.
        # Empty dict means "no fresh prices yet — use drift". Entries
        # auto-expire when the position is closed (we sweep on close).
        self._latest_prices: dict[int, float] = {}
        self._price_refresh_inflight: bool = False

    def _next(self) -> int:
        n = self._next_id
        self._next_id += 1
        return n

    # ─── Hydration ────────────────────────────────────────────────────────────
    async def hydrate(self) -> None:
        """Load persisted state from Supabase on startup. Safe to call
        even if db isn't configured — it'll no-op. Idempotent."""
        import db
        if not db.is_configured():
            log.info("Engine hydration skipped: Supabase not configured")
            return
        log.info("Hydrating engine state from Supabase…")
        try:
            settings = await db.load_settings()
            trades = await db.load_trades(limit=200)
            positions = await db.load_positions()
            equity = await db.load_equity(limit=500)

            async with self._lock:
                if settings:
                    self.start_balance = settings["start_balance"]
                    self.cash = settings["cash"]
                    if settings["params"]:
                        self.params.update(settings["params"])

                # Convert DB rows back to Position dataclasses
                self.positions = {}
                for p in positions:
                    pos = Position(
                        id=p["id"], chain=p["chain"], symbol=p["symbol"],
                        address=p.get("address"), qty=p["qty"],
                        entry_px=p["entry_px"], mark_px=p["mark_px"],
                        bias=p["bias"], tp_pct=p["tp_pct"], sl_pct=p["sl_pct"],
                        opened_at=p["opened_at"],
                        pool_address=p.get("pool_address"),
                        quote_address=p.get("quote_address"),
                    )
                    self.positions[pos.id] = pos
                    # Keep _next_id ahead of all restored IDs
                    if pos.id >= self._next_id:
                        self._next_id = pos.id + 1

                # Same for trades
                self.trades = []
                for t in trades:
                    trade = Trade(
                        id=t["id"], chain=t["chain"], symbol=t["symbol"],
                        address=t.get("address"), qty=t["qty"],
                        entry_px=t["entry_px"], exit_px=t["exit_px"],
                        pnl_usd=t["pnl_usd"], pnl_pct=t["pnl_pct"],
                        reason=t["reason"],
                        opened_at=t["opened_at"], closed_at=t["closed_at"],
                    )
                    self.trades.append(trade)
                    if trade.id >= self._next_id:
                        self._next_id = trade.id + 1

                if equity:
                    self.equity = equity
                    # Resume tick counter from where we left off
                    self.tick_count = equity[-1]["t"]

            log.info(
                "Hydrated: %d positions, %d trades, %d equity points, cash=$%.2f",
                len(self.positions), len(self.trades), len(self.equity), self.cash
            )
        except Exception:
            log.exception("Engine hydration failed; starting fresh")

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
            params_copy = dict(self.params)
            sb = self.start_balance
            cash = self.cash
        # DB writes outside the lock — clear_all + fresh settings row
        import db
        if db.is_configured():
            await db.clear_all()
            await db.save_settings(sb, cash, params_copy)
        log.info("Engine reset")

    async def set_params(self, new_params: dict) -> None:
        """Merge incoming params over current. Frontend sends partial updates."""
        async with self._lock:
            self.params.update(new_params)
            log.info("Params updated: %s", {k: new_params[k] for k in new_params})
            snapshot = (self.start_balance, self.cash, dict(self.params))
        # Persist outside the lock
        await self._save_settings_safe(*snapshot)

    async def set_start_balance(self, amount: float) -> None:
        async with self._lock:
            self.start_balance = max(0.0, float(amount))
            # If we haven't opened any positions yet, sync cash too
            if not self.positions and not self.trades:
                self.cash = self.start_balance
            snapshot = (self.start_balance, self.cash, dict(self.params))
        await self._save_settings_safe(*snapshot)

    async def _save_settings_safe(self, start_balance: float, cash: float, params: dict) -> None:
        """Fire-and-forget settings persist; never raises."""
        import db
        if not db.is_configured():
            return
        try:
            await db.save_settings(start_balance, cash, params)
        except Exception:
            log.exception("save_settings failed")

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

            # ── 1. Mark to market ────────────────────────────────────────────
            # On refresh ticks, pull real spot prices from on-chain AMMs via
            # the price oracle. On other ticks (or when oracle didn't return
            # for a position), apply small random-walk drift so the chart
            # animates and stale positions can still time-out gracefully.
            #
            # The actual oracle fetch happens in a separate task spawned
            # below (outside the lock) — here we just consume whatever the
            # last refresh produced. This keeps the lock-hold time bounded
            # to in-memory work.
            refreshed = self._latest_prices  # dict[pos_id, price] populated by oracle task
            for pos in self.positions.values():
                fresh = refreshed.get(pos.id)
                if fresh is not None:
                    pos.mark_px = max(1e-9, float(fresh))
                else:
                    drift = random.uniform(-0.04, 0.05) + pos.bias
                    pos.mark_px = max(1e-9, pos.mark_px * (1 + drift))

            # Track what changed so we can flush to DB after the lock releases.
            # Trades and positions are the "expensive" writes; equity is
            # throttled inside db.append_equity_point.
            closed_trades: list[Trade] = []
            position_set_dirty = False
            opened_signals_in_tick: list[tuple] = []  # (chain, pool_address, position_id)

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
                # Drop any cached oracle price for this closed position
                self._latest_prices.pop(pos_id, None)
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
                closed_trades.append(trade)
                position_set_dirty = True
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
                            pool_address=sig.get("pool_address"),
                            quote_address=sig.get("quote_address"),
                        )
                        self.positions[pos.id] = pos
                        self.cash -= self.params["positionSizeUsd"]
                        position_set_dirty = True
                        log.info(
                            "OPEN %s %s qty=%.4f @ $%.6g",
                            pos.chain, pos.symbol, qty, entry_px
                        )
                        # Mark the corresponding signal row as acted-on.
                        # The signal's `pool_address` was set when the signal
                        # was queued. chain normalization: signals table
                        # stores internal names ("ethereum") not codes ("ETH"),
                        # so map back via the same helper used in main.py.
                        sig_pool = sig.get("pool_address")
                        if sig_pool:
                            opened_signal = (sig.get("chain", "?"), sig_pool, pos.id)
                            opened_signals_in_tick.append(opened_signal)

            # ── 4. Equity snapshot ──────────────────────────────────────────
            open_value = sum(p.qty * p.mark_px for p in self.positions.values())
            total_equity = self.cash + open_value
            self.equity.append({"t": self.tick_count, "v": total_equity})
            if len(self.equity) > EQUITY_CAP:
                # Drop oldest to keep bounded
                self.equity = self.equity[-EQUITY_CAP:]

            # Snapshots used by post-lock flush
            sb_snap = self.start_balance
            cash_snap = self.cash
            params_snap = dict(self.params)
            tick_t = self.tick_count
            equity_v = total_equity
            positions_snap = [self._position_to_dict(p) for p in self.positions.values()] if position_set_dirty else None
            # Always snapshot for price-oracle quoting — we need a stable
            # view of which positions to quote even if the set didn't change.
            positions_to_quote = [
                self._position_to_dict(p) for p in self.positions.values()
            ]

        # ── 5. Flush to DB OUTSIDE the lock ─────────────────────────────────
        # Fire-and-forget: DB latency must not throttle the tick rate.
        # Each task catches its own exceptions; no failure here can break
        # the loop.
        import db
        if db.is_configured():
            # Trades: one insert per closed trade
            for trade in closed_trades:
                asyncio.create_task(self._safe_insert_trade(trade.to_dict()))
            # Positions: only re-sync if the set changed
            if positions_snap is not None:
                asyncio.create_task(self._safe_sync_positions(positions_snap))
            # Settings: cash changed if we opened or closed
            if closed_trades or position_set_dirty:
                asyncio.create_task(self._save_settings_safe(sb_snap, cash_snap, params_snap))
            # Equity: throttled inside db module to ~once per 5s
            asyncio.create_task(self._safe_append_equity(tick_t, equity_v))
            # Signals: mark acted_on for any position opened this tick
            for chain_code, pool_addr, position_id in opened_signals_in_tick:
                asyncio.create_task(self._safe_mark_signal_acted(
                    chain_code, pool_addr, position_id
                ))

        # ── 6. Price oracle refresh ───────────────────────────────────────────
        # Outside the lock so RPC latency (up to 6s) doesn't block the tick
        # loop. Only spawn if not already in flight (avoids stacked tasks
        # under network slowness).
        if tick_t % PRICE_REFRESH_TICKS == 0 and not self._price_refresh_inflight:
            asyncio.create_task(self._refresh_prices(positions_to_quote=positions_to_quote))

    async def _refresh_prices(self, positions_to_quote: list[dict]) -> None:
        """Fetch live spot prices for the given positions; store in cache.
        Caller (tick loop) populates the cache from this dict next tick."""
        if not positions_to_quote:
            return
        self._price_refresh_inflight = True
        try:
            from price_oracle import fetch_position_prices
            fresh = await fetch_position_prices(positions_to_quote)
            if fresh:
                # Merge (don't replace) so cached entries for unaffected
                # positions linger across refreshes.
                self._latest_prices.update(fresh)
                log.debug("Refreshed %d position prices", len(fresh))
        except Exception:
            log.exception("price refresh failed")
        finally:
            self._price_refresh_inflight = False

    async def _safe_mark_signal_acted(
        self, chain_code: str, pool_address: str, position_id: int
    ) -> None:
        """Map the engine's chain code (ETH/SOL) to the internal name used
        by the signals table (ethereum/solana) and mark the signal."""
        import db
        chain_map = {
            "ETH": "ethereum", "SOL": "solana", "SUI": "sui",
            "BASE": "base", "BSC": "bsc", "POLY": "polygon", "ARB": "arbitrum",
        }
        chain = chain_map.get(chain_code.upper(), chain_code.lower())
        try:
            await db.mark_signal_acted_on(
                chain=chain, pool_address=pool_address, position_id=position_id
            )
        except Exception:
            log.exception("mark_signal_acted_on failed")

    @staticmethod
    def _position_to_dict(p: "Position") -> dict:
        return {
            "id": p.id, "chain": p.chain, "symbol": p.symbol, "address": p.address,
            "qty": p.qty, "entry_px": p.entry_px, "mark_px": p.mark_px,
            "bias": p.bias, "tp_pct": p.tp_pct, "sl_pct": p.sl_pct,
            "opened_at": p.opened_at,
        }

    async def _safe_insert_trade(self, trade_dict: dict) -> None:
        import db
        try:
            await db.insert_trade(trade_dict)
        except Exception:
            log.exception("insert_trade failed")

    async def _safe_sync_positions(self, positions: list[dict]) -> None:
        import db
        try:
            await db.sync_positions(positions)
        except Exception:
            log.exception("sync_positions failed")

    async def _safe_append_equity(self, t: int, v: float) -> None:
        import db
        try:
            await db.append_equity_point(t, v)
        except Exception:
            log.exception("append_equity_point failed")

    def _signal_passes(self, sig: dict) -> bool:
        """Source-aware strategy filter.

        Two signal sources with different available data:
          - "scanner": on-chain new-pool detection. Has 30s enrichment:
            unique_buyers, price_change_pct, breakout. Full filter applies.
          - "dexscreener-trending": tokens already trending. We have
            liquidity, price, and 1h change, but NO 30s buyer count or
            breakout (those are launch-window concepts). Applying the
            buyer/breakout filters here would reject 100% of trending
            signals, since the data is structurally absent.

        So: liquidity + price-change + audit-score apply to BOTH. Buyer
        count and breakout apply ONLY to scanner signals.
        """
        p = self.params
        source = sig.get("source", "scanner")
        is_trending = source == "dexscreener-trending"

        # Audit score — always applies
        if (sig.get("audit_score") or 0) < p["minAuditScore"]:
            return False

        # Liquidity — always applies (both sources carry liq_usd)
        liq = sig.get("liq_usd")
        if p["minLiqUsd"] > 0 and (liq is None or liq < p["minLiqUsd"]):
            return False

        # Price change — always applies (trending uses 1h change as proxy)
        pc = sig.get("price_change_pct")
        if p["minPriceChange"] > 0 and (pc is None or pc < p["minPriceChange"]):
            return False

        # Buyer count — scanner signals only. Trending tokens have no 30s
        # buyer count, so skip this filter for them rather than auto-reject.
        if not is_trending:
            buyers = sig.get("unique_buyers")
            if p["minBuyers"] > 0 and (buyers is None or buyers < p["minBuyers"]):
                return False

        # Breakout — scanner signals only. Same rationale.
        if not is_trending:
            if p["requireBreakout"] and not sig.get("breakout_triggered"):
                return False

        return True


# Module-level singleton; main.py imports this and wires it up.
engine = SimulationEngine()
