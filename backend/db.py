"""
Backend persistence to Supabase.

We hit the Supabase REST API directly via httpx instead of the supabase-py
library, for three reasons:
  1. httpx is already a dependency
  2. We only need INSERT/UPDATE/UPSERT/DELETE on four tables — the full
     SDK with auth/realtime/storage is overkill
  3. Behavior is explicit: when something goes wrong, you can trace the
     exact HTTP request

Schema requirements: see supabase/schema.sql at the repo root.

Auth model: we use the *service role* key here, not the anon key. The
frontend uses anon key + RLS policies for safety. The backend has no
user to authenticate as; it's a privileged writer. Service role bypasses
RLS, which is appropriate for a trusted server. Never log this key.

Configuration:
  SUPABASE_URL          — your project URL (same as frontend)
  SUPABASE_SERVICE_KEY  — service_role key from Supabase Dashboard → API

If either is missing, persistence is silently disabled. Engine works fine
without it; state just doesn't survive restarts.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Optional

import httpx

log = logging.getLogger(__name__)

URL = os.getenv("SUPABASE_URL", "").rstrip("/")
KEY = os.getenv("SUPABASE_SERVICE_KEY", "").strip()

# Single fixed UUID for now. Matches frontend persistence.js DEFAULT_USER_ID.
# When/if you add real Supabase Auth, swap this for the authenticated user.
DEFAULT_USER_ID = "00000000-0000-0000-0000-000000000001"


def is_configured() -> bool:
    return bool(URL and KEY)


# Per-process HTTP client. AsyncClient is safe to reuse; saves connection
# setup overhead on each call (which is the dominant cost over the LAN to
# Supabase). Created lazily so unconfigured deployments don't try.
_client: Optional[httpx.AsyncClient] = None


async def _get_client() -> Optional[httpx.AsyncClient]:
    global _client
    if not is_configured():
        return None
    if _client is None:
        _client = httpx.AsyncClient(
            base_url=f"{URL}/rest/v1",
            headers={
                "apikey": KEY,
                "Authorization": f"Bearer {KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal",  # don't ship back inserted rows by default
            },
            timeout=8.0,
        )
    return _client


async def close():
    """Call on shutdown. Idempotent."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


# ─────────────────────────────────────────────────────────────────────────────
# Write helpers — all return True on success, False on (logged) failure
# ─────────────────────────────────────────────────────────────────────────────
async def _request(method: str, path: str, **kwargs) -> Optional[httpx.Response]:
    c = await _get_client()
    if c is None:
        return None
    try:
        resp = await c.request(method, path, **kwargs)
        if resp.status_code >= 400:
            # Log body for debugging but truncate aggressively — Supabase
            # errors can include schema dumps.
            body_preview = resp.text[:200] if resp.text else ""
            log.warning(
                "Supabase %s %s → %d: %s",
                method, path, resp.status_code, body_preview
            )
            return None
        return resp
    except httpx.HTTPError as e:
        log.warning("Supabase %s %s failed: %s", method, path, e)
        return None
    except Exception:
        log.exception("Supabase %s %s unexpected error", method, path)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Settings — single row, upserted on every change
# ─────────────────────────────────────────────────────────────────────────────
async def save_settings(start_balance: float, cash: float, params: dict) -> bool:
    row = {
        "user_id": DEFAULT_USER_ID,
        "start_balance": start_balance,
        "cash": cash,
        "params": params,
    }
    resp = await _request(
        "POST", "/settings",
        json=row,
        headers={"Prefer": "resolution=merge-duplicates"},
    )
    return resp is not None


async def load_settings() -> Optional[dict]:
    """Returns {start_balance, cash, params} or None if no row / unconfigured."""
    resp = await _request(
        "GET", "/settings",
        params={"user_id": f"eq.{DEFAULT_USER_ID}", "select": "*"},
    )
    if resp is None:
        return None
    rows = resp.json()
    if not rows:
        return None
    r = rows[0]
    return {
        "start_balance": float(r["start_balance"]),
        "cash": float(r["cash"]),
        "params": r.get("params") or {},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Trades — append-only, upserted by (user_id, client_id) for idempotency
# ─────────────────────────────────────────────────────────────────────────────
async def insert_trade(trade: dict) -> bool:
    """Insert one closed trade. `trade` is the dict form from engine.Trade.to_dict()."""
    row = {
        "user_id": DEFAULT_USER_ID,
        "client_id": trade["id"],
        "chain": trade["chain"],
        "symbol": trade["symbol"],
        "address": trade.get("address"),
        "qty": trade["qty"],
        "entry_px": trade["entry_px"],
        "exit_px": trade["exit_px"],
        "pnl_usd": trade["pnl_usd"],
        "pnl_pct": trade["pnl_pct"],
        "reason": trade["reason"],
        # opened_at/closed_at are epoch ms — convert to ISO timestamps for tz column
        "opened_at": _epoch_ms_to_iso(trade["opened_at"]),
        "closed_at": _epoch_ms_to_iso(trade["closed_at"]),
    }
    resp = await _request(
        "POST", "/trades",
        json=row,
        # ignore_duplicates avoids 409 on retry. The unique index on
        # (user_id, client_id) is what makes this idempotent.
        headers={"Prefer": "resolution=ignore-duplicates"},
    )
    return resp is not None


async def load_trades(limit: int = 200) -> list[dict]:
    """Most recent trades first."""
    resp = await _request(
        "GET", "/trades",
        params={
            "user_id": f"eq.{DEFAULT_USER_ID}",
            "select": "*",
            "order": "closed_at.desc",
            "limit": str(limit),
        },
    )
    if resp is None:
        return []
    return [_trade_row_to_engine_shape(r) for r in resp.json()]


def _trade_row_to_engine_shape(r: dict) -> dict:
    """Shape DB row back to engine.Trade dict form."""
    return {
        "id": int(r["client_id"]),
        "chain": r["chain"],
        "symbol": r["symbol"],
        "address": r.get("address"),
        "qty": float(r["qty"]),
        "entry_px": float(r["entry_px"]),
        "exit_px": float(r["exit_px"]),
        "pnl_usd": float(r["pnl_usd"]),
        "pnl_pct": float(r["pnl_pct"]),
        "reason": r["reason"],
        "opened_at": _iso_to_epoch_ms(r["opened_at"]),
        "closed_at": _iso_to_epoch_ms(r["closed_at"]),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Positions — current open state; full sync (replace set on each call)
# ─────────────────────────────────────────────────────────────────────────────
async def sync_positions(positions: list[dict]) -> bool:
    """Replace the open-positions set with the given list. Two SQL calls:
    one DELETE for anything not in the keep set, one UPSERT for everything."""
    c = await _get_client()
    if c is None:
        return False

    keep_ids = [p["id"] for p in positions]

    # Delete positions not in the current set. Note that PostgREST uses
    # `not.in.(1,2,3)` syntax in URL filters.
    if not keep_ids:
        # Delete all positions for this user
        resp = await _request(
            "DELETE", "/positions",
            params={"user_id": f"eq.{DEFAULT_USER_ID}"},
        )
    else:
        ids_csv = ",".join(str(i) for i in keep_ids)
        resp = await _request(
            "DELETE", "/positions",
            params={
                "user_id": f"eq.{DEFAULT_USER_ID}",
                "client_id": f"not.in.({ids_csv})",
            },
        )
    if resp is None:
        return False

    if not positions:
        return True

    rows = [{
        "user_id": DEFAULT_USER_ID,
        "client_id": p["id"],
        "chain": p["chain"],
        "symbol": p["symbol"],
        "address": p.get("address"),
        "qty": p["qty"],
        "entry_px": p["entry_px"],
        "mark_px": p["mark_px"],
        "bias": p.get("bias", 0.0),
        "tp_pct": p["tp_pct"],
        "sl_pct": p["sl_pct"],
        "opened_at": _epoch_ms_to_iso(p["opened_at"]),
        "pool_address": p.get("pool_address"),
        "quote_address": p.get("quote_address"),
        "dex": p.get("dex"),
    } for p in positions]

    resp = await _request(
        "POST", "/positions",
        json=rows,
        params={"on_conflict": "user_id,client_id"},
        headers={"Prefer": "resolution=merge-duplicates"},
    )
    return resp is not None


async def load_positions() -> list[dict]:
    resp = await _request(
        "GET", "/positions",
        params={
            "user_id": f"eq.{DEFAULT_USER_ID}",
            "select": "*",
            "order": "opened_at.desc",
        },
    )
    if resp is None:
        return []
    return [_position_row_to_engine_shape(r) for r in resp.json()]


async def load_seen_devs(limit: int = 20000) -> set[str]:
    """Return the set of all pump.fun deployer addresses we've ever observed.

    Used by the engine to hydrate the skipRepeatDev filter on startup so
    that Railway redeploys don't reset the seen-set. The default limit of
    20k is generous — at our rate of ~5k pump.fun signals per day, this
    holds about 4 days of history, which is plenty for the filter to
    behave correctly after a deploy.

    Returns an empty set if Supabase isn't configured or the query fails.
    The engine treats an empty seen-set as "fresh start" which is the safe
    default — worst case is the filter is too lenient for the first hour.
    """
    resp = await _request(
        "GET", "/signals",
        params={
            "user_id": f"eq.{DEFAULT_USER_ID}",
            "source": "eq.pumpfun",
            "dev_address": "not.is.null",
            "select": "dev_address",
            "order": "detected_at.desc",
            "limit": str(limit),
        },
    )
    if resp is None:
        return set()
    return {row["dev_address"] for row in resp.json() if row.get("dev_address")}


def _position_row_to_engine_shape(r: dict) -> dict:
    return {
        "id": int(r["client_id"]),
        "chain": r["chain"],
        "symbol": r["symbol"],
        "address": r.get("address"),
        "qty": float(r["qty"]),
        "entry_px": float(r["entry_px"]),
        "mark_px": float(r["mark_px"]),
        "bias": float(r.get("bias") or 0.0),
        "tp_pct": float(r["tp_pct"]),
        "sl_pct": float(r["sl_pct"]),
        "opened_at": _iso_to_epoch_ms(r["opened_at"]),
        "pool_address": r.get("pool_address"),
        "quote_address": r.get("quote_address"),
        "dex": r.get("dex"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Equity — time series; throttled at the caller level (engine.py)
# ─────────────────────────────────────────────────────────────────────────────
_last_equity_write_at = 0.0
EQUITY_WRITE_INTERVAL_SECONDS = 5.0


async def append_equity_point(t: int, v: float) -> bool:
    """Append one equity point. Throttled — at most one write per 5 seconds
    regardless of how often called. Returns True on success or throttled-skip;
    False only on persistence error."""
    global _last_equity_write_at
    now = time.time()
    if now - _last_equity_write_at < EQUITY_WRITE_INTERVAL_SECONDS:
        return True  # skipped, not failed
    _last_equity_write_at = now

    resp = await _request(
        "POST", "/equity",
        json={"user_id": DEFAULT_USER_ID, "t": t, "v": v},
    )
    return resp is not None


async def load_equity(limit: int = 500) -> list[dict]:
    """Most recent N points, returned in chronological order (oldest first)
    so the chart can append cleanly."""
    resp = await _request(
        "GET", "/equity",
        params={
            "user_id": f"eq.{DEFAULT_USER_ID}",
            "select": "t,v",
            "order": "t.desc",
            "limit": str(limit),
        },
    )
    if resp is None:
        return []
    rows = resp.json()
    rows.reverse()  # oldest first for chart
    return [{"t": int(r["t"]), "v": float(r["v"])} for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# Reset — wipe everything for the user
# ─────────────────────────────────────────────────────────────────────────────
async def clear_all() -> bool:
    """Used by /control reset. Wipes trades, positions, equity. Settings
    is left intact; the engine's reset() will write a fresh row."""
    if not is_configured():
        return True

    ok = True
    for table in ("equity", "positions", "trades"):
        resp = await _request(
            "DELETE", f"/{table}",
            params={"user_id": f"eq.{DEFAULT_USER_ID}"},
        )
        ok = ok and (resp is not None)
    return ok


# ─────────────────────────────────────────────────────────────────────────────
# Time conversion helpers
# ─────────────────────────────────────────────────────────────────────────────
def _epoch_ms_to_iso(ms: float) -> str:
    """1715539200000.0 → '2024-05-12T15:20:00.000+00:00'"""
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()


def _iso_to_epoch_ms(iso: str) -> float:
    """Reverse of _epoch_ms_to_iso; handles both with and without microseconds."""
    from datetime import datetime
    # PostgREST returns timestamps like "2024-05-12T15:20:00.123456+00:00"
    # or "2024-05-12T15:20:00+00:00". datetime.fromisoformat handles both
    # on Python 3.11+.
    return datetime.fromisoformat(iso).timestamp() * 1000.0


# ─────────────────────────────────────────────────────────────────────────────
# Signals — Phase 1 INSERT on detection, Phase 2 UPDATE on enrichment
# ─────────────────────────────────────────────────────────────────────────────
async def upsert_signal_phase1(
    *, source: str, chain: str, dex: str, pool_address: str,
    token_address: str, quote_address: Optional[str],
    symbol: Optional[str],
    audit_passed: bool, audit_score: int, audit_reason: str,
    # Optional pump.fun-specific fields. Captured at signal time from the
    # PumpPortal payload. Used later for filter analysis (which deployer
    # patterns correlate with profitable vs rugged trades) and eventually
    # to drive a "skip if dev took >X% of supply" filter.
    dev_address: Optional[str] = None,
    dev_initial_buy_sol: Optional[float] = None,
    dev_pct_supply: Optional[float] = None,
) -> bool:
    """Record the audit phase. Idempotent on (user, chain, pool_address) —
    if the same pool is detected twice, the second call is a no-op rather
    than creating a duplicate row."""
    row = {
        "user_id": DEFAULT_USER_ID,
        "source": source,
        "chain": chain,
        "dex": dex,
        "pool_address": pool_address,
        "token_address": token_address,
        "quote_address": quote_address,
        "symbol": symbol,
        "audit_passed": audit_passed,
        "audit_score": audit_score,
        "audit_reason": audit_reason,
    }
    # Only include dev fields when they're populated, so non-pump.fun
    # signals don't carry null pump.fun-specific columns in their POST body.
    if dev_address is not None:
        row["dev_address"] = dev_address
    if dev_initial_buy_sol is not None:
        row["dev_initial_buy_sol"] = dev_initial_buy_sol
    if dev_pct_supply is not None:
        row["dev_pct_supply"] = dev_pct_supply
    resp = await _request(
        "POST", "/signals",
        json=row,
        headers={"Prefer": "resolution=ignore-duplicates"},
    )
    return resp is not None


async def update_signal_phase2(
    *, chain: str, pool_address: str, metrics: dict,
) -> bool:
    """Update an existing signal row with enrichment metrics. Fails silently
    if no matching row exists (could happen if Phase 1 write failed)."""
    row = {
        "initial_liq_usd": metrics.get("initial_liq_usd"),
        "final_liq_usd": metrics.get("final_liq_usd"),
        "buy_count": metrics.get("buy_count"),
        "sell_count": metrics.get("sell_count"),
        "unique_buyers": metrics.get("unique_buyers"),
        "price_change_pct": metrics.get("price_change_pct"),
        "final_price_usd": metrics.get("final_price_usd"),
        "breakout_triggered": metrics.get("breakout_triggered"),
        "breakout_score": metrics.get("breakout_score"),
        "breakout_reason": metrics.get("breakout_reason"),
        "enrich_error": metrics.get("error"),
        "enriched_at": _now_iso(),
    }
    # Strip None values so we don't overwrite Phase 1 data with nulls
    row = {k: v for k, v in row.items() if v is not None}
    if not row:
        return True

    resp = await _request(
        "PATCH", "/signals",
        json=row,
        params={
            "user_id": f"eq.{DEFAULT_USER_ID}",
            "chain": f"eq.{chain}",
            "pool_address": f"eq.{pool_address}",
        },
    )
    return resp is not None


async def mark_signal_acted_on(*, chain: str, pool_address: str, position_id: int) -> bool:
    """Set acted_on=true on a signal when the engine opens a position from it."""
    resp = await _request(
        "PATCH", "/signals",
        json={"acted_on": True, "acted_position_id": position_id},
        params={
            "user_id": f"eq.{DEFAULT_USER_ID}",
            "chain": f"eq.{chain}",
            "pool_address": f"eq.{pool_address}",
        },
    )
    return resp is not None


async def load_recent_signals(limit: int = 100) -> list[dict]:
    """For dashboard 'recent signals' view."""
    resp = await _request(
        "GET", "/signals",
        params={
            "user_id": f"eq.{DEFAULT_USER_ID}",
            "select": "*",
            "order": "detected_at.desc",
            "limit": str(limit),
        },
    )
    if resp is None:
        return []
    try:
        rows = resp.json()
        # Defensive: PostgREST should always return a list, but if config
        # is off we might get an error dict instead. Don't crash the
        # endpoint.
        if not isinstance(rows, list):
            log.warning("signals: unexpected response shape %r", type(rows).__name__)
            return []
        return rows
    except Exception as e:
        log.warning("signals JSON parse failed: %s", e)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Trending observations — append-only, write-time deduped per hour
# ─────────────────────────────────────────────────────────────────────────────
async def insert_trending_observation(token) -> bool:
    """Insert one trending token observation. The schema's unique index on
    (user, chain, address, observation_hour) means PostgREST returns 409
    on duplicate — we use ignore-duplicates so duplicates are silently
    dropped."""
    pair_created = None
    if token.pair_created_at:
        pair_created = _epoch_ms_to_iso(token.pair_created_at)
    row = {
        "user_id": DEFAULT_USER_ID,
        "chain": token.chain,
        "address": token.address,
        "symbol": token.symbol,
        "name": token.name,
        "price_usd": token.price_usd,
        "liq_usd": token.liq_usd,
        "volume_24h": token.volume_24h,
        "price_change_24h": token.price_change_24h,
        "price_change_h1": token.price_change_h1,
        "market_cap": token.market_cap,
        "pair_address": token.pair_address,
        "pair_url": token.pair_url,
        "pair_created_at": pair_created,
        "boost_amount": token.boost_amount,
        # Don't set observation_hour or observed_at — let DB defaults handle them
    }
    resp = await _request(
        "POST", "/trending_observations",
        json=row,
        headers={"Prefer": "resolution=ignore-duplicates"},
    )
    return resp is not None


async def load_recent_trending(limit: int = 100) -> list[dict]:
    resp = await _request(
        "GET", "/trending_observations",
        params={
            "user_id": f"eq.{DEFAULT_USER_ID}",
            "select": "*",
            "order": "observed_at.desc",
            "limit": str(limit),
        },
    )
    if resp is None:
        return []
    try:
        rows = resp.json()
        if not isinstance(rows, list):
            log.warning("trending: unexpected response shape %r", type(rows).__name__)
            return []
        return rows
    except Exception as e:
        log.warning("trending JSON parse failed: %s", e)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# System logs — high write rate, write-batched in the caller
# ─────────────────────────────────────────────────────────────────────────────
async def insert_log_batch(entries: list[dict]) -> bool:
    """Write a batch of log entries in one HTTP call. Caller is responsible
    for batching to avoid hammering the DB. Each entry should have
    'level', 'category', 'message' keys."""
    if not entries:
        return True
    rows = [{
        "user_id": DEFAULT_USER_ID,
        "level": e.get("level", "info"),
        "category": e.get("category", "sys"),
        "message": e.get("message", ""),
    } for e in entries]
    resp = await _request("POST", "/system_logs", json=rows)
    return resp is not None


async def load_recent_logs(limit: int = 200, category: Optional[str] = None) -> list[dict]:
    params = {
        "user_id": f"eq.{DEFAULT_USER_ID}",
        "select": "*",
        "order": "recorded_at.desc",
        "limit": str(limit),
    }
    if category:
        params["category"] = f"eq.{category}"
    resp = await _request("GET", "/system_logs", params=params)
    if resp is None:
        return []
    try:
        rows = resp.json()
        if not isinstance(rows, list):
            log.warning("logs: unexpected response shape %r", type(rows).__name__)
            return []
        return rows
    except Exception as e:
        log.warning("logs JSON parse failed: %s", e)
        return []


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()
