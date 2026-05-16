"""
Outbound: Telegram + Discord notifications.
Inbound:  FastAPI /tv-webhook (TradingView), /events (WebSocket for dashboard).

The WebSocket endpoint maintains a set of connected dashboard clients and
broadcasts every PoolEvent + AuditResult pair as JSON. If no dashboard is
connected, broadcasts are no-ops.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import asdict
from typing import Set

import httpx
from fastapi import Depends, FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from auth import auth_is_enabled, require_auth, require_ws_auth

from auditor import AuditResult
from scanners.base import PoolEvent

log = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")


# ─────────────────────────────────────────────────────────────────────────────
# Outbound chat alerts
# ─────────────────────────────────────────────────────────────────────────────
async def _send(text: str) -> None:
    async with httpx.AsyncClient(timeout=10) as client:
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                await client.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
                )
            except Exception:
                log.exception("Telegram dispatch failed")
        if DISCORD_WEBHOOK_URL:
            try:
                await client.post(DISCORD_WEBHOOK_URL, json={"content": text})
            except Exception:
                log.exception("Discord dispatch failed")


def _format_pool(event: PoolEvent, audit: AuditResult) -> str:
    return (
        f"🚨 New {event.chain.upper()} pool ({event.dex})\n"
        f"Pair:  {event.token0[:12]}.. / {event.token1[:12]}..\n"
        f"Pool:  {event.pool_address}\n"
        f"Tx:    {event.tx_hash}\n"
        f"Audit: {audit.reason} (score {audit.score})"
    )


async def dispatch_alert(event: PoolEvent, audit: AuditResult) -> None:
    # 1. broadcast to connected dashboards (real-time)
    await broadcast_pool_event(event, audit)
    # 2. send chat alerts (durable)
    await _send(_format_pool(event, audit))


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket broadcaster — dashboard live feed
# ─────────────────────────────────────────────────────────────────────────────
_clients: Set[WebSocket] = set()
_clients_lock = asyncio.Lock()


async def broadcast_pool_event(event: PoolEvent, audit: AuditResult) -> None:
    """Push a single signal to every connected dashboard. Disconnected
    clients are pruned silently."""
    if not _clients:
        return

    payload = json.dumps({
        "type": "pool_event",
        "event": _serialize_pool_event(event),
        "audit": {
            "passed": audit.passed,
            "score": audit.score,
            "reason": audit.reason,
        },
    })

    await _broadcast(payload)


async def broadcast_metrics(event: PoolEvent, metrics) -> None:
    """Second-phase update: pool metrics arrived after the sample window.
    The dashboard merges this into the existing signal by pool_address."""
    if not _clients:
        return

    payload = json.dumps({
        "type": "pool_metrics",
        "pool_address": event.pool_address,
        "metrics": {
            "sample_seconds": metrics.sample_seconds,
            "initial_liq_usd": metrics.initial_liq_usd,
            "final_liq_usd": metrics.final_liq_usd,
            "buy_count": metrics.buy_count,
            "sell_count": metrics.sell_count,
            "unique_buyers": metrics.unique_buyers,
            "price_change_pct": metrics.price_change_pct,
            "final_price_usd": metrics.final_price_usd,
            "price_samples": metrics.price_samples,
            "breakout_triggered": metrics.breakout_triggered,
            "breakout_score": metrics.breakout_score,
            "breakout_reason": metrics.breakout_reason,
            "error": metrics.error,
        },
    })

    await _broadcast(payload)


async def broadcast_trending(token) -> None:
    """Push a TrendingToken (from DEXScreener poller) to all dashboards.
    Distinct message type so the frontend can show these separately from
    on-chain pool events."""
    if not _clients:
        return
    payload = json.dumps({
        "type": "trending_token",
        "token": {
            "chain": token.chain,
            "chain_id_raw": token.chain_id_raw,
            "address": token.address,
            "quote_address": token.quote_address,
            "symbol": token.symbol,
            "name": token.name,
            "icon_url": token.icon_url,
            "price_usd": token.price_usd,
            "liq_usd": token.liq_usd,
            "volume_24h": token.volume_24h,
            "price_change_24h": token.price_change_24h,
            "price_change_h1": token.price_change_h1,
            "market_cap": token.market_cap,
            "pair_address": token.pair_address,
            "pair_url": token.pair_url,
            "pair_created_at": token.pair_created_at,
            "boost_amount": token.boost_amount,
            "first_seen_at": token.first_seen_at,
        },
    })
    await _broadcast(payload)


async def _broadcast(payload: str) -> None:
    """Shared sender — prunes disconnected clients."""
    async with _clients_lock:
        dead = []
        for ws in _clients:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            _clients.discard(ws)


def _serialize_pool_event(event: PoolEvent) -> dict:
    """Strip the `raw` field — it's debug data and bloats the payload."""
    d = asdict(event)
    d.pop("raw", None)
    return d


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI app
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Forerunner Alerts API")

# CORS origins. Always include localhost for local dev. Append any
# comma-separated origins from CORS_ORIGINS env var for prod (e.g.
# the Vercel URL). Wildcards are not allowed when allow_credentials=True.
_cors_extra = [o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()]
_cors_origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
] + _cors_extra

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/tv-webhook")
async def tradingview_webhook(req: Request, _: None = Depends(require_auth)):
    try:
        payload = await req.json()
    except Exception:
        return {"ok": False, "error": "invalid json"}

    text = (
        f"📈 TradingView signal\n"
        f"Symbol: {payload.get('ticker')}\n"
        f"Action: {payload.get('action')}\n"
        f"Price:  {payload.get('price')}\n"
        f"Vol:    {payload.get('volume')}\n"
        f"RSI:    {payload.get('rsi')}\n"
        f"Note:   {payload.get('message', '')}"
    )
    await _send(text)
    return {"ok": True}


@app.get("/health")
async def health():
    """Open endpoint — Railway healthcheck hits this without auth.
    Returns minimal info so it can't be used to enumerate state."""
    return {"ok": True, "clients": len(_clients), "auth": auth_is_enabled()}


@app.get("/debug/routes")
async def debug_routes():
    """Public diagnostic — lists every registered route. Use this to verify
    that the deployed binary actually has the endpoints you expect. If
    /signals etc. are missing from the output here, you're running an old
    build, no matter what the source code says.

    Example: curl https://your-backend.railway.app/debug/routes
    """
    routes = []
    for r in app.routes:
        path = getattr(r, "path", str(r))
        methods = list(getattr(r, "methods", []) or [])
        routes.append({"path": path, "methods": sorted(methods)})
    routes.sort(key=lambda x: x["path"])
    return {"routes": routes, "count": len(routes)}


# ─────────────────────────────────────────────────────────────────────────────
# Engine control + state endpoints
# ─────────────────────────────────────────────────────────────────────────────
# These are the "remote control" surface the dashboard uses to drive the
# backend simulation engine. Auth-protected so a random visitor can't
# start/stop your bot or read your account state.
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/state")
async def get_state(_: None = Depends(require_auth)):
    """Snapshot of engine state. Dashboard polls this every 1-2 seconds."""
    from engine import engine
    snap = await engine.snapshot()
    return {
        "running": snap.running,
        "start_balance": snap.start_balance,
        "cash": snap.cash,
        "positions": snap.positions,
        "trades": snap.trades,
        "equity": snap.equity,
        "params": snap.params,
        "pending_signals": snap.pending_signals,
        "tick_count": snap.tick_count,
        "last_tick_at": snap.last_tick_at,
    }


@app.post("/params")
async def update_params(req: Request, _: None = Depends(require_auth)):
    """Frontend pushes strategy param updates here. Merges over current."""
    from engine import engine
    try:
        body = await req.json()
    except Exception:
        return {"ok": False, "error": "invalid json"}
    if not isinstance(body, dict):
        return {"ok": False, "error": "body must be an object"}

    # Optional: separate field for start_balance — frontend wants to adjust
    # this independently of strategy params.
    if "start_balance" in body:
        await engine.set_start_balance(body.pop("start_balance"))

    if body:
        await engine.set_params(body)
    return {"ok": True}


@app.post("/control")
async def control(req: Request, _: None = Depends(require_auth)):
    """{"action": "start" | "stop" | "reset"}"""
    from engine import engine
    try:
        body = await req.json()
    except Exception:
        return {"ok": False, "error": "invalid json"}
    action = (body or {}).get("action", "").lower()
    if action == "start":
        await engine.start()
    elif action == "stop":
        await engine.stop()
    elif action == "reset":
        await engine.reset()
    else:
        return {"ok": False, "error": f"unknown action: {action!r}"}
    return {"ok": True, "action": action}


# ─────────────────────────────────────────────────────────────────────────────
# Observability read endpoints — for the dashboard's analysis views
# ─────────────────────────────────────────────────────────────────────────────
# These are paginated reads of the observability tables. The dashboard
# uses them to render "what did we see and what did we do" history.
# Bounded `limit` so a misbehaving client can't ask for the whole table.

@app.get("/signals")
async def get_signals(limit: int = 100, _: None = Depends(require_auth)):
    """Most recent signals (passed and failed). Useful for retrospective
    analysis: 'what did the scanner see at 3am yesterday?'"""
    import db
    if not db.is_configured():
        return {"signals": [], "configured": False}
    limit = max(1, min(500, limit))
    rows = await db.load_recent_signals(limit=limit)
    return {"signals": rows, "configured": True}


@app.get("/trending-history")
async def get_trending_history(limit: int = 100, _: None = Depends(require_auth)):
    """Most recent DEXScreener trending observations."""
    import db
    if not db.is_configured():
        return {"observations": [], "configured": False}
    limit = max(1, min(500, limit))
    rows = await db.load_recent_trending(limit=limit)
    return {"observations": rows, "configured": True}


@app.get("/logs")
async def get_logs(
    limit: int = 200,
    category: str = "",
    _: None = Depends(require_auth),
):
    """Recent system logs. Optional `?category=engine` filter narrows to
    one category. Categories: engine, scanner, audit, dexscreener, sys."""
    import db
    if not db.is_configured():
        return {"logs": [], "configured": False}
    limit = max(1, min(500, limit))
    cat = category.strip() or None
    rows = await db.load_recent_logs(limit=limit, category=cat)
    return {"logs": rows, "configured": True}


@app.websocket("/events")
async def events_socket(ws: WebSocket):
    """Dashboard connects here and receives every audited pool event in real
    time. Server-push only — client messages are ignored."""
    if not await require_ws_auth(ws):
        return  # connection already closed by the auth check
    await ws.accept()
    async with _clients_lock:
        _clients.add(ws)
    log.info("Dashboard connected. clients=%d", len(_clients))
    try:
        await ws.send_text(json.dumps({"type": "hello", "clients": len(_clients)}))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("WS error")
    finally:
        async with _clients_lock:
            _clients.discard(ws)
        log.info("Dashboard disconnected. clients=%d", len(_clients))
