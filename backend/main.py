"""
Orchestrator: spin up scanners + API, route every PoolEvent through audit,
broadcast a first 'pool_event' immediately, then fire-and-forget an enrichment
task that broadcasts a second 'pool_metrics' update ~30s later.

Two-phase design rationale:
  - Latency: the dashboard sees new pools instantly (audit only takes ~1s).
  - Filtering: the strategy filter on the dashboard can ignore pre-metrics
    signals if it wants high-confidence trades, or act early if it wants
    sniper-style entries.
  - Throughput: a slow enricher RPC can't block the scanner pipeline.
"""
from __future__ import annotations

# Load .env BEFORE any other imports. Several modules (auth.py, config.py,
# etc.) read env vars at import time, so dotenv must be applied first or
# those reads see the unconfigured environment. config.py also calls
# load_dotenv() but by then it's too late for early-binding consumers.
from dotenv import load_dotenv
load_dotenv()

import asyncio
import logging
import os
import signal

import uvicorn

from alerts import app as fastapi_app, broadcast_metrics, broadcast_trending, dispatch_alert
from auditor import quick_audit
from config import load_config
from scanners.base import PoolEvent
from scanners.ethereum import EthereumScanner
from scanners.solana import SolanaScanner
from scanners.sui import SuiScanner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("main")

# API_HOST default: 0.0.0.0 binds to all interfaces, which is what Railway,
# Docker, and most PaaS environments need to reach the process from outside
# the container. For local development this is also fine — your firewall
# is the gatekeeper. Override to "127.0.0.1" via env var if you specifically
# want loopback-only binding on a multi-user machine.
API_HOST = os.getenv("API_HOST", "0.0.0.0")
# Railway and most PaaS providers inject $PORT for the container to bind.
# Fall back to API_PORT (our local convention) then 8080 (the default).
API_PORT = int(os.getenv("PORT") or os.getenv("API_PORT") or "8080")
ENRICH_ENABLED = os.getenv("ENRICH_ENABLED", "true").lower() == "true"
ENRICH_WINDOW = int(os.getenv("ENRICH_WINDOW_SECONDS", "30"))
DEXSCREENER_ENABLED = os.getenv("DEXSCREENER_ENABLED", "true").lower() == "true"
DEXSCREENER_INTERVAL = int(os.getenv("DEXSCREENER_INTERVAL_SECONDS", "60"))
DEXSCREENER_CHAINS = os.getenv("DEXSCREENER_CHAINS", "ETH,SOL,BASE,BSC,POLY,ARB").split(",")

# Bound the number of concurrent enrichment tasks so a launch storm can't
# saturate our RPC budget. Each enrichment makes ~3 calls.
_enrich_sem = asyncio.Semaphore(5)


async def on_trending(token) -> None:
    """Hand-off from the DEXScreener poller to the WS broadcast."""
    log.info(
        "Trending: %s/%s sym=%s liq=$%s vol=$%s",
        token.chain, token.address[:10],
        token.symbol or "?",
        f"{token.liq_usd:,.0f}" if token.liq_usd else "?",
        f"{token.volume_24h:,.0f}" if token.volume_24h else "?",
    )
    await broadcast_trending(token)


async def on_pool(event: PoolEvent) -> None:
    log.info(
        "New pool: chain=%s dex=%s pool=%s pair=%s/%s",
        event.chain, event.dex, event.pool_address[:16],
        event.token0[:10], event.token1[:10],
    )
    audit = await quick_audit(event)
    if not audit.passed:
        log.info("Filtered (score=%d, %s): %s",
                 audit.score, audit.reason, event.pool_address[:16])
        return

    # Phase 1: immediate broadcast + chat alert.
    await dispatch_alert(event, audit)

    # Phase 2: fire-and-forget enrichment, dispatched per-chain.
    if not ENRICH_ENABLED:
        return

    if event.chain == "ethereum":
        rpc_url = os.getenv("ETH_HTTP_URL", "")
        if rpc_url:
            asyncio.create_task(_enrich_and_broadcast_evm(rpc_url, event))
        else:
            log.debug("ETH enrichment skipped: ETH_HTTP_URL not set")

    elif event.chain == "solana":
        rpc_url = os.getenv("SOL_HTTP_URL", "")
        if rpc_url:
            asyncio.create_task(_enrich_and_broadcast_solana(rpc_url, event))
        else:
            log.debug("SOL enrichment skipped: SOL_HTTP_URL not set")
            # Still mark ready (audit-only) so signal isn't stuck pending.
            from scanners.base import LaunchMetrics
            empty = LaunchMetrics(sample_seconds=0, error="solana RPC not configured")
            asyncio.create_task(broadcast_metrics(event, empty))

    elif event.chain == "sui":
        # SUI enrichment not yet implemented. Broadcast empty so signals are
        # actionable on audit score alone.
        from scanners.base import LaunchMetrics
        empty = LaunchMetrics(sample_seconds=0, error="sui enrichment not implemented")
        asyncio.create_task(broadcast_metrics(event, empty))


async def _enrich_and_broadcast_evm(rpc_url: str, event: PoolEvent) -> None:
    """Background EVM enrichment. Throttled by a semaphore."""
    async with _enrich_sem:
        try:
            from enricher import enrich_pool
            metrics = await enrich_pool(rpc_url, event, sample_seconds=ENRICH_WINDOW)
        except Exception as e:
            log.warning("EVM enrichment crashed for %s: %s", event.pool_address[:10], e)
            return

        _log_metrics("EVM", event, metrics)
        await broadcast_metrics(event, metrics)
        await _feed_engine(event, metrics)


async def _enrich_and_broadcast_solana(rpc_url: str, event: PoolEvent) -> None:
    """Background Solana enrichment. Throttled by the same semaphore."""
    async with _enrich_sem:
        try:
            from enricher_solana import enrich_solana_pool
            metrics = await enrich_solana_pool(rpc_url, event, sample_seconds=ENRICH_WINDOW)
        except Exception as e:
            log.warning("SOL enrichment crashed for %s: %s", event.pool_address[:10], e)
            return

        _log_metrics("SOL", event, metrics)
        await broadcast_metrics(event, metrics)
        await _feed_engine(event, metrics)


async def _feed_engine(event: PoolEvent, metrics) -> None:
    """Hand off a fully-enriched signal to the simulation engine.
    Only fires when the metrics succeed (no error). Audit-only signals
    (SOL/SUI with `error` set) don't have prices to trade off, so they're
    skipped — they still appear in the dashboard via the WS broadcast."""
    if metrics.error:
        return
    from engine import engine
    # Reach into the audit cache for this event's score. The audit ran
    # before enrichment in the on_pool handler — we'd ideally pass it
    # through, but the simplest fix is to re-derive from the event's
    # stored audit_score (set during dispatch). For now, accept a
    # missing score as 100 (already passed AUDIT_MIN_SCORE filter to
    # have gotten here).
    audit_score = getattr(event, "audit_score", 100) or 100
    await engine.add_signal({
        "chain": event.chain.upper()[:4],
        "symbol": getattr(event, "symbol", None) or event.pool_address[:8],
        "address": getattr(event, "token0", None),
        "pool_address": event.pool_address,
        "audit_score": audit_score,
        "liq_usd": metrics.final_liq_usd or metrics.initial_liq_usd,
        "unique_buyers": metrics.unique_buyers,
        "price_change_pct": metrics.price_change_pct,
        "final_price_usd": metrics.final_price_usd,
        "breakout_triggered": metrics.breakout_triggered,
    })


def _log_metrics(tag: str, event: PoolEvent, metrics) -> None:
    if metrics.error:
        log.info("%s enrich %s: %s", tag, event.pool_address[:10], metrics.error)
        return
    log.info(
        "%s enrich %s: liq=$%s buyers=%s Δ=%s%% price=$%s",
        tag,
        event.pool_address[:10],
        f"{metrics.final_liq_usd:,.0f}" if metrics.final_liq_usd else "?",
        metrics.unique_buyers if metrics.unique_buyers is not None else "?",
        f"{metrics.price_change_pct:+.1f}" if metrics.price_change_pct is not None else "?",
        f"{metrics.final_price_usd:.6g}" if metrics.final_price_usd else "?",
    )


async def run_api():
    config = uvicorn.Config(
        fastapi_app,
        host=API_HOST,
        port=API_PORT,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(config)
    await server.serve()


async def main() -> None:
    cfg = load_config()
    scanners = []

    eth = cfg.chains["ethereum"]
    if eth.enabled:
        scanners.append(EthereumScanner(on_pool, eth.rpc_ws, eth.factories))

    sol = cfg.chains["solana"]
    if sol.enabled:
        scanners.append(SolanaScanner(on_pool, sol.rpc_ws, sol.rpc_http, sol.factories))

    sui = cfg.chains["sui"]
    if sui.enabled:
        scanners.append(SuiScanner(on_pool, sui.rpc_ws, sui.factories))

    if not scanners:
        log.warning("No scanners enabled — running API only.")

    tasks = [asyncio.create_task(s.run(), name=s.name) for s in scanners]
    tasks.append(asyncio.create_task(run_api(), name="api"))

    if DEXSCREENER_ENABLED:
        from dexscreener import run_polling_loop
        chains = [c.strip() for c in DEXSCREENER_CHAINS if c.strip()]
        tasks.append(asyncio.create_task(
            run_polling_loop(on_trending, lambda: chains, interval=DEXSCREENER_INTERVAL),
            name="dexscreener",
        ))

    log.info("Started %d scanner(s) + API on %s:%d",
             len(scanners), API_HOST, API_PORT)
    log.info("Enrichment: %s (window=%ds)",
             "enabled" if ENRICH_ENABLED else "disabled", ENRICH_WINDOW)
    log.info("DEXScreener: %s (interval=%ds, chains=%s)",
             "enabled" if DEXSCREENER_ENABLED else "disabled",
             DEXSCREENER_INTERVAL,
             ",".join(DEXSCREENER_CHAINS))
    log.info("Dashboard WS: ws://%s:%d/events", API_HOST, API_PORT)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    await stop.wait()
    log.info("Shutdown signal received, stopping tasks")
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
