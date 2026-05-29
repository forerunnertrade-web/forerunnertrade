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
from scanners.pumpfun import PumpFunScanner

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

# Trending-as-signal: when true, DEXScreener-trending tokens are routed
# through the same audit pipeline as on-chain new pools, and ones that
# pass the audit are fed into the engine. This is the "opt-in to also
# consider already-created coins" path. Off by default because trending
# tokens have already moved before we see them — paper P&L on these will
# skew worse than on fresh launches.
TRENDING_AS_SIGNAL = os.getenv("TRENDING_AS_SIGNAL", "false").lower() == "true"
# Skip trending tokens whose pool was created more than this many hours
# ago. Older tokens are likely established coins (WBTC, USDC, etc.) that
# we don't want to audit-and-trade. Default 24h.
TRENDING_MAX_AGE_HOURS = int(os.getenv("TRENDING_MAX_AGE_HOURS", "24"))

# Minimum liquidity (USD) required before we'll spend an RPC call to audit
# a trending token. Free-tier Helius rate-limits around 10 req/s; without
# this filter, every trending poll burns dozens of audit RPCs on microcap
# scams. $10k cuts that to ~10-20% of the feed which fits comfortably.
TRENDING_MIN_LIQ_USD = int(os.getenv("TRENDING_MIN_LIQ_USD", "10000"))

# Engine auto-start: when true, the engine begins ticking immediately on
# backend boot rather than waiting for a dashboard /control start click.
# This is the "bot" behavior — Railway redeploys don't pause trading.
ENGINE_AUTOSTART = os.getenv("ENGINE_AUTOSTART", "false").lower() == "true"

# Bound the number of concurrent enrichment tasks so a launch storm can't
# saturate our RPC budget. Each enrichment makes ~3 calls.
_enrich_sem = asyncio.Semaphore(5)


# Tracks (chain, address) pairs we've already pushed to the engine so
# repeated trending updates don't spam the queue. Bounded to ~1000 entries.
_engine_fed_keys: set[tuple[str, str]] = set()


async def on_trending(token) -> None:
    """Hand-off from the DEXScreener poller. Always broadcasts to dashboards.
    If TRENDING_AS_SIGNAL is on, also routes through audit+engine for tokens
    young enough to be plausibly tradeable."""
    log.info(
        "Trending: %s/%s sym=%s liq=$%s vol=$%s",
        token.chain, token.address[:10],
        token.symbol or "?",
        f"{token.liq_usd:,.0f}" if token.liq_usd else "?",
        f"{token.volume_24h:,.0f}" if token.volume_24h else "?",
    )
    await broadcast_trending(token)

    # Always record the observation — even if trending-as-signal is off,
    # this stream is independently useful for "what was trending on day X".
    # Schema's hourly dedup keeps row count bounded.
    import db
    if db.is_configured():
        asyncio.create_task(db.insert_trending_observation(token))

    if not TRENDING_AS_SIGNAL:
        return

    # Recency gate. Tokens without a pair_created_at are skipped — usually
    # established quote coins (USDC, WETH) or DEXScreener indexing lag.
    if token.pair_created_at is None:
        return
    import time
    age_hours = (time.time() * 1000 - token.pair_created_at) / 1000 / 3600
    if age_hours > TRENDING_MAX_AGE_HOURS:
        return

    # Liquidity gate. Most trending tokens are microcap rugs with <$5k
    # liquidity. Auditing them all burns through RPC quota and produces
    # uninteresting signals. Skip below the threshold.
    if not token.liq_usd or token.liq_usd < TRENDING_MIN_LIQ_USD:
        return

    # Dedup: don't re-audit the same token every poll cycle.
    global _engine_fed_keys
    key = (token.chain, token.address.lower())
    if key in _engine_fed_keys:
        return
    _engine_fed_keys.add(key)
    if len(_engine_fed_keys) > 1000:
        # Drop oldest 200 — set doesn't preserve order, so just clear half
        _engine_fed_keys = set(list(_engine_fed_keys)[500:])

    # Synthesize a PoolEvent so the existing audit pipeline can score it.
    # We use the pair_address as pool_address (correct for EVM Uniswap V2
    # pools and approximately right for Raydium — the auditor cares about
    # the token mint/address, not the pool).
    #
    # quote_address is captured during the DEXScreener pair fetch — without
    # it the auditor can't tell which side is the new token vs the
    # established quote (WSOL/USDC/WETH), and fails with
    # "missing token addresses".
    if not token.quote_address:
        # Skip silently — without the quote side we can't audit. This
        # happens occasionally for tokens DEXScreener hasn't fully indexed.
        return

    from scanners.base import PoolEvent
    pseudo_event = PoolEvent(
        chain=_chain_code_to_internal(token.chain),
        dex="dexscreener-trending",
        pool_address=token.pair_address or token.address,
        token0=token.address,
        token1=token.quote_address,
        block_or_slot=0,
        tx_hash="",
    )

    # Run audit. Same flow as on_pool, but condensed — no enrichment task
    # since DEXScreener already gives us the metrics (liq, volume, change).
    try:
        audit = await quick_audit(pseudo_event)
    except Exception:
        log.exception("audit failed for trending %s", token.address[:10])
        return

    # Record the signal regardless of audit pass/fail — failed audits on
    # trending tokens are the most interesting data of all ("DEXScreener
    # surfaced this scammy token, our audit caught it").
    if db.is_configured():
        asyncio.create_task(db.upsert_signal_phase1(
            source="dexscreener-trending",
            chain=pseudo_event.chain,
            dex="dexscreener-trending",
            pool_address=pseudo_event.pool_address,
            token_address=token.address,
            quote_address=token.quote_address,
            symbol=token.symbol,
            audit_passed=audit.passed,
            audit_score=audit.score,
            audit_reason=audit.reason,
        ))

    if not audit.passed:
        log.info("trending %s %s: audit FAIL — %s",
                 token.chain, token.symbol or "?", audit.reason)
        return

    log.info(
        "trending → engine: %s %s score=%d liq=$%s",
        token.chain, token.symbol or "?", audit.score,
        f"{token.liq_usd:,.0f}" if token.liq_usd else "?",
    )

    # Push directly into the engine queue, bypassing the WS broadcast for
    # signals (the dashboard already saw this via broadcast_trending above).
    from engine import engine
    await engine.add_signal({
        "chain": token.chain,
        "symbol": token.symbol or token.address[:8],
        "address": token.address,
        "pool_address": token.pair_address,
        "audit_score": audit.score,
        "liq_usd": token.liq_usd,
        "unique_buyers": None,        # we don't have 30s buyer counts here
        "price_change_pct": token.price_change_h1,  # 1h move is the closest analog
        "final_price_usd": token.price_usd,
        "breakout_triggered": None,   # not computable from trending data
        "source": "dexscreener-trending",
    })


def _chain_code_to_internal(code: str) -> str:
    """Convert dashboard chain codes (ETH/SOL/BASE) to scanner internal names."""
    return {
        "ETH": "ethereum",
        "SOL": "solana",
        "SUI": "sui",
        "BASE": "base",
        "BSC": "bsc",
        "POLY": "polygon",
        "ARB": "arbitrum",
    }.get(code, code.lower())


async def on_pool(event: PoolEvent) -> None:
    log.info(
        "New pool: chain=%s dex=%s pool=%s pair=%s/%s",
        event.chain, event.dex, event.pool_address[:16],
        event.token0[:10], event.token1[:10],
    )
    audit = await quick_audit(event)

    # Record the signal regardless of audit outcome. Failed signals are
    # equally valuable for analysis — "what got filtered out and why".
    # Fire-and-forget so DB latency doesn't slow the audit pipeline.
    import db
    if db.is_configured():
        asyncio.create_task(db.upsert_signal_phase1(
            source="scanner",
            chain=event.chain,
            dex=event.dex,
            pool_address=event.pool_address,
            token_address=event.token0,
            quote_address=event.token1,
            symbol=None,  # not known at this phase
            audit_passed=audit.passed,
            audit_score=audit.score,
            audit_reason=audit.reason,
        ))

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


# ─────────────────────────────────────────────────────────────────────────────
# pump.fun launch handler
# ─────────────────────────────────────────────────────────────────────────────
async def on_pumpfun_launch(event: PoolEvent) -> None:
    """Handle a pump.fun token launch.

    Different from on_pool because pump.fun tokens are on a bonding curve,
    not an AMM. The "audit" we do for Raydium pools (mint authority revoked,
    freeze authority revoked, etc.) is meaningless here — pump.fun itself
    controls the mint until graduation, then it's burned. So we treat
    pump.fun events as auto-passing audit and skip enrichment; the engine
    decides whether to act based on the strategy filter + the marketCap-based
    entry price.

    Persisted as a "scanner" source signal with source label adjusted so
    downstream analytics can split out pump.fun trades vs Raydium ones.
    """
    raw = event.raw or {}
    symbol = raw.get("symbol") or event.token0[:8]
    market_cap_sol = float(raw.get("marketCapSol") or 0)
    v_sol = float(raw.get("vSolInBondingCurve") or 0)

    log.info(
        "PumpFun launch: %s mint=%s mcap=%.1f SOL liq=%.1f SOL",
        symbol, event.token0[:10], market_cap_sol, v_sol,
    )

    # Convert SOL → USD for the engine's filters. Using a hardcoded SOL/USD
    # is the same approximation we use elsewhere; engine TP/SL is %-based
    # so absolute USD error cancels at entry/exit.
    SOL_USD = 180.0  # rough; in line with price_oracle fallback
    market_cap_usd = market_cap_sol * SOL_USD
    liq_usd = v_sol * SOL_USD

    # Persist phase-1 signal (auto-pass, score 100 for "trust pump.fun")
    import db
    if db.is_configured():
        asyncio.create_task(db.upsert_signal_phase1(
            source="pumpfun",
            chain="solana",
            dex="pumpfun",
            pool_address=event.pool_address,
            token_address=event.token0,
            quote_address=event.token1,
            symbol=symbol,
            audit_passed=True,
            audit_score=100,
            audit_reason="pump.fun bonding curve (audit n/a)",
        ))

    # Derive a per-token price from the bonding-curve numbers.
    # On pump.fun, price ≈ vSolInBondingCurve / vTokensInBondingCurve.
    v_tokens = float(raw.get("vTokensInBondingCurve") or 0)
    if v_tokens > 0 and v_sol > 0:
        price_sol = v_sol / v_tokens
        price_usd = price_sol * SOL_USD
    else:
        # Fall back to deriving from marketCap if curve isn't populated yet
        # (pump.fun reports 0/0 for the first block after launch occasionally).
        price_usd = (market_cap_usd / 1_000_000_000) if market_cap_usd else 0.0

    # Feed the engine. Trending-style payload — no breakout/buyer data,
    # source-aware filter will skip those checks.
    from engine import engine
    await engine.add_signal({
        "chain": "SOL",
        "symbol": symbol,
        "address": event.token0,
        "pool_address": event.pool_address,
        "quote_address": event.token1,
        "audit_score": 100,
        "liq_usd": liq_usd,
        "unique_buyers": None,           # not available pre-launch
        "price_change_pct": None,        # no history yet
        "final_price_usd": price_usd,
        "breakout_triggered": None,
        "source": "pumpfun",
    })


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
        await _persist_signal_phase2(event, metrics)


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
        await _persist_signal_phase2(event, metrics)


async def _persist_signal_phase2(event: PoolEvent, metrics) -> None:
    """Update the signal row with enrichment metrics. Safe no-op when
    Supabase isn't configured or when the row doesn't exist."""
    import db
    if not db.is_configured():
        return
    try:
        # LaunchMetrics has flat attrs; dump them to a dict the db helper
        # knows how to consume.
        metrics_dict = {
            "initial_liq_usd": getattr(metrics, "initial_liq_usd", None),
            "final_liq_usd": getattr(metrics, "final_liq_usd", None),
            "buy_count": getattr(metrics, "buy_count", None),
            "sell_count": getattr(metrics, "sell_count", None),
            "unique_buyers": getattr(metrics, "unique_buyers", None),
            "price_change_pct": getattr(metrics, "price_change_pct", None),
            "final_price_usd": getattr(metrics, "final_price_usd", None),
            "breakout_triggered": getattr(metrics, "breakout_triggered", None),
            "breakout_score": getattr(metrics, "breakout_score", None),
            "breakout_reason": getattr(metrics, "breakout_reason", None),
            "error": getattr(metrics, "error", None),
        }
        await db.update_signal_phase2(
            chain=event.chain,
            pool_address=event.pool_address,
            metrics=metrics_dict,
        )
    except Exception:
        log.exception("phase2 persist failed for %s", event.pool_address[:10])


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
    # Install the DB log handler first thing so startup logs get captured too.
    # The flusher task spawns after the engine hydrates — until then,
    # records sit in the buffer (capped at 1000).
    import db_logging
    db_logging.install()

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

    # Pump.fun: separate path because it doesn't follow the
    # factory-detect-then-audit flow. Bonding-curve tokens need different
    # treatment — see on_pumpfun_launch below.
    if os.getenv("PUMPFUN_ENABLED", "false").lower() == "true":
        scanners.append(PumpFunScanner(on_pumpfun_launch))
        log.info("PumpFun: scanner enabled")
    else:
        log.info("PumpFun: scanner disabled (set PUMPFUN_ENABLED=true)")

    if not scanners:
        log.warning("No scanners enabled — running API only.")

    # Hydrate engine state from Supabase BEFORE starting tasks, so the API
    # serves the right values from the first /state call. Safe no-op if
    # Supabase isn't configured.
    from engine import engine
    import db
    await engine.hydrate()
    await db_logging.start_flusher()
    log.info("DB persistence: %s",
             "enabled" if db.is_configured() else "disabled (state ephemeral)")

    # Auto-start the engine if configured. This is the "bot" behavior —
    # Railway redeploys / container restarts don't pause trading. Without
    # this, the engine sits idle until a dashboard clicks /control start.
    if ENGINE_AUTOSTART:
        await engine.start()
        log.info("Engine auto-started (ENGINE_AUTOSTART=true)")
    else:
        log.info("Engine waiting for /control start (set ENGINE_AUTOSTART=true to skip)")

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
    log.info("Trending-as-signal: %s (max age=%dh)",
             "ENABLED — trending tokens routed through engine" if TRENDING_AS_SIGNAL else "off",
             TRENDING_MAX_AGE_HOURS)
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
    # Stop the log flusher last — gives shutdown logs a chance to persist
    await db_logging.stop_flusher()


if __name__ == "__main__":
    asyncio.run(main())
