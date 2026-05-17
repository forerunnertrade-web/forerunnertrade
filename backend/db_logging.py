"""
Buffered system-log writer.

Plugs into Python's standard logging module as a Handler. Captures log
records into an in-memory buffer; a separate async task flushes the buffer
to Supabase every few seconds.

Design choices:

  Batching: writes go in batches of up to BATCH_SIZE records, flushed at
    intervals of FLUSH_INTERVAL_SECONDS. This means up to ~30s of log
    loss on container kill, which is acceptable for a debug stream.

  Selectivity: only logs from our application modules are captured. We
    drop noise from httpx, uvicorn, asyncio. The category dimension lets
    you filter later.

  Bounded buffer: if writes fall behind, we cap the buffer at BUFFER_CAP
    and start dropping. Dropped count is logged itself (one warning per
    drop event) so you know it happened.

  Graceful degrade: if Supabase isn't configured, the handler is a no-op.
    All logging still goes to stderr via the existing console handler.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import db

log = logging.getLogger(__name__)

BATCH_SIZE = 50
FLUSH_INTERVAL_SECONDS = 5.0
BUFFER_CAP = 1000

# Categories — derived from logger name. Anything not matching here is
# filtered out (we don't want urllib3, websockets, etc. spamming the
# system_logs table).
_CATEGORY_PATTERNS = {
    "main": "engine",
    "engine": "engine",
    "scanners": "scanner",
    "auditor": "audit",
    "auditor_evm": "audit",
    "auditor_solana": "audit",
    "enricher": "audit",
    "enricher_solana": "audit",
    "dexscreener": "dexscreener",
    "alerts": "sys",
    "db": "sys",
    "auth": "sys",
}


def _classify(name: str) -> Optional[str]:
    """Map a logger name to a category, or None to drop the record."""
    # Logger names look like "scanners.ethereum" — we match on the first
    # component.
    root = name.split(".")[0]
    return _CATEGORY_PATTERNS.get(root)


class _DBLogHandler(logging.Handler):
    """Buffers records; doesn't actually do I/O — that's the flusher's job."""

    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        # Don't include DEBUG; would explode the table
        self._buffer: list[dict] = []
        self._dropped_since_last_warn = 0

    def emit(self, record: logging.LogRecord) -> None:
        category = _classify(record.name)
        if category is None:
            return
        try:
            message = self.format(record)
        except Exception:
            return

        if len(self._buffer) >= BUFFER_CAP:
            self._dropped_since_last_warn += 1
            return

        self._buffer.append({
            "level": record.levelname.lower(),
            "category": category,
            "message": message[:1000],  # truncate insanely long log lines
        })

    def drain(self) -> tuple[list[dict], int]:
        """Atomically grab the buffered records and reset. Called by flusher."""
        out = self._buffer
        dropped = self._dropped_since_last_warn
        self._buffer = []
        self._dropped_since_last_warn = 0
        return out, dropped


_handler: Optional[_DBLogHandler] = None
_flusher_task: Optional[asyncio.Task] = None


def install() -> None:
    """Attach the handler to the root logger. Call once at startup."""
    global _handler
    if _handler is not None:
        return
    _handler = _DBLogHandler()
    # Simple format — we already have timestamps via the DB column
    _handler.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger().addHandler(_handler)


async def start_flusher() -> None:
    """Spawn the background task that drains the buffer to Supabase."""
    global _flusher_task
    if _flusher_task is not None:
        return
    if not db.is_configured():
        log.info("system_logs persistence: disabled (Supabase not configured)")
        return
    _flusher_task = asyncio.create_task(_flush_loop(), name="db_log_flusher")
    log.info("system_logs persistence: enabled (batch=%d, flush=%.1fs)",
             BATCH_SIZE, FLUSH_INTERVAL_SECONDS)


async def stop_flusher() -> None:
    """Cancel the flusher; flush once more before returning so we don't
    lose the last batch on shutdown."""
    global _flusher_task
    if _flusher_task is None:
        return
    _flusher_task.cancel()
    try:
        await _flusher_task
    except (asyncio.CancelledError, Exception):
        pass
    _flusher_task = None
    await _flush_once()


async def _flush_loop() -> None:
    try:
        while True:
            await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
            await _flush_once()
    except asyncio.CancelledError:
        raise


async def _flush_once() -> None:
    if _handler is None:
        return
    records, dropped = _handler.drain()
    if dropped:
        # One warning per drop event, fed back into the buffer via the
        # logger — the next flush will write it.
        log.warning("system_logs buffer overflowed; %d records dropped", dropped)
    if not records:
        return
    # Write in chunks of BATCH_SIZE so a single huge backlog doesn't
    # become a single huge HTTP request.
    for i in range(0, len(records), BATCH_SIZE):
        chunk = records[i:i + BATCH_SIZE]
        try:
            await db.insert_log_batch(chunk)
        except Exception:
            # Don't log this — would create infinite recursion through the handler
            pass
