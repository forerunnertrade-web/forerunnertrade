from __future__ import annotations

"""
SUI scanner.

SUI exposes Move events natively. We use suix_subscribeEvent with a Package
filter so the fullnode pushes only events emitted by Cetus / Turbos / etc.

The event types follow the pattern:
    <package>::<module>::CreatePoolEvent
or  <package>::factory::PoolCreated

Field names differ across DEXes — Cetus uses snake_case (coin_type_a),
Turbos uses camelCase (coinTypeA). We probe both and bail if neither matches.
"""
import asyncio
import json
import logging

import websockets

from .base import BaseScanner, PoolEvent

log = logging.getLogger(__name__)


class SuiScanner(BaseScanner):
    def __init__(self, handler, ws_url: str, packages: dict):
        super().__init__("sui", handler)
        self.ws_url = ws_url
        self.packages = packages

    async def run(self) -> None:
        if not self.ws_url:
            log.warning("SUI WS URL not set, skipping scanner")
            return

        backoff = 1
        while True:
            try:
                async with websockets.connect(self.ws_url, ping_interval=20) as ws:
                    log.info("SUI scanner connected")
                    backoff = 1
                    await self._subscribe_all(ws)
                    await self._consume(ws)
            except (websockets.ConnectionClosed, OSError) as e:
                log.warning("SUI WS dropped (%s); reconnecting in %ds", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _subscribe_all(self, ws):
        for i, (dex, package_id) in enumerate(self.packages.items(), start=1):
            sub = {
                "jsonrpc": "2.0",
                "id": i,
                "method": "suix_subscribeEvent",
                "params": [{"Package": package_id}],
            }
            await ws.send(json.dumps(sub))
            ack = json.loads(await ws.recv())
            log.info("Subscribed %s -> sub_id=%s", dex, ack.get("result"))

    async def _consume(self, ws):
        async for raw in ws:
            try:
                msg = json.loads(raw)
                event = msg.get("params", {}).get("result")
                if not event:
                    continue
                pool = self._maybe_pool_event(event)
                if pool:
                    await self.handler(pool)
            except Exception:
                log.exception("Failed to process SUI event")

    def _maybe_pool_event(self, event: dict) -> PoolEvent | None:
        type_str = event.get("type", "")
        if "CreatePool" not in type_str and "PoolCreated" not in type_str:
            return None

        fields = event.get("parsedJson") or {}
        pool_id = (
            fields.get("pool_id")
            or fields.get("pool")
            or fields.get("poolId")
            or event.get("id", {}).get("txDigest", "")
        )
        coin_a = (
            fields.get("coin_type_a")
            or fields.get("coinTypeA")
            or fields.get("coin_a")
            or ""
        )
        coin_b = (
            fields.get("coin_type_b")
            or fields.get("coinTypeB")
            or fields.get("coin_b")
            or ""
        )
        if not coin_a or not coin_b:
            return None

        return PoolEvent(
            chain="sui",
            dex=self._dex_for_type(type_str),
            pool_address=str(pool_id),
            token0=str(coin_a),
            token1=str(coin_b),
            block_or_slot=int(event.get("checkpoint", 0) or 0),
            tx_hash=event.get("id", {}).get("txDigest", ""),
            raw=fields,
        )

    def _dex_for_type(self, type_str: str) -> str:
        for dex, pkg in self.packages.items():
            if pkg in type_str:
                return dex
        return "unknown"
