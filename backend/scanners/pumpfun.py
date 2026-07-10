"""
Pump.fun scanner via PumpPortal WebSocket.

Why pump.fun: it's the single biggest source of memecoin launches on
Solana — dozens of tokens per minute during active hours. Catching them
at launch is exactly the use case this harness was built for, far more
than the long-tail of Raydium pools that the generic Solana scanner sees.

How it works:
  1. Connect to PumpPortal WebSocket (wss://pumpportal.fun/api/data)
  2. Subscribe to `subscribeNewToken` — pushes every new pump.fun mint
  3. Emit each as a PoolEvent with chain='solana', dex='pumpfun'

Caveats:
  - PumpPortal is an unofficial free aggregator. If it goes down or rate-
    limits, this scanner stops producing signals; the rest of the system
    keeps running. Reconnects on disconnect.
  - The "pool" address for a pump.fun token is the bonding-curve PDA, not
    a traditional AMM pool. The enricher / price oracle won't be able to
    quote prices the same way as Raydium pools — pump.fun bonding-curve
    pricing has its own math. For now: the entry price comes from
    pump.fun's own marketCapSol field, and the position rides on
    random-walk drift until graduation (when it becomes a real Raydium
    pool and price_oracle.py can read it). That's not ideal but it's
    consistent with how trending-source signals already work.

Wire-up: main.py adds this scanner conditionally via PUMPFUN_ENABLED env.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

import websockets

from .base import BaseScanner, PoolEvent

log = logging.getLogger(__name__)

PUMPPORTAL_WS = "wss://pumpportal.fun/api/data"

# WSOL — pump.fun tokens are always paired against SOL on the curve.
WSOL_MINT = "So11111111111111111111111111111111111111112"


class PumpFunScanner(BaseScanner):
    """Subscribe to pump.fun newToken events via PumpPortal."""

    def __init__(self, handler, *, reconnect_seconds: float = 5.0):
        super().__init__(name="pumpfun", handler=handler)
        self.reconnect_seconds = reconnect_seconds
        self._stopped = False

    async def run(self) -> None:
        """Connect, subscribe, loop. Reconnect on failure."""
        backoff = self.reconnect_seconds
        while not self._stopped:
            try:
                async with websockets.connect(PUMPPORTAL_WS, ping_interval=20) as ws:
                    log.info("pumpfun: connected to PumpPortal")
                    # Subscribe to new token launches. The API expects
                    # {"method": "subscribeNewToken"} per the docs.
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    # Also subscribe to migration events — when a token graduates
                    # from bonding curve to Raydium or PumpSwap. This lets us
                    # detect the migration event without running our own
                    # logsSubscribe listener on account 39azUYFWPz3VHgKCf...
                    # We treat migrations as a distinct signal source that
                    # main.py routes to a separate handler.
                    await ws.send(json.dumps({"method": "subscribeMigration"}))

                    backoff = self.reconnect_seconds  # reset backoff on successful connect

                    async for msg in ws:
                        try:
                            await self._handle_message(msg)
                        except Exception:
                            log.exception("pumpfun: handler crashed on message")

            except (websockets.WebSocketException, ConnectionError, OSError) as e:
                log.warning("pumpfun: connection error: %s — reconnecting in %.0fs",
                            type(e).__name__, backoff)
                await asyncio.sleep(backoff)
                # Exponential backoff up to 60s
                backoff = min(60.0, backoff * 1.5)
            except Exception:
                log.exception("pumpfun: unexpected error — reconnecting in %.0fs",
                              backoff)
                await asyncio.sleep(backoff)
                backoff = min(60.0, backoff * 1.5)

    async def _handle_message(self, msg: str) -> None:
        """Route incoming PumpPortal messages by txType.

        - "create"                → new token launch → emit dex='pumpfun' event
        - "migrate" / "migration" → graduation → emit dex='graduation' event
        - anything else (subscription acks, heartbeats, unknown types) → ignore

        Expected newToken payload (from PumpPortal docs):
        {
          "signature": "...", "mint": "...", "traderPublicKey": "...",
          "txType": "create", "initialBuy": 67000000,
          "bondingCurveKey": "...", "vTokensInBondingCurve": ...,
          "vSolInBondingCurve": ..., "marketCapSol": 27.95,
          "name": "Token name", "symbol": "TICKER", "uri": "https://..."
        }

        Migration payload shape is less well-documented publicly; we accept
        several field naming conventions defensively and preserve the raw
        payload in event.raw for downstream analysis.
        """
        try:
            data = json.loads(msg)
        except json.JSONDecodeError:
            return

        # PumpPortal pushes a confirmation when you subscribe, like
        # {"message": "Successfully subscribed to ..."}. Skip it.
        if "txType" not in data:
            return
        tx_type = data.get("txType")
        if tx_type == "create":
            await self._handle_new_token(data)
        elif tx_type in ("migrate", "migration"):
            await self._handle_migration(data)
        # Other txTypes (trade events on subscribed tokens, etc.) are ignored.

    async def _handle_new_token(self, data: dict) -> None:
        """Emit a PoolEvent for a new pump.fun token launch."""
        mint = data.get("mint")
        if not mint:
            return

        # Pool/bonding-curve address (called bondingCurveKey on pump.fun)
        pool = data.get("bondingCurveKey") or data.get("pool") or mint

        symbol = data.get("symbol") or mint[:8]
        name = data.get("name")
        sig = data.get("signature", "")
        trader = data.get("traderPublicKey")

        event = PoolEvent(
            chain="solana",
            dex="pumpfun",
            pool_address=pool,
            token0=mint,
            token1=WSOL_MINT,  # pump.fun pairs against SOL
            block_or_slot=0,   # PumpPortal doesn't include slot in feed
            tx_hash=sig,
            deployer=trader,
            raw={
                "name": name,
                "symbol": symbol,
                "marketCapSol": data.get("marketCapSol"),
                "vSolInBondingCurve": data.get("vSolInBondingCurve"),
                "vTokensInBondingCurve": data.get("vTokensInBondingCurve"),
                "initialBuy": data.get("initialBuy"),
                "uri": data.get("uri"),
                "received_at": time.time(),
            },
        )

        log.info(
            "pumpfun: new token %s (%s) cap=%.1f SOL trader=%s",
            symbol, mint[:10],
            float(data.get("marketCapSol") or 0),
            (trader or "?")[:10],
        )

        await self.handler(event)

    async def _handle_migration(self, data: dict) -> None:
        """Emit a graduation event with dex='graduation'. main.py routes
        these to a separate handler that schedules delayed evaluation."""
        mint = data.get("mint")
        if not mint:
            return
        # PumpPortal's migration payload may include the destination pool
        # under different key names; try common variants and fall back to
        # the mint (main.py will resolve pool via DEXScreener if needed).
        pool = (
            data.get("pool")
            or data.get("poolAddress")
            or data.get("raydiumPool")
            or data.get("pumpswapPool")
            or mint
        )
        # Destination detection is best-effort — the payload may or may not
        # include an explicit field. We check common keys and fall back to
        # string-matching the raw payload since it's cheap and often works.
        destination = (
            data.get("destination")
            or data.get("dex")
        )
        if not destination:
            raw_str = str(data).lower()
            if "pumpswap" in raw_str or "pamm" in raw_str:
                destination = "pumpswap"
            elif "raydium" in raw_str:
                destination = "raydium"
            else:
                destination = "unknown"

        event = PoolEvent(
            chain="solana",
            dex="graduation",
            pool_address=pool,
            token0=mint,
            token1=WSOL_MINT,
            block_or_slot=0,
            tx_hash=data.get("signature", ""),
            deployer=None,
            raw={
                "destination": destination,
                "symbol": data.get("symbol"),
                "name": data.get("name"),
                "received_at": time.time(),
                # Preserve the whole raw payload in case PumpPortal's schema
                # evolves and we later need fields we didn't know about now.
                "raw_payload": data,
            },
        )
        log.info(
            "pumpfun: graduation mint=%s dest=%s pool=%s",
            mint[:10], destination, (pool[:10] if pool else "?"),
        )
        await self.handler(event)
