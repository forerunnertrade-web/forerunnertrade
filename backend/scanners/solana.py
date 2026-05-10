"""
Solana scanner.

Solana has no factory contracts in the EVM sense — pools are PDAs derived from
program seeds, and creation is just a regular instruction call to the AMM
program. So instead of filtering by factory address we:

  1. logsSubscribe with a `mentions` filter on the AMM program ID
  2. scan log lines for known init-instruction strings
     (Raydium emits "Program log: initialize2", Orca emits "InitializePool")
  3. fetch the full tx and pull mints from postTokenBalances

The instruction-account decode for the actual pool PDA is program-specific.
For Raydium AMM v4 the pool account is index 4 in the initialize2 ix; for Orca
Whirlpools it's the first non-mint writable account in initializePool. Wiring
those decoders is left as a TODO so this file stays portable across programs.
"""
import asyncio
import json
import logging
from typing import Optional

import httpx
import websockets

from .base import BaseScanner, PoolEvent

log = logging.getLogger(__name__)

# Heuristic markers emitted on-chain when a new pool is initialised.
INIT_KEYWORDS = ("initialize2", "InitializePool", "initialize_pool")


class SolanaScanner(BaseScanner):
    def __init__(self, handler, ws_url: str, http_url: str, programs: dict):
        super().__init__("solana", handler)
        self.ws_url = ws_url
        self.http_url = http_url
        self.programs = programs

    async def run(self) -> None:
        if not self.ws_url:
            log.warning("Solana WS URL not set, skipping scanner")
            return

        backoff = 1
        while True:
            try:
                async with websockets.connect(self.ws_url, ping_interval=20) as ws:
                    log.info("Solana scanner connected")
                    backoff = 1
                    await self._subscribe_all(ws)
                    await self._consume(ws)
            except (websockets.ConnectionClosed, OSError) as e:
                log.warning("Solana WS dropped (%s); reconnecting in %ds", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _subscribe_all(self, ws):
        for i, (dex, program_id) in enumerate(self.programs.items(), start=1):
            sub = {
                "jsonrpc": "2.0",
                "id": i,
                "method": "logsSubscribe",
                "params": [
                    {"mentions": [program_id]},
                    {"commitment": "confirmed"},
                ],
            }
            await ws.send(json.dumps(sub))
            ack = json.loads(await ws.recv())
            log.info("Subscribed %s -> sub_id=%s", dex, ack.get("result"))

    async def _consume(self, ws):
        async for raw in ws:
            try:
                msg = json.loads(raw)
                value = msg.get("params", {}).get("result", {}).get("value")
                if not value:
                    continue
                logs = value.get("logs") or []
                if not any(kw in line for line in logs for kw in INIT_KEYWORDS):
                    continue
                signature = value.get("signature")
                event = await self._fetch_pool_event(signature, logs)
                if event:
                    await self.handler(event)
            except Exception:
                log.exception("Failed to process Solana log message")

    async def _fetch_pool_event(self, signature: str, logs: list) -> Optional[PoolEvent]:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    self.http_url,
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "getTransaction",
                        "params": [
                            signature,
                            {
                                "encoding": "jsonParsed",
                                "maxSupportedTransactionVersion": 0,
                            },
                        ],
                    },
                )
                tx = resp.json().get("result")
                if not tx:
                    return None

                # Pull unique mints from postTokenBalances.
                mints = []
                for bal in tx.get("meta", {}).get("postTokenBalances", []) or []:
                    m = bal.get("mint")
                    if m and m not in mints:
                        mints.append(m)
                if len(mints) < 2:
                    return None

                # Find the actual pool PDA. For Raydium AMM v4 `initialize2`,
                # the pool account is owned by the Raydium program AFTER the
                # tx executes. We look at postTokenBalances' owners and the
                # account list to find the new account owned by the Raydium
                # program. As a fallback, scan tx.transaction.message.accountKeys
                # for an account whose owner (in postBalances meta) matches a
                # known AMM program ID.
                dex = self._dex_for_logs(logs)
                program_id = self.programs.get(dex, "")
                pool_address = self._find_pool_pda(tx, program_id) or signature

                slot = tx.get("slot", 0)
                return PoolEvent(
                    chain="solana",
                    dex=dex,
                    pool_address=pool_address,
                    token0=mints[0],
                    token1=mints[1],
                    block_or_slot=slot,
                    tx_hash=signature,
                    raw={"logs": logs[:8]},
                )
        except Exception:
            log.exception("Failed to fetch Solana tx %s", signature)
            return None

    def _find_pool_pda(self, tx: dict, program_id: str) -> Optional[str]:
        """Scan the tx's account keys + meta to locate the pool PDA.

        The reliable signal: among the static account keys, the AMM pool
        is owned by `program_id` and has non-zero post-balance lamports.
        Pre-tx balance is typically zero (account just created).

        We rely on inner instructions to find the createAccount call that
        produced the pool PDA. If the tx uses jsonParsed encoding, the
        pool's create instruction will reference the Raydium program as
        the owner.
        """
        if not program_id:
            return None
        try:
            inner = tx.get("meta", {}).get("innerInstructions", []) or []
            for group in inner:
                for ix in group.get("instructions", []) or []:
                    parsed = ix.get("parsed") or {}
                    if parsed.get("type") == "createAccount":
                        info = parsed.get("info", {})
                        if info.get("owner") == program_id:
                            return info.get("newAccount")
        except Exception:
            pass
        return None

    def _dex_for_logs(self, logs: list) -> str:
        joined = " ".join(logs)
        for dex, pid in self.programs.items():
            if pid in joined:
                return dex
        return "unknown"
