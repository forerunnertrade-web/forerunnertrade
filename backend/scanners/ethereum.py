from __future__ import annotations

"""
Ethereum / EVM scanner.

Subscribes via eth_subscribe('logs', {address, topics}) to the factory contracts
and decodes the two canonical events:

  Uniswap V2: PairCreated(address indexed token0, address indexed token1,
                          address pair, uint256)
  Uniswap V3: PoolCreated(address indexed token0, address indexed token1,
                          uint24 indexed fee, int24 tickSpacing, address pool)

Topic hashes are derived at runtime from the canonical signatures so a typo
can't silently make the filter match nothing — a real failure mode in the wild.

Same module works on every EVM chain. Just point ETH_WS_URL at the L2 / sidechain
RPC and override the factory addresses in config.py.
"""
import asyncio
import json
import logging

import websockets
from eth_abi import decode
from eth_utils import keccak, to_checksum_address

from .base import BaseScanner, PoolEvent

log = logging.getLogger(__name__)

PAIR_CREATED_TOPIC = "0x" + keccak(text="PairCreated(address,address,address,uint256)").hex()
POOL_CREATED_TOPIC = "0x" + keccak(text="PoolCreated(address,address,uint24,int24,address)").hex()


class EthereumScanner(BaseScanner):
    def __init__(self, handler, ws_url: str, factories: dict):
        super().__init__("ethereum", handler)
        self.ws_url = ws_url
        self.factories = factories
        self._factory_lookup = {to_checksum_address(a): n for n, a in factories.items()}

    async def run(self) -> None:
        if not self.ws_url:
            log.warning("Ethereum WS URL not set, skipping scanner")
            return

        backoff = 1
        while True:
            try:
                async with websockets.connect(self.ws_url, ping_interval=20) as ws:
                    log.info("Ethereum scanner connected")
                    backoff = 1
                    await self._subscribe(ws)
                    await self._consume(ws)
            except (websockets.ConnectionClosed, OSError) as e:
                log.warning("Ethereum WS dropped (%s); reconnecting in %ds", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _subscribe(self, ws):
        sub = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": [
                "logs",
                {
                    "address": list(self.factories.values()),
                    "topics": [[PAIR_CREATED_TOPIC, POOL_CREATED_TOPIC]],
                },
            ],
        }
        await ws.send(json.dumps(sub))
        ack = json.loads(await ws.recv())
        log.info("Ethereum subscription ack: %s", ack.get("result"))

    async def _consume(self, ws):
        async for raw in ws:
            try:
                msg = json.loads(raw)
                result = (msg.get("params") or {}).get("result")
                if not result:
                    continue
                event = self._decode_log(result)
                if event:
                    await self.handler(event)
            except Exception:
                log.exception("Failed to process Ethereum log")

    def _decode_log(self, log_obj: dict) -> PoolEvent | None:
        topics = log_obj.get("topics", [])
        if not topics:
            return None
        topic0 = topics[0].lower()
        factory_addr = to_checksum_address(log_obj["address"])
        block = int(log_obj["blockNumber"], 16)
        tx = log_obj["transactionHash"]
        data = bytes.fromhex(log_obj["data"][2:])

        if topic0 == PAIR_CREATED_TOPIC:
            token0 = to_checksum_address("0x" + topics[1][-40:])
            token1 = to_checksum_address("0x" + topics[2][-40:])
            pair, _index = decode(["address", "uint256"], data)
            return PoolEvent(
                chain="ethereum",
                dex=self._factory_lookup.get(factory_addr, "unknown"),
                pool_address=to_checksum_address(pair),
                token0=token0,
                token1=token1,
                block_or_slot=block,
                tx_hash=tx,
                raw=log_obj,
            )

        if topic0 == POOL_CREATED_TOPIC:
            token0 = to_checksum_address("0x" + topics[1][-40:])
            token1 = to_checksum_address("0x" + topics[2][-40:])
            # fee is indexed (topic3); data carries (int24 tickSpacing, address pool)
            _tick_spacing, pool = decode(["int24", "address"], data)
            return PoolEvent(
                chain="ethereum",
                dex=self._factory_lookup.get(factory_addr, "unknown"),
                pool_address=to_checksum_address(pool),
                token0=token0,
                token1=token1,
                block_or_slot=block,
                tx_hash=tx,
                raw=log_obj,
            )
        return None
