"""Environment-driven config. Factory / program / package IDs live here so chain
upgrades are a one-file change."""
import os
from dataclasses import dataclass, field
from typing import Dict

from dotenv import load_dotenv

load_dotenv()


@dataclass
class ChainConfig:
    name: str
    enabled: bool
    rpc_ws: str
    rpc_http: str
    factories: Dict[str, str] = field(default_factory=dict)


@dataclass
class AppConfig:
    chains: Dict[str, ChainConfig]
    telegram_bot_token: str
    telegram_chat_id: str
    discord_webhook_url: str
    min_initial_liquidity_usd: float
    audit_enabled: bool


def load_config() -> AppConfig:
    return AppConfig(
        chains={
            "ethereum": ChainConfig(
                name="Ethereum",
                enabled=os.getenv("ETH_ENABLED", "true").lower() == "true",
                rpc_ws=os.getenv("ETH_WS_URL", ""),
                rpc_http=os.getenv("ETH_HTTP_URL", ""),
                factories={
                    # Verify against current deployments before going live.
                    "uniswap_v2": "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",
                    "uniswap_v3": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
                },
            ),
            "solana": ChainConfig(
                name="Solana",
                enabled=os.getenv("SOL_ENABLED", "true").lower() == "true",
                rpc_ws=os.getenv("SOL_WS_URL", ""),
                rpc_http=os.getenv("SOL_HTTP_URL", ""),
                factories={
                    # Raydium AMM v4
                    "raydium_amm_v4": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
                    # Orca Whirlpools
                    "orca_whirlpools": "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
                },
            ),
            "sui": ChainConfig(
                name="SUI",
                enabled=os.getenv("SUI_ENABLED", "true").lower() == "true",
                rpc_ws=os.getenv("SUI_WS_URL", "wss://fullnode.mainnet.sui.io:443"),
                rpc_http=os.getenv("SUI_HTTP_URL", "https://fullnode.mainnet.sui.io:443"),
                factories={
                    # Cetus CLMM package - rotates on upgrades; verify before prod.
                    "cetus_clmm": "0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb",
                },
            ),
        },
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
        discord_webhook_url=os.getenv("DISCORD_WEBHOOK_URL", ""),
        min_initial_liquidity_usd=float(os.getenv("MIN_LIQ_USD", "5000")),
        audit_enabled=os.getenv("AUDIT_ENABLED", "true").lower() == "true",
    )
