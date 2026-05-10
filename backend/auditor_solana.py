"""
Solana token auditor.

Decodes SPL Token mint accounts at the byte level and reports whether the
two riskiest powers — mint authority and freeze authority — have been
revoked. Both should be None on any token a non-malicious dev launches.

Why these two checks matter:

  mint_authority: holder can call MintTo and create new tokens out of thin
    air. If not revoked, the dev can dilute holders to zero. This is the
    single most common rug pattern on Solana.

  freeze_authority: holder can call FreezeAccount on any token account
    holding this mint. If not revoked, the dev can freeze your tokens
    after you buy, locking you out of selling. This is the Solana
    equivalent of a transfer-blocking honeypot.

SPL Token Mint layout (82 bytes, packed, little-endian):
  0..4    mint_authority_option   (u32: 0=None, 1=Some)
  4..36   mint_authority          (Pubkey, only valid if option == 1)
  36..44  supply                  (u64)
  44..45  decimals                (u8)
  45..46  is_initialized          (bool)
  46..50  freeze_authority_option (u32)
  50..82  freeze_authority        (Pubkey, only valid if option == 1)

Reference: solana-program-library/token/program/src/state.rs

We do NOT check LP token burn here. Raydium AMM v4 issues an LP token
mint, but verifying it's been burned to a null address is non-trivial
(many launches lock liquidity in third-party programs like Streamflow
or Meteora's lock vault, which look like "non-burned" but are actually
fine). False positives would dominate. Skipping this check is honest;
including a wrong one would be worse.
"""
from __future__ import annotations

import base64
import logging
from dataclasses import dataclass
from typing import Optional

import httpx

log = logging.getLogger(__name__)

# Constants from the SPL Token program
MINT_ACCOUNT_LEN = 82
OFF_MINT_AUTH_OPTION   = 0
OFF_MINT_AUTH          = 4
OFF_SUPPLY             = 36
OFF_DECIMALS           = 44
OFF_FREEZE_AUTH_OPTION = 46
OFF_FREEZE_AUTH        = 50

# Common quote mints — we never audit these (they're the wrapped/stable side)
KNOWN_QUOTE_MINTS = {
    "So11111111111111111111111111111111111111112",  # WSOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
}


@dataclass
class SolanaTokenChecks:
    """Result of decoding a single SPL mint account."""
    mint_authority_revoked: Optional[bool] = None
    freeze_authority_revoked: Optional[bool] = None
    supply: Optional[int] = None
    decimals: Optional[int] = None
    is_initialized: Optional[bool] = None
    error: Optional[str] = None


def pick_solana_audit_target(mint0: str, mint1: str) -> Optional[str]:
    """Return the non-quote mint (the actual NEW token), or None if both
    are quotes (which would mean a stable/wrapped pair we don't care about)."""
    m0_quote = mint0 in KNOWN_QUOTE_MINTS
    m1_quote = mint1 in KNOWN_QUOTE_MINTS
    if m0_quote and m1_quote:
        return None
    if not m0_quote:
        return mint0
    return mint1


def _decode_mint_account(data: bytes) -> SolanaTokenChecks:
    """Decode an 82-byte SPL Mint account into the fields we care about."""
    checks = SolanaTokenChecks()
    if len(data) < MINT_ACCOUNT_LEN:
        checks.error = f"mint data too short ({len(data)} < {MINT_ACCOUNT_LEN})"
        return checks

    try:
        mint_opt = int.from_bytes(data[OFF_MINT_AUTH_OPTION:OFF_MINT_AUTH_OPTION + 4], "little")
        # 0 = None (revoked) — good
        # 1 = Some (active authority) — red flag
        # anything else means the account isn't a valid Mint
        if mint_opt == 0:
            checks.mint_authority_revoked = True
        elif mint_opt == 1:
            checks.mint_authority_revoked = False
        else:
            checks.error = f"invalid mint_authority option: {mint_opt}"
            return checks

        checks.supply = int.from_bytes(data[OFF_SUPPLY:OFF_SUPPLY + 8], "little")
        checks.decimals = data[OFF_DECIMALS]
        checks.is_initialized = data[OFF_DECIMALS + 1] == 1

        freeze_opt = int.from_bytes(
            data[OFF_FREEZE_AUTH_OPTION:OFF_FREEZE_AUTH_OPTION + 4], "little"
        )
        if freeze_opt == 0:
            checks.freeze_authority_revoked = True
        elif freeze_opt == 1:
            checks.freeze_authority_revoked = False
        else:
            checks.error = f"invalid freeze_authority option: {freeze_opt}"

    except Exception as e:
        checks.error = f"decode error: {type(e).__name__}: {e}"

    return checks


async def audit_solana_token(rpc_url: str, mint_address: str) -> SolanaTokenChecks:
    """Fetch and decode a Solana token mint account."""
    if not rpc_url:
        return SolanaTokenChecks(error="no RPC configured")

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.post(
                rpc_url,
                json={
                    "jsonrpc": "2.0", "id": 1,
                    "method": "getAccountInfo",
                    "params": [
                        mint_address,
                        {"encoding": "base64", "commitment": "confirmed"},
                    ],
                },
            )
            resp.raise_for_status()
            body = resp.json()
            if "error" in body:
                return SolanaTokenChecks(error=f"RPC: {body['error']}")

            value = (body.get("result") or {}).get("value")
            if not value:
                return SolanaTokenChecks(error="mint account not found")

            data_b64 = value.get("data", [None])[0]
            if not data_b64:
                return SolanaTokenChecks(error="empty account data")

            return _decode_mint_account(base64.b64decode(data_b64))

    except httpx.HTTPError as e:
        return SolanaTokenChecks(error=f"http: {type(e).__name__}")
    except Exception as e:
        log.debug("audit_solana_token unexpected error: %s", e)
        return SolanaTokenChecks(error=f"{type(e).__name__}: {str(e)[:80]}")
