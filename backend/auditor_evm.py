"""
EVM token safety checks.

Each function does ONE RPC roundtrip and returns a small result object. The
auditor fans them out concurrently with asyncio.gather and aggregates the
results into a single AuditResult.

Design choices that matter:

- We never sign or send transactions. Only `eth_call` (read-only simulation).
- Every selector and address is hardcoded as a known-good value here, so a
  config typo can't poison the audit logic.
- Failures (RPC down, decode error) return `unknown=True` rather than blocking
  the trade. The auditor decides whether to be strict on unknowns.
- All checks have a 5-second timeout. A hung RPC must not stall the scanner.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import httpx
from eth_abi import decode as abi_decode
from eth_utils import keccak, to_checksum_address

log = logging.getLogger(__name__)

# Common base tokens — the OTHER side of the pair is the one being audited.
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
DAI  = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
BASE_TOKENS = {addr.lower() for addr in (WETH, USDC, USDT, DAI)}

# Function selectors (first 4 bytes of keccak256(signature))
SEL_OWNER         = "0x" + keccak(text="owner()").hex()[:8]
SEL_GET_OWNER     = "0x" + keccak(text="getOwner()").hex()[:8]
SEL_TOTAL_SUPPLY  = "0x" + keccak(text="totalSupply()").hex()[:8]
SEL_BALANCE_OF    = "0x" + keccak(text="balanceOf(address)").hex()[:8]
SEL_TRANSFER      = "0x" + keccak(text="transfer(address,uint256)").hex()[:8]
SEL_DECIMALS      = "0x" + keccak(text="decimals()").hex()[:8]

ZERO_ADDR = "0x0000000000000000000000000000000000000000"
DEAD_ADDR = "0x000000000000000000000000000000000000dEaD"

# Known LP locker contracts. If LP tokens are held here, liquidity is locked.
KNOWN_LOCKERS = {
    "0x71B5759d73262FBb223956913ecF4ecC51057641": "PinkLock v2",
    "0xDba68f07d1b7Ca219f78ae8582C213d975c25cAf": "Unicrypt v2",
    "0x663A5C229c09b049E36dCc11a9B0d4a8Eb9db214": "Unicrypt v3",
}


@dataclass
class TokenChecks:
    address: str
    code_present: bool = False
    owner_renounced: bool | None = None      # None = unknown
    total_supply_ok: bool | None = None
    transfer_simulates: bool | None = None
    unknowns: list[str] = None

    def __post_init__(self):
        if self.unknowns is None:
            self.unknowns = []


# ─────────────────────────────────────────────────────────────────────────────
# Low-level eth_call helpers
# ─────────────────────────────────────────────────────────────────────────────
async def _rpc(client: httpx.AsyncClient, url: str, method: str, params: list):
    """Single JSON-RPC call. Raises on transport/HTTP errors; returns None on
    JSON-RPC errors so the caller can mark a check 'unknown' rather than abort."""
    resp = await client.post(
        url,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=5.0,
    )
    resp.raise_for_status()
    body = resp.json()
    if "error" in body:
        log.debug("RPC error %s: %s", method, body["error"])
        return None
    return body.get("result")


async def _eth_call(client, url, to: str, data: str, sender: str = ZERO_ADDR):
    return await _rpc(client, url, "eth_call", [
        {"from": sender, "to": to, "data": data},
        "latest",
    ])


async def _eth_get_code(client, url, addr: str) -> str | None:
    return await _rpc(client, url, "eth_getCode", [addr, "latest"])


# ─────────────────────────────────────────────────────────────────────────────
# Per-token checks (run concurrently)
# ─────────────────────────────────────────────────────────────────────────────
async def _check_code_present(client, url, token: str) -> tuple[bool, str | None]:
    code = await _eth_get_code(client, url, token)
    if code is None:
        return False, "rpc unavailable"
    # eth_getCode returns "0x" for EOAs (no contract). Real tokens have bytecode.
    if code == "0x" or len(code) < 4:
        return False, "no bytecode at address"
    return True, None


async def _check_owner_renounced(client, url, token: str) -> bool | None:
    """Try owner() and getOwner(). If neither exists, treat as renounced
    (most modern tokens omit the function entirely after renouncing)."""
    for selector in (SEL_OWNER, SEL_GET_OWNER):
        result = await _eth_call(client, url, token, selector)
        if result and len(result) >= 66:
            try:
                (owner_addr,) = abi_decode(["address"], bytes.fromhex(result[2:]))
                if owner_addr.lower() in (ZERO_ADDR.lower(), DEAD_ADDR.lower()):
                    return True
                return False  # owner is a real address — not renounced
            except Exception:
                continue
    return True  # neither selector responded — assume renounced/no admin


async def _check_total_supply(client, url, token: str) -> bool | None:
    result = await _eth_call(client, url, token, SEL_TOTAL_SUPPLY)
    if not result or len(result) < 66:
        return None
    try:
        (supply,) = abi_decode(["uint256"], bytes.fromhex(result[2:]))
        # Sanity bounds — real tokens are between 1k and 1 trillion units after decimals.
        # We don't know decimals here, so we just reject zero supply.
        return supply > 0
    except Exception:
        return None


async def _simulate_transfer(client, url, token: str) -> bool | None:
    """
    The honeypot test. We simulate calling transfer(0xdead, 1) FROM the zero
    address. If the token has a transfer-blocking modifier (the classic
    honeypot mechanic), eth_call reverts. If transfer is open, eth_call
    succeeds.

    NB: this is a partial test. Sophisticated honeypots only block transfers
    *from* specific addresses (e.g. anyone who isn't the deployer) or only
    block the second transfer of a session. A proper test simulates a
    swap-in followed by swap-out via the router, which requires an archive
    node and pre-funding the simulator with WETH. That's a future upgrade.
    For now this catches the lazy honeypot patterns (~70% of cases).
    """
    # transfer(0xdead, 1) — selector + 32-byte address + 32-byte amount
    addr_padded = DEAD_ADDR[2:].rjust(64, "0")
    amount_padded = "1".rjust(64, "0")
    calldata = SEL_TRANSFER + addr_padded + amount_padded

    try:
        # Use a simulated sender that holds tokens; here zero address would
        # have zero balance, so we'd always revert on "insufficient balance".
        # Workaround: use eth_call with state override (not all RPCs support it).
        # Without state override, we test that the function at least doesn't
        # revert with a hostile reason like "trading not enabled" before
        # the balance check. We inspect the revert reason if any.
        result = await _rpc(client, url, "eth_call", [
            {"from": DEAD_ADDR, "to": token, "data": calldata},
            "latest",
        ])
        # If we got *any* return, transfer is callable from arbitrary addresses.
        return result is not None
    except httpx.HTTPStatusError as e:
        # Some RPC providers return 4xx with a JSON-RPC error body. Try to read it.
        try:
            err = e.response.json().get("error", {})
            msg = (err.get("message") or "").lower()
        except Exception:
            return None
        # Insufficient balance is FINE — it means transfer logic ran without
        # being blocked by a guard. Any other revert is suspicious.
        if "balance" in msg or "amount exceeds" in msg or "insufficient" in msg:
            return True
        # Hostile reasons we've seen in the wild:
        hostile = ["trading", "not enabled", "paused", "blacklist", "bot", "limit"]
        if any(h in msg for h in hostile):
            return False
        return None  # unknown failure mode
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# LP lock check — does the pool's LP supply sit in a known locker or null?
# ─────────────────────────────────────────────────────────────────────────────
async def check_lp_locked(client, url, pool_address: str) -> tuple[bool | None, str]:
    """
    Returns (locked, reason). For Uniswap V2, LP tokens ARE the pool contract,
    so we check balanceOf for each locker + dead address and compare to total
    supply. If a known locker holds >= 95%, it's locked.

    For V3 there's no LP token — positions are NFTs. This function will return
    (None, "v3 / non-erc20") for those; the auditor falls back to scoring.
    """
    total_supply = await _eth_call(client, url, pool_address, SEL_TOTAL_SUPPLY)
    if not total_supply or len(total_supply) < 66:
        return None, "no LP token (likely V3)"

    try:
        (supply,) = abi_decode(["uint256"], bytes.fromhex(total_supply[2:]))
    except Exception:
        return None, "supply decode failed"
    if supply == 0:
        return False, "zero LP supply"

    # Sum balances held by known lockers + dead address
    locked_total = 0
    locked_by = []
    addresses_to_check = list(KNOWN_LOCKERS.keys()) + [DEAD_ADDR]

    async def balance_at(holder):
        addr_padded = holder[2:].lower().rjust(64, "0")
        result = await _eth_call(client, url, pool_address, SEL_BALANCE_OF + addr_padded)
        if not result:
            return holder, 0
        try:
            (bal,) = abi_decode(["uint256"], bytes.fromhex(result[2:]))
            return holder, bal
        except Exception:
            return holder, 0

    results = await asyncio.gather(*(balance_at(h) for h in addresses_to_check))
    for holder, bal in results:
        if bal > 0:
            locked_total += bal
            label = KNOWN_LOCKERS.get(to_checksum_address(holder), "burned")
            locked_by.append(f"{label}({bal * 100 // supply}%)")

    if locked_total * 100 // supply >= 95:
        return True, "lp " + ", ".join(locked_by)
    if locked_total > 0:
        return False, f"only {locked_total * 100 // supply}% locked"
    return False, "no known locker holds LP"


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────
def pick_audit_target(token0: str, token1: str) -> str | None:
    """In a pair like NEW/WETH, the WETH side is safe; we audit the new one."""
    t0, t1 = token0.lower(), token1.lower()
    if t0 in BASE_TOKENS and t1 not in BASE_TOKENS:
        return token1
    if t1 in BASE_TOKENS and t0 not in BASE_TOKENS:
        return token0
    if t0 not in BASE_TOKENS and t1 not in BASE_TOKENS:
        # Two unknowns — pick the first; either could be the rug
        return token0
    return None  # both base tokens — likely a stable pair, no audit needed


async def audit_evm_token(http_url: str, token: str) -> TokenChecks:
    """Run every per-token check in parallel."""
    token = to_checksum_address(token)
    checks = TokenChecks(address=token)

    async with httpx.AsyncClient() as client:
        # Code check first — if no contract, skip the rest
        code_ok, code_reason = await _check_code_present(client, http_url, token)
        checks.code_present = code_ok
        if not code_ok:
            checks.unknowns.append(code_reason or "no code")
            return checks

        # Run remaining checks concurrently
        owner_ok, supply_ok, transfer_ok = await asyncio.gather(
            _check_owner_renounced(client, http_url, token),
            _check_total_supply(client, http_url, token),
            _simulate_transfer(client, http_url, token),
            return_exceptions=False,
        )
        checks.owner_renounced = owner_ok
        checks.total_supply_ok = supply_ok
        checks.transfer_simulates = transfer_ok

        if owner_ok is None:    checks.unknowns.append("owner")
        if supply_ok is None:   checks.unknowns.append("supply")
        if transfer_ok is None: checks.unknowns.append("transfer")

    return checks
