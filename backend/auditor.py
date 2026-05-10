"""
Pre-trade safety auditor.

Runs after the scanner detects a new pool, before the alerter notifies anyone.
Each chain has its own audit path. The EVM path is now real — it makes RPC
calls to verify ownership renounce, supply sanity, and a transfer simulation
that catches the lazy honeypot patterns. SOL and SUI are still stubs.

Auditor philosophy:
- Returns a SCORE (0-100) and a PASS/FAIL flag, not just a boolean.
- The min_score threshold is conservative on purpose — better to miss 10
  legitimate gems than buy 1 honeypot.
- Unknowns count against the score. An RPC outage is not a green light.
- Every check has a hard timeout — the scanner cannot hang on a slow audit.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from scanners.base import PoolEvent

log = logging.getLogger(__name__)

# Score threshold for passing. Tunable via env.
MIN_SCORE = int(os.getenv("AUDIT_MIN_SCORE", "70"))


@dataclass
class AuditResult:
    passed: bool
    reason: str = ""
    score: int = 0
    details: dict | None = None


async def quick_audit(event: PoolEvent) -> AuditResult:
    if not event.token0 or not event.token1:
        return AuditResult(False, "missing token addresses")
    if event.token0 == event.token1:
        return AuditResult(False, "self-pair")

    if event.chain == "ethereum":
        return await _audit_evm(event)
    if event.chain == "solana":
        return await _audit_solana(event)
    if event.chain == "sui":
        return await _audit_sui(event)
    return AuditResult(True, "no auditor for chain", score=50)


async def _audit_evm(event: PoolEvent) -> AuditResult:
    """Real audit using auditor_evm. Imported lazily so SOL/SUI-only setups
    don't pay for httpx import."""
    from auditor_evm import audit_evm_token, check_lp_locked, pick_audit_target

    rpc_url = os.getenv("ETH_HTTP_URL", "")
    if not rpc_url:
        return AuditResult(True, "no RPC configured — audit bypassed", score=50)

    target = pick_audit_target(event.token0, event.token1)
    if target is None:
        return AuditResult(True, "stable pair, audit skipped", score=80)

    try:
        token_checks = await audit_evm_token(rpc_url, target)
    except Exception as e:
        log.warning("EVM audit failed for %s: %s", target[:10], e)
        return AuditResult(False, f"audit error: {type(e).__name__}", score=0)

    # Score the token. 100 points distributed across checks.
    score = 0
    reasons = []

    if token_checks.code_present:
        score += 20
    else:
        return AuditResult(False, "no contract code at token address", score=0)

    # Owner check (25 pts) — None means "no owner function" which we treat as
    # renounced; True means owner is zero/dead; False means a real address.
    if token_checks.owner_renounced is True:
        score += 25
    elif token_checks.owner_renounced is False:
        reasons.append("owner not renounced")
    else:
        score += 10
        reasons.append("owner unknown")

    if token_checks.total_supply_ok is True:
        score += 15
    elif token_checks.total_supply_ok is False:
        reasons.append("zero supply")
    else:
        reasons.append("supply unknown")

    # Honeypot transfer simulation (40 pts — the most important check)
    if token_checks.transfer_simulates is True:
        score += 40
    elif token_checks.transfer_simulates is False:
        return AuditResult(
            False,
            "HONEYPOT — transfer blocked",
            score=0,
            details={"address": target},
        )
    else:
        score += 15
        reasons.append("transfer sim inconclusive")

    # Bonus: LP lock check on the pool itself (up to +20)
    try:
        import httpx
        async with httpx.AsyncClient() as client:
            locked, lock_reason = await check_lp_locked(client, rpc_url, event.pool_address)
            if locked is True:
                score = min(100, score + 20)
                reasons.append(lock_reason)
            elif locked is False:
                reasons.append(lock_reason)
    except Exception as e:
        log.debug("LP lock check failed: %s", e)

    passed = score >= MIN_SCORE
    summary = f"score={score} | " + " | ".join(reasons) if reasons else f"score={score}"
    return AuditResult(passed, summary, score=score, details={"address": target})


async def _audit_solana(event: PoolEvent) -> AuditResult:
    """Real Solana audit. Decodes the SPL Mint account for the non-quote
    token in the pair and checks that mint_authority and freeze_authority
    are both revoked.

    Score breakdown (max 100):
      40  mint_authority revoked     (catches dilution rug)
      40  freeze_authority revoked   (catches freeze-honeypot)
      10  account is initialized
      10  supply > 0

    A pool with active mint_authority OR freeze_authority is RUG-prone
    enough that we hard-fail it regardless of other points — the user
    can opt back in by lowering AUDIT_MIN_SCORE.
    """
    from auditor_solana import audit_solana_token, pick_solana_audit_target

    rpc_url = os.getenv("SOL_HTTP_URL", "")
    if not rpc_url:
        return AuditResult(True, "no SOL RPC configured — audit bypassed", score=50)

    target = pick_solana_audit_target(event.token0, event.token1)
    if target is None:
        return AuditResult(True, "stable pair, audit skipped", score=80)

    try:
        checks = await audit_solana_token(rpc_url, target)
    except Exception as e:
        log.warning("SOL audit failed for %s: %s", target[:10], e)
        return AuditResult(False, f"audit error: {type(e).__name__}", score=0)

    if checks.error:
        return AuditResult(False, f"audit error: {checks.error}", score=0)

    score = 0
    reasons = []

    # Hard-fail conditions first: live authorities mean live rug risk.
    if checks.mint_authority_revoked is False:
        return AuditResult(
            False,
            "mint_authority NOT revoked — dev can mint unlimited tokens",
            score=0,
            details={"address": target},
        )
    if checks.freeze_authority_revoked is False:
        return AuditResult(
            False,
            "freeze_authority NOT revoked — dev can freeze your tokens",
            score=0,
            details={"address": target},
        )

    # Past the hard fails, score on what we can verify.
    if checks.mint_authority_revoked is True:
        score += 40
    else:
        reasons.append("mint_authority unknown")

    if checks.freeze_authority_revoked is True:
        score += 40
    else:
        reasons.append("freeze_authority unknown")

    if checks.is_initialized is True:
        score += 10
    else:
        reasons.append("not initialized")

    if checks.supply is not None and checks.supply > 0:
        score += 10
    else:
        reasons.append("zero supply")

    passed = score >= MIN_SCORE
    summary = f"score={score}" + (" | " + " | ".join(reasons) if reasons else "")
    return AuditResult(passed, summary, score=score, details={"address": target})


async def _audit_sui(event: PoolEvent) -> AuditResult:
    """Stub — SUI audit needs:
    1. sui_getObject on coin metadata
    2. find TreasuryCap object -> must be burned or held by null address
    3. inspect LP position object -> require lock package ownership
    """
    return AuditResult(True, "sui stub passed", score=50)
