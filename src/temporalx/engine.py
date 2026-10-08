"""Pure-Python oracle: deterministic replay and invariant enforcement.

No IO or cluster behavior here. Spark results must be reconciled to this oracle.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from temporalx.model import Event, parse_instant


class ReplayConflictError(ValueError):
    """The same (tenant, event_id) has conflicting business content."""


class InvariantViolation(ValueError):
    """A ledger invariant is violated; result must not be promoted."""


@dataclass(frozen=True)
class ReplayResult:
    policy: str
    valid_at: str | None
    known_at: str | None
    visible_events: int
    applied_events: int
    duplicate_messages: int
    account_balances: dict[str, int]
    transaction_balances: dict[str, int]

    def to_dict(self) -> dict:
        return {
            "policy": self.policy, "valid_at": self.valid_at,
            "known_at": self.known_at, "visible_events": self.visible_events,
            "applied_events": self.applied_events,
            "duplicate_messages": self.duplicate_messages,
            "account_balances": self.account_balances,
            "transaction_balances": self.transaction_balances,
        }


def replay(
    events: Iterable[Event],
    policy: str = "v2",
    *,
    valid_at: str | datetime | None = None,
    known_at: str | datetime | None = None,
    enforce_invariants: bool = True,
) -> ReplayResult:
    if policy not in ("v1", "v2"):
        raise ValueError("policy must be 'v1' or 'v2'")
    valid_limit = parse_instant(valid_at) if valid_at is not None else None
    known_limit = parse_instant(known_at) if known_at is not None else None
    visible = [e for e in events
               if (valid_limit is None or e.occurred_at <= valid_limit)
               and (known_limit is None or e.ingested_at <= known_limit)]
    # Stable sorting ensures no dependence on transport/list arrival order.
    visible.sort(key=lambda e: (e.occurred_at, e.ingested_at, e.tenant_id, e.event_id))
    seen: dict[tuple[str, str], Event] = {}
    applied: list[Event] = []
    duplicates = 0
    for event in visible:
        key = (event.tenant_id, event.event_id)
        prior = seen.get(key)
        if prior is not None:
            if prior.semantic_fingerprint() != event.semantic_fingerprint():
                raise ReplayConflictError(f"Conflicting event identity {key}")
            duplicates += 1
            if policy == "v2":
                continue
        else:
            seen[key] = event
        applied.append(event)

    tx_amount: dict[tuple[str, str], int] = defaultdict(int)
    tx_account: dict[tuple[str, str], str] = {}
    account_amount: dict[tuple[str, str], int] = defaultdict(int)
    for event in applied:
        key = (event.tenant_id, event.transaction_id)
        previous_account = tx_account.setdefault(key, event.account_id)
        if previous_account != event.account_id:
            raise InvariantViolation(f"Transaction {key} changed owner account")
        sign = 1 if event.kind == "CAPTURE" else -1
        delta = sign * event.amount_cents
        tx_amount[key] += delta
        account_amount[(event.tenant_id, event.account_id)] += delta
    if enforce_invariants:
        invalid = [(k, v) for k, v in tx_amount.items() if v < 0]
        if invalid:
            raise InvariantViolation(f"Refund exceeds captured amount: {invalid}")
        if sum(account_amount.values()) != sum(tx_amount.values()):
            raise InvariantViolation("Account and transaction aggregates diverged")
    # Encode composite keys unambiguously with JSON-compatible separators.
    def encoded(k: tuple[str, str]) -> str:
        import json
        return json.dumps(list(k), separators=(",", ":"))

    return ReplayResult(
        policy=policy,
        valid_at=valid_limit.isoformat() if valid_limit else None,
        known_at=known_limit.isoformat() if known_limit else None,
        visible_events=len(visible), applied_events=len(applied),
        duplicate_messages=duplicates,
        account_balances={encoded(k): v for k, v in sorted(account_amount.items())},
        transaction_balances={encoded(k): v for k, v in sorted(tx_amount.items())},
    )


def counterfactual(events: Iterable[Event], **kwargs) -> dict:
    """Compare historical buggy policy to corrected replay over the same snapshot."""
    frozen = tuple(events)
    baseline = replay(frozen, "v1", **kwargs)
    candidate = replay(frozen, "v2", **kwargs)
    keys = sorted(set(baseline.account_balances) | set(candidate.account_balances))
    deltas = {k: candidate.account_balances.get(k, 0) - baseline.account_balances.get(k, 0)
              for k in keys if candidate.account_balances.get(k, 0) != baseline.account_balances.get(k, 0)}
    return {
        "baseline": baseline.to_dict(),
        "candidate": candidate.to_dict(),
        "account_delta_cents": deltas,
        "total_delta_cents": sum(deltas.values()),
        "reconciled": sum(candidate.account_balances.values()) == sum(candidate.transaction_balances.values()),
    }
