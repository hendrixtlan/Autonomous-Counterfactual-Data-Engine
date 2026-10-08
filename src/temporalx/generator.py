"""Deterministic financial event fixture with intentional transport faults."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from temporalx.model import Event


def generate_events(count: int = 100, seed: int = 42) -> list[Event]:
    if count <= 0:
        raise ValueError("count must be positive")
    rng = random.Random(seed)
    start = datetime(2026, 10, 1, tzinfo=timezone.utc)
    result: list[Event] = []
    for i in range(count):
        tenant = f"tenant-{i % 2 + 1}"
        # Deliberately reuse IDs across tenants; only composite IDs are unique.
        tx = f"tx-{i // 2:06d}"
        account = f"customer-{(i // 2) % 11:02d}"
        occurred = start + timedelta(minutes=i * 3)
        captured = 1000 + (i % 17) * 199
        event = Event(
            tenant_id=tenant, event_id=f"capture-{i // 2}",
            transaction_id=tx, account_id=account, kind="CAPTURE",
            amount_cents=captured, occurred_at=occurred,
            ingested_at=occurred + timedelta(minutes=1),
            schema_version=1 if i % 2 == 0 else 2,
        )
        result.append(event)
        if i % 7 == 0:
            # A duplicate delivery has identical business identity, later ingestion time.
            result.append(Event(**{**event.__dict__, "ingested_at": event.ingested_at + timedelta(minutes=2)}))
        if i % 5 == 0:
            refund = Event(
                tenant_id=tenant, event_id=f"refund-{i // 2}", transaction_id=tx,
                account_id=account, kind="REFUND", amount_cents=captured // 5,
                occurred_at=occurred + timedelta(minutes=4),
                # Late discovery: happens on day one, learned several days later.
                ingested_at=occurred + timedelta(days=3), schema_version=2,
            )
            result.append(refund)
    rng.shuffle(result)
    return result
