import random
from datetime import timedelta

import pytest

from temporalx.engine import InvariantViolation, ReplayConflictError, counterfactual, replay
from temporalx.generator import generate_events
from temporalx.model import Event, EventContractError


def test_bug_reproduced_and_corrected():
    events = generate_events(1)
    comparison = counterfactual(events)
    assert comparison["baseline"]["account_balances"]["[\"tenant-1\",\"customer-00\"]"] == 1800
    assert comparison["candidate"]["account_balances"]["[\"tenant-1\",\"customer-00\"]"] == 800
    # Late refund is accounted for in both policies after it becomes known.
    assert comparison["total_delta_cents"] == -1000
    assert comparison["candidate"]["duplicate_messages"] == 1


def test_replay_stable_under_permutation():
    events = generate_events(200, 123)
    expected = counterfactual(events)
    for seed in range(5):
        current = events[:]
        random.Random(seed).shuffle(current)
        assert counterfactual(current) == expected


def test_idempotent_duplicate_delivery():
    events = generate_events(20)
    expected = replay(events, "v2")
    repeated = replay(events + events, "v2")
    assert expected.account_balances == repeated.account_balances
    assert repeated.applied_events == expected.applied_events


def test_temporal_cutoffs_visibility():
    events = generate_events(1)
    capture = next(e for e in events if e.kind == "CAPTURE")
    refund = next(e for e in events if e.kind == "REFUND")
    known_early = replay(events, "v2", known_at=capture.ingested_at + timedelta(hours=1))
    known_late = replay(events, "v2", known_at=refund.ingested_at)
    assert sum(known_early.account_balances.values()) == 1000
    assert sum(known_late.account_balances.values()) == 800
    valid_early = replay(events, "v2", valid_at=capture.occurred_at + timedelta(minutes=2))
    assert sum(valid_early.account_balances.values()) == 1000


def test_tenant_isolation_even_with_reused_ids():
    results = replay(generate_events(2), "v2")
    assert len(results.account_balances) == 2
    assert sum(results.account_balances.values()) == 800 + 1199


def test_collision_fails_closed():
    events = generate_events(1)
    capture = next(e for e in events if e.kind == "CAPTURE")
    corrupt = Event(**{**capture.__dict__, "amount_cents": capture.amount_cents + 1})
    with pytest.raises(ReplayConflictError):
        replay(events + [corrupt], "v2")


def test_over_refund_invariant_fails_closed():
    event = next(e for e in generate_events(1) if e.kind == "REFUND")
    with pytest.raises(InvariantViolation):
        replay([event], "v2")


def test_rejects_naive_timestamps():
    event = generate_events(1)[0].to_dict()
    event["occurred_at"] = "2026-10-01T00:00:00"
    with pytest.raises(EventContractError):
        Event.from_dict(event)


def test_rejects_bool_as_integer_amount():
    event = generate_events(1)[0].to_dict()
    event["amount_cents"] = True
    with pytest.raises(EventContractError):
        Event.from_dict(event)


def test_no_cross_tenant_id_collision_on_conflict():
    first = next(e for e in generate_events(1) if e.kind == "CAPTURE")
    second = Event(**{**first.__dict__, "tenant_id": "other", "amount_cents": 2000})
    result = replay([first, second])
    assert sum(result.account_balances.values()) == 3000


def test_schema_migration_default_currency():
    first = generate_events(1)[0].to_dict()
    first.pop("currency")
    first.pop("schema_version")
    assert Event.from_dict(first).currency == "USD"


def test_v1_and_v2_converge_without_duplicates():
    events = [e for e in generate_events(17) if e.kind != "CAPTURE" or e.ingested_at.minute % 3 != 0]
    # Use unique event IDs instead of relying on which captures happen to match.
    unique = {}
    for event in events:
        unique.setdefault((event.tenant_id, event.event_id), event)
    events = list(unique.values())
    assert replay(events, "v1").account_balances == replay(events, "v2").account_balances
