"""Deterministic reference (NOT Spark/RocksDB) for TTL and event-time timers.

Model explicit batch time/watermark; no wall-clock calls or accidental nondeterminism.
It demonstrates semantic risks, not exact internal scheduling of Spark tasks.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Observation:
    tenant_id: str
    account_id: str
    event_id: str
    event_ms: int
    ingestion_ms: int

    def __post_init__(self) -> None:
        if not all((self.tenant_id, self.account_id, self.event_id)) or self.event_ms < 0 or self.ingestion_ms < 0:
            raise ValueError('Invalid observation')


class ProcessingTimeDeduper:
    """Keyed processing-time TTL; duplicates AFTER expiry may be reaccepted."""
    def __init__(self, ttl_ms: int):
        if ttl_ms <= 0:
            raise ValueError('TTL must be positive')
        self.ttl_ms = ttl_ms
        self._seen: dict[tuple[str, str], tuple[int, tuple[str, str, int]]] = {}

    def accept(self, event: Observation, now_ms: int) -> str:
        if now_ms < 0:
            raise ValueError('Negative processing time')
        key = (event.tenant_id, event.event_id)
        identity = (event.tenant_id, event.account_id, event.event_ms)
        existing = self._seen.get(key)
        if existing is not None and existing[0] > now_ms:
            if existing[1] != identity:
                return 'IDENTITY_CONFLICT'
            return 'DUPLICATE'
        self._seen[key] = (now_ms + self.ttl_ms, identity)
        return 'ACCEPTED_AFTER_EXPIRY' if existing is not None else 'ACCEPTED'

    def visible_entries(self, now_ms: int) -> int:
        return sum(expires > now_ms for expires, _ in self._seen.values())


class EventTimeSessionEngine:
    """Reference engine: watermark advancement closes sessions with explicit timers.

    A late event is identified against CURRENT watermark; updates can re-arm timers.
    Separately sourced accounts cannot affect one another.
    """
    def __init__(self, inactivity_gap_ms: int):
        if inactivity_gap_ms <= 0:
            raise ValueError('Gap must be positive')
        self.gap = inactivity_gap_ms
        self.watermark_ms = -1
        self.sessions: dict[tuple[str, str], dict[str, Any]] = {}
        self.closed: list[dict[str, Any]] = []
        self.late: list[str] = []
        self._seen: set[tuple[str, str]] = set()

    def ingest(self, event: Observation) -> str:
        if event.event_ms <= self.watermark_ms:
            self.late.append(event.event_id)
            return 'LATE'
        identity = (event.tenant_id, event.event_id)
        if identity in self._seen:
            return 'DUPLICATE'
        key = (event.tenant_id, event.account_id)
        state = self.sessions.get(key)
        if state is None:
            state = {'event_count': 0, 'first_event_ms': event.event_ms, 'last_event_ms': event.event_ms}
            self.sessions[key] = state
        state['event_count'] += 1
        state['last_event_ms'] = max(state['last_event_ms'], event.event_ms)
        state['first_event_ms'] = min(state['first_event_ms'], event.event_ms)
        self._seen.add(identity)
        return 'ACCEPTED'

    def advance_watermark(self, new_watermark_ms: int) -> list[dict[str, Any]]:
        if new_watermark_ms < self.watermark_ms:
            raise ValueError('Watermark cannot decrease')
        self.watermark_ms = new_watermark_ms
        emitted = []
        for key in sorted(self.sessions):
            state = self.sessions[key]
            deadline = state['last_event_ms'] + self.gap
            if deadline <= self.watermark_ms:
                emitted.append({'tenant_id': key[0], 'account_id': key[1], **state,
                                'timer_ms': deadline, 'closed_by': 'EVENT_TIME_WATERMARK'})
        for value in emitted:
            del self.sessions[(value['tenant_id'], value['account_id'])]
        self.closed.extend(emitted)
        return emitted


def example() -> dict[str, Any]:
    o1 = Observation('blue', 'acct1', 'capture1', 1_000, 1_050)
    o2 = Observation('blue', 'acct1', 'capture1', 1_000, 9_000)
    ttl = ProcessingTimeDeduper(ttl_ms=5_000)
    decisions = [ttl.accept(o1, 1_050), ttl.accept(o2, 2_000), ttl.accept(o2, 9_000)]
    sessions = EventTimeSessionEngine(inactivity_gap_ms=3_000)
    events = [Observation('blue', 'acct1', 'e1', 1_000, 1_000), Observation('blue', 'acct1', 'e2', 2_000, 3_000)]
    for event in events:
        sessions.ingest(event)
    no_close = sessions.advance_watermark(4_900)
    close = sessions.advance_watermark(5_000)
    late_decision = sessions.ingest(Observation('blue', 'acct1', 'too-late', 2_500, 7_000))
    return {'ttl_decisions': decisions, 'sessions_closed_before_deadline': len(no_close),
            'closed_sessions': close, 'late_decision': late_decision}
