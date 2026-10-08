"""Validated immutable events with distinct event and knowledge time."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


class EventContractError(ValueError):
    """A source event does not satisfy our canonical data contract."""


def parse_instant(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        if not isinstance(value, str):
            raise EventContractError("A timestamp must be an ISO-8601 string")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise EventContractError(f"Invalid timestamp: {value}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise EventContractError("Naive timestamps are forbidden; include UTC offset")
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class Event:
    tenant_id: str
    event_id: str
    transaction_id: str
    account_id: str
    kind: str
    amount_cents: int
    occurred_at: datetime
    ingested_at: datetime
    currency: str = "USD"
    schema_version: int = 1

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> Event:
        required = ("tenant_id", "event_id", "transaction_id", "account_id", "kind", "amount_cents", "occurred_at", "ingested_at")
        if any(key not in row for key in required):
            raise EventContractError(f"Missing required fields: {[k for k in required if k not in row]}")
        for key in required[:4]:
            if not isinstance(row[key], str) or not row[key].strip():
                raise EventContractError(f"{key} must be a nonempty string")
        if row["kind"] not in ("CAPTURE", "REFUND"):
            raise EventContractError("kind must be CAPTURE or REFUND")
        if type(row["amount_cents"]) is not int or row["amount_cents"] <= 0:
            raise EventContractError("amount_cents must be a positive integer")
        version = row.get("schema_version", 1)
        if type(version) is not int or version not in (1, 2):
            raise EventContractError("Only schema versions 1 and 2 are supported")
        currency = row.get("currency", "USD")
        if currency != "USD":
            raise EventContractError("Only USD is supported in Sprint 01")
        occurred = parse_instant(row["occurred_at"])
        ingested = parse_instant(row["ingested_at"])
        return cls(tenant_id=row["tenant_id"], event_id=row["event_id"],
                   transaction_id=row["transaction_id"], account_id=row["account_id"],
                   kind=row["kind"], amount_cents=row["amount_cents"],
                   occurred_at=occurred, ingested_at=ingested,
                   currency=currency, schema_version=version)

    def semantic_fingerprint(self) -> tuple[Any, ...]:
        """Retry metadata such as ingestion time is intentionally excluded."""
        return (self.tenant_id, self.event_id, self.transaction_id, self.account_id,
                self.kind, self.amount_cents, self.occurred_at, self.currency)

    def to_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id, "event_id": self.event_id,
            "transaction_id": self.transaction_id, "account_id": self.account_id,
            "kind": self.kind, "amount_cents": self.amount_cents,
            "occurred_at": self.occurred_at.isoformat(),
            "ingested_at": self.ingested_at.isoformat(),
            "currency": self.currency, "schema_version": self.schema_version,
        }
