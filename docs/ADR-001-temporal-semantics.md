# ADR-001: Bitemporal, multi-tenant replay semantics

**Status:** Accepted for Sprint 01. This is a research reference, not a payment ledger.

## Decision

Every immutable input record carries two separate timestamps:

- `occurred_at`: when the business event occurred, in UTC (valid/event time).
- `ingested_at`: when this specific delivery became visible, in UTC (knowledge time).

A snapshot `(valid_at=T, known_at=K)` includes events where
`occurred_at <= T AND ingested_at <= K`. Without a bound the respective
axis is unlimited. This permits evaluating "what had occurred by T, according
to data the system knew by K".

## Identity and replay

Logical event identity is `(tenant_id, event_id)`, not transaction alone.
Retransmissions may have a different ingestion timestamp but *must* share
business content. Collisions fail closed. Physical delivery order is arbitrary;
results are independent of this order. The reference engine sorts visible
messages deterministically, deduplicates **v2** by event identity, and sums
signed CAPTURE/REFUND amounts per transaction and account.

Policy **v1** is intentionally defective: it counts every physical delivery,
including retries. It exists strictly to reproduce an incident in the lab.

## Invariants

1. An event ID cannot map to two different business payloads in one tenant.
2. A transaction must keep its owning account.
3. Transaction net captured amount cannot be negative.
4. Total account balance equals total transaction net balance.
5. Replay of a fixed set with v2 is independent of physical message order.
6. Tenants are segregated by composite keys, even when raw IDs overlap.

## Important limits / future improvements

- `occurred_at` and `ingested_at` describe event and delivery times; real
  business-effective corrections can require **additional validity intervals**.
- A negative running balance at an intermediate event-time instant is not
  checked independently; only the snapshot's final net balance is checked.
- The current reference replay runs in memory and is not a production
  transaction system. No transaction lifecycle or cross-currency settlement.
- A source must archive original events *independently* of Delta version/CDF
  retention. A checkpoint alone is not an archival source of truth.
- The Spark job validates account aggregates on the checked-in demo fixture.
  Next sprint adds UC Delta ingestion, event-time bounds, conflict quarantine,
  transaction-level invariants and property-based distributed tests.
- No autonomous changes to production tables are permitted at this stage.
