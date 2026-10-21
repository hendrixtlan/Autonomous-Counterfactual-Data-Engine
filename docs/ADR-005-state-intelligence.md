# ADR-005 — Versioned Stateful Intelligence & Change Safety

Status: **ACCEPTED for research**. Spark behavior: **requires actual Databricks validation**.

## Context

Streaming recovery and versioned state involve three different dimensions:

1. *Physical compatibility*: can a checkpoint be deserialized by the new operator?
2. *Semantic compatibility*: can state computed under old logic be trusted for the new output?
3. *Business equivalence*: do reconstructed outputs reconcile with an immutable source of truth?

Spark and Databricks can help with physical schema evolution. They do **not** prove (2) or (3).

## Decision

- Maintain a signed-or-versioned state contract per stream outside the checkpoint: source/sink identity, grouping keys, shuffle partitions, operator ID, time mode, state variables, encoding, logic revision, watermark.
- Compare existing and proposed contracts with `assess_upgrade` before any deployment. The Python gate is **only advisory**, never a deployment authorization.
- Classify `BLOCK` for source identity, output identity, keys, operator name, time mode, number of shuffle partitions or state encoding changes.
- Classify `REBUILD_REQUIRED` for logic revision, TTL duration, watermark, incompatible fields or removal of state variables. Create a **new checkpoint/branch**, replay Bronze, validate against independent oracle.
- Classify `REVIEW_FOR_RESUME` for identical schemas, compatible widening or addition of nullable fields under Avro and adding a state variable; require explicit human approval and an isolated checkpoint test.
- Invariants (uniqueness, reconciliation, completeness, tenant isolation) must pass before promoting recovery. Neither AI nor CLI writes production repairs automatically.

## TTL vs Timers

- `ProcessingTime`: `ValueState`, `MapState` or `ListState` TTL, automatic silent expiry, and processing-time timers. Updating values refreshes TTL for that value (map entries individually).
- `EventTime`: explicit timers evaluated against the watermark; **no TTL**. Session closure emits a record and clears state only upon a fired timer.
- If TTL dedup key retention is shorter than the maximum duplicate replay horizon, duplicates **can become accepted again**. Therefore TTL dedup is not enough to guarantee payment idempotency.
- For strict financial identity guarantees, back uniqueness with an external authoritative dedup/transaction ledger and correct sink idempotency; this lab never processes real payments.

## Why the local reference is useful (and insufficient)

It uses explicit processing times and watermark steps for deterministic tests without Spark. It does **not** implement Spark task scheduling, watermark advancement details, partition placement, native RocksDB, or Delta transactions. Real end-to-end recovery and schema evolution require Databricks runtime testing.

## Deployment guardrails

- Databricks Runtime >= 17.3 LTS recommended for row-based `transformWithState` and RocksDB default provider. Check actual supported compute mode.
- `spark.sql.streaming.stateStore.encodingFormat=avro` before creating the initial checkpoint if schema evolution will be tested.
- No production catalog/schema; isolated UC Volume per experiment branch, one active query per checkpoint. No deletion or recreation of existing checkpoint in this lab.
- `rate` is a live synthetic source, **not replayable historical evidence**. Run true deterministic recovery from Bronze from Sprint 02/03.
- Store progress samples and acceptance evidence separately; checkpoint retention does not guarantee unlimited historical inspection.
- For schema evolution v1→v2, deploy changed processor code in a controlled staged test. Merely supplying fixture JSON contracts does not migrate a state store.

## Official references (checked Oct 8, 2026)

- https://docs.databricks.com/aws/en/stateful-applications
- https://docs.databricks.com/aws/en/stateful-applications/schema-evolution
- https://docs.databricks.com/aws/en/structured-streaming/read-state
- https://docs.databricks.com/aws/en/structured-streaming/checkpoints
- https://docs.databricks.com/aws/en/structured-streaming/stream-monitoring
