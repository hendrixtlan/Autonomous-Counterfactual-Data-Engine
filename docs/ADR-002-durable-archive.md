# ADR-002 — Durable delivery archive and replay provenance

**Status:** Accepted for research Sprint 02; production design requires review.

## Context

Recovering only Silver state destroys evidence of transport duplicates. A
historical truth engine must retain the physical deliveries and separate them
from logical event identities. Delta CDF is a change transport, not a durable
indefinite source event archive.

## Decision

1. Bronze persists every validated physical delivery. Identity is
   `(source_batch_id, message_ordinal)`, not `event_id`.
2. Valid business identity is `(tenant_id, event_id)` with a stable semantic
   fingerprint excluding retry arrival metadata and schema version.
3. Conflicting business content behind the same event identity **fails closed**.
4. Silver retains one logical event and earliest `ingested_at` per business ID,
   plus physical-delivery count. It can be updated without erasing Bronze.
5. Every local replay run has a fixed ID and SHA-256 evidence digest covering
   batch sources, visible deliveries, algorithm version and result.
6. The Databricks research job creates a replay manifest containing a digest
   of source batches, the filtered delivery IDs, code bytes, policy versions
   and temporal cutoffs, with fail-closed `run_id` collision checks.
7. All events use signed-integer cents to prevent decimal precision drift.
8. `occurred_at` and `ingested_at` are normalized to UTC; the Spark experiment
   stores ISO strings to match the checked-in reference fixture. Production
   implementation should store typed timestamps and source offsets.
9. The sandbox-only published ledger must be explicitly requested and is not
   allowed to write to a real business schema.

## Guarantees actually tested locally

- Single-transaction SQLite batch append or rollback.
- SQLite trigger blocks UPDATE and DELETE on deliveries and replay evidence.
- Duplicate batch submission yields stable counts and unchanged deliveries.
- Two distinct batches preserve identical physical messages separately.
- Schema-invalid JSON is quarantined; it never affects transaction totals.
- Conflicting logical identity aborts the entire candidate batch.
- Replay evidence hash detects attempted `run_id` reuse after different inputs.

## Deliberate non-guarantees

- SQLite triggers are not cryptographic WORM and privileged database users can
  remove them; use object-locking storage for regulated durability.
- A hash without signed timestamp/notarization does not establish provenance
  against a privileged malicious operator.
- No external side-effect replay, CDC offsets, Kafka KRaft, streaming job
  checkpoints, or state-store rebuild exists in this sprint.
- Databricks runtime, cluster grants, CDF semantics and serverless behavior
  remain untested until actual execution in a workspace.
- There is no transaction across multiple Delta tables. Concurrency is
  restrained by a one-at-a-time demo job, not solved generally.

## Next step

Sprint 03 must implement a replay fork with its own checkpoint, reconcile
source offsets and code/config SHA, and evaluate failure injection under
watermarks. Only after that can we claim real streaming state reconstruction.
