# ADR-003 — Stateful streaming recovery uses immutable branches

**Status:** Accepted for research prototype (Sprint 03). **Scope:** synthetic transactions, no real money.

## Decision

1. Separate the transport-offset identity `(topic, partition, offset)` from the
   logical event identity `(tenant_id, event_id)`. The first identifies a physical
   Kafka delivery; the latter supports application-level idempotency.
2. Persist the source delivery to Bronze before stateful processing. Do not depend
   on Delta table time travel / CDF retention for a regulated long-term archive.
3. Every stateful branch has an immutable contract: policy version, window size,
   watermark allowance, state schema/operator revision, input and sink identity.
   Changing the contract requires a NEW branch and NEW checkpoint directory.
4. Two operations are different: `resume` starts from a compatible checkpoint;
   `backfill` recomputes from a preserved source snapshot with a fresh identity.
5. A watermark is a trade-off for bounding state, **not a promise that all historical
   records eventually become included**. Late events must remain in Bronze.
6. The local simulator applies the watermark from the preceding committed
   microbatch, updates its high-water mark after completing the batch, and
   creates window state transactionally in SQLite. **Spark may behave differently**
   because of scheduling, event-time ordering, idle sources and operator semantics.
7. Financial invariant and branch-diff checks gate any subsequent promotion.
   Sprint 03 does not promote outputs to production and does not have an agent.

## Invariants

- No physical Kafka coordinate can be silently rewritten in one branch.
- No logical `(tenant,event_id)` can have conflicting business content.
- A committed batch ID is immutable by content digest; retries do not apply again.
- A failed batch rolls back its entire checkpoint + account/window state.
- Tenant IDs always participate in deduplication and financial account keys.
- The state evidence includes hashes for input offsets, batches, state signature
  and the resulting window totals.
- Refuse incompatible resumes instead of deleting or overwriting checkpoints.

## Gaps before production

- Real CDC/archive persistence guarantees, WORM/retention, cross-system audit.
- Precise Spark end-to-end testing on a selected DBR release and access mode.
- Late-data quarantine/DLQ instrumentation for the Spark branch, robust data-quality metrics.
- Transactional/cross-resource policy approval, stronger concurrency controls.
- State migrator and source offset/CDC expiration handling.
- End-to-end Kafka connectivity and authentication with secret management.

## Alternatives not chosen

- Reset an existing production checkpoint: can silently mix incompatible state;
  destructive and not a reproducible backfill.
- Just increase the watermark indefinitely: unbounded/expensive state and no
  guarantee of complete eventual ingestion.
- Deduplicate Kafka offsets only: does not remove repeated business events with
  distinct broker offsets.
