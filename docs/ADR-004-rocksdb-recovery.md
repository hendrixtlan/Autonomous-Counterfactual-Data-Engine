# ADR-004: RocksDB state recovery and experimentation

Status: experimental. 2026-10-08.

## Context

Large keyed state in Structured Streaming can pressure JVM heap and make
streaming microbatch latency inconsistent. RocksDB stores most keyed state in
native memory and local files, while Spark persists state changes to recoverable
checkpoints. RocksDB is not an independent system of record for TemporalX.

## Decision

1. Archive events in append-only Bronze independent of checkpoint retention.
2. Use a stateful dedup operator backed by RocksDB for a genuine state-store workload.
3. Compare changelog and snapshot checkpoints using **different branches** for
   controlled benchmarking, not a mutable shared checkpoint.
4. Pin partitioning, state operator revision, source identity, sink, watermark,
   cache/memory policy and runtime in provenance. A branch fingerprint protects
   against accidental cross-policy checkpoint reuse. The local manifest is
   filesystem-atomic only; Databricks registry assumes a single writer.
5. Use business-event IDs scoped to tenant for logical dedup. Treat Kafka
   topic/partition/offset as physical delivery identity, never business identity.
6. Inject failure between durable sink publication and checkpoint completion;
   durable Delta `txnAppId`/`txnVersion` protects replays within log-retention
   limits. Verify final sink content after retry.
7. Query metrics and state metadata; missing metrics are null and not zero.
8. Never manually alter RocksDB's SST files or checkpoint internals.

## Distinguish the recovery cases

- **Retry same microbatch:** Spark and an idempotent sink should avoid duplicate
  durable outcomes after failure before checkpoint completion.
- **Resume from compatible checkpoint:** process remaining source data under the
  same state schema, provider, operator semantics, and query graph.
- **Rebuild from Bronze:** allocate a new branch and recompute when semantics,
  source history, or required state evolved incompatibly.

## Consequences and unresolved questions

Changelog checkpointing reduces synchronous checkpoint overhead in many
workloads but can change recovery characteristics. TTL and watermark retention
can discard keys, after which replays beyond the retention bound might be
accepted as new events. Permanent deduplication requires a durable business
identity registry or sufficiently broad archival reconciliation, not infinite
RocksDB state. `transformWithState` (ValueState/MapState/ListState) enables
customized expiration and timers and will form Sprint 05. Different DBR
versions have different state schema evolution and format support.

This experiment is not a financial ledger and does not establish production
exactly-once across arbitrary external systems.
