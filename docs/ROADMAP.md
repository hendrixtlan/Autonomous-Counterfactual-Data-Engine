# TemporalX Research Roadmap

## Sprint 01 — Reference oracle

- [x] Deterministic generator and bitemporal replay.
- [x] Broken baseline vs fixed candidate, tenant-scoped reconciliation.
- [x] Spark comparison fixture task (written, **not executed** on Databricks).

## Sprint 02 — Archive and UC Delta research (this release)

- [x] Local durable archive, append-only protection and atomic ingest tests.
- [x] Quarantine, batch digest and immutable run-evidence manifest.
- [x] UC Bronze append-only table code, idempotent fixture seeding, batch IDs.
- [x] UC Silver Delta MERGE code and logical deduplication.
- [x] Reconciliation against the fixed Sprint 01 oracle in Spark (code).
- [x] Sandbox-only publication and Change Data Feed demo (code).
- [ ] Execute and certify Spark/Delta/CDF results in a real Databricks workspace.
- [ ] Kafka/KRaft or production CDC producer and streaming ingestion.
- [ ] Distributed collision detection without collecting all keys on driver.

## Sprint 03 — Streaming state reconstruction

- Kafka + checkpointed Spark Structured Streaming pipelines.
- Explicit checkpoint fork for altered state schema.
- Watermark and out-of-order event failure injection.
- Rebuild from archived source offsets and policy/code/config versions.
- Quantitative correctness and throughput benchmarks.

## Sprint 04 — Counterfactual agentic investigation

- LangGraph diagnostic graph, tool allowlists and observability.
- MLflow trace / experiment lineage linked to replay evidence hashes.
- Policy-controlled, human-approved recovery candidate generation.
- Cost, time-to-recovery, nondeterminism and reliability measurements.


## Sprint 03 — implemented, workspace validation pending
- SQLite atomic stateful microbatch checkpoint and replay branches.
- Separate source-offset identity, business-event deduplication, late watermark quarantine.
- Optional Redpanda Compose + producer and SQLite consumer using broker offsets.
- Databricks Kafka Delta Bronze, watermarked window branches, isolated replay code.
- Explicit boundary: local reference passed, Docker/Databricks not executed.

## Sprint 04 candidate — actual distributed reliability lab
- Integration-test Redpanda + Spark, crash/restart injection, and RocksDB state store.
- Structured Streaming progress metrics and watermark monitoring.
- Validate Delta replay vs reference outputs under known microbatch ordering.
- Flink vs Spark replay benchmark or state-store migration (separate research claim).
