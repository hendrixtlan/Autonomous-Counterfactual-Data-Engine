# Sprint 04 — RocksDB Failure, Tuning and Forensics Lab

## What is tested versus proposed

| Capability | Implementation | Execution status |
|---|---|---|
| Preflight branch/checkpoint fingerprint protection | `src/temporalx/rocksdb_lab.py` | Automated unit tests run locally |
| Spark StreamingQueryProgress RocksDB metrics parser | `rocksdb_metrics_report.py` | Automated tests on **synthetic sample metrics**, NOT measured Spark |
| Deterministic duplicate-delivery generator | `prepare_rocksdb_fixture.py` | Automated unit tests run locally |
| Spark stateful dedup with RocksDB | `rocksdb/local_spark.py` | Code supplied; needs real PySpark to execute |
| Crash after sink commit before checkpoint | `rocksdb/local_spark.py` | Code supplied; needs real Spark to verify |
| Databricks state store + Delta `txnAppId`/`txnVersion` sink | `rocksdb/databricks_experiment.py` | Code supplied; needs real DBR/UC workspace to verify |
| State forensics read-only `statestore` / `state-metadata` | `rocksdb/inspect_checkpoint.py` | Code supplied; needs DBR permissions to verify |
| RocksDB backend performance/restore time in production | Not measured | Not yet measured; do not quote sample metrics as benchmark |

## Level A — run tests and metrics parser without Spark

```bash
python -m pip install -e '.[test]'
python -m pytest -q
python scripts/rocksdb_metrics_report.py --input fixtures/rocksdb/sample_progress.json \
  --output artifacts/example-report.json
```

**WARNING**: `fixtures/rocksdb/sample_progress.json` is FAKE DATA built to exercise parsing. It is not an actual Spark execution and cannot support performance conclusions.

## Level B — local real Spark (when PySpark >=3.5 and appropriate Java are installed)

```bash
python -m pip install -e '.[test,spark]'
python scripts/prepare_rocksdb_fixture.py --output artifacts/rocksdb-input --accounts 20 --chunks 2

# Trigger an intentional crash AFTER atomic fixture sink publication.
python rocksdb/local_spark.py --input artifacts/rocksdb-input \
  --root artifacts/rocksdb-runs --branch crash_a --strategy changelog \
  --fail-after-batch 0
# The preceding run MUST fail. DO NOT delete the checkpoint or output.

# Retry exactly the same checkpoint, no crash flag.
python rocksdb/local_spark.py --input artifacts/rocksdb-input \
  --root artifacts/rocksdb-runs --branch crash_a --strategy changelog

python scripts/rocksdb_metrics_report.py \
  --input artifacts/rocksdb-runs/metrics/crash_a.json
python scripts/verify_rocksdb_sink.py --input artifacts/rocksdb-input \
  --accepted artifacts/rocksdb-runs/accepted/crash_a
```

The first crash occurs inside a custom local filesystem sink. A restart tests Spark's ability to replay the unfinished batch and recognizes the pre-existing sink file by matching bytes. This is an experiment with a single local writer and atomic rename, **not** a generalized cross-system exactly-once guarantee. Spark may produce additional empty batches; examine the progress records and source batch counts, not only output file count.

**Controlled A/B:** use identical immutable input files and new branch names:

```bash
python rocksdb/local_spark.py --input artifacts/rocksdb-input \
  --root artifacts/rocksdb-runs --branch snap_a --strategy snapshot
python rocksdb/local_spark.py --input artifacts/rocksdb-input \
  --root artifacts/rocksdb-runs --branch change_a --strategy changelog
```

Measure `rocksdbCommitCheckpointLatency`, `rocksdbSstFileSize`, `rocksdbWriterStallLatencyMs`, trigger latency, and recovery time. The runbook's only result for now is the **design**, not the result of that A/B benchmark. Do at least 3 independent warm runs per configuration, control input keys, cores, memory and partitions; reserve an unseen set for validation.

For stress use the efficient Parquet sink instead of the tiny driver-collecting atomic sink:

```bash
python scripts/prepare_rocksdb_fixture.py \
  --output artifacts/rocksdb-100k --accounts 100000 --chunks 3
python rocksdb/local_spark.py --input artifacts/rocksdb-100k \
  --root artifacts/rocksdb-stress --branch load_100k --strategy changelog --sink parquet
```

The stress generator's files MUST be created in an empty directory and should be generated once, then reused identically for A/B. The local FS sink remains test code; no cloud credentials are needed for this level. If the initial streaming query fails because of inadequate Java/Spark memory, allocate more resources or reduce accounts.

**Compatibility:** spark's `dropDuplicatesWithinWatermark` requires Spark >= 3.5. RocksDB provider classname and JVM libraries must be present. Spark 4.x uses Java 17+; check compatibility with your installation. This repo has not executed this path in the current build environment.

## Level C — Databricks state and Delta sink (requires UC and DBR)

1. Existing isolated Unity Catalog lab catalog/schema and a writable UC Volume path.
2. Synthetic data already ingested into `<catalog>.<schema>.temporalx_s03_bronze_kafka` (Sprint 03). No production table.
3. Existing cluster with DBR 17.3 LTS or higher recommended; have permission to create research tables. Deploy Python package `temporalx-lab` or ensure the synced `src/` directory is available beside the bundled script.
4. Deploy bundle (`databricks bundle validate` and `deploy`) with the same `cluster_id`, `catalog`, `schema`, `checkpoint_root` variables as Sprint 03.

```bash
# After configured variables / bundle deploy
# Run separately to avoid concurrent resource and input contention.
databricks bundle run -t dev temporalx_sprint04_changelog
databricks bundle run -t dev temporalx_sprint04_snapshot
```

Both branches create their own isolated Delta output and checkpoints. Measure on the same controlled Bronze snapshot. If you append events between A/B runs they will not be comparable. Delta transactions use `txnAppId` and `txnVersion` to protect against a repeated `foreachBatch` **within the documented transaction retention window**; they do not atomically commit a Kafka offset and an arbitrary downstream API.

**Explicit failure trial (via a one-off task invocation):** run `databricks_experiment.py ... --branch crash_dbx --strategy changelog --fail-after-batch 0`; expect failure after sink commit. Run again with **all the same variables but** `--fail-after-batch -1`. Never reset the checkpoint or reuse the branch with different state settings. The bundle jobs do NOT inject crashes automatically.

**Inspect the actual state** (DBR capabilities/permissions required):

```bash
# In a Databricks notebook attached to compatible dedicated-mode compute:
spark.read.format('state-metadata').load('/Volumes/CATALOG/SCHEMA/VOLUME/checkpoints/s04/rocksdb_change_a').show(truncate=False)
spark.read.format('statestore').load('/Volumes/CATALOG/SCHEMA/VOLUME/checkpoints/s04/rocksdb_change_a').printSchema()
```

`rocksdb/inspect_checkpoint.py` provides a read-only CLI for inspecting synthetic state and avoids dumping key/value data unless explicitly requested. Inspect a stable checkpoint or a copy where necessary.

## RocksDB knobs for controlled factorial experiments

- `max_memory_mb`: shared per-node RocksDB memory budget, **soft** limit; not the Spark executor heap size.
- `block_cache_mb`: per-state-instance block cache budget; increasing may reduce read misses but consumes off-heap/native memory.
- `write_buffer_mb`: RocksDB memtable buffer; increasing can improve ingest until flush pressure or native memory contention dominates.
- `compact_on_commit`: may reduce file fragmentation, at the cost of additional per-batch commit latency; evaluate rather than enabling blindly.
- `track_total_rows`: disabling may improve writes at the expense of reported `numRowsTotal` accuracy; zero is **not** a reliable 'empty state' signal when disabled.
- `strategy`: `snapshot` vs `changelog`; branch fingerprint intentionally requires isolation for an A/B test although Spark supports compatible mode transitions in some runtimes.

Run only one variable at a time at first, and record Spark/DBR version, input hash, partitions, instance type, and checkpoint retention. Compare median/p95 across repeated independent trials.

## Known limitations / failure safety

- The baseline generator does not exercise real executor eviction or Kubernetes pod termination; a cluster fault-injection framework is later work.
- The local Spark path has *no* built-in Kafka broker and uses readStream JSON files with persistent RocksDB state; actual Kafka + RocksDB is the Databricks research path from S03 Bronze.
- Databricks contract manifest in Delta uses check-then-insert and requires **single active writer per branch**; it is not an atomic distributed lock.
- The local branch guard protects configuration fingerprint, not unauthorized file edits or simultaneous writers. Do not place actual production credentials or sensitive state in these samples.
- The `statestore` reader is read-only, *not* an API for manual alteration of checkpoint files. Never patch SST files by hand or copy checkpoint contents to another incompatible query.
- No TTL/`transformWithState` is implemented in Sprint 04. They are explicitly scoped for Sprint 05 and require careful state semantics and runtime verification.

## Primary vendor documentation

- https://docs.databricks.com/aws/en/structured-streaming/rocksdb-state-store
- https://docs.databricks.com/aws/en/structured-streaming/stream-monitoring
- https://docs.databricks.com/aws/en/structured-streaming/read-state
- https://docs.databricks.com/aws/en/stateful-applications
- https://docs.databricks.com/aws/en/stateful-applications/schema-evolution

## Recovery with new source data, and the watermark boundary attack

To **force a subsequent streaming run to restore checkpoint state and process new files** (rather than merely start an empty query), append a different source file:

```bash
python scripts/append_recovery_batch.py --input artifacts/rocksdb-input \
  --batch-name new_restart1 --count 20
python rocksdb/local_spark.py --input artifacts/rocksdb-input \
  --root artifacts/rocksdb-runs --branch crash_a --strategy changelog
python scripts/verify_rocksdb_sink.py --input artifacts/rocksdb-input \
  --accepted artifacts/rocksdb-runs/accepted/crash_a
```

This tests preservation of prior keys and acceptance of new keys after restart. Do not edit or remove already-observed input files. Compare restore timings from driver logs and `recentProgress`, accounting for the small-state/noisy-local-benchmark limitation.

**Adversarial scenario**: generate more than ten 1-minute-spaced batches that reuse the same event IDs, while `withWatermark(..., '10 minutes')` governs state retention. When an identity reappears outside the supported deduplication horizon it **might be accepted again**, depending on operator semantics and source admission. Run the verifier; any duplicates must fail. This motivates durable business identity reconciliation rather than treating RocksDB as an infinite dedup index.

```bash
python scripts/prepare_rocksdb_fixture.py --output artifacts/rocksdb-retention-attack \
  --accounts 100 --chunks 15
python rocksdb/local_spark.py --input artifacts/rocksdb-retention-attack \
  --root artifacts/rocksdb-retention --branch retention_a --strategy changelog
python scripts/verify_rocksdb_sink.py --input artifacts/rocksdb-retention-attack \
  --accepted artifacts/rocksdb-retention/accepted/retention_a
```

A failing verification here is **a meaningful detection**, not necessarily a Spark defect: deduplication within a configured watermark deliberately does not promise permanent uniqueness. Run this only in the synthetic lab. Capture actual Spark progress to distinguish eviction from late-row filtering.
