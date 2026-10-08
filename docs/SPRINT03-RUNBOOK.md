# Sprint 03 — Stateful streaming and historical recovery runbook

The project has THREE execution levels. Do not mistake an executed local simulator
for an executed Databricks streaming job.

## Level A — fully executed local reference (no external services)

```bash
python -m pip install -e '.[test]'
python -m pytest -q
PYTHONPATH=src python scripts/local_stream_demo.py --db artifacts/stateful-s03.db
```

The local demo processes two fixture files in order, using the watermark from the
PREVIOUS microbatch and 5-minute tumbling windows. Expected blue/account-1:

- `stream-v1`: 25,500 cents (captures include duplicated $100, late refund excluded).
- `stream-v2`: 15,500 cents (duplicate removed, late refund excluded).
- `backfill-v2`: 12,500 cents (deduplicated and includes late refund).

Other tenant `green/account-1` contributes 7,000 cents in every branch.
The evidence hashes are deterministic over the fixture and branch state.
The SQLite model is intentionally a reference simulator, **not Spark's state store**.

## Level B — local Kafka broker (code supplied; not executed here)

Prerequisites: Docker/Compose with enough memory and optional Kafka client:

```bash
python -m pip install -e '.[kafka]'
docker compose up -d
docker compose exec redpanda rpk topic create temporalx.transactions -p 2

# This order reproduces the two delivery batches:
python scripts/produce_kafka.py --batch 01
python scripts/consume_to_local.py --batch-id source01
python scripts/produce_kafka.py --batch 02
python scripts/consume_to_local.py --batch-id source02

docker compose down
```

CAUTION: Consumer commits to Kafka **after** its SQLite transaction. This is
at-least-once delivery with application dedup, not a cross-system transaction.
Use fresh Docker volumes and database for repeated experiments; after
`docker compose down -v` you irrevocably delete synthetic broker data.

Do not run the consumer for `v1` and `v2` with the same Kafka group. Each group
owns its own source offsets and branch. Use `--group distinct-group --branch other`
when running a separate policy. Do not alter an existing branch policy.

A local Docker port **is not reachable from a remote Databricks workspace**.
You need a network-accessible broker with correctly advertised addresses and
appropriate secure connectivity to run the next level.

## Level C — Databricks Spark + UC Delta (requires external validation)

Preconditions:

1. An existing workspace with a suitable Databricks Runtime (target 14.3 LTS+
   with `dropDuplicatesWithinWatermark` support), an existing cluster, and
   Unity Catalog catalog/schema for disposable research use.
2. A pre-created UC Volume at `/Volumes/CATALOG/SCHEMA/VOLUME/checkpoints`
   where the job can write separate checkpoint directories.
3. A **Databricks-reachable**, authenticated and encrypted Kafka-compatible
   broker for real use. This demo uses plaintext options only: lab brokers only.
4. A dedicated synthetic Kafka topic, retained until both source ingestion and
   replay have completed. No production data.

```bash
# Replace ALL markers with your actual Databricks resources.
# Preserve the SAME checkpoint_root and branch names between phase 1 and 2.
databricks bundle validate -t dev \
  --var 'cluster_id=CLUSTER_ID' --var 'catalog=CATALOG' \
  --var 'schema=SCHEMA' \
  --var 'checkpoint_root=/Volumes/CATALOG/SCHEMA/VOLUME/checkpoints' \
  --var 'kafka_bootstrap=HOSTNAME:9092'

databricks bundle deploy -t dev \
  --var 'cluster_id=CLUSTER_ID' --var 'catalog=CATALOG' \
  --var 'schema=SCHEMA' \
  --var 'checkpoint_root=/Volumes/CATALOG/SCHEMA/VOLUME/checkpoints' \
  --var 'kafka_bootstrap=HOSTNAME:9092'
```

In a real workspace, use the same variables to call the 3 named jobs below.
(Commands shown without repeating overrides assume you configured them in your
bundle target.)

**Phase 1:** publish batch01 to cloud-reachable Kafka; run:

```bash
databricks bundle run -t dev temporalx_sprint03_capture
databricks bundle run -t dev temporalx_sprint03_state
```

**Phase 2:** publish batch02; run the SAME capture and state jobs again. This
resumes compatible checkpoints and should expose the late-arrival limitation.

**Recovery:** run `temporalx_sprint03_recovery` once. It constructs a separate
Delta table (default branch `repair_001`) from all valid events in Bronze,
using `(tenant_id,event_id)` deduplication with a business conflict check.
It never resets or overwrites streaming output. Running recovery again with the
same branch intentionally fails; choose a fresh, approved branch name.

**Inspect using SQL:**

```sql
SELECT parse_status, count(*)
FROM CATALOG.SCHEMA.temporalx_s03_bronze_kafka
GROUP BY parse_status;

SELECT * FROM CATALOG.SCHEMA.temporalx_s03_windows_baseline_v1;
SELECT * FROM CATALOG.SCHEMA.temporalx_s03_windows_candidate_v2;
SELECT * FROM CATALOG.SCHEMA.temporalx_s03_backfill_repair_001;
```

**Limitations:** real Spark watermark outcomes depend on actual microbatch
boundaries, source order and window finalization. `AvailableNow` processes what
is available, not a controlled single Spark microbatch. An output window in
append mode is withheld until watermarks finalize it. The precise local example
is an acceptance oracle for the SIMULATOR, not proof of the same Spark outputs.

The Spark job uses `MERGE` into a single Bronze Delta table on source offset
coordinates. This helps retried batch idempotency but is not a general
end-to-end exactly-once guarantee. Invalid JSON is retained in the SAME Bronze
table with `parse_status=invalid`; there is no mutating external DLQ yet.

**Safety:** Do not point to production namespace, tables, Kafka topics,
checkpoint paths, or agent tool connections. Databricks deployment has NOT
been executed or benchmarked in the current environment.

## Official documentation

- https://docs.databricks.com/aws/en/structured-streaming/checkpoints
- https://docs.databricks.com/aws/en/structured-streaming/watermarks
- https://docs.databricks.com/gcp/en/connect/streaming/kafka/
- https://docs.databricks.com/aws/en/structured-streaming/triggers
- https://docs.redpanda.com/labs/docker-compose/single-broker/
