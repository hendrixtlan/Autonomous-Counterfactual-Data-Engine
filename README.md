# TemporalX — Autonomous Counterfactual Data Engine

**Sprint 08 (v0.8.0): Databricks Lakehouse Modernization + Distributed State Diff.**
The complete Sprints 01–07 are retained. This sprint adds two research tracks:

1. **Enterprise Lakehouse modernization:** an Azure SQL → SSIS → ADF *representative migration assessment*, synthetic CDC export fixtures, a real-code Databricks Auto Loader ingestion job, Delta Bronze/Silver/Gold transformations, source-LSN idempotent `MERGE`, a read-only CDF audit, Unity Catalog least-privilege SQL, lineage inspection, and layout/performance experiment configurations (Photon, partitioning, ZORDER, liquid clustering).
2. **Microbatch time travel:** a deterministic offline timeline oracle and an authored **distributed** State Reader change-feed comparator using Spark joins/windows rather than a full-state `collect()`. A checkpoint must be isolated, retained and provenance-aligned; the comparator cannot authorize repair.

### Sprint 08 — local verification (no Databricks account required)

```bash
python -m pip install -e '.[test]'
python -m pytest -q
python s08/assess_migration.py --out artifacts/s08/migration-report.json
python scripts/time_travel_lab.py demo --out-dir artifacts/s08-time-travel
```

Synthetic migration result: seven physical CDC deliveries → four Silver business-event identities → two Gold account aggregates (**blue: 16,000 cents; red: 7,000 cents**). A synthetic time-travel anomaly first appears in microbatch **2** and heals by batch **4**; a final-state-only comparison would miss it.

**Honest test boundary:** the Python reference path has been exercised locally. **No Databricks workspace, actual Auto Loader, Photon, Delta CDF, UC lineage, native RocksDB or state reader has been executed for Sprint 08 here.** Source migrations are representative mappings only, not a real SSIS/ADF deployment. Databricks jobs require an explicitly pre-provisioned `lab_` or `temporalx_lab` UC schema and authorized source landing; no production cutover is authorized. Prior sprints' Databricks code also remains unvalidated unless otherwise stated.

**Docs:** [Sprint 08 runbook](docs/SPRINT08-RUNBOOK.md), [ADR-008](docs/ADR-008-lakehouse-modernization.md), [synthetic migration manifest](s08/fixtures/legacy_migration.json), [T-SQL source](s08/sql/legacy_azure_sql.sql) vs [Spark SQL example](s08/sql/translated_spark.sql), [benchmark matrix](s08/benchmark_matrix.yml), [checkpoint time travel core](src/temporalx/time_travel.py), [distributed state diff](s08/distributed_diff.py).

---


**Sprint 07 (v0.7.0): State Reconstruction & Forensic Diff.** The existing Sprints
01–06 are retained. New features: strict state provenance contract, tamper-evident
logical state bundles, deterministic replay oracle, key-by-key forensic diff,
operator-schema projection, tests for missing/phantom/wrong accounts, and a
three-stage Databricks research workflow (`freeze Delta -> RocksDB state ->
read-only checkpoint compare`). No repair/promotion occurs automatically.

```bash
python -m pip install -e '.[test]'
python -m pytest -q
python scripts/forensic_diff.py demo --out-dir artifacts/s07-demo
python scripts/forensic_diff.py compare \
  --reference artifacts/s07-demo/reference.json \
  --observed artifacts/s07-demo/observed-synthetic.json \
  --out artifacts/s07-demo/rechecked-report.json
```

**Local fixture result:** 8 matching, 1 missing, 1 phantom and 1 different
amount. The observed checkpoint is **synthetic**, not an actual RocksDB export.
The Databricks bundle and `s07/databricks_lab.py` are authored but **NOT run on
a Databricks cluster in this environment**. Actual state reader schemas must be
validated in the workspace and mapped explicitly. The lab caps state evidence
at 10,000 rows and uses isolated UC tables/Volumes with no production writes.

See [Sprint 07 runbook](docs/SPRINT07-RUNBOOK.md),
[ADR-007](docs/ADR-007-state-forensic-diff.md),
[local test report](docs/SPRINT07-TEST-REPORT.md) and
[JSON schema](schemas/forensic-account-state-v1.schema.json).

---

**Sprint 06 (v0.6.0): Evidence-first State Causality & Autonomous Recovery Research.** Contains **all Sprint 01–05 artifacts**. The core incident investigator now produces deterministic, SHA-256-anchored evidence reports, tenant-isolated policy replays, RocksDB progress assessments, and fail-closed repair recommendations. Optional LangGraph is a real orchestrator of the same tools; no LLM-based causal certainty is claimed.

### Run the new incident study

```bash
python -m pip install -e '.[test]'
python -m pytest -q
python scripts/causality_investigate.py --incident-dir fixtures/causality/incident-001 --out reports/my-investigation.json
python scripts/causality_investigate.py --incident-dir fixtures/causality/incident-001 --verify reports/my-investigation.json
# Optional real LangGraph compiled graph (requires installation):
python -m pip install -e '.[agent]'
python scripts/causality_investigate.py --engine langgraph --incident-dir fixtures/causality/incident-001 --out reports/my-langgraph-investigation.json
```

The sample case includes one duplicate delivery and a late-arriving refund. Historical v1 produces $225; v2 produces $125; reported streaming balance $255 is a **separate observation** because the online watermark may have excluded events. Result: `PROPOSE_ISOLATED_REBUILD`, **not** permission to modify production or reuse the checkpoint. Input files are synthetically generated. No actual Databricks/RocksDB experiment or LangGraph/MLflow integration test was performed in this build environment.

See [ADR-006](docs/ADR-006-causal-recovery.md), [Sprint 06 runbook](docs/SPRINT06-RUNBOOK.md) and [test evidence](docs/SPRINT06-TEST-REPORT.md). Databricks has a separate read-only RocksDB checkpoint metadata job that must be explicitly executed against a synthetic UC Volume checkpoint.

---

**Sprint 05 (v0.5.0): Stateful Intelligence & Schema Evolution Gate.** All Sprint 01–04 files are retained.

This release adds **two different** row-based Databricks `transformWithState` probes: processing-time `MapState` TTL and event-time `ValueState` timers, along with a conservative state-contract migration gate, a deterministic reference engine, metrics-based rules advisor, and forensic state reader.

**Verified locally:** Python regression, contract classifications and reference simulation. **Not verified:** actual Spark/Databricks execution, native RocksDB, checkpoint format migrations or performance benchmarking.

```bash
PYTHONPATH=src python -m pytest -q
PYTHONPATH=src python scripts/state_intelligence_demo.py simulate
PYTHONPATH=src python scripts/state_intelligence_demo.py upgrade \
  --old fixtures/state_intelligence/contract-v1.json \
  --new fixtures/state_intelligence/contract-v2-compatible.json
PYTHONPATH=src python scripts/state_intelligence_demo.py diagnose \
  --progress fixtures/rocksdb/sample_progress.json
```

Key distinction: **a compatible state schema is not proof of semantic correctness**. The gate never approves production mutation. A changed logic revision requires isolated replay and reconciliation against the historical archive.

Start with [Sprint 05 runbook](docs/SPRINT05-RUNBOOK.md), [ADR-005](docs/ADR-005-state-intelligence.md), and [test report](docs/SPRINT05-TEST-REPORT.md). Databricks bundles include explicitly invoked `temporalx_sprint05_ttl` and `temporalx_sprint05_timers` research jobs; requires DBR >=17.3 LTS and UC sandbox; not executed here.

---

**Sprint 04 (v0.4.0): RocksDB Deep Recovery Lab.** Includes all Sprint 01–03 code, fixtures, tests, Kafka, Delta, and stateful replay.

This release adds a versioned Spark RocksDB experiment, fault injection after an
idempotent sink write, changelog-vs-snapshot branches, tunable memory/cache/
compaction settings, a Spark query progress analyzer and a Databricks state
inspection tool. **Only Python unit/regression tests ran here; actual Spark and
Databricks execution must still be performed by the user in a compatible runtime.**

```bash
PYTHONPATH=src python -m pytest -q
PYTHONPATH=src python scripts/prepare_rocksdb_fixture.py --output artifacts/rocksdb-input --accounts 20
PYTHONPATH=src python scripts/rocksdb_metrics_report.py --input fixtures/rocksdb/sample_progress.json
# On an environment with PySpark >=3.5 installed:
PYTHONPATH=src python rocksdb/local_spark.py --input artifacts/rocksdb-input --root artifacts/rocksdb-runs --branch changelog_a --strategy changelog
```

**Start with [Sprint 04 runbook](docs/SPRINT04-RUNBOOK.md), [ADR-004](docs/ADR-004-rocksdb-recovery.md), and [Sprint 04 test report](docs/SPRINT04-TEST-REPORT.md).** The metrics sample is synthetic, not measured Spark performance.

---

**Sprint 03 (v0.3.0): Kafka + stateful window processing, checkpoint-isolated branches,
late-arrival incident and historical recovery.** Includes all Sprint 01/02 code.

## Sprint 03: run the new experiment now

```bash
python -m pip install -e '.[test]'
python -m pytest -q
PYTHONPATH=src python scripts/local_stream_demo.py --db artifacts/stateful-s03.db
```

Expected balances for `blue/acct-1`:

| Branch | Blue balance | Why |
|---|---:|---|
| `stream-v1` | $255.00 | Counts duplicate $100; drops late $30 refund |
| `stream-v2` | $155.00 | Removes duplicate; still drops late $30 refund |
| `backfill-v2` | $125.00 | Rebuilds from all deliveries, deduplicates, includes refund |

Each branch uses its own checkpoint contract. Trying to reuse a checkpoint
with a different policy, window, schema revision or backfill mode **fails**.
The local SQLite experiment is reproducible and fully tested; the new Spark
and broker integrations are authored but **not yet executed** in this environment.

To run a local Kafka broker/consumer, and eventually the Databricks jobs, see
[**Sprint 03 runbook**](docs/SPRINT03-RUNBOOK.md) and
[**ADR-003**](docs/ADR-003-stateful-recovery.md).

New files: `src/temporalx/stateful.py`, `fixtures/streaming/`,
`docker-compose.yml`, `scripts/local_stream_demo.py`,
`scripts/produce_kafka.py`, `scripts/consume_to_local.py`,
`streaming/databricks_stream.py`, Sprint 03 test suite.

**Do not reuse production checkpoints or overwrite any production Delta table.**

---

> **Research code, not a payment processor.** Deliberately broken v1 financial
> policy exists solely to prove replay divergence. Run the Databricks jobs in a
> disposable lab schema; **do not run against a production schema**.

## What is real and what requires Databricks

| Feature | Local Python + SQLite | Databricks Spark + UC Delta |
|---|---|---|
| Immutable logical delivery archive | **Executed and tested** using SQLite triggers | **Code supplied; unverified in workspace**, Delta append-only Bronze |
| Duplicate delivery replay and tenant isolation | **Executed and tested** | Spark code supplied; fixture oracle assertion included |
| Bitemporal as-of queries | **Executed and tested** | Spark code supplied using canonical UTC timestamps |
| Source-batch digest and replay evidence hash | **Executed and tested** | Source-batch digest and replay branch supplied |
| Quarantine for bad JSON and invalid schema | **Executed and tested** | Job code supplied (fails batch after quarantine) |
| Delta Bronze / Silver logical event merge | N/A (SQLite reference engine instead) | **Requires actual Databricks execution** |
| Delta Change Data Feed | N/A | Configured on UC tables; sandbox update demo supplied |
| Streaming from Kafka, live CDC, scale benchmarks | Kafka producer/consumer scripts and state reference engine authored | Databricks Spark streaming implemented as code; **not executed**, no live CDC or scale benchmarks |

Do not mistake the local SQLite results for a Spark/Delta acceptance test.

## Reproducible local demonstration (no cloud account)

Requires Python >=3.10.

```bash
python -m pip install -e '.[test]'
python -m pytest -q

# Sprint 01: old baseline and corrected candidate on the same dataset
python -m temporalx generate --count 100 --seed 42 --output artifacts/events.jsonl
python -m temporalx compare --input artifacts/events.jsonl --output artifacts/comparison.json

# Sprint 02: immutable physical deliveries + replay provenance
python -m temporalx archive-ingest \
  --db artifacts/temporalx.db \
  --input examples/demo_events.jsonl --batch-id demo-001
python -m temporalx archive-status --db artifacts/temporalx.db
python -m temporalx archive-compare \
  --db artifacts/temporalx.db --run-id replay-001 \
  > artifacts/replay-001.json
```

If installing Python packages is impossible in an offline environment but
`pytest` is already installed, the local implementation also works directly
from source with `PYTHONPATH=src python -m pytest -q` and
`PYTHONPATH=src python -m temporalx ...`.

The checked-in fixture has **27 physical deliveries**, including three duplicate
captures and late refunds. Replaying the baseline (v1) vs deduplicated
candidate (v2) finds **-7,179 cents** (= -$71.79) in account balances. Both
ledgers reconcile with transaction totals. The result is a **synthetic incident**.

A checked-in example manifest is available at
[`examples/demo_sprint02_evidence.json`](examples/demo_sprint02_evidence.json).

Try reusing `demo-001` with different file bytes: the archive rejects the
batch-ID collision. Try reusing `replay-001` after loading another batch: the
replay evidence hash changes and reuse is rejected. Use new immutable IDs for
new snapshots.

### Two temporal axes

`occurred_at` describes when the business event happened. `ingested_at`
describes when it became known to this simulated platform (not the database
insert commit time). Both must include a timezone offset; the event contract
normalizes them to UTC.

```bash
python -m temporalx archive-compare --db artifacts/temporalx.db \
  --run-id known-on-oct2 --known-at 2026-10-02T00:00:00+00:00
```

## Execute the UC Delta experiments on Databricks

**Prerequisites:** an existing Databricks workspace, a compatible Spark cluster,
Unity Catalog catalog and **separate, empty lab schema** created by an
administrator, permissions for creating Delta tables, and a configured recent
Databricks CLI. Runtime 14.3 LTS or later is a reasonable target for this
legacy-CDF Delta lab; no claim of compatibility with untested configurations.

The bundle intentionally does **not** provision cloud accounts, storage,
clusters, networks, credentials or permissions.

```bash
# Existing cluster + disposable UC namespace must be supplied.
databricks bundle validate -t dev \
  --var 'cluster_id=<CLUSTER_ID>' --var 'catalog=<CATALOG>' --var 'schema=<LAB_SCHEMA>'
databricks bundle deploy -t dev \
  --var 'cluster_id=<CLUSTER_ID>' --var 'catalog=<CATALOG>' --var 'schema=<LAB_SCHEMA>'
databricks bundle run -t dev \
  --var 'cluster_id=<CLUSTER_ID>' --var 'catalog=<CATALOG>' --var 'schema=<LAB_SCHEMA>' \
  temporalx_sprint02
```

The job performs four ordered tasks: (1) append captured deliveries to Bronze,
(2) append late refunds to Bronze, (3) deduplicate by `(tenant_id,event_id)`
into Silver via Delta `MERGE`, and (4) verify the v1/v2 balance aggregates
against the checked-in Sprint 01 Python reference fixture, recording a replay manifest with a source, snapshot, code and policy digest. Any failed gate
fails the job. This workflow is a fixture experiment, **not** live streaming.

**Optional, destructive-to-its-own-sandbox-only CDF demonstration:** after the
previous job succeeds, run the following **only in the lab schema**. It publishes
v1 and then v2 into `temporalx_sandbox_balances` using guarded `MERGE`, enabling
CDF records with `update_preimage` and `update_postimage`. It never publishes to
business production tables. Repeated runs flip sandbox v1→v2 again and add
unnecessary change history.

```bash
databricks bundle run -t dev \
  --var 'cluster_id=<CLUSTER_ID>' --var 'catalog=<CATALOG>' --var 'schema=<LAB_SCHEMA>' \
  temporalx_sandbox_cdf_demo
```

At the end, use SQL in Databricks SQL to inspect versions and CDF:

```sql
DESCRIBE HISTORY <CATALOG>.<LAB_SCHEMA>.temporalx_sandbox_balances;
SELECT _change_type, _commit_version, tenant_id, account_id,
       balance_cents, policy
FROM table_changes('<CATALOG>.<LAB_SCHEMA>.temporalx_sandbox_balances', 0)
ORDER BY _commit_version, tenant_id, account_id;
```

These angle-bracket markers are user-supplied identifiers, **not literal SQL**.

## Logical architecture

```text
Synthetic financial events (schema v1/v2, duplicate delivery, late refund)
   ├── Local validation -> SQLite archive + quarantine -> replay evidence JSON
   └── Databricks job -> UC Delta Bronze (append-only delivery archive)
                                |
                                v
                            UC Delta Silver (one logical event per tenant/event)
                                |
                +---------------+---------------+
                |                               |
           v1 baseline                     v2 candidate
                +-------------+-----------------+
                              |
                        Reconciliation gate
                              |
                        Replay branches in Delta
                              |
                     Optional lab sandbox MERGE
                              |
                     Delta Change Data Feed
```

### Limits / caveats

- Bronze is append-only Delta and stores deliberate transport duplicates. It is
  **not immutable storage** against administrators who can modify/drop it.
  For WORM regulatory retention, external storage controls are required.
- `txnAppId`/`txnVersion` reduce duplicate writes on retries, subject to Delta
  transaction log retention and job coordination. The Bronze and source-batch
  manifest live in **two tables**; their writes are not a cross-table atomic
  transaction. The retry reconciles an interrupted manifest write.
- Silver is a *mutable logical view* of Bronze, never the authoritative replay
  archive. UC lineage is not an infinite-retention event log.
- Legacy Delta Change Data Feed is enabled explicitly in this lab for broad
  runtime compatibility. Automatic CDF has additional newer runtime and table
  prerequisites. CDF and Delta Time Travel have retention limits; **keep the
  independent Bronze event archive**.
- This fixture path validates source rows on the Spark driver, scans identity
  history and collects final account balances. It is intentionally **not
  scale-ready** or a parallel ingestion/streaming implementation.
- The `--approve-sandbox` flag is only a deliberate demo action. A real
  production approval would require authenticated, governed workflows and
  separate permissions; none are implemented here.

See [ADR-002](docs/ADR-002-durable-archive.md), [Databricks runbook](docs/DATABRICKS-RUNBOOK.md),
[research plan](docs/ROADMAP.md), and [test status](docs/TEST-REPORT.md).

Official platform documentation:

- [Delta Lake Change Data Feed](https://docs.databricks.com/aws/en/tables/features/change-data-feed)
- [Delta and Structured Streaming](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)
- [Databricks bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/settings)
