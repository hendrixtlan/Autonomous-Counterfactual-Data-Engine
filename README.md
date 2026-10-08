# TemporalX — Autonomous Counterfactual Data Engine

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
