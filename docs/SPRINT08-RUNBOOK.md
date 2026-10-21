# Sprint 08 — Lakehouse modernization and checkpoint time travel

**Status:** local reference tests runnable, Databricks scripts authored but not executed in a workspace.

## 1. Offline proof and migration assessment

```bash
python -m pip install -e '.[test]'
python -m pytest -q
python s08/assess_migration.py --out artifacts/s08/migration-report.json
python scripts/time_travel_lab.py demo --out-dir artifacts/s08-time-travel
```

`assess_migration` checks exactly three mappings (Azure SQL extraction, SSIS transformations, ADF orchestration), documents unresolved blockers, and produces a synthetic seven-delivery reconciliation. The fixture uses CDC revisions, physical duplicates and tombstones. The time travel demo simulates a divergence that appears in batch 2 and self-heals by batch 4.

## 2. Prepare Azure Databricks

Provision an existing Unity Catalog catalog, a dedicated **`lab_...` schema**, UC Volumes for input and checkpoints, and a synthetic/approved CDC JSONL extract. Grant the job identity `USE CATALOG`, `USE SCHEMA`, table-create/write, and volume read/write according to role; do not automatically grant broad catalog privileges. Review `s08/sql/unity_catalog_grants.sql` for example read-only BI users and separate pipeline identity. The example SQL is **a template**, not a deployment script.

Prepare a UC Volume landing folder containing a copy of `s08/fixtures/azure_sql_cdc.jsonl` uploaded by an authorized Databricks user, not via this tool. The code intentionally **does not connect directly to Azure SQL** or inspect genuine SSIS/ADF projects.

For every actual CDC extraction: establish an initial snapshot watermark, commit LSN ordering, the canonical SQL Server datetime timezone interpretation, and a strategy for schema changes; LSN in the fixture is only an integer ordinal. Document all upstream security and compliance reviews.

## 3. Execute the Lakehouse pipeline

Once a compatible existing cluster and your UC resources are configured:

```bash
databricks bundle validate -t dev \
  --var 'cluster_id=<CLUSTER_ID>' --var 'catalog=<UC_CATALOG>' --var 'schema=lab_temporalx' \
  --var 'lakehouse_source_volume=/Volumes/<CATALOG>/lab_temporalx/source/landing' \
  --var 'lakehouse_checkpoint_volume=/Volumes/<CATALOG>/lab_temporalx/state/checkpoints'

databricks bundle deploy -t dev --var 'cluster_id=<CLUSTER_ID>' --var 'catalog=<UC_CATALOG>' --var 'schema=lab_temporalx' \
  --var 'lakehouse_source_volume=/Volumes/<CATALOG>/lab_temporalx/source/landing' \
  --var 'lakehouse_checkpoint_volume=/Volumes/<CATALOG>/lab_temporalx/state/checkpoints'

databricks bundle run -t dev temporalx_sprint08_lakehouse
```

Bundle run often requires supplying the same variables at invocation, or a checked-in local configuration/profile with resolved values; see the earlier sprints' bundle usage. The job chain executes `bootstrap -> Auto Loader Bronze -> Source-LSN Silver MERGE -> immutable Gold`. **The Gold task intentionally fails if the snapshot branch already exists**; provide a new unique `lakehouse_gold_branch` after any successful run. Bronze checkpoint must remain unchanged between append runs.

If the first ingest fails the quality gate: inspect Bronze and the input files in the isolated lab; **do not delete a checkpoint to conceal the issue**.

## 4. Read Silver CDF and benchmark table layout

```bash
databricks bundle run -t dev temporalx_sprint08_cdf_audit

databricks bundle run -t dev temporalx_sprint08_layout_preview
```

CDF requires an explicit starting Delta version; this lab defaults to 0 but production jobs should derive it from controlled checkpoint/version tracking. To actually create independent layout candidates on a disposable branch, run `s08/databricks_lakehouse.py layout --layout liquid|partition` with `--execute-maintenance` manually. `ZORDER` is available only on non-liquid Delta tables. Never run multiple competing physical layouts on the same table; for a fair A/B compare one versioned source dataset at a time.

The `benchmark` phase measures a filtered SQL/DataFrame aggregate without collecting individual account state. Run the same query on a classic compute configured as `STANDARD`, then on matched `PHOTON` compute and collect query plans, latency distributions, engine/worker configuration and DBU+storage costs. Explicitly distinguish Spark SQL/DataFrame queries from stateful streaming that Photon does not accelerate.

## 5. Distributed RocksDB forensic diff

`distributed_diff.py` requires two **actual isolated** synthetic Spark stateful checkpoints and two JSON provenance manifests with matching source version, digest, operator semantics, tenant scope, state schema and batch ranges. The branch identifiers must be distinct; the first batch must currently be **0** because a checkpoint baseline importer is not yet implemented; state-reader access may require dedicated compatible compute and DBR >=16.4 LTS. Checkpoint metadata and stream state schema must be inspected before attempting comparison.

Once manifests are authored by a trusted evidence pipeline, run the explicitly configured bundle job:

```bash
databricks bundle run -t dev temporalx_sprint08_distributed_diff
```

The Spark state change reader and full-outer windowed comparator keep account-level differences distributed; only aggregate counts/first batch are returned. **The code currently expects key fields `key.tenant_id` and `key.account_id`, plus `value.net_cents`.** Actual Spark state schema may need a purpose-built projection. It refuses mismatched metadata and existing result tables and does not mutate checkpoints.

## 6. Operational hardening before claiming production readiness

- Capture actual ADF JSON and SSIS `.dtsx` inventory; classify custom transforms, lookups, data types and unsupported tasks.
- Add CDC ingestion with managed credentials, secret scopes or workload identities, independent extraction checkpoint, reliable transaction sequence and DLQ/replay.
- Add continuous Bronze-Silver streaming and CDC SCD with target locks/transaction fences rather than a full Bronze rescan.
- Add performance regression suite, field-level RBAC/masking tests, actual UC lineage verification and CI/CD deployment gates.
- Execute Databricks state-reader and RocksDB benchmarks on compatible compute and publish evidence reports.

## References

- Databricks: State reader and state metadata <https://docs.databricks.com/aws/en/structured-streaming/read-state>
- Databricks: Liquid clustering <https://docs.databricks.com/aws/en/tables/clustering>
- Databricks: Unity Catalog privileges <https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/privileges-reference>
- Databricks: Photon compute <https://docs.databricks.com/aws/en/compute/photon>
- Azure Databricks: Migrate ETL pipelines <https://learn.microsoft.com/en-us/azure/databricks/migration/etl>

## 7. Read-only lineage audit

Once the pipeline has run and the UC system table is available/authorized:

```bash
databricks bundle run -t dev temporalx_sprint08_lineage_audit
```

The job queries `system.access.table_lineage` for source/target UC table names. Missing records can reflect permissions, capture latency, retention or path-based lineage; they do **not** prove a missing relationship. Compare Catalog Explorer lineage as a second perspective. For offline review of the legacy workflow, see `s08/fixtures/adf_legacy_pipeline.json`, `s08/fixtures/ssis_package_inventory.json` and `s08/benchmark_matrix.yml`.
