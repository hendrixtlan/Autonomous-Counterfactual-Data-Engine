# ADR-008 — Databricks Lakehouse Modernization & Distributed State Diff

Status: PROTOTYPE / LOCAL REFERENCE VALIDATED; SPARK/DATABRICKS UNTESTED.

## Decisions

1. **Explicit lab isolation:** only existing UC schemas starting `lab_` or `temporalx_lab` can be used in Sprint 08 jobs. Other sprints retain their separate contracts. Dedicated UC Volumes isolate source landing and checkpoint paths. No implicit grant or production mutation.
2. **Bronze retains *physical deliveries*.** Auto Loader ingests structured JSON from an approved, synthetic staged file export. Append-only events are never deduplicated in Bronze; replay retains delivery multiplicity and timestamps. Production Azure SQL CDC ingestion, secret management and connector wiring are intentionally outside the included code.
3. **Silver is a logical CDC table.** `(tenant_id,event_id,source_lsn)` conflicts fail the quality gate; `(tenant_id,event_id)` picks the highest LSN. A later tombstone is retained so Gold excludes logically deleted events. Exact duplicate deliveries are allowed in Bronze but not counted twice in Silver. A new unique branch is required for each Gold snapshot.
4. **Gold is immutable by branch.** Gold materializes a net-cents and event-count table with an `errorifexists` write, avoiding accidental overwrites. Comparisons between successive Gold branches support A/B modernization experiments.
5. **Legacy system conversion is reviewed, not inferred.** Azure SQL, SSIS and ADF manifests describe extraction, transformations and task dependencies; the repository includes representative T-SQL / Spark SQL transformations and a migration assessment. Nothing migrates real SSIS packages, Azure SQL connections or ADF JSON automatically.
6. **CDF and governance:** Silver CDF is enabled at table creation. Governance SQL template uses least-privilege UC grants; operators must inspect Catalog Explorer lineage and appropriate system tables with actual platform permissions.
7. **Performance:** liquid clustering is preferred for new managed tables. ZORDER on non-liquid Delta tables and explicit partitioning remain educational controlled comparisons. Photon benchmark must run on separately documented `PHOTON` and `STANDARD` computes; never claim Photon accelerated RocksDB stateful operators.
8. **Distributed forensic diff:** checkpoints must be separate and match source snapshot, semantic schema, operator ID and batch interval. Read `statestore` state-change feeds in batch, do a full-outer key/batch comparison and carry forward states with Spark windows. No full-state `.collect()`. Only small aggregate summary/metadata may reach driver. Current code requires batch history beginning at 0 (or it refuses comparison because a baseline is not implemented) and assumes key fields `key.tenant_id/account_id` and value `value.net_cents`, which must be verified against physical operator schema in workspace.

## Failure modes / non-goals

- `source_lsn` is a synthetic monotonic integer. Real SQL Server CDC LSNs are binary and require an explicitly verified order-preserving normalization. Snapshot isolation, exact-once extraction and transaction boundaries need source-side acceptance tests.
- Partitioning by tenant can create skew or excessive partition cardinality; it is an **experiment**, not a recommended default.
- Neither CDF nor checkpoints are eternal event archives. Retention must be planned independently.
- Reconstructing from only the remaining checkpoint history **cannot prove first-ever divergence**. Batch-range checks are necessary but not sufficient; archived source lineage and microbatch audit must also be checked.
- Large Bronze history is scanned by the Sprint 08 Silver job; for production, plan incremental CDF processing with snapshot fencing, checkpointed streaming and robust retry semantics.
- Databricks streaming state-reader APIs have access-mode and runtime restrictions; not all pipelines expose queryable state.
- No Spark/PySpark, Photon or Databricks benchmark has run in the development environment. No direct Azure SQL / SSIS / ADF instance connection has been tested.

## Acceptance gates

- Full Bronze physical row count and source export digests match, including duplicates and tombstones.
- Silver logical key uniqueness and LSN conflict checking are enforced; no cross-tenant merging.
- Compare expected per-account balances and record counts against the local reference oracle.
- CDF must surface changes to the Silver source when a revised LSN arrives; verify exact Delta commit version and retention.
- Grants verified with least-privilege service principal and unauthorized-reader negative tests.
- Photon STANDARD vs PHOTON SQL/DataFrame benchmark with matched data, cluster size, plan, query warmup, concurrency and cost.
- RockDB forensic comparator refuses missing checkpoints or mismatched source versions. Run first divergence evaluation with isolated state branches only.
