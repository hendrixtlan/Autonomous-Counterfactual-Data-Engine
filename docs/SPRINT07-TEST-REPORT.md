# Sprint 07 Local Verification Report

## Scope

Pure-Python contract validation and deterministic replay/forensic diff.
Previous six sprints' tests are preserved. Includes unit tests for authored
Databricks task paths, but **does not execute Spark or RocksDB**.

## Executed locally

- Source archive hash and tenant-scoped replay reference
- Synthetic mismatched value, phantom key, missing key, unchanged keys
- Failed provenance alignment (source digest, policy, operator, batch, tenant)
- Reject duplicate IDs, out-of-scope tenant, amounts not 64-bit integers,
  modified signed envelope, and evidence overwrites
- Validate Spark `statestore` projected `key/value` dict shape
- Run all earlier regression suites and compile Python source

## Measured result (local execution)

- **133 tests passed; 1 skipped** (optional LangGraph dependency absent).
- `databricks.yml` parsed successfully with the configured Sprint 07 task DAG.
- Frozen synthetic evidence SHA-256 checks verified; changed batch ID rejected
  with `INCOMPARABLE snapshots: checkpoint_batch_id`.
- Python modules compiled without syntax errors.

## Synthetic demonstration

Source fixture: `examples/demo_events.jsonl`, tenant `tenant-1`, replay policy
`v2`, checkpoint observation **synthetically corrupted** (not RocksDB).
Counts: `MATCH=8`, `MISSING_IN_CHECKPOINT=1`,
`UNEXPECTED_IN_CHECKPOINT=1`, `AMOUNT_MISMATCH=1`.

Results demonstrate local detection only. **No physical checkpoint**, Spark,
Databricks, Kafka broker, MLflow tracing, or agent integration was validated.
