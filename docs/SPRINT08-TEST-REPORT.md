# Sprint 08 Test Report — Local Reference Only

Generated during this artifact build. Run all tests from the repository root:

```bash
python -m pip install -e '.[test]'
python -m pytest -o addopts='' -q
python s08/assess_migration.py --out artifacts/s08/migration-report.json
python scripts/time_travel_lab.py demo --out-dir artifacts/s08-time-travel
```

The suite exercises all Sprints 01–08, including golden CDC fixtures, schema and tenancy validation, tombstones, duplicate identity conflicts, provenance mismatch, missing checkpoint coverage and safe SQL/volume path policies. The optional LangGraph integration test is skipped unless its optional dependency is available.

**Scope of evidence:** pure Python tests and metadata/template validation. Spark jobs and Databricks feature integrations are code-only, not verified in a cluster. No benchmark timings, Photon performance claims, SSIS migration execution or native RocksDB read was performed. The state-timeline demonstration uses synthetic evidence.

## Recorded local verification (2026-10-21)

- **175 passed; 1 skipped** (`python -m pytest -o addopts='' -q`).
- `python -m compileall`: no syntax errors in the Python source tree.
- `databricks.yml`: parsed successfully with PyYAML, five Sprint 08 workflows and four ordered medallion tasks.
- Local synthetic CDC oracle: 7 deliveries / 4 Silver identities / 2 Gold accounts, balances 16,000 cents and 7,000 cents.
- Local synthetic time-travel investigation: first divergent batch 2, two divergence episodes, no differences by the end of batch 4.

A native Databricks execution is a separate acceptance gate and is still pending.
