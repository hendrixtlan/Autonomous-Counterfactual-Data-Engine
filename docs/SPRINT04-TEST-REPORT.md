# Sprint 04 test report

Reproducible verification is run with `PYTHONPATH=src python -m pytest -q`.

- Sprint 01–03 regression suites: exercised alongside Sprint 04 tests.
- Sprint 04: branch contract isolation; invalid configs; Spark setting
  generation; parser behavior for real-shaped (but synthetic) Spark progress;
  missing metric handling, dual operators, source generator determinism,
  Spark Python code syntax checks.
- A real local Spark state store invocation was **not executed** in this
  environment because `pyspark` is not installed.
- Live Databricks Delta/UC/Volumes/Kafka integration was **not executed**;
  requires credentials, a compatible Databricks Runtime, network, and lab resources.
- Real metrics, state-store recovery timing, and benchmark rankings remain
  **unknown**. The sample JSON is explicitly synthetic.

See `SPRINT04-RUNBOOK.md` for external acceptance test commands and caveats.

Verified locally on 2026-10-08: **65 tests passed**, Python source parsing
and Databricks bundle YAML syntactic load succeeded. A 20-key, 40-delivery
synthetic input was generated. Native RocksDB, PySpark, Docker, and Databricks
were not available for execution in this environment, so latency and restore
benchmarks remain unverified.
