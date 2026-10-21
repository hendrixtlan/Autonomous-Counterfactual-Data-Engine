# Sprint 06 — Test Evidence

## Verified locally

* Python source syntax and existing regression suite including Sprints 01–05.
* New fixtures and read-only investigation, cross-tenant filtering, exact financial divergence,
  source/report hash integrity, invalid manifest/path/symlink rejection, conflict handling,
  state-compatibility refusal, report no-overwrite and deterministic replay.
* Reference CLI and verification of its output against the source evidence.
* Databricks Bundle YAML parsed locally (not applied to a Databricks workspace).

## Not yet verified

* Optional LangGraph dependency is not installed in this build environment, so compiled
  graph execution parity must be verified where the dependency is available.
* MLflow trace export; Spark query progress from a real Databricks workload; native
  RocksDB checkpoint state inspection, restore and cluster recovery.
* Actual incident false-positive rate on a labeled production-quality dataset.

This is a research prototype and a reproducible incident simulation, not an automatic repair system.
