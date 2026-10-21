# ADR-006 — Evidence-first State Causality & Recovery

**Status:** Accepted for research use. 2026-10-08.

## Context

Sprints 01–05 supplied historical replay, durable archive, checkpoint-isolated state branches,
RocksDB metrics and state-schema preflight. None of those alone proves the root cause of
an operational incident. A language model's free-text explanation also does not constitute proof.

## Decision

1. Every investigation is **read-only** against evidence in a bounded incident directory.
   No tool has authority to modify Spark checkpoints, Delta tables, cloud resources or Kafka.
2. Source inputs are hashed with SHA-256, and the report covers the exact evidence bytes.
   The checksum proves equality of bytes, **not** provenance or cryptographic authenticity.
3. Source events are filtered by tenant before analysis. An empty tenant partition fails closed.
4. Distinguish observed symptoms and tests from unproven causal hypotheses.
5. Compare policy v1 and v2 against the **same archived events**, with ordering invariance
   and tenant-scope checks; isolate research from any live sink.
6. State compatibility is evaluated by the prior `assess_upgrade` gate. Changed semantics
   require a fresh checkpoint and independent Spark/Delta reconciliation.
7. The decision engine may only return `BLOCK`, `INVESTIGATE_FURTHER` or
   `PROPOSE_ISOLATED_REBUILD`. No decision contains deploy credentials or a mutation tool.
8. Optional LangGraph orchestrates the same deterministic steps; MLflow autolog may trace
   graph execution. It is not an LLM-based root-cause oracle.

## Data flow

`incident manifest → immutable input hash → evidence extraction → hypotheses`
`→ read-only counterfactual replay → invariant + state-contract gate → signed-by-hash research report`

Checkpoints and Unity Catalog tables are not mutated or re-used. A human must separately
review, verify source retention and permissions, execute an independent Databricks sandbox
experiment, reconcile outputs, and approve change control before any production process.

## Threat model and limitations

* Protect against relative/absolute path escapes, symlinks, malformed events, conflicting IDs,
  tenant crossover and accidental overwrites of an existing report.
* Bound source file size to 16 MiB for this single-node research prototype.
* Root evidence directory is trusted by the investigator; avoid sharing it between adversarial
  processes. There is no filesystem snapshot or filesystem-level immutability guarantee.
* A hash alone does not establish authorship, prevent malicious replacement of a report plus
  all corresponding source bytes, or establish business truth.
* A duplicate delivery supports a duplicate-amplification hypothesis but does not prove that
  RocksDB caused it. Wall-clock arrival lag does not prove a Spark watermark drop.
* Python replay is a reference oracle, not a Spark execution or distributed-state benchmark.
* Databricks `statestore` forensic inspection is a batch read: access requires compatible DBR,
  a committed and retained checkpoint, and appropriate permissions.

## Follow-on research

Capture `read_statestore` output snapshots in a governed volume and diff against reconstructed
state; measure restore times and compare changelog checkpointing against incremental snapshot
recovery. Introduce a separate untrusted-LLM explanation layer with citation checks and replay
experiments as the only grounds for operational recommendations.
