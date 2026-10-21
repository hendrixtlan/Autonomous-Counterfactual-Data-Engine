# Sprint 06 — Causal Investigation Runbook

## Requirements

Python 3.10+; run from repository root. Reference path is **dependency-free**, except
installation with `pip install -e ".[test]"` for tests. LangGraph is optional.

```bash
python -m pip install -e '.[test]'
python -m pytest -q
python scripts/causality_investigate.py \
  --incident-dir fixtures/causality/incident-001 \
  --out reports/my-investigation.json
python scripts/causality_investigate.py \
  --incident-dir fixtures/causality/incident-001 \
  --verify reports/my-investigation.json
```

Expected: v1 historical replay $225.00, v2 $125.00, delta **-$100.00**;
observed synthetic stream balance $255.00. v1 and stream disagreement is **not**
proof of one unique root cause; inspect late-arrival/watermark history.

### Optional graph execution

```bash
pip install -e '.[agent]'
python scripts/causality_investigate.py \
  --incident-dir fixtures/causality/incident-001 \
  --out reports/my-langgraph-investigation.json \
  --engine langgraph
```

`--engine langgraph` executes a compiled `StateGraph` with nodes `collect`, `hypothesize`,
`isolated_replay`, `policy_gate`, `produce_evidence` and must produce identical evidence
and report digest to the reference implementation (assuming identical input bytes).
No LLM or remote credentials are needed for the base graph.

### Optional tracing

```bash
pip install -e '.[tracing]'
# Configure MLflow tracking URI and authenticated Databricks workspace separately
python scripts/causality_investigate.py \
  --incident-dir fixtures/causality/incident-001 \
  --out reports/traced.json --engine langgraph --mlflow
```

MLflow `mlflow.langchain.autolog()` traces graph execution. **Review tracing privacy and
redact sensitive input values** before using outside the synthetic lab.

### Databricks RocksDB forensics (requires actual workspace)

The bundle contains a read-only job, `temporalx_sprint06_forensic_metadata`, pointing to
the synthetic Sprint 05 timers branch. Set cluster, catalog, schema and checkpoint-root
variables for your lab. Explicitly invoke the job only after creating its checkpoint.
The script prints metadata and schema; it does **not** print state rows by default.
`--sample-synthetic-values` is opt-in and must not be enabled against sensitive workloads.

For ad-hoc inspection inside a cluster:

```bash
databricks bundle validate -t dev
# Deploy only after reviewing all job permissions and workspace variables:
databricks bundle deploy -t dev
# Run explicitly; never target production:
databricks bundle run temporalx_sprint06_forensic_metadata -t dev
```

Workspace execution is **not validated** by this repository's CI.

## Safety gates before any real recovery

1. Verify event coverage, retention and tenant permissions.
2. Freeze immutable input digest in an independent evidence store.
3. Run candidate logic against a **new**, isolated checkpoint and sink.
4. Reconcile Spark+Delta output against Python oracle using the same frozen input.
5. Confirm retry behavior, watermark handling, business invariants and state metadata.
6. Obtain independent human approval; production promotion is outside this repository.

## Ground truth and limitations

The included `progress.json` is synthetic. The default report cannot infer that RocksDB
is the root cause, nor distinguish all effects of a watermark without actual committed
microbatch progress and state-store observations. Use the source hashes and test results to
identify a reproducible divergence, **not** to claim live disaster recovery success.

### Fault-injection evaluation harness

```bash
python scripts/benchmark_causality.py
```

This runs all 8 combinations of duplicate delivery, simulated late arrival and
state-contract semantic changes. The reported detection accuracy is **only for these
controlled injected signals**, not a measurement of real causal root-cause accuracy.
