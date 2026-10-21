# Sprint 05 — Runbook / Acceptance Plan

## A. Local baseline (no cloud)

```bash
python -m pip install -e '.[test]'
python -m pytest -q
python scripts/state_intelligence_demo.py simulate
python scripts/state_intelligence_demo.py upgrade \
  --old fixtures/state_intelligence/contract-v1.json \
  --new fixtures/state_intelligence/contract-v2-compatible.json
python scripts/state_intelligence_demo.py upgrade \
  --old fixtures/state_intelligence/contract-v1.json \
  --new fixtures/state_intelligence/contract-v3-replay.json
python scripts/state_intelligence_demo.py diagnose \
  --progress fixtures/rocksdb/sample_progress.json
```

Without an editable installation, prefix the commands with `PYTHONPATH=src`.

Expected decisions: compatible fixture `REVIEW_FOR_RESUME`; new business logic `REBUILD_REQUIRED`. Neither case modifies any checkpoint.

## B. Databricks stateful experimentation

Prerequisites:

- Databricks Runtime **17.3 LTS or newer** compatible with row-based Python `transformWithState`. Confirm actual workspace runtime and access mode.
- Writable disposable Unity Catalog catalog/schema, an existing cluster and pre-created writable UC Volume at `/Volumes/...`.
- Databricks CLI and a valid profile configured separately. No secrets or permissions in this repository.
- The two research jobs are intentionally finite (`--run-seconds=60`), but they create checkpoint directories and lab Delta tables.

```bash
databricks bundle validate -t dev \
  --var cluster_id=<CLUSTER_ID> --var catalog=<SANDBOX_CATALOG> \
  --var schema=<SANDBOX_SCHEMA> --var checkpoint_root=/Volumes/<catalog>/<schema>/<volume>/checkpoints

databricks bundle deploy -t dev \
  --var cluster_id=<CLUSTER_ID> --var catalog=<SANDBOX_CATALOG> \
  --var schema=<SANDBOX_SCHEMA> --var checkpoint_root=/Volumes/<catalog>/<schema>/<volume>/checkpoints

# Run explicitly in UI or CLI one at a time:
# temporalx_sprint05_ttl: processing-time TTL MapState
# temporalx_sprint05_timers: event-time ValueState and timers
```

The synthetic `rate` source is not a durable replay source. Its data and processing clock will differ across runs.

## C. Forensic state inspection

Run on a suitable Databricks compute with checkpoint read permission:

```bash
python state_lab/inspect_state.py \
  --checkpoint-root /Volumes/<catalog>/<schema>/<volume>/checkpoints \
  --mode ttl --branch ttl_v1 --operator-id 0

python state_lab/inspect_state.py \
  --checkpoint-root /Volumes/<catalog>/<schema>/<volume>/checkpoints \
  --mode timer --branch timer_v1 --operator-id 0
```

If the operator ID is not zero, first inspect `state-metadata` and rerun with the actual operator ID. For research fixtures only, `--show-synthetic-values` reveals keys/values and registered timers; production data must not be exposed. The state reader is **batch/read-only** and may only return retained committed batches. For change-feed experiments on DBR >=16.4 LTS use `stateVarName` and `readChangeFeed` with the documented options.

## D. Acceptance gates for a real cloud verification

1. Separate checkpoints created for both modes without collisions.
2. TTL MapState emits a bounded recent-key dedup trace and demonstrates duplicate acceptance *after* expiry.
3. Timer state emits a closure for an inactive account after event-time watermark advances, without processing-time TTL.
4. State metadata identifies the stateful operator. State variable read succeeds with `stateVarName`; timer read succeeds when available.
5. Capture `StreamingQueryProgress` JSON from actual queries; compare metric distributions and recovery timing (not the checked-in synthetic sample).
6. Restart same version with same checkpoint to evaluate continuation. For changed logic use an isolated checkpoint, archived Bronze and reconciliation. For physical compatible schema change, stage an intentional checkpoint migration test with rollback plan and validate state before approval.
7. Repeat under controlled executor termination and store proof of sink idempotency and completeness. **This repository cannot claim these tests passed without a real workspace.**

## Danger boundaries

Do not use payment/customer production data or reuse/drop live checkpoints. The script creates new sandbox table names by mode and branch. Automatic agent remediation is not implemented.
