# Sprint 07 Runbook — Read-only RocksDB Forensics

## Fast local proof (no Databricks required)

```bash
python -m pip install -e '.[test]'
python -m pytest -q
python scripts/forensic_diff.py demo --out-dir artifacts/s07-my-demo
python scripts/forensic_diff.py verify --bundle artifacts/s07-my-demo/reference.json
python scripts/forensic_diff.py compare \
  --reference artifacts/s07-my-demo/reference.json \
  --observed artifacts/s07-my-demo/observed-synthetic.json \
  --out artifacts/s07-my-demo/rechecked-report.json
```

Expected: 8 matching accounts, one missing, one unexpected and one wrong
amount, on **entirely synthetic evidence**. Nothing in this demo reads RocksDB.
To compare an actual state export later, use `compare` with a compatible
normalized `checkpoint` bundle; both SHA and provenance must validate.

## Databricks requirements and restrictions

- Isolated pre-existing Unity Catalog schema with synthetic S03 Bronze table.
- **Exact Bronze Delta version**, from `DESCRIBE HISTORY`, selected by operator.
  The bundle currently uses version `0` only as a **placeholder**; change it
  to the actual version containing the synthetic fixture before running.
- Databricks Runtime 17.3 LTS or later recommended, supporting RocksDB state
  provider and state metadata/state store readers on supported compute.
- Separate **non-overlapping** Unity Catalog Volume roots for checkpoint and
  forensic evidence; both under `/Volumes/` and not `REPLACE_ME`.
- Cluster privileges to read checkpoint and table, plus write access to the
  disposable schema and evidence Volume. Avoid serverless access modes that
  cannot read state stores in your workspace/runtime.
- Keep checkpoint writers stopped before forensic extraction; old batch
  versions can be cleaned up by streaming execution.
- This is an **explicit 10k-row max**, synthetic-only research lab. Do not
  point at a production table, PII, or a production checkpoint.

## Three-stage Databricks job

Set `cluster_id`, `catalog`, `schema`, `checkpoint_root`,
`forensic_evidence_root`, and select a unique branch. Do not rerun `seed`
with an old branch: it is intentionally immutable.

```bash
# From the repo after configuring databricks CLI authentication:
databricks bundle validate -t dev
# Review configured source-version and UC destinations BEFORE this command.
databricks bundle run -t dev temporalx_sprint07_forensic_diff
```

1. `seed`: pins one Bronze table version and deduplicates into a NEW Delta
   seed table (rejects conflicting identities). Writes frozen seed manifest.
2. `stream`: aggregates account cents with one Spark stateful operator; writes
   RocksDB checkpoint only under its isolated branch. The sink is diagnostic
   and does not publish a financial ledger.
3. `compare`: reads `state-metadata`, selects latest committed batch, reads
   `statestore` checkpoint key/value, independently groups seed in batch,
   writes three **new** JSON files in the evidence Volume.

The comparison defaults to `operatorId=0` and field mappings
`key.tenant_id`, `key.account_id`, `value.net_cents`. Spark state store schemas
are operator-specific; if the fields differ, **stop** and inspect the logged
`ACTUAL_STATSTORE_SCHEMA`. Edit only the projection mapping after technical
review. Do not silently cast a deduplication state to balances.

### Useful read-only SQL (inspect before mapping)

```sql
SELECT * FROM read_state_metadata('/Volumes/<catalog>/<schema>/<volume>/checkpoints/s07/account_forensic_a');
SELECT * FROM read_statestore(
  '/Volumes/<catalog>/<schema>/<volume>/checkpoints/s07/account_forensic_a',
  operatorId => 0
) LIMIT 10;
```

Exact SQL syntax and reader support should be checked against the workspace
runtime. See [Databricks docs: read streaming state](https://docs.databricks.com/aws/en/structured-streaming/read-state).

## Acceptance criteria for an actual physical checkpoint

- Capture runtime, operator ID, latest committed batch, checkpoint path,
  seed digest and source table version into immutable external audit logs.
- Validate source table version and seed checksum before stream and compare.
- Compare exactly one business operator/state representation.
- Produce `checkpoint.json`, `reference.json` and `diff.json`; verify their
  hashes and preserve immutable copies.
- Inject divergence in a **new isolated branch** (never modify an existing
  checkpoint) and confirm the expected mismatch classifications.
- A clean diff can only return `CONSISTENT_FOR_REVIEW`, never production approval.

**Not validated as of v0.7:** running Databricks bundle, RocksDB physical
checkpoint parsing, runtime field names, correctness at scale or incident
recovery. The pytest suite exercises local code and safety contracts.
