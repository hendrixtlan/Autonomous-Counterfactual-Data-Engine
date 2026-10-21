# ADR-007 — Explainable State Forensics without Inventing Evidence

**Status:** Accepted for Sprint 07 research; physical Databricks integration pending execution.

## Decision

We compare **logical state projections**, not raw RocksDB SST bytes. `statestore`
returns operator-specific structs. A `dropDuplicates` state key is not comparable
to an account aggregate, and a windowed aggregate is not the same as an
unwindowed one. Sprint 07 therefore uses a dedicated Spark aggregation:
`groupBy(tenant_id, account_id).sum(signed_cents)` on a frozen, deduplicated,
synthetic Delta seed. RocksDB stores this operator state. A separate *batch*
aggregation on the same frozen seed is the reference oracle.

State reconciliation is meaningful only with aligned:

- Source table and Delta **version** (and the frozen seed digest)
- Business policy version (`v2` deduplication)
- Operator revision and state interpretation (`account-net-cents-v1`)
- Operator ID, latest committed batch, watermark/TTL policy and tenant scope
- Currency and units (signed **cents**, never floats)

If any dimension differs, result is **INCOMPARABLE** and no numerical
correction is suggested. Missing keys differ from explicit `net_cents=0`.

## Physical boundary

Spark, not Python, owns RocksDB. The S07 Databricks job reads Spark's supported
`.format('state-metadata')` / `.format('statestore')` DataFrame readers. The
physical schema is operator/version dependent, so explicit `key.tenant_id`,
`key.account_id`, `value.net_cents` projections are required. If those
fields are not present, stop and inspect the reported schema; do not infer
amounts from deduplication IDs or flatten arbitrary values.

State is frozen in a separate UC Volume checkpoint, and evidence is emitted to
a **different** Volume root. The checkpoint export is read-only. The pipeline
never calls `RESTORE`, deletes a checkpoint, overwrites a table, or promotes
results to production.

## Bounded vs. distributed forensics

The S07 job caps physical/source rows at **10,000** so collecting to the
isolated driver for traceable fixture-sized evidence is bounded. Production
size comparisons require distributed full outer joins, per-partition checksums,
row-level security, verified checkpoint-to-source commit alignment, and a
separate operational/security review. An account-level exact match in this
small lab does **not** demonstrate universal correctness for streaming joins,
TTL, timers, or windows.

## Adversarial tests

1. One account balance incorrect -> `AMOUNT_MISMATCH`.
2. Expected account absent -> `MISSING_IN_CHECKPOINT`.
3. Unexpected account -> `UNEXPECTED_IN_CHECKPOINT`.
4. Fully aligned -> `CONSISTENT_FOR_REVIEW`, never automatic publish.
5. Source SHA, Delta version, operator ID, batch, semantic version or tenant
   mismatch -> reject as incompatible, not a false divergence.
6. Duplicate keys, cross-tenant rows, floating-point amount, tampered bundle,
   or oversized evidence -> fail closed.

## Security assumptions and limitations

SHA-256 covers serialized evidence and detects modification, but **does not
prove origin or authenticity**. A malicious actor who can replace both payload
and digest can forge a bundle. Configure UC permissions and external auditing.
The Databricks environment, runtime, credentials, cluster policy and physical
`statestore` projection have **not been exercised** in this packaging environment.
