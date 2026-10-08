# Databricks operational runbook — TemporalX Sprint 02

## Before execution

- Create a disposable, empty schema in Unity Catalog (not a production schema).
- Confirm the cluster supports Python/PySpark and Delta Lake table operations.
- Confirm job principal has `USE CATALOG`, `USE SCHEMA`, and `CREATE TABLE`.
- `pip` is not needed for extra packages on a supported Databricks runtime:
  Spark and Delta APIs are provided by the runtime. The bundle syncs `src/`.
- Choose one unique namespace per test environment; do not share replay IDs
  between unrelated data sets.

## Task order

1. `archive_captures`: inserts the source captures with `txnVersion=1`.
2. `archive_refunds`: inserts late refunds with `txnVersion=2`.
3. `materialize_silver`: validates identity and derives logical events via
   Delta `MERGE` with first-observed time and physical delivery count.
4. `compare_with_oracle`: enforces transaction owner and non-negative-balance
   invariants and verifies both aggregates equal the checked-in fixture oracle.

## Failure handling

- If a fixture is invalid, the seed job writes a quarantine row and stops.
- If a source batch ID is reused with different content, stop and investigate.
- If Bronze was appended before the batch receipt persisted, rerun the seed
  job to finish receipt recording. Observe Delta txn retention restrictions.
- If Silver or reconciliation fails, do not run sandbox publication.
- Do not use `VACUUM` or disable the CDF feed during this study. Never
  delete/regenerate Bronze to make mismatched counts disappear.
- Table changes in CDF are not indefinite. Capture explicit source
  delivery records in Bronze for replay and offline reconstruction.

## Research SQL checks

```sql
SELECT batch_id, count(*) AS physical_deliveries,
       count(DISTINCT CONCAT(tenant_id, ':', event_id)) AS approximate_keys
FROM <CATALOG>.<LAB_SCHEMA>.temporalx_bronze_deliveries
GROUP BY batch_id;

SELECT count(*) AS logical_event_count,
       sum(delivery_count) AS physical_delivery_count
FROM <CATALOG>.<LAB_SCHEMA>.temporalx_silver_events;

SELECT policy, sum(balance_cents) AS cents
FROM <CATALOG>.<LAB_SCHEMA>.temporalx_replay_accounts
WHERE run_id = 'demo-full'
GROUP BY policy;
```

`CONCAT` on arbitrary IDs is **not** an injective encoding. In this fixture
it is useful for human inspection only, not an identity key or invariant.
Always use separate columns `tenant_id`, `event_id` for real identity checks.

## Intended observations

For fixture-only data: Bronze has 27 physical deliveries, Silver 24 logical
events, v2 account total is 7,179 cents lower than v1. The manual sandbox
publication (v1 then v2) causes Delta CDF update pre/post images on affected
accounts. These are **expected research targets**, not claims of executed
Databricks test results.
