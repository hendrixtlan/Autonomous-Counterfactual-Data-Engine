-- Spark SQL equivalent logic for synthetic captured Bronze CDC.
-- A tie with different payloads at the same source_lsn MUST be blocked by
-- the quality gate BEFORE running this query.
WITH ranked AS (
 SELECT tenant_id, event_id, transaction_id, account_id, kind,
        amount_cents, occurred_at, source_updated_at, source_lsn, is_deleted,
        ROW_NUMBER() OVER (
          PARTITION BY tenant_id, event_id
          ORDER BY source_lsn DESC, source_updated_at DESC
        ) AS rn
 FROM ${LAB_CATALOG}.${LAB_SCHEMA}.temporalx_s08_bronze
 WHERE _rescued_data IS NULL
)
SELECT tenant_id, event_id, transaction_id, account_id, kind,
       amount_cents, occurred_at, source_updated_at, source_lsn, is_deleted
FROM ranked WHERE rn=1;
