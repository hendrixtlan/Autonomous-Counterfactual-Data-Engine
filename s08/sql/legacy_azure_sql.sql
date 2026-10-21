-- SYNTHETIC Azure SQL source query / SSIS-style latest-update rule.
-- Do not run against a real payments database without change approval.
WITH RankedChanges AS (
 SELECT tenant_id, event_id, transaction_id, account_id, kind,
        amount_cents, occurred_at, source_updated_at, source_lsn, is_deleted,
        ROW_NUMBER() OVER (
          PARTITION BY tenant_id, event_id
          ORDER BY source_lsn DESC, source_updated_at DESC
        ) AS rn
 FROM dbo.PaymentEventCDC
)
SELECT tenant_id, event_id, transaction_id, account_id, kind,
       amount_cents, occurred_at, source_updated_at, source_lsn, is_deleted
FROM RankedChanges WHERE rn = 1;
