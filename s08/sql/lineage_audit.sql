-- Read only. Requires access to system.access.table_lineage.
-- Substitute a reviewed, isolated UC catalog/schema, or use the safe job CLI.
SELECT event_time, source_table_full_name, target_table_full_name, entity_type
FROM system.access.table_lineage
WHERE lower(source_table_full_name) LIKE lower('${CATALOG}.${SCHEMA}.temporalx_s08_%')
   OR lower(target_table_full_name) LIKE lower('${CATALOG}.${SCHEMA}.temporalx_s08_%')
ORDER BY event_time DESC LIMIT 50;
