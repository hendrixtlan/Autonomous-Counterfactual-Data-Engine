-- GOVERNANCE TEMPLATE ONLY. Do not execute with placeholders.
-- Run as an authorized UC administrator after provisioning an isolated lab.
-- Narrow privileges to the dedicated lab schema and to an approved Gold table.
-- Replace $CATALOG, $SCHEMA, $READER_GROUP, $ENGINEER_GROUP safely using a
-- reviewed provisioning process (not untrusted string substitution).
GRANT USE CATALOG ON CATALOG ${CATALOG} TO `${READER_GROUP}`;
GRANT USE SCHEMA ON SCHEMA ${CATALOG}.${SCHEMA} TO `${READER_GROUP}`;
GRANT SELECT ON TABLE ${CATALOG}.${SCHEMA}.temporalx_s08_gold_trial_a TO `${READER_GROUP}`;

GRANT USE CATALOG ON CATALOG ${CATALOG} TO `${ENGINEER_GROUP}`;
GRANT USE SCHEMA ON SCHEMA ${CATALOG}.${SCHEMA} TO `${ENGINEER_GROUP}`;
GRANT CREATE TABLE ON SCHEMA ${CATALOG}.${SCHEMA} TO `${ENGINEER_GROUP}`;
-- Explicit data write privileges per table, not broad write permissions on catalog:
GRANT SELECT, MODIFY ON TABLE ${CATALOG}.${SCHEMA}.temporalx_s08_silver TO `${ENGINEER_GROUP}`;
GRANT SELECT, MODIFY ON TABLE ${CATALOG}.${SCHEMA}.temporalx_s08_bronze TO `${ENGINEER_GROUP}`;
-- Separately manage READ VOLUME / WRITE VOLUME on approved landing/checkpoint Volumes.
-- Review lineage in Databricks Catalog Explorer; optional system.access.table_lineage
-- requires appropriate system-table visibility and may not include every operation.
