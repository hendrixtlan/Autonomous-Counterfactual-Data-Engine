# Test report — Sprint 02

## Executed locally

Run `python -m pytest -q` to repeat all reference engine, persistence, CLI and
contract tests. The local lab also executes `archive-ingest` / `archive-compare`
on `examples/demo_events.jsonl` with SHA-256 evidence manifest.

Key assertions:

- Duplicate batch retry does not append again.
- Distinct source batches do preserve physical duplicate deliveries.
- Append-only SQLite guard rejects changes to archived deliveries.
- Invalid row quarantined; valid rows remain usable for replay.
- A logical identity collision cannot alter account balances silently.
- A replay ID cannot silently point to a new event snapshot.
- Knowledge-time cutoffs change the visible historical truth.

## Not executed in this environment

- Spark job in Databricks.
- Delta `MERGE` / Unity Catalog permissions / Delta CDF lifecycle.
- Kafka CDC source offsets, checkpoint recovery, distributed load tests.
- Platform smoke tests for the workspace runtime and cluster architecture.

Local passing tests do not attest any of these features. Execute the bundle
and attach the Databricks job run ID to future test reports before treating
them as accepted.
