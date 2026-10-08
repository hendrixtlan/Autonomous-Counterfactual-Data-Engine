# Sprint 03 — verification levels

| Component | Status | Evidence |
|---|---|---|
| Sprint 01 deterministic oracle | Local tested | `tests/test_engine.py`, `tests/test_cli.py` |
| Sprint 02 SQLite immutable archive | Local tested | `tests/test_archive.py`, `tests/test_sprint02_contract.py` |
| Sprint 03 atomic microbatch simulator | Local tested | `tests/test_stateful.py` |
| Sprint 03 fixture batch replay | Local tested | `scripts/local_stream_demo.py` |
| Redpanda broker and producer/consumer | Authored only | `docker-compose.yml`, `scripts/*kafka.py`, `scripts/consume_to_local.py` |
| Spark Kafka → UC Delta Bronze | Authored only | `streaming/databricks_stream.py`, `databricks.yml` |
| Spark stateful branches and offline backfill | Authored only | Same |
| Unity Catalog/RocksDB state behavior | Not tested | Needs a Databricks workspace |
| Production scalability and recovery RTO | Not tested | Sprint 04+ |

The local microbatch simulator is NOT identical to Spark semantics. Its purpose
is to verify state-incompatibility guards, content digests, transactionality,
and expected financial outcomes deterministically.
