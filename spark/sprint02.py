"""TemporalX Sprint 02: fixture-sized Delta/Unity Catalog research job.

Requires a configured Databricks workspace and an existing UC catalog + schema.
Spark/Delta execution is NOT part of the local pytest suite. The prototype
intentionally collects small fixture aggregates on the driver; not scalable
for production. No customer data, credentials or destructive SQL are used.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F
from delta.tables import DeltaTable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from temporalx.model import Event, parse_instant  # noqa: E402


IDENT = re.compile(r"^[A-Za-z_][A-Za-z_0-9]*$")
NAMES = {
    "bronze": "temporalx_bronze_deliveries",
    "quarantine": "temporalx_quarantine",
    "batches": "temporalx_source_batches",
    "silver": "temporalx_silver_events",
    "replays": "temporalx_replay_accounts",
    "manifests": "temporalx_replay_manifests",
    "sandbox": "temporalx_sandbox_balances",
}


def table_names(catalog: str, schema: str) -> dict[str, str]:
    if not IDENT.fullmatch(catalog) or not IDENT.fullmatch(schema):
        raise ValueError("catalog and schema must be simple SQL identifiers")
    return {k: f"`{catalog}`.`{schema}`.`{v}`" for k, v in NAMES.items()}


def create_tables(spark, tables):
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {tables['bronze']} (
       batch_id STRING, message_ordinal INT, delivery_id STRING,
       tenant_id STRING, event_id STRING, transaction_id STRING,
       account_id STRING, kind STRING, amount_cents BIGINT, currency STRING,
       occurred_at STRING, ingested_at STRING, schema_version INT,
       event_hash STRING, source_sha256 STRING
    ) USING DELTA TBLPROPERTIES (
       'delta.appendOnly'='true', 'delta.enableChangeDataFeed'='true')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {tables['quarantine']} (
        batch_id STRING, message_ordinal INT, raw_line STRING, reason STRING
    ) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {tables['batches']} (
        batch_id STRING, input_sha256 STRING, accepted BIGINT
    ) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {tables['silver']} (
        tenant_id STRING, event_id STRING, transaction_id STRING,
        account_id STRING, kind STRING, amount_cents BIGINT, currency STRING,
        occurred_at STRING, first_ingested_at STRING, event_hash STRING,
        delivery_count BIGINT
    ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {tables['replays']} (
        run_id STRING, policy STRING, tenant_id STRING, account_id STRING,
        balance_cents BIGINT
    ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {tables['manifests']} (
        run_id STRING, evidence_sha256 STRING, evidence_json STRING
    ) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {tables['sandbox']} (
        tenant_id STRING, account_id STRING, balance_cents BIGINT,
        policy STRING, run_id STRING
    ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')""")


def json_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def semantic_hash(event):
    data = event.to_dict()
    for field in ("ingested_at", "schema_version"):
        data.pop(field)
    return json_digest(data)


def fixture_rows(phase):
    input_file = ROOT / "examples" / "demo_events.jsonl"
    messages = input_file.read_text(encoding="utf-8").splitlines()
    rows, invalid = [], []
    for lineno, line in enumerate(messages, start=1):
        if not line.strip():
            continue
        try:
            raw = json.loads(line)
            if not isinstance(raw, dict):
                raise ValueError("not an object")
            event = Event.from_dict(raw)
            if phase != "all" and event.kind != ("CAPTURE" if phase == "captures" else "REFUND"):
                continue
            rows.append((lineno, event))
        except (ValueError, TypeError) as exc:
            invalid.append((lineno, line, str(exc)))
    return rows, invalid


def seed(spark, tables, phase, version):
    if version < 0:
        raise ValueError("batch version must be nonnegative")
    batch_id = f"fixture-{phase}-v{version}"
    events, invalid = fixture_rows(phase)
    if invalid:
        spark.createDataFrame(
            [(batch_id, int(n), raw, reason) for n, raw, reason in invalid],
            "batch_id string, message_ordinal int, raw_line string, reason string"
        ).write.format("delta").mode("append").saveAsTable(tables["quarantine"])
        raise ValueError(f"{len(invalid)} invalid fixture deliveries quarantined; aborting batch")
    if not events:
        raise ValueError("Fixture phase contains no valid events")
    payload = [(n, ev.to_dict()) for n, ev in events]
    sha = json_digest(payload)
    old_bronze = (spark.table(tables["bronze"])
                  .filter(F.col("batch_id") == batch_id)
                  .select("source_sha256").distinct().collect())
    if old_bronze and (len(old_bronze) != 1 or old_bronze[0][0] != sha):
        raise ValueError("Batch identifier collision with previously archived bytes")
    old_receipts = (spark.table(tables["batches"])
                    .filter(F.col("batch_id") == batch_id).collect())
    if any(x["input_sha256"] != sha for x in old_receipts):
        raise ValueError("Batch identifier collision with recorded manifest")
    if old_receipts:
        print(json.dumps({"batch": batch_id, "status": "ALREADY_INGESTED", "count": len(events)}))
        return
    # Check business identity against BOTH this batch and previously recorded
    # deliveries. Different ingestion timestamps are legitimate retry metadata.
    candidates = [(ev.tenant_id, ev.event_id, semantic_hash(ev)) for _, ev in events]
    local_ids = {}
    for tenant, event_id, fingerprint in candidates:
        key = (tenant, event_id)
        if key in local_ids and local_ids[key] != fingerprint:
            raise ValueError(f"Conflicting identity within batch: {key}")
        local_ids[key] = fingerprint
    existing = spark.table(tables["bronze"]).select(
        "tenant_id", "event_id", "event_hash"
    ).distinct().collect()
    for row in existing:
        key = (row["tenant_id"], row["event_id"])
        if key in local_ids and local_ids[key] != row["event_hash"]:
            raise ValueError(f"Conflicting archived business identity: {key}")
    source = []
    for lineno, ev in events:
        row = ev.to_dict()
        delivery_id = hashlib.sha256(f"{batch_id}:{lineno}".encode()).hexdigest()
        source.append((batch_id, lineno, delivery_id, row["tenant_id"], row["event_id"],
                       row["transaction_id"], row["account_id"], row["kind"],
                       row["amount_cents"], row["currency"], row["occurred_at"],
                       row["ingested_at"], row["schema_version"], semantic_hash(ev), sha))
    schema = """batch_id string, message_ordinal int, delivery_id string,
        tenant_id string, event_id string, transaction_id string,
        account_id string, kind string, amount_cents long, currency string,
        occurred_at string, ingested_at string, schema_version int,
        event_hash string, source_sha256 string"""
    frame = spark.createDataFrame(source, schema)
    # txnAppId+txnVersion guard job retries on Bronze, but NOT a permanent
    # global exactly-once guarantee (transaction log retention matters).
    (frame.write.format("delta").mode("append")
     .option("txnAppId", "temporalx-fixture-s02")
     .option("txnVersion", version)
     .saveAsTable(tables["bronze"]))
    manifest = spark.createDataFrame([(batch_id, sha, len(events))],
                                     "batch_id string, input_sha256 string, accepted long")
    # A crash between Bronze and receipt write is repaired by the retry.
    (DeltaTable.forName(spark, tables["batches"]).alias("t")
     .merge(manifest.alias("s"), "t.batch_id=s.batch_id")
     .whenNotMatchedInsertAll().execute())
    print(json.dumps({"batch": batch_id, "status": "ARCHIVED", "deliveries": len(events),
                      "source_sha256": sha}))


def reject_conflicts(df):
    broken = (df.groupBy("tenant_id", "event_id")
              .agg(F.countDistinct("event_hash").alias("variants"))
              .filter(F.col("variants") > 1).limit(1).count())
    if broken:
        raise ValueError("Fail closed: tenant-scoped event identity collision")
    owner = (df.groupBy("tenant_id", "transaction_id")
             .agg(F.countDistinct("account_id").alias("owners"))
             .filter(F.col("owners") > 1).limit(1).count())
    if owner:
        raise ValueError("Fail closed: transaction reassigned to another account")


def materialize_silver(spark, tables):
    bronze = spark.table(tables["bronze"])
    if not bronze.limit(1).count():
        raise ValueError("No archived events; seed fixture first")
    reject_conflicts(bronze)
    by_identity = Window.partitionBy("tenant_id", "event_id")
    ordered = by_identity.orderBy("ingested_at", "batch_id", "message_ordinal")
    silver = (bronze.withColumn("delivery_count", F.count("*").over(by_identity))
              .withColumn("ordinal_rank", F.row_number().over(ordered))
              .filter(F.col("ordinal_rank") == 1)
              .select("tenant_id", "event_id", "transaction_id", "account_id",
                      "kind", "amount_cents", "currency", "occurred_at",
                      F.col("ingested_at").alias("first_ingested_at"),
                      "event_hash", "delivery_count"))
    (DeltaTable.forName(spark, tables["silver"]).alias("t")
     .merge(silver.alias("s"), "t.tenant_id=s.tenant_id AND t.event_id=s.event_id")
     .whenMatchedUpdate(
         condition="t.first_ingested_at <> s.first_ingested_at OR t.delivery_count <> s.delivery_count",
         set={"first_ingested_at": "s.first_ingested_at", "delivery_count": "s.delivery_count"}
     ).whenNotMatchedInsertAll().execute())
    print(json.dumps({"status": "SILVER_MATERIALIZED", "logical_events": silver.count()}))


def balances(df):
    reject_conflicts(df)
    signed = df.withColumn("signed", F.when(F.col("kind") == "CAPTURE", F.col("amount_cents"))
                           .otherwise(-F.col("amount_cents")))
    invalid = (signed.groupBy("tenant_id", "transaction_id")
               .agg(F.sum("signed").alias("balance"))
               .filter(F.col("balance") < 0).limit(1).count())
    if invalid:
        raise ValueError("Invariant violation: refund exceeds captures")
    return {(row["tenant_id"], row["account_id"]): int(row["balance"])
            for row in (signed.groupBy("tenant_id", "account_id")
                        .agg(F.sum("signed").alias("balance")).collect())}


def reconcile(spark, tables, run_id, known_at, valid_at, assert_fixture):
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", run_id):
        raise ValueError("Unsafe run_id")
    bronze = spark.table(tables["bronze"])
    silver = spark.table(tables["silver"])
    # Normalize the CLI bounds before doing lexical comparison on canonical UTC
    # fixture timestamps. Production must use TIMESTAMP-typed columns.
    valid_at = parse_instant(valid_at).isoformat() if valid_at else None
    known_at = parse_instant(known_at).isoformat() if known_at else None
    if valid_at:
        bronze = bronze.filter(F.col("occurred_at") <= valid_at)
        silver = silver.filter(F.col("occurred_at") <= valid_at)
    if known_at:
        bronze = bronze.filter(F.col("ingested_at") <= known_at)
        silver = silver.filter(F.col("first_ingested_at") <= known_at)
    baseline = balances(bronze)
    candidate = balances(silver)
    # For the fixed demo, a global anti-join ensures Silver and Bronze agree on
    # tenant/event identity after deduplication. This is not a scale benchmark.
    unique = bronze.select("tenant_id", "event_id").distinct()
    logical = silver.select("tenant_id", "event_id").distinct()
    if unique.subtract(logical).limit(1).count() or logical.subtract(unique).limit(1).count():
        raise AssertionError("Silver does not reflect the filtered Bronze logical event set")
    if assert_fixture and not valid_at and not known_at:
        expected = json.loads((ROOT / "examples" / "demo_expected.json").read_text())
        encode = lambda d: {json.dumps(list(k), separators=(",", ":")): v for k, v in d.items()}
        if encode(baseline) != expected["baseline"]["account_balances"]:
            raise AssertionError("Bronze v1 diverges from local oracle")
        if encode(candidate) != expected["candidate"]["account_balances"]:
            raise AssertionError("Silver v2 diverges from local oracle")
    # Research-sized manifest: input delivery IDs, policy version, source
    # source-batch hashes, source code digest and temporal cutoffs.
    input_ids = sorted(r["delivery_id"] for r in bronze.select("delivery_id").collect())
    batches = sorted((r["batch_id"], r["source_sha256"])
                     for r in bronze.select("batch_id", "source_sha256").distinct().collect())
    manifest = {
        "run_id": run_id, "snapshot_sha256": json_digest(input_ids),
        "source_batches": batches,
        "code_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "known_at": known_at, "valid_at": valid_at,
        "policies": ["v1", "v2"],
        "baseline_total_cents": sum(baseline.values()),
        "candidate_total_cents": sum(candidate.values()),
    }
    manifest_json = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    manifest_hash = hashlib.sha256(manifest_json.encode()).hexdigest()
    old_manifests = (spark.table(tables["manifests"])
                     .filter(F.col("run_id") == run_id).collect())
    if old_manifests and (len(old_manifests) != 1 or
                          old_manifests[0]["evidence_sha256"] != manifest_hash):
        raise AssertionError("run_id replay manifest changed: reject reuse")
    records = [(run_id, policy, tenant, account, value)
               for policy, values in (("v1", baseline), ("v2", candidate))
               for (tenant, account), value in sorted(values.items())]
    existing_rows = spark.table(tables["replays"]).filter(F.col("run_id") == run_id).collect()
    old = {(r["policy"], r["tenant_id"], r["account_id"]): int(r["balance_cents"])
           for r in existing_rows}
    new = {(p, t, a): amount for _, p, t, a, amount in records}
    if old and old != new:
        raise AssertionError("Existing run_id has different replay evidence")
    frame = spark.createDataFrame(records,
                                  "run_id string, policy string, tenant_id string, account_id string, balance_cents long")
    (DeltaTable.forName(spark, tables["replays"]).alias("t")
     .merge(frame.alias("s"), "t.run_id=s.run_id AND t.policy=s.policy AND "
                              "t.tenant_id=s.tenant_id AND t.account_id=s.account_id")
     .whenNotMatchedInsertAll().execute())
    saved_manifest = spark.createDataFrame(
        [(run_id, manifest_hash, manifest_json)],
        "run_id string, evidence_sha256 string, evidence_json string")
    (DeltaTable.forName(spark, tables["manifests"]).alias("t")
     .merge(saved_manifest.alias("s"), "t.run_id=s.run_id")
     .whenNotMatchedInsertAll().execute())
    delta = sum(candidate.values()) - sum(baseline.values())
    print(json.dumps({"status": "RECONCILED", "run_id": run_id,
                      "baseline_cents": sum(baseline.values()),
                      "candidate_cents": sum(candidate.values()),
                      "delta_cents": delta, "accounts": len(candidate),
                      "evidence_sha256": manifest_hash}))


def publish_sandbox(spark, tables, run_id, policy, approved):
    if not approved:
        raise PermissionError("Sandbox publication requires --approve-sandbox")
    branch = (spark.table(tables["replays"])
              .filter((F.col("run_id") == run_id) & (F.col("policy") == policy)))
    if not branch.limit(1).count():
        raise ValueError("No approved candidate: run must exist before sandbox publication")
    (DeltaTable.forName(spark, tables["sandbox"]).alias("t")
     .merge(branch.alias("s"), "t.tenant_id=s.tenant_id AND t.account_id=s.account_id")
     .whenMatchedUpdate(
         condition="t.balance_cents <> s.balance_cents OR t.policy <> s.policy OR t.run_id <> s.run_id",
         set={"balance_cents": "s.balance_cents", "policy": "s.policy", "run_id": "s.run_id"}
     ).whenNotMatchedInsert(values={
         "tenant_id": "s.tenant_id", "account_id": "s.account_id",
         "balance_cents": "s.balance_cents", "policy": "s.policy", "run_id": "s.run_id"
     }).execute())
    print(json.dumps({"status": "SANDBOX_ONLY", "run_id": run_id, "policy": policy}))


def read_cdf(spark, tables, start_version):
    if start_version < 0:
        raise ValueError("starting-version must be nonnegative")
    df = (spark.read.format("delta").option("readChangeFeed", "true")
          .option("startingVersion", start_version).table(tables["sandbox"]))
    rows = [row.asDict(recursive=True) for row in df.limit(100).collect()]
    print(json.dumps({"table": tables["sandbox"], "rows_first_100": rows}, default=str))


def main():
    parser = argparse.ArgumentParser(description="TemporalX Sprint 02 Databricks Delta lab")
    parser.add_argument("action", choices=["seed", "silver", "compare", "publish", "cdf"])
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--phase", choices=["captures", "refunds", "all"], default="all")
    parser.add_argument("--batch-version", type=int, default=1)
    parser.add_argument("--run-id", default="demo-full")
    parser.add_argument("--known-at", default=None)
    parser.add_argument("--valid-at", default=None)
    parser.add_argument("--policy", choices=["v1", "v2"], default="v2")
    parser.add_argument("--assert-fixture", action="store_true")
    parser.add_argument("--approve-sandbox", action="store_true")
    parser.add_argument("--starting-version", type=int, default=0)
    args = parser.parse_args()
    tables = table_names(args.catalog, args.schema)
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    create_tables(spark, tables)
    if args.action == "seed":
        seed(spark, tables, args.phase, args.batch_version)
    elif args.action == "silver":
        materialize_silver(spark, tables)
    elif args.action == "compare":
        reconcile(spark, tables, args.run_id, args.known_at, args.valid_at, args.assert_fixture)
    elif args.action == "publish":
        publish_sandbox(spark, tables, args.run_id, args.policy, args.approve_sandbox)
    else:
        read_cdf(spark, tables, args.starting_version)


if __name__ == "__main__":
    main()
