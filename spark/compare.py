"""Databricks / PySpark demonstration of TemporalX counterfactual replay.

RUN STATUS: Requires a Databricks Spark cluster; not exercised in local-only CI.
A deliberately tiny Spark implementation is reconciled against the checked-in
Python oracle, making it useful as a first executable acceptance gate.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def compute(df, *, policy: str) -> dict[str, int]:
    if policy == "v2":
        df = df.dropDuplicates(["tenant_id", "event_id"])
    signed = df.withColumn(
        "signed_cents",
        F.when(F.col("kind") == "CAPTURE", F.col("amount_cents"))
         .otherwise(-F.col("amount_cents")),
    )
    rows = signed.groupBy("tenant_id", "account_id").agg(
        F.sum("signed_cents").alias("balance_cents")
    ).collect()
    return {
        json.dumps([r["tenant_id"], r["account_id"]], separators=(",", ":")): int(r["balance_cents"])
        for r in rows
    }


def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit-table", default="", help="Optional existing UC catalog.schema.table write permission")
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    root = Path(__file__).resolve().parents[1]
    events_path = root / "examples" / "demo_events.jsonl"
    oracle_path = root / "examples" / "demo_expected.json"
    if not events_path.exists() or not oracle_path.exists():
        raise FileNotFoundError("Bundle must sync examples/demo_events.jsonl and demo_expected.json")
    # The checked-in fixture fits in driver memory. Production input will be UC Delta.
    rows = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    df = spark.createDataFrame(rows)

    # Strong identity check: retries may have a new ingestion timestamp, but not
    # a different business payload. Do not use first() as conflict resolution.
    business_columns = ["transaction_id", "account_id", "kind", "amount_cents", "occurred_at", "currency"]
    conflicts = (df.groupBy("tenant_id", "event_id")
                 .agg(F.countDistinct(F.struct(*[F.col(c) for c in business_columns])).alias("variants"))
                 .filter(F.col("variants") > 1).limit(1).count())
    if conflicts:
        raise ValueError("Event identity collision: business payload changed")

    baseline = compute(df, policy="v1")
    candidate = compute(df, policy="v2")
    oracle = json.loads(oracle_path.read_text(encoding="utf-8"))
    if baseline != oracle["baseline"]["account_balances"]:
        raise AssertionError("Spark v1 did not match reference oracle")
    if candidate != oracle["candidate"]["account_balances"]:
        raise AssertionError("Spark v2 did not match reference oracle")
    print(json.dumps({"status": "PASS", "records": len(rows),
                      "baseline_accounts": baseline, "candidate_accounts": candidate,
                      "total_delta_cents": sum(candidate.values()) - sum(baseline.values())},
                     sort_keys=True))
    if args.audit_table:
        records = [("v1", key, amount) for key, amount in baseline.items()]
        records += [("v2", key, amount) for key, amount in candidate.items()]
        spark.createDataFrame(records, "policy string, tenant_account string, balance_cents long") \
            .write.format("delta").mode("overwrite").saveAsTable(args.audit_table)


if __name__ == "__main__":
    run()
