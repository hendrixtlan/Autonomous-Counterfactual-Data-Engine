"""Databricks + Unity Catalog RocksDB state lab, isolated branch per experiment.

Prerequisites: DBR >= 17.3 recommended, pre-existing UC schema and checkpoint
Volume, isolated Spark job, S03 Bronze table populated with synthetic events.
Deliberately no DROP/OVERWRITE and no automatic production publish.

This integration is not yet validated on a Databricks workspace.
"""
from __future__ import annotations
import argparse
from hashlib import sha256
import json
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import SparkSession, functions as F
from temporalx.rocksdb_lab import Experiment, parse_progress, summary

IDENT = re.compile(r'^[A-Za-z_][A-Za-z_0-9]*$')


def qualified(*names: str) -> str:
    if any(not IDENT.fullmatch(n) for n in names):
        raise ValueError('Invalid UC identifier')
    return '.'.join('`' + n + '`' for n in names)


def checkpoint(root: str, branch: str) -> str:
    if not root.startswith('/Volumes/') or '..' in root or '//' in root or any(c.isspace() for c in root):
        raise ValueError('checkpoint-root must be a UC Volume path; do not use a production checkpoint')
    return root.rstrip('/') + '/s04/rocksdb_' + branch


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--catalog', required=True)
    p.add_argument('--schema', required=True)
    p.add_argument('--checkpoint-root', required=True)
    p.add_argument('--branch', required=True)
    p.add_argument('--strategy', choices=['changelog', 'snapshot'], required=True)
    p.add_argument('--fail-after-batch', type=int, default=-1,
                   help='Inject failure AFTER Delta commit, BEFORE Spark checkpoint commit; restart with -1')
    p.add_argument('--shuffle-partitions', type=int, default=2)
    p.add_argument('--max-memory-mb', type=int, default=512)
    p.add_argument('--block-cache-mb', type=int, default=32)
    p.add_argument('--write-buffer-mb', type=int, default=32)
    p.add_argument('--compact-on-commit', action='store_true')
    p.add_argument('--disable-state-row-count', action='store_true')
    args = p.parse_args()
    exp = Experiment(branch=args.branch, strategy=args.strategy,
                     shuffle_partitions=args.shuffle_partitions, max_memory_mb=args.max_memory_mb,
                     block_cache_mb=args.block_cache_mb, write_buffer_mb=args.write_buffer_mb,
                     compact_on_commit=args.compact_on_commit, track_total_rows=not args.disable_state_row_count,
                     source_identity='s03_bronze_kafka_v1', sink_identity=f'delta_accepted_{args.branch}_v1')
    source = qualified(args.catalog, args.schema, 'temporalx_s03_bronze_kafka')
    target = qualified(args.catalog, args.schema, 'temporalx_s04_accepted_' + exp.branch)
    contracts = qualified(args.catalog, args.schema, 'temporalx_s04_contracts')
    cp = checkpoint(args.checkpoint_root, exp.branch)
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    for name, value in exp.spark_config().items():
        spark.conf.set(name, value)
    if not spark.catalog.tableExists(source.replace('`', '')):
        raise ValueError('Missing Sprint 03 Bronze table. Capture SYNTHETIC events before running S04')
    spark.sql(f'''CREATE TABLE IF NOT EXISTS {contracts} (
       branch STRING, fingerprint STRING, checkpoint STRING, sink STRING
    ) USING DELTA''')
    matches = spark.table(contracts).where(F.col('branch') == exp.branch).collect()
    if any((r['fingerprint'], r['checkpoint'], r['sink']) != (exp.fingerprint, cp, target) for r in matches):
        raise ValueError('CHECKPOINT CONTRACT MISMATCH: use another branch, never reset an existing production checkpoint')
    if not matches:
        # Single writer lab! This check+append is not a distributed compare-and-swap.
        spark.createDataFrame([(exp.branch, exp.fingerprint, cp, target)],
            'branch string, fingerprint string, checkpoint string, sink string').write.format('delta').mode('append').saveAsTable(contracts)
    spark.sql(f'''CREATE TABLE IF NOT EXISTS {target} (
        tenant_id STRING, event_id STRING, account_id STRING,
        kind STRING, amount_cents BIGINT, event_time TIMESTAMP
    ) USING DELTA''')
    original = (spark.readStream.option('withEventTimeOrder', 'true').table(source)
                .filter(F.col('parse_status') == 'valid')
                .filter(F.col('occurred_ts').isNotNull())
                .withWatermark('occurred_ts', exp.watermark)
                .dropDuplicatesWithinWatermark(['tenant_id', 'event_id'])
                .select('tenant_id', 'event_id', 'account_id', 'kind', 'amount_cents',
                        F.col('occurred_ts').alias('event_time')))

    def write_batch(batch, batch_id):
        if not batch.isEmpty():
            (batch.write.format('delta').mode('append')
                 .option('txnAppId', 'temporalx-s04-' + exp.branch)
                 .option('txnVersion', int(batch_id))
                 .saveAsTable(target))
        # A deliberate first attempt fails after the Delta sink has accepted the
        # transaction. Resubmit identical branch with --fail-after-batch -1.
        if batch_id == args.fail_after_batch:
            raise RuntimeError(f'INJECTED_CRASH_AFTER_DELTA_COMMIT batch={batch_id}')

    q = (original.writeStream.foreachBatch(write_batch).option('checkpointLocation', cp)
         .trigger(availableNow=True).start())
    q.awaitTermination()
    if q.exception():
        raise RuntimeError(str(q.exception()))
    progress = [json.loads(x.json) for x in q.recentProgress]
    report = summary(parse_progress(progress))
    print(json.dumps({'branch': exp.branch, 'checkpoint': cp, 'target': target,
                      'contract_sha256': exp.fingerprint, 'metrics': report}, sort_keys=True))
    print('TEMPORALX_S04_PROGRESS_JSON=' + json.dumps(progress, separators=(',', ':')))


if __name__ == '__main__':
    main()
