"""Actual Spark RocksDB state store lab (requires PySpark 3.5+).

Spark owns RocksDB. The tiny atomic file sink supports a reproducible
crash-after-commit-before-checkpoint experiment, NOT production throughput.
For benchmarks, the native Spark Parquet sink avoids collecting microbatches.

This code has not been exercised by CI without PySpark installed.
"""
from __future__ import annotations
import argparse
from hashlib import sha256
import json
import os
from pathlib import Path
from temporalx.rocksdb_lab import Experiment, ensure_local_contract, parse_progress, summary


def atomic_sink(batch, batch_id: int, directory: Path, fail_after_batch: int | None) -> None:
    if batch.isEmpty():
        return
    # Hard limit prevents accidental driver OOM; this is a tiny lab only.
    capped = batch.limit(10001).collect()
    if len(capped) > 10000:
        raise ValueError('Atomic demo sink supports <= 10k rows per microbatch; use --sink parquet for scale')
    rows = [row.asDict(recursive=True) for row in capped]
    payload = (''.join(json.dumps(r, sort_keys=True, default=str) + '\n' for r in sorted(rows, key=lambda d: (d['tenant_id'], d['event_id'])))).encode()
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f'batch-{batch_id:012d}.jsonl'
    if target.exists():
        if target.read_bytes() != payload:
            raise ValueError('Microbatch content changed across retry; refusing duplicate publication')
    else:
        temporary = directory / f'.batch-{batch_id:012d}-{os.getpid()}.tmp'
        with temporary.open('xb') as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        # Only one query writer per branch. os.replace atomic on same local FS.
        os.replace(temporary, target)
    if fail_after_batch == batch_id:
        # Restart with --fail-after-batch omitted. A repeated batch should find
        # the same durable file; Spark's state commit will then complete.
        raise RuntimeError(f'INJECTED_CRASH_AFTER_SINK_COMMIT batch={batch_id}')


def run(args) -> dict:
    from pyspark.sql import SparkSession, functions as F, types as T
    root = Path(args.root).resolve()
    input_dir = Path(args.input).resolve()
    if not input_dir.is_dir() or not list(input_dir.glob('*.json')):
        raise ValueError('Prepare a non-empty JSON source directory first')
    config = Experiment(branch=args.branch, strategy=args.strategy,
                        shuffle_partitions=args.shuffle_partitions,
                        max_memory_mb=args.max_memory_mb, block_cache_mb=args.block_cache_mb,
                        write_buffer_mb=args.write_buffer_mb, compact_on_commit=args.compact_on_commit,
                        track_total_rows=not args.disable_state_row_count,
                        source_identity=args.source_identity,
                        sink_identity='atomic_batch_files_v1' if args.sink == 'atomic' else 'spark_parquet_sink_v1')
    ensure_local_contract(root, config)
    # The branch fingerprint includes query shape and state provider; changing
    # snapshot/changelog creates a different branch for controlled A/B tests.
    builder = SparkSession.builder.appName('TemporalX-S04-' + args.branch)
    for key, value in config.spark_config().items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    for key, value in config.spark_config().items():
        spark.conf.set(key, value)
    schema = T.StructType([
        T.StructField('tenant_id', T.StringType()),
        T.StructField('event_id', T.StringType()),
        T.StructField('account_id', T.StringType()),
        T.StructField('kind', T.StringType()),
        T.StructField('amount_cents', T.LongType()),
        T.StructField('occurred_at', T.StringType()),
        T.StructField('source_batch', T.IntegerType()),
    ])
    df = (spark.readStream.schema(schema).option('maxFilesPerTrigger', 1).json(str(input_dir))
          .withColumn('event_time', F.to_timestamp('occurred_at'))
          .filter(F.col('tenant_id').isNotNull() & F.col('event_id').isNotNull() & F.col('event_time').isNotNull())
          .withWatermark('event_time', config.watermark)
          .dropDuplicatesWithinWatermark(['tenant_id', 'event_id'])
          .select('tenant_id', 'event_id', 'account_id', 'kind', 'amount_cents', 'event_time'))
    checkpoint = root / 'checkpoints' / args.branch
    target = root / 'accepted' / args.branch
    if args.sink == 'atomic':
        q = (df.writeStream.foreachBatch(lambda batch, idx: atomic_sink(batch, idx, target, args.fail_after_batch))
             .option('checkpointLocation', str(checkpoint)).trigger(availableNow=True).start())
    else:
        if args.fail_after_batch is not None:
            raise ValueError('Crash injection only supported with --sink atomic')
        q = (df.writeStream.format('parquet').outputMode('append').option('path', str(target))
             .option('checkpointLocation', str(checkpoint)).trigger(availableNow=True).start())
    q.awaitTermination()
    if q.exception():
        raise RuntimeError(str(q.exception()))
    raw = [json.loads(x.json) for x in q.recentProgress]
    result = {'branch': args.branch, 'provider': config.provider, 'checkpoint': str(checkpoint),
              'sink': str(target), 'progress': summary(parse_progress(raw)),
              'contract_sha256': config.fingerprint}
    metrics_path = root / 'metrics' / (args.branch + '.json')
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(raw, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(result, indent=2))
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input', default='artifacts/rocksdb-input')
    p.add_argument('--root', default='artifacts/rocksdb-runs')
    p.add_argument('--branch', required=True)
    p.add_argument('--strategy', choices=['snapshot', 'changelog'], required=True)
    p.add_argument('--sink', choices=['atomic', 'parquet'], default='atomic')
    p.add_argument('--fail-after-batch', type=int, default=None)
    p.add_argument('--max-memory-mb', type=int, default=512)
    p.add_argument('--shuffle-partitions', type=int, default=2)
    p.add_argument('--block-cache-mb', type=int, default=32)
    p.add_argument('--write-buffer-mb', type=int, default=32)
    p.add_argument('--compact-on-commit', action='store_true')
    p.add_argument('--disable-state-row-count', action='store_true')
    p.add_argument('--source-identity', default='synthetic_json_v1')
    args = p.parse_args()
    run(args)

if __name__ == '__main__':
    main()
