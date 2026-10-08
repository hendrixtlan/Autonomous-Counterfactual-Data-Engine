"""Databricks-only experimental Kafka->Delta->stateful stream + isolated backfill.

Requires access to a pre-provisioned UC catalog/schema, a UC Volume for checkpoints,
network connectivity to Kafka, and a compatible Databricks Runtime >=14.3 LTS.
No destructive operations; outputs restricted by validated lab table names.

NOT validated against a live workspace in this release. See Sprint 03 runbook.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re

from pyspark.sql import SparkSession, functions as F, types as T, Window
from delta.tables import DeltaTable

IDENT = re.compile(r'^[A-Za-z_][A-Za-z_0-9]*$')
BRANCH = re.compile(r'^[a-z][a-z0-9_]{0,35}$')
FIELDS = T.StructType([
    T.StructField('tenant_id', T.StringType()),
    T.StructField('event_id', T.StringType()),
    T.StructField('transaction_id', T.StringType()),
    T.StructField('account_id', T.StringType()),
    T.StructField('kind', T.StringType()),
    T.StructField('amount_cents', T.LongType()),
    T.StructField('occurred_at', T.StringType()),
    T.StructField('ingested_at', T.StringType()),
    T.StructField('currency', T.StringType()),
    T.StructField('schema_version', T.IntegerType()),
])


def qualified(catalog: str, schema: str, name: str) -> str:
    if not all(IDENT.fullmatch(v) for v in (catalog, schema, name)):
        raise ValueError('Unsafe Unity Catalog identifier')
    return f'`{catalog}`.`{schema}`.`{name}`'


def checkpoint_path(root: str, suffix: str) -> str:
    if not root.startswith('/Volumes/') or '..' in root or '//' in root or any(c.isspace() for c in root):
        raise ValueError('Checkpoint root must be a UC Volume path: /Volumes/catalog/schema/volume/...')
    return root.rstrip('/') + '/' + suffix


def create_tables(spark, names):
    spark.sql(f'''CREATE TABLE IF NOT EXISTS {names['bronze']} (
        topic STRING, partition_id INT, offset_id BIGINT, broker_timestamp TIMESTAMP,
        raw_json STRING, raw_sha256 STRING, tenant_id STRING, event_id STRING,
        transaction_id STRING, account_id STRING, kind STRING, amount_cents BIGINT,
        occurred_at STRING, ingested_at STRING, currency STRING, schema_version INT,
        occurred_ts TIMESTAMP, parse_status STRING
    ) USING DELTA''')
    spark.sql(f'''CREATE TABLE IF NOT EXISTS {names['contract']} (
        branch STRING, signature STRING, sink_name STRING
    ) USING DELTA''')


def safe_parse(source):
    b = source.select(F.col('topic'), F.col('partition').alias('partition_id'),
                      F.col('offset').alias('offset_id'), F.col('timestamp').alias('broker_timestamp'),
                      F.col('value').cast('string').alias('raw_json'))
    b = b.withColumn('payload', F.from_json('raw_json', FIELDS))
    for name in FIELDS.names:
        b = b.withColumn(name, F.col('payload')[name])
    b = b.withColumn('occurred_ts', F.to_timestamp('occurred_at'))
    valid = (F.col('tenant_id').isNotNull() & (F.length(F.trim('tenant_id')) > 0) &
             F.col('event_id').isNotNull() & (F.length(F.trim('event_id')) > 0) &
             F.col('transaction_id').isNotNull() & (F.length(F.trim('transaction_id')) > 0) &
             F.col('account_id').isNotNull() & (F.length(F.trim('account_id')) > 0) &
             F.col('kind').isin('CAPTURE', 'REFUND') & (F.col('amount_cents') > 0) &
             (F.col('currency') == 'USD') & F.col('schema_version').isin(1, 2) &
             F.col('occurred_ts').isNotNull() &
             F.col('occurred_at').rlike(r'(Z|[+-][0-9]{2}:[0-9]{2})$') &
             F.col('ingested_at').rlike(r'(Z|[+-][0-9]{2}:[0-9]{2})$'))
    return (b.withColumn('parse_status', F.when(valid, F.lit('valid')).otherwise(F.lit('invalid')))
             .withColumn('raw_sha256', F.sha2('raw_json', 256))
             .drop('payload'))


def ingest_kafka(spark, args, names):
    if args.bootstrap.startswith('REPLACE') or not args.bootstrap.strip():
        raise ValueError('Set a Kafka broker reachable from your Databricks compute')
    source = (spark.readStream.format('kafka')
              .option('kafka.bootstrap.servers', args.bootstrap)
              .option('subscribe', args.topic)
              .option('startingOffsets', 'earliest')
              .option('failOnDataLoss', 'true')
              .option('maxOffsetsPerTrigger', 50000).load())
    parsed = safe_parse(source)

    def commit_batch(batch, batch_id):
        if batch.isEmpty():
            return
        # MERGE and source offset key ensure retry-idempotence into ONE Delta table.
        # Check for conflicting re-use of a source coordinate before MERGE.
        original = spark.table(names['bronze']).select('topic', 'partition_id', 'offset_id', 'raw_sha256')
        collision = (batch.alias('n').join(original.alias('o'), ['topic', 'partition_id', 'offset_id'])
                     .where(F.col('n.raw_sha256') != F.col('o.raw_sha256')).limit(1).count())
        if collision:
            raise ValueError('Kafka physical coordinate has conflicting payload; refusing merge')
        (DeltaTable.forName(spark, names['bronze']).alias('old')
         .merge(batch.alias('new'), '''old.topic = new.topic AND
             old.partition_id = new.partition_id AND old.offset_id = new.offset_id''')
         .whenNotMatchedInsertAll().execute())
        print(json.dumps({'phase': 'bronze', 'batch_id': batch_id}))

    q = (parsed.writeStream.foreachBatch(commit_batch)
         .option('checkpointLocation', checkpoint_path(args.checkpoint_root, 's03/kafka_bronze_v1'))
         .trigger(availableNow=True).start())
    q.awaitTermination()
    if q.exception():
        raise RuntimeError(q.exception())


def register_branch(spark, names, branch, signature, sink_name):
    found = spark.table(names['contract']).where(F.col('branch') == branch).collect()
    if any(row['signature'] != signature or row['sink_name'] != sink_name for row in found):
        raise ValueError('Incompatible checkpoint contract: allocate a NEW branch')
    if not found:
        spark.createDataFrame([(branch, signature, sink_name)], 'branch string, signature string, sink_name string').write.format('delta').mode('append').saveAsTable(names['contract'])
    # Single-writer lab only. The manifest INSERT is not distributed CAS.


def streaming_state(spark, args, names):
    if not BRANCH.fullmatch(args.branch):
        raise ValueError('Unsafe branch: use lowercase letters, digits, underscores')
    if args.policy not in ('v1', 'v2'):
        raise ValueError('unsupported policy')
    sink_name = f'temporalx_s03_windows_{args.branch}'
    sink = qualified(args.catalog, args.schema, sink_name)
    definition = {'source': 's03_bronze', 'watermark': '5 minutes', 'window': '5 minutes',
                  'policy': args.policy, 'dedup': ['tenant_id', 'event_id'] if args.policy == 'v2' else [],
                  'sink': sink_name, 'operator_revision': 's03-spark-r1'}
    signature = hashlib.sha256(json.dumps(definition, sort_keys=True).encode()).hexdigest()
    register_branch(spark, names, args.branch, signature, sink_name)
    spark.sql(f'''CREATE TABLE IF NOT EXISTS {sink} (
        tenant_id STRING, account_id STRING, window_start TIMESTAMP,
        window_end TIMESTAMP, net_cents BIGINT
    ) USING DELTA''')
    source = (spark.readStream.option('withEventTimeOrder', 'true').table(names['bronze'])
              .filter(F.col('parse_status') == 'valid')
              .withWatermark('occurred_ts', '5 minutes'))
    if args.policy == 'v2':
        source = source.dropDuplicatesWithinWatermark(['tenant_id', 'event_id'])
    aggregations = (source.groupBy('tenant_id', 'account_id', F.window('occurred_ts', '5 minutes').alias('w'))
                    .agg(F.sum(F.when(F.col('kind') == 'CAPTURE', F.col('amount_cents'))
                               .otherwise(-F.col('amount_cents'))).cast('long').alias('net_cents'))
                    .select('tenant_id', 'account_id', F.col('w.start').alias('window_start'),
                            F.col('w.end').alias('window_end'), 'net_cents'))
    # Append output emits a window after watermark finalizes it. A last open
    # window may not appear until a later event advances the watermark.
    q = (aggregations.writeStream.format('delta').outputMode('append')
         .option('checkpointLocation', checkpoint_path(args.checkpoint_root, 's03/state_' + args.branch))
         .trigger(availableNow=True).toTable(sink))
    q.awaitTermination()
    if q.exception():
        raise RuntimeError(q.exception())


def verify_identity_consistency(source):
    fields = ['tenant_id', 'event_id', 'transaction_id', 'account_id', 'kind',
              'amount_cents', 'occurred_at', 'currency']
    stamped = source.withColumn('business_fingerprint', F.sha2(F.to_json(F.struct(*[F.col(c) for c in fields])), 256))
    inconsistent = (stamped.groupBy('tenant_id', 'event_id')
                    .agg(F.countDistinct('business_fingerprint').alias('variants'))
                    .where(F.col('variants') > 1).limit(1).count())
    if inconsistent:
        raise ValueError('Conflicting logical event identity in Bronze; reject replay')
    return stamped


def offline_backfill(spark, args, names):
    if not BRANCH.fullmatch(args.branch):
        raise ValueError('Invalid isolated replay branch')
    # Never overwrite an existing repair branch. Repeat execution must use a new name.
    target = qualified(args.catalog, args.schema, 'temporalx_s03_backfill_' + args.branch)
    if spark.catalog.tableExists(target.replace('`', '')):
        raise ValueError('Replay branch already exists; choose a new branch')
    # Fixture-sized first: intentionally choose stable earliest physical delivery.
    source = verify_identity_consistency(spark.table(names['bronze']).filter("parse_status = 'valid'"))
    sort = Window.partitionBy('tenant_id', 'event_id').orderBy('broker_timestamp', 'topic', 'partition_id', 'offset_id')
    unique = (source.withColumn('rn', F.row_number().over(sort)).filter('rn = 1').drop('rn'))
    result = (unique.groupBy('tenant_id', 'account_id')
              .agg(F.sum(F.when(F.col('kind') == 'CAPTURE', F.col('amount_cents'))
                         .otherwise(-F.col('amount_cents'))).cast('long').alias('net_cents')))
    result.write.format('delta').mode('errorifexists').saveAsTable(target)
    print(json.dumps({'replay': target, 'rows': result.count(),
                      'total_cents': result.agg(F.sum('net_cents')).first()[0]}))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['capture', 'stream', 'backfill', 'inspect'])
    parser.add_argument('--catalog', required=True)
    parser.add_argument('--schema', required=True)
    parser.add_argument('--checkpoint-root', default='/Volumes/REPLACE_ME/schema/volume/checkpoints')
    parser.add_argument('--bootstrap', default='REPLACE_WITH_REACHABLE_KAFKA_HOST:9092')
    parser.add_argument('--topic', default='temporalx.transactions')
    parser.add_argument('--policy', choices=['v1', 'v2'], default='v2')
    parser.add_argument('--branch', default='candidate_v2')
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    names = {'bronze': qualified(args.catalog, args.schema, 'temporalx_s03_bronze_kafka'),
             'contract': qualified(args.catalog, args.schema, 'temporalx_s03_contracts')}
    create_tables(spark, names)
    if args.action == 'capture':
        ingest_kafka(spark, args, names)
    elif args.action == 'stream':
        streaming_state(spark, args, names)
    elif args.action == 'backfill':
        offline_backfill(spark, args, names)
    else:
        b = spark.table(names['bronze'])
        print(json.dumps({'count': b.count(), 'valid': b.filter("parse_status = 'valid'").count(),
                          'invalid': b.filter("parse_status = 'invalid'").count()}))


if __name__ == '__main__':
    main()
