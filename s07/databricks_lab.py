"""S07 Databricks state forensics: freeze synthetic Bronze -> state -> read checkpoint.

NO checkpoint mutation in `compare`. No writes to production; all phases require
an explicitly pre-provisioned isolated UC schema and an independent UC Volume.
The job has not been executed against a Databricks workspace in this release.
"""
from __future__ import annotations
import argparse
from hashlib import sha256
import json
from pathlib import Path
import re
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

from temporalx.forensics import (ForensicEvidenceError, make_bundle, compare_bundles,
                                 project_statestore, write_new, canonical)

IDENT = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]{0,62}$')
BRANCH = re.compile(r'^[a-z][a-z0-9_]{0,39}$')
MAX_ROWS = 10_000  # intentionally bounded small research lab


def qualified(*parts):
    if not all(IDENT.fullmatch(p) for p in parts):
        raise ValueError('Invalid Unity Catalog identifier')
    return '.'.join(f'`{part}`' for part in parts)


def validate_path(p: str, purpose: str) -> str:
    if (not p.startswith('/Volumes/') or 'REPLACE' in p.upper() or '..' in p or '//' in p or
        any(c.isspace() for c in p) or '\\' in p):
        raise ValueError(f'{purpose} must be a safe Unity Catalog Volume path')
    return p.rstrip('/')


def paths(args):
    cp_root = validate_path(args.checkpoint_root, 'checkpoint root')
    evidence_root = validate_path(args.evidence_root, 'evidence root')
    if evidence_root.startswith(cp_root + '/') or cp_root.startswith(evidence_root + '/') or cp_root == evidence_root:
        raise ValueError('Checkpoint and evidence roots must not be nested or equal')
    if not BRANCH.fullmatch(args.branch):
        raise ValueError('Unsafe branch name')
    cp = f'{cp_root}/s07/account_{args.branch}'
    ev = f'{evidence_root}/s07/{args.branch}'
    return cp, ev


def seed_info_path(root):
    return f'{root}/seed-manifest.json'


def get_seed_info(root):
    p = Path(seed_info_path(root))
    info = json.loads(p.read_text(encoding='utf-8'))
    digest = info.pop('manifest_sha256')
    if digest != sha256(canonical(info)).hexdigest():
        raise ForensicEvidenceError('Seed manifest altered after creation')
    info['manifest_sha256'] = digest
    return info


def assert_seed_unchanged(spark, info):
    # Detect changes in the isolated seed table after its SHA-256 manifest was written.
    rows = capped_collect(spark.table(info['seed_table']))
    stable = sorted(rows, key=lambda d: (d['tenant_id'], d['event_id']))
    if len(rows) != info['seed_rows'] or sha256(canonical(stable)).hexdigest() != info['source_sha256']:
        raise ForensicEvidenceError('Frozen seed table changed after manifest: abort')


def capped_collect(df, limit=MAX_ROWS):
    rows = df.limit(limit + 1).collect()
    if len(rows) > limit:
        raise ForensicEvidenceError('Research row cap exceeded; use distributed diff for scale')
    return [r.asDict(recursive=True) for r in rows]


def freeze_seed(spark, args, evidence_root):
    from pyspark.sql import functions as F, Window
    if args.source_version is None or args.source_version < 0:
        raise ValueError('seed requires explicit nonnegative --source-version for reproducibility')
    source = qualified(args.catalog, args.schema, 'temporalx_s03_bronze_kafka')
    seed_table = qualified(args.catalog, args.schema, 'temporalx_s07_seed_' + args.branch)
    if spark.catalog.tableExists(seed_table.replace('`', '')):
        raise ValueError('Seed table already exists; allocate a fresh branch, never overwrite')
    raw = (spark.read.option('versionAsOf', args.source_version).table(source)
           .where((F.col('parse_status') == 'valid') & (F.col('tenant_id') == args.tenant)))
    physical = capped_collect(raw.select('tenant_id','event_id','account_id','transaction_id',
                                         'kind','amount_cents','occurred_at','currency',
                                         'topic','partition_id','offset_id','broker_timestamp'))
    if not physical:
        raise ValueError('No frozen Bronze data for requested synthetic tenant')
    signatures = {}
    for row in physical:
        key = (row['tenant_id'], row['event_id'])
        semantic = tuple(row[c] for c in ('tenant_id','event_id','transaction_id',
                            'account_id','kind','amount_cents','occurred_at','currency'))
        if key in signatures and signatures[key] != semantic:
            raise ForensicEvidenceError('Conflicting duplicate event identity in frozen Bronze')
        signatures[key] = semantic
    order = Window.partitionBy('tenant_id', 'event_id').orderBy('broker_timestamp','topic','partition_id','offset_id')
    dedup = (raw.withColumn('_rn',F.row_number().over(order)).where('_rn = 1').drop('_rn')
             .select('tenant_id','account_id','event_id',
                     F.when(F.col('kind') == 'CAPTURE', F.col('amount_cents'))
                      .otherwise(-F.col('amount_cents')).cast('long').alias('signed_cents')))
    # Initial and sole write to a NEW isolated Delta table; no overwrite.
    dedup.write.format('delta').mode('errorifexists').saveAsTable(seed_table)
    seed_rows = capped_collect(spark.table(seed_table))
    stable_rows = sorted(seed_rows, key=lambda d: (d['tenant_id'], d['event_id']))
    seed_sha = sha256(canonical(stable_rows)).hexdigest()
    snapshot_id = f'{args.catalog}.{args.schema}.temporalx_s03_bronze_kafka@{args.source_version}#seed:{seed_sha}'
    info = {'seed_table': seed_table, 'source_snapshot_id': snapshot_id,
            'source_sha256': seed_sha, 'checkpoint_branch': args.branch,
            'policy': 'v2', 'operator_revision': 's07-aggregate-v1',
            'watermark_policy': 'unbounded-history', 'tenant_scope': [args.tenant],
            'seed_rows': len(seed_rows), 'physical_deliveries': len(physical)}
    info['manifest_sha256'] = sha256(canonical(info)).hexdigest()
    write_new(seed_info_path(evidence_root), info)
    print(json.dumps({'phase':'seed','manifest_file':seed_info_path(evidence_root),
                      'seed_sha256':seed_sha, 'source_snapshot_id':snapshot_id},sort_keys=True))


def stream_aggregation(spark, args, cp, evidence_root):
    from pyspark.sql import functions as F
    info = get_seed_info(evidence_root)
    if info['checkpoint_branch'] != args.branch or info['tenant_scope'] != [args.tenant]:
        raise ForensicEvidenceError('Seed branch/tenant mismatch')
    assert_seed_unchanged(spark, info)
    src = spark.readStream.table(info['seed_table'])
    # Exactly one stateful operator: unwindowed cumulative groupBy.
    # No TTL/watermark; compare only with the exact same operator semantics.
    aggregates = (src.groupBy('tenant_id','account_id')
                  .agg(F.sum(F.col('signed_cents')).cast('long').alias('net_cents')))

    def consume_complete(batch, batch_id):
        # Intentional bounded lab with read-only sink. Materialize all state
        # in the query output; Spark owns checkpoint consistency.
        if batch.limit(MAX_ROWS+1).count() > MAX_ROWS:
            raise ForensicEvidenceError('Aggregate row limit exceeded')
        print(json.dumps({'phase':'stateful_microbatch','batch_id':int(batch_id)}))

    q = (aggregates.writeStream.outputMode('complete').foreachBatch(consume_complete)
         .option('checkpointLocation', cp).trigger(availableNow=True).start())
    q.awaitTermination()
    if q.exception():
        raise RuntimeError(str(q.exception()))
    print(json.dumps({'phase':'stateful_complete','checkpoint':cp, 'query_id':str(q.id)}, sort_keys=True))


def compare_checkpoint(spark, args, cp, evidence_root):
    from pyspark.sql import functions as F
    info = get_seed_info(evidence_root)
    if info['checkpoint_branch'] != args.branch or info['tenant_scope'] != [args.tenant]:
        raise ForensicEvidenceError('Seed/tenant mismatch')
    assert_seed_unchanged(spark, info)
    meta = capped_collect(spark.read.format('state-metadata').load(cp))
    ops = [r for r in meta if int(r['operatorId']) == args.operator_id]
    if len(ops) != 1:
        raise ForensicEvidenceError('Could not uniquely identify aggregate state operator')
    op = ops[0]
    batch_id = int(op['maxBatchId']) if args.batch_id is None else args.batch_id
    if batch_id != int(op['maxBatchId']):
        raise ForensicEvidenceError('Only latest committed batch can be compared to full frozen seed')
    if not (int(op['minBatchId']) <= batch_id <= int(op['maxBatchId'])):
        raise ForensicEvidenceError('Selected batch not present in retained checkpoint')
    reader = spark.read.format('statestore').option('operatorId', args.operator_id).option('batchId',batch_id)
    observed_df = reader.load(cp)
    print('ACTUAL_STATSTORE_SCHEMA', observed_df.schema.simpleString())
    raw_rows = capped_collect(observed_df)
    observed = project_statestore(raw_rows, tenant_scope=[args.tenant],
                 tenant_field=args.key_tenant, account_field=args.key_account, cents_field=args.value_field,
                 max_rows=MAX_ROWS)
    oracle = (spark.table(info['seed_table']).groupBy('tenant_id','account_id')
              .agg(F.sum(F.col('signed_cents')).cast('long').alias('net_cents')))
    reference = capped_collect(oracle)
    md = dict(state_kind='account-net-cents-v1', source_snapshot_id=info['source_snapshot_id'],
              source_sha256=info['source_sha256'], policy=info['policy'],
              operator_revision=info['operator_revision'], checkpoint_batch_id=batch_id,
              operator_id=args.operator_id, watermark_policy=info['watermark_policy'],
              tenant_scope=[args.tenant], currency='USD')
    a = make_bundle({**md, 'origin':'replay'}, reference)
    b = make_bundle({**md, 'origin':'checkpoint'}, observed)
    result = compare_bundles(a,b)
    # All output files are append-only, separated from checkpoint path.
    prefix = f'{evidence_root}/batch-{batch_id:012d}'
    write_new(prefix + '-reference.json', a)
    write_new(prefix + '-checkpoint.json', b)
    write_new(prefix + '-diff.json', result)
    print(json.dumps({'phase':'forensic_diff', 'decision':result['decision'],
                      'counts':result['counts'], 'checkpoint':cp,
                      'report_file':prefix+'-diff.json', 'checkpoint_modified':False},sort_keys=True))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('phase',choices=('seed','stream','compare'))
    p.add_argument('--catalog',required=True)
    p.add_argument('--schema',required=True)
    p.add_argument('--checkpoint-root',required=True)
    p.add_argument('--evidence-root',required=True)
    p.add_argument('--branch',required=True)
    p.add_argument('--tenant',required=True)
    p.add_argument('--source-version',type=int)
    p.add_argument('--operator-id',type=int,default=0)
    p.add_argument('--batch-id',type=int)
    p.add_argument('--key-tenant',default='key.tenant_id')
    p.add_argument('--key-account',default='key.account_id')
    p.add_argument('--value-field',default='value.net_cents')
    args = p.parse_args()
    cp, ev = paths(args)
    if args.operator_id < 0 or (args.batch_id is not None and args.batch_id < 0):
        p.error('operator-id/batch-id must be nonnegative')
    # Late imports prevent Spark from being required for local unit testing.
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    if args.phase == 'seed':
        freeze_seed(spark, args, ev)
    elif args.phase == 'stream':
        # Spark owns the RocksDB state store; use DBR runtime with proper provider.
        provider = 'org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider'
        spark.conf.set('spark.sql.streaming.stateStore.providerClass',provider)
        spark.conf.set('spark.sql.shuffle.partitions','2')
        stream_aggregation(spark,args,cp,ev)
    else:
        compare_checkpoint(spark,args,cp,ev)


if __name__ == '__main__':
    main()
