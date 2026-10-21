"""Sprint 08 Databricks Lakehouse modernization: executable research jobs.

All writes are confined to a PREEXISTING lab_* / temporalx_lab* UC schema.
Stages intentionally do not connect to Azure SQL: an authorized upstream CDC
extract must export approved, non-sensitive records to a UC Volume first.
PySpark/Delta/Auto Loader execution requires a Databricks workspace.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import re
import sys
from time import perf_counter

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from temporalx.lakehouse_modernization import safe_uc_name, safe_lab_schema, ModernizationError

BRANCH = re.compile(r'^[a-z][a-z0-9_]{0,30}$')
DATA_COLS = ('tenant_id','event_id','transaction_id','account_id','kind',
             'amount_cents','occurred_at','source_updated_at','source_lsn','is_deleted')


def ns(args):
    return f'{safe_uc_name(args.catalog)}.{safe_lab_schema(args.schema)}'


def table(args, suffix):
    if not re.fullmatch('[a-z0-9_]+', suffix):
        raise ModernizationError('Unsafe table suffix')
    return f'{ns(args)}.`temporalx_s08_{suffix}`'


def safe_volume(p: str, *, placeholder: bool = False):
    if not isinstance(p,str) or not p.startswith('/Volumes/') or any(x in p for x in ('..','//','\\',' ', '\n')):
        raise ModernizationError('Source and checkpoint paths must be within UC Volumes')
    components = p.split('/')[2:]
    if len(components) < 3 or not all(re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,62}', s) for s in components[:3]):
        raise ModernizationError('Invalid UC Volume path namespace')
    if not placeholder and 'REPLACE' in p.upper():
        raise ModernizationError('Replace all placeholder paths')
    return p.rstrip('/')


def schema():
    from pyspark.sql.types import (StructType,StructField,StringType,LongType,BooleanType)
    return StructType([
        StructField('tenant_id', StringType()), StructField('event_id', StringType()),
        StructField('transaction_id', StringType()), StructField('account_id', StringType()),
        StructField('kind', StringType()), StructField('amount_cents', LongType()),
        StructField('occurred_at', StringType()), StructField('source_updated_at', StringType()),
        StructField('source_lsn', LongType()), StructField('is_deleted', BooleanType()),
        StructField('_rescued_data', StringType())
    ])


def bootstrap(spark, args):
    bronze = table(args, 'bronze')
    silver = table(args, 'silver')
    # Bronze deliberately retains repeated physical deliveries. Replays depend on it.
    spark.sql(f'''CREATE TABLE IF NOT EXISTS {bronze} (
      tenant_id STRING, event_id STRING, transaction_id STRING,
      account_id STRING, kind STRING, amount_cents BIGINT,
      occurred_at STRING, source_updated_at STRING, source_lsn BIGINT,
      is_deleted BOOLEAN, _rescued_data STRING, source_file STRING,
      ingested_at TIMESTAMP
    ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')''')
    spark.sql(f'''CREATE TABLE IF NOT EXISTS {silver} (
      tenant_id STRING, event_id STRING, transaction_id STRING,
      account_id STRING, kind STRING, amount_cents BIGINT,
      occurred_at STRING, source_updated_at STRING, source_lsn BIGINT,
      is_deleted BOOLEAN, payload_hash STRING
    ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')''')
    print(json.dumps({'stage':'bootstrap','bronze':bronze,'silver':silver,'cdf_enabled':True}))


def ingest(spark, args):
    """Auto Loader file-notification/directory-listing configuration belongs to workspace."""
    from pyspark.sql import functions as F
    source = safe_volume(args.source_volume)
    cp_root = safe_volume(args.checkpoint_volume)
    checkpoint = f'{cp_root}/s08/bronze_autoloader'
    # Use a fixed checkpoint for a single ingest stream; reruns discover only new files.
    df = (spark.readStream.format('cloudFiles')
          .option('cloudFiles.format','json')
          .option('cloudFiles.schemaLocation',f'{cp_root}/s08/bronze_schema')
          .option('rescuedDataColumn','_rescued_data')
          .schema(schema()).load(source)
          .withColumn('source_file',F.col('_metadata.file_path'))
          .withColumn('ingested_at',F.current_timestamp()))
    q = (df.writeStream.format('delta').outputMode('append')
         .option('checkpointLocation', checkpoint)
         .trigger(availableNow=True).toTable(table(args,'bronze')))
    q.awaitTermination()
    if q.exception():
        raise RuntimeError(str(q.exception()))
    print(json.dumps({'stage':'bronze','checkpoint':checkpoint,'query_id':str(q.id)}))


def quality_gate(spark, args):
    """Fail closed on malformed/tied CDC identities before any Silver MERGE."""
    from pyspark.sql import functions as F
    raw = spark.table(table(args,'bronze'))
    invalid = (F.col('tenant_id').isNull() | (F.length(F.trim(F.col('tenant_id'))) == 0) |
               F.col('event_id').isNull() | (F.length(F.trim(F.col('event_id'))) == 0) |
               F.col('account_id').isNull() | F.col('transaction_id').isNull() |
               ~F.col('kind').isin('CAPTURE','REFUND') |
               F.col('amount_cents').isNull() | (F.col('amount_cents') <= 0) |
               F.col('source_lsn').isNull() | (F.col('source_lsn') < 0) |
               F.col('is_deleted').isNull() |
               F.col('occurred_at').isNull() | F.col('source_updated_at').isNull() |
               F.to_timestamp('occurred_at').isNull() | F.to_timestamp('source_updated_at').isNull() |
               (~F.col('occurred_at').rlike(r'(Z|[+-][0-9]{2}:[0-9]{2})$')) |
               (~F.col('source_updated_at').rlike(r'(Z|[+-][0-9]{2}:[0-9]{2})$')) |
               F.col('_rescued_data').isNotNull())
    if raw.where(invalid).limit(1).count():
        raise ModernizationError('Bronze DQ failed; inspect malformed records, quarantine offline, do not MERGE')
    payload = [F.col(k) for k in DATA_COLS]
    stamped = raw.withColumn('payload_hash',F.sha2(F.to_json(F.struct(*payload)),256))
    collision = (stamped.groupBy('tenant_id','event_id','source_lsn')
                 .agg(F.countDistinct('payload_hash').alias('versions'))
                 .where(F.col('versions') > 1).limit(1).count())
    if collision:
        raise ModernizationError('Conflicting payload for same (tenant,event,LSN)')
    return stamped


def silver(spark, args):
    from pyspark.sql import functions as F, Window
    raw = quality_gate(spark,args)
    order = Window.partitionBy('tenant_id','event_id').orderBy(F.col('source_lsn').desc(),
                              F.col('source_updated_at').desc())
    selected = (raw.withColumn('_rk',F.row_number().over(order)).where('_rk=1')
                .select(*DATA_COLS,'payload_hash'))
    target = spark.table(table(args,'silver'))
    conflicting_existing = (selected.alias('s').join(target.alias('t'), ['tenant_id','event_id'])
               .where((F.col('s.source_lsn') == F.col('t.source_lsn')) &
                      (F.col('s.payload_hash') != F.col('t.payload_hash'))))
    if conflicting_existing.limit(1).count():
        raise ModernizationError('Cross-run CDC LSN conflict against existing Silver')
    selected.createOrReplaceTempView('temporalx_s08_new_silver')
    columns = ', '.join(DATA_COLS+('payload_hash',))
    spark.sql(f'''MERGE INTO {table(args,'silver')} AS t
      USING temporalx_s08_new_silver AS s
      ON t.tenant_id=s.tenant_id AND t.event_id=s.event_id
      WHEN MATCHED AND s.source_lsn > t.source_lsn THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({', '.join('s.'+c for c in DATA_COLS+('payload_hash',))})''')
    print(json.dumps({'stage':'silver','source':'bronze','mode':'idempotent-source-lsn-merge'}))


def gold(spark, args):
    """Immutable per-branch Gold snapshot. No overwrite and no production promotion."""
    from pyspark.sql import functions as F
    if not BRANCH.fullmatch(args.branch):
        raise ModernizationError('Invalid immutable Gold snapshot branch')
    dest = table(args,'gold_'+args.branch)
    src = spark.table(table(args,'silver')).where(F.col('is_deleted') == False)
    out = (src.withColumn('signed_cents',F.when(F.col('kind')=='CAPTURE',F.col('amount_cents'))
                                      .otherwise(-F.col('amount_cents')))
           .groupBy('tenant_id','account_id')
           .agg(F.sum('signed_cents').cast('long').alias('net_cents'),
                F.count('*').alias('event_count')))
    if spark.catalog.tableExists(dest.replace('`','')):
        raise ModernizationError('Gold branch exists: new immutable snapshot branch required')
    out.write.format('delta').mode('errorifexists').saveAsTable(dest)
    print(json.dumps({'stage':'gold','table':dest,'semantics':'snapshot-from-silver','immutable_branch':args.branch}))


def cdf(spark, args):
    from pyspark.sql import functions as F
    if args.start_version is None or args.start_version < 0:
        raise ModernizationError('CDF requires explicit --start-version >=0')
    df=(spark.read.format('delta').option('readChangeFeed','true')
        .option('startingVersion',args.start_version).table(table(args,'silver')))
    counts = df.groupBy('_change_type').count().orderBy('_change_type').collect()
    print(json.dumps({'stage':'cdf','starting_version':args.start_version,
                      'counts':{r['_change_type']:r['count'] for r in counts}}))


def layout(spark,args):
    """Explicit maintenance only. Never silently rewrite a production table."""
    if not BRANCH.fullmatch(args.branch):
        raise ModernizationError('Unsafe branch')
    source=table(args,'gold_'+args.branch)
    if args.layout == 'zorder':
        sql=f'OPTIMIZE {source} ZORDER BY (tenant_id, account_id)'
    elif args.layout == 'liquid':
        # Always create a separate table; ZORDER and liquid are not interchangeable.
        target=table(args,'gold_liquid_'+args.branch)
        sql=f'CREATE TABLE {target} CLUSTER BY (tenant_id, account_id) AS SELECT * FROM {source}'
    elif args.layout == 'partition':
        target=table(args,'gold_partition_'+args.branch)
        sql=f'CREATE TABLE {target} USING DELTA PARTITIONED BY (tenant_id) AS SELECT * FROM {source}'
    else:
        raise ModernizationError('Unsupported layout')
    if not args.execute_maintenance:
        print(json.dumps({'stage':'layout','mode':'dry_run','sql':sql}));return
    spark.sql(sql)
    print(json.dumps({'stage':'layout','mode':'executed','layout':args.layout}))


def benchmark(spark, args):
    """Single-run measurements; A/B Photon needs separate independently configured compute."""
    if not BRANCH.fullmatch(args.branch):
        raise ModernizationError('Unsafe benchmark branch')
    table_name = table(args,'gold_'+args.branch) if args.layout == 'zorder' else table(args,'gold_'+args.layout+'_'+args.branch)
    # Require a tenant filter, do not interpolate user-provided values into SQL.
    from pyspark.sql import functions as F
    query=(spark.table(table_name).where(F.col('tenant_id') == F.lit(args.tenant))
           .agg(F.sum('net_cents').alias('net_total_cents'), F.count('*').alias('accounts')))
    samples=[]
    for _ in range(args.iterations):
        start=perf_counter()
        result=query.collect()[0]
        samples.append(round((perf_counter()-start)*1000,3))
    print(json.dumps({'stage':'benchmark','layout':args.layout,'timings_ms':samples,
         'accounts':result['accounts'],'net_total_cents':result['net_total_cents'],
         'photon_confirmed':False,'note':'To compare Photon run on separately configured Photon and Standard compute; this job does not attest the runtime engine.'}))


def lineage(spark,args):
    """Read-only UC lineage inspection; requires system-table privileges.

    Absence of rows is NOT evidence lineage is complete or missing: the system
    table has ingestion latency, retention and permission boundaries.
    """
    from pyspark.sql import functions as F
    prefix=f'{args.catalog}.{args.schema}.temporalx_s08_'.lower()
    records=(spark.table('system.access.table_lineage')
        .where(F.lower(F.coalesce(F.col('source_table_full_name'),F.lit(''))).startswith(prefix) |
               F.lower(F.coalesce(F.col('target_table_full_name'),F.lit(''))).startswith(prefix))
        .select('event_time','source_table_full_name','target_table_full_name')
        .orderBy(F.col('event_time').desc()).limit(50).collect())
    print(json.dumps({'stage':'lineage','rows':[r.asDict() for r in records],
         'note':'Read-only, limited to last 50 visible matching system-table records; may lag behind executions'},default=str))


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('phase',choices=['bootstrap','bronze','silver','gold','cdf','layout','benchmark','lineage'])
    ap.add_argument('--catalog',required=True)
    ap.add_argument('--schema',required=True)
    ap.add_argument('--source-volume',default='/Volumes/REPLACE_ME/lab_schema/source/landing')
    ap.add_argument('--checkpoint-volume',default='/Volumes/REPLACE_ME/lab_schema/checkpoints')
    ap.add_argument('--branch',default='trial_a')
    ap.add_argument('--tenant',default='blue')
    ap.add_argument('--start-version',type=int)
    ap.add_argument('--layout',choices=['zorder','liquid','partition'],default='zorder')
    ap.add_argument('--execute-maintenance',action='store_true')
    ap.add_argument('--iterations',type=int,default=3)
    args=ap.parse_args()
    ns(args)
    if not BRANCH.fullmatch(args.branch) or not (1 <= args.iterations <= 20):
        raise ModernizationError('Unsafe branch/iterations')
    # Validate all paths for any stage that touches storage. Ingest requires real Volume paths.
    if args.phase=='bronze':
        safe_volume(args.source_volume)
        safe_volume(args.checkpoint_volume)
    from pyspark.sql import SparkSession
    spark=SparkSession.builder.getOrCreate()
    {'bootstrap':bootstrap,'bronze':ingest,'silver':silver,'gold':gold,
     'cdf':cdf,'layout':layout,'benchmark':benchmark,'lineage':lineage}[args.phase](spark,args)


if __name__=='__main__':
    main()
