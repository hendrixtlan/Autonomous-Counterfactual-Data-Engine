"""Distributed microbatch state-diff lab for Databricks State Reader.

Uses state-change feeds from two ISOLATED synthetic checkpoints with the same
source lineage and state contract. No collect of account state to driver:
full outer join + per-key windows + aggregate-only materialization.

Requires DBR >=16.4 for readChangeFeed; checkpoint history must span *all*
requested batches, otherwise detection is refused as unprovable.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from temporalx.lakehouse_modernization import safe_uc_name, safe_lab_schema, ModernizationError

_FIELDS=('source_snapshot_id','source_sha256','batch_lineage_sha256',
         'operator_revision','state_kind','watermark_policy','tenant_scope','currency')


def aligned_manifests(a: dict, b: dict) -> dict:
    """Only compare independent checkpoints whose reference semantics are aligned."""
    if not isinstance(a,dict) or not isinstance(b,dict):
        raise ModernizationError('Missing evidence provenance')
    expected=set(_FIELDS)|{'checkpoint_id','first_batch_id','last_batch_id','operator_id'}
    if set(a)!=expected or set(b)!=expected:
        raise ModernizationError('Incomplete forensic provenance contract')
    for key in _FIELDS+('first_batch_id','last_batch_id','operator_id'):
        if a[key]!=b[key]:
            raise ModernizationError('INCOMPARABLE: '+key)
    if a['checkpoint_id']==b['checkpoint_id']:
        raise ModernizationError('Forensic comparison must use independent checkpoint branches')
    for key in ('source_sha256','batch_lineage_sha256'):
        v=a[key]
        if not isinstance(v,str) or not re.fullmatch('[0-9a-f]{64}',v):
            raise ModernizationError('Malformed digest in '+key)
    lo=a['first_batch_id'];hi=a['last_batch_id']
    if type(lo) is not int or type(hi) is not int or not (0 <= lo <= hi and hi-lo < 50000):
        raise ModernizationError('Invalid checkpoint batch interval')
    if type(a['operator_id']) is not int or a['operator_id']<0:
        raise ModernizationError('Invalid operator')
    if a['state_kind']!='account-net-cents-v1' or a['currency']!='USD':
        raise ModernizationError('Unsupported state semantics')
    if (not isinstance(a['tenant_scope'],list) or not a['tenant_scope'] or
        any(not isinstance(t,str) or not t for t in a['tenant_scope']) or
        sorted(set(a['tenant_scope'])) != a['tenant_scope']):
        raise ModernizationError('Explicit tenant scope required')
    return {k:a[k] for k in _FIELDS+('first_batch_id','last_batch_id','operator_id')}


def check_batch_coverage(available: list[dict], first: int, last: int) -> None:
    """Presence of all retained changelog batches must be independently attested.

    State metadata min/max is a necessary, not sufficient, condition: gaps or
    truncated changelogs can still make a query fail. Never claim 'first-ever'
    unless coverage begins at zero.
    """
    if len(available)!=2 or any(int(x['minBatchId'])>first or int(x['maxBatchId'])<last for x in available):
        raise ModernizationError('Incomplete retained microbatch coverage: abort comparison')


def safe_path(p: str):
    if (not isinstance(p,str) or not p.startswith('/Volumes/') or '..' in p or '\\' in p or
            '//' in p or any(c.isspace() for c in p)):
        raise ModernizationError('Only explicit UC Volume checkpoint paths allowed')
    return p.rstrip('/')


def read_changes(spark,checkpoint: str,operator: int,first: int,last: int):
    from pyspark.sql import functions as F
    source=(spark.read.format('statestore').option('operatorId',operator)
            .option('readChangeFeed',True)
            .option('changeStartBatchId',first)
            .option('changeEndBatchId',last).load(checkpoint))
    # The default state reader returns nested key/value structs; this projection
    # must be reviewed for the operator's physical schema in the actual cluster.
    return (source.select(F.col('batch_id').cast('long').alias('batch_id'),
                          F.col('key.tenant_id').alias('tenant_id'),
                          F.col('key.account_id').alias('account_id'),
                          F.col('change_type').alias('change_type'),
                          F.col('value.net_cents').cast('long').alias('net_cents'))
            .where(F.col('tenant_id').isNotNull() & F.col('account_id').isNotNull()))


def validate_changes(changes,side):
    from pyspark.sql import functions as F
    dup=(changes.groupBy('tenant_id','account_id','batch_id').count()
         .where(F.col('count')>1).limit(1).count())
    if dup:
        raise ModernizationError('Ambiguous multiple state transitions per key/batch: '+side)
    if changes.where(~F.col('change_type').isin('update','delete')).limit(1).count():
        raise ModernizationError('Unknown statestore change_type: '+side)


def distributed_first_divergence(reference, observed):
    """Return Spark DataFrame of divergence at changed keys, including healings.

    No collect, toPandas or Python set of keys. Both input dataframes must have
    one row per key/batch and explicit updates/deletions. The baseline is absent.
    """
    from pyspark.sql import functions as F, Window
    k=['tenant_id','account_id','batch_id']
    r=(reference.select(*k,
              F.struct(F.lit(True).alias('changed'), F.col('change_type').alias('op'),
                       F.col('net_cents').alias('v')).alias('r')))
    o=(observed.select(*k,
              F.struct(F.lit(True).alias('changed'), F.col('change_type').alias('op'),
                       F.col('net_cents').alias('v')).alias('o')))
    joint=r.join(o,k,'full_outer')
    w=(Window.partitionBy('tenant_id','account_id').orderBy('batch_id')
       .rowsBetween(Window.unboundedPreceding,Window.currentRow))
    state_schema='struct<present:boolean,value:bigint>'
    def current(side):
        return F.when(F.col(f'{side}.changed').isNotNull(),
             F.struct((F.col(f'{side}.op')!='delete').alias('present'),
                 F.when(F.col(f'{side}.op')!='delete', F.col(f'{side}.v'))
                  .otherwise(F.lit(None).cast('long')).alias('value')))
    data=(joint.withColumn('r_transition',current('r'))
          .withColumn('o_transition',current('o'))
          .withColumn('r_current',F.last('r_transition',ignorenulls=True).over(w))
          .withColumn('o_current',F.last('o_transition',ignorenulls=True).over(w)))
    # A key with value 0 exists; absent state is represented separately.
    rp=F.coalesce(F.col('r_current.present'),F.lit(False))
    op=F.coalesce(F.col('o_current.present'),F.lit(False))
    diff=(rp!=op) | ((rp & op) & (~F.col('r_current.value').eqNullSafe(F.col('o_current.value'))))
    return data.select(*k,rp.alias('reference_present'),op.alias('observed_present'),
                       F.col('r_current.value').alias('reference_cents'),
                       F.col('o_current.value').alias('observed_cents'),
                       diff.alias('is_divergent'))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--reference-checkpoint',required=True)
    p.add_argument('--observed-checkpoint',required=True)
    p.add_argument('--reference-manifest',required=True)
    p.add_argument('--observed-manifest',required=True)
    p.add_argument('--catalog',required=True)
    p.add_argument('--schema',required=True)
    p.add_argument('--branch',required=True)
    a=p.parse_args()
    cp_r=safe_path(a.reference_checkpoint);cp_o=safe_path(a.observed_checkpoint)
    if cp_r==cp_o: raise ModernizationError('Checkpoints must be independent')
    if not re.fullmatch('[a-z][a-z0-9_]{0,30}',a.branch):
        raise ModernizationError('Invalid branch')
    safe_uc_name(a.catalog); safe_lab_schema(a.schema)
    manifest_r=json.loads(Path(safe_path(a.reference_manifest)).read_text())
    manifest_o=json.loads(Path(safe_path(a.observed_manifest)).read_text())
    m=aligned_manifests(manifest_r,manifest_o)
    if m['first_batch_id'] != 0:
        raise ModernizationError('A baseline snapshot is required when checkpoint history does not start at batch 0; refusing incomplete-state replay')
    from pyspark.sql import SparkSession, functions as F
    spark=SparkSession.builder.getOrCreate()
    opid=m['operator_id'];first=m['first_batch_id'];last=m['last_batch_id']
    avail=[]
    for cp in (cp_r,cp_o):
        rows=(spark.read.format('state-metadata').load(cp)
              .where(F.col('operatorId') == opid)
              .select('minBatchId','maxBatchId').limit(2).collect())
        if len(rows)!=1: raise ModernizationError('Ambiguous or missing operator in state metadata')
        avail.append(rows[0].asDict())
    check_batch_coverage(avail,first,last)
    r=read_changes(spark,cp_r,opid,first,last)
    o=read_changes(spark,cp_o,opid,first,last)
    validate_changes(r,'reference');validate_changes(o,'observed')
    for side,data in (('reference',r),('observed',o)):
        if data.where(~F.col('tenant_id').isin(m['tenant_scope'])).limit(1).count():
            raise ModernizationError('Out-of-scope tenant present in '+side+' state evidence')
    result=distributed_first_divergence(r,o)
    result=result.where(F.col('tenant_id').isin(m['tenant_scope']))
    # Only small aggregate rows return to driver; detailed changes stay distributed.
    summary=(result.where('is_divergent')
             .agg(F.min('batch_id').alias('first_divergent_batch'),
                  F.count('*').alias('divergent_key_transitions')).first())
    out=f'{safe_uc_name(a.catalog)}.{safe_lab_schema(a.schema)}.`temporalx_s08_diff_{a.branch}`'
    if spark.catalog.tableExists(out.replace('`','')):
        raise ModernizationError('Diff branch exists; never overwrite previous evidence')
    result.write.format('delta').mode('errorifexists').saveAsTable(out)
    print(json.dumps({'out':out,'first_divergent_batch':summary['first_divergent_batch'],
      'divergent_key_transitions':summary['divergent_key_transitions'],
      'global_first_proven':False,'coverage_starts_at_zero':first==0,
      'checkpoint_mutated':False,'production_mutation_authorized':False}))

if __name__=='__main__':
    main()
