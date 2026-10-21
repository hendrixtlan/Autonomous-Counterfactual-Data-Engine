from copy import deepcopy
from pathlib import Path
import json
import pytest

from temporalx.lakehouse_modernization import (ModernizationError, safe_uc_name,safe_lab_schema,
    normalize_delivery,strict_utc,reconcile_deliveries,assess_legacy_manifest)
from s08.distributed_diff import aligned_manifests,check_batch_coverage,safe_path

ROOT=Path(__file__).resolve().parents[1]/'s08'/'fixtures'

def deliveries():
    return [json.loads(s) for s in (ROOT/'azure_sql_cdc.jsonl').read_text().splitlines()]


def test_bronze_silver_gold_reconciliation():
    result=reconcile_deliveries(deliveries())
    assert result['physical_deliveries']==7
    assert result['silver_events']==4
    assert result['gold']==[{'tenant_id':'blue','account_id':'acct-1','net_cents':16000},
                            {'tenant_id':'red','account_id':'acct-x','net_cents':7000}]


def test_late_out_of_order_lsn_replay_equivalence():
    assert reconcile_deliveries(list(reversed(deliveries()))) == reconcile_deliveries(deliveries())


def test_repeated_exact_delivery_idempotent():
    assert reconcile_deliveries(deliveries()+[deliveries()[0]])['gold'] == reconcile_deliveries(deliveries())['gold']


def test_conflicting_same_lsn_rejected():
    rows=deliveries();mut=deepcopy(rows[0]);mut['amount_cents']=1;rows.append(mut)
    with pytest.raises(ModernizationError,match='Conflicting CDC'):
        reconcile_deliveries(rows)


@pytest.mark.parametrize('field,bad', [('source_lsn',True),('is_deleted','False'),
    ('amount_cents',-1),('kind','CHARGEBACK'),('tenant_id','')])
def test_invalid_delivery_rejected(field,bad):
    x=deliveries()[0];x[field]=bad
    with pytest.raises(ModernizationError): normalize_delivery(x)


def test_timestamp_normalization_and_naive_rejection():
    assert strict_utc('2026-10-21T01:00:00-06:00') == '2026-10-21T07:00:00.000+00:00'
    with pytest.raises(ModernizationError):strict_utc('2026-10-21T01:00:00')


@pytest.mark.parametrize('malicious',['a.b','lab;DROP TABLE','`main`','', 'é'])
def test_identifiers_cannot_escape_lab_schema(malicious):
    with pytest.raises(ModernizationError):safe_uc_name(malicious)


def test_restrict_to_explicit_lab_schema():
    assert safe_lab_schema('lab_temporalx')=='`lab_temporalx`'
    with pytest.raises(ModernizationError):safe_lab_schema('production')


def test_assessment_requires_all_three_legacy_stages_and_blocks_cutover():
    obj=json.loads((ROOT/'legacy_migration.json').read_text())
    r=assess_legacy_manifest(obj)
    assert r['mapped_stages']==3 and r['decision']=='REVIEW_REQUIRED'
    assert r['production_cutover_authorized'] is False
    bad=deepcopy(obj);bad['mappings'].pop()
    with pytest.raises(ModernizationError):assess_legacy_manifest(bad)


def manifest(checkpoint):
    return dict(checkpoint_id=checkpoint,source_snapshot_id='blue@1',source_sha256='a'*64,
        batch_lineage_sha256='b'*64,operator_revision='sum-v1',state_kind='account-net-cents-v1',
        watermark_policy='none',tenant_scope=['blue'],currency='USD',
        first_batch_id=0,last_batch_id=4,operator_id=0)


def test_distributed_forensic_manifest_contract():
    assert aligned_manifests(manifest('a'),manifest('b'))['first_batch_id']==0
    with pytest.raises(ModernizationError,match='independent'):
        aligned_manifests(manifest('a'),manifest('a'))
    wrong=manifest('b');wrong['source_sha256']='c'*64
    with pytest.raises(ModernizationError,match='INCOMPARABLE'):
        aligned_manifests(manifest('a'),wrong)


def test_missing_batch_coverage_refused():
    check_batch_coverage([dict(minBatchId=0,maxBatchId=4)]*2,0,4)
    with pytest.raises(ModernizationError,match='Incomplete'):
        check_batch_coverage([dict(minBatchId=1,maxBatchId=4)]*2,0,4)


def test_checkpoint_path_restriction():
    assert safe_path('/Volumes/lab/schema/checkpoints/a') == '/Volumes/lab/schema/checkpoints/a'
    with pytest.raises(ModernizationError):safe_path('dbfs:/shared/prod')


def test_layout_preview_is_safe_and_includes_all_three_strategies(capsys):
    from types import SimpleNamespace
    from s08.databricks_lakehouse import layout
    class NeverWrite:
        def sql(self,_): raise AssertionError('dry-run must not execute SQL')
    for mode,expected in [('liquid','CLUSTER BY'),('partition','PARTITIONED BY'),('zorder','ZORDER BY')]:
        args=SimpleNamespace(catalog='demo',schema='lab_test',branch='trial_a',layout=mode,
                             execute_maintenance=False)
        layout(NeverWrite(),args)
        assert expected in capsys.readouterr().out


def test_autoloader_requires_safe_uc_volume():
    from s08.databricks_lakehouse import safe_volume
    assert safe_volume('/Volumes/demo/lab_test/incoming/foo')=='/Volumes/demo/lab_test/incoming/foo'
    for bad in ['/tmp/data','/Volumes/REPLACE_ME/lab/x', '/Volumes/demo/lab/../foo']:
        with pytest.raises(ModernizationError):safe_volume(bad)
