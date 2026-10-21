"""Sprint 08 reference oracle and fail-closed Azure SQL/SSIS/ADF migration assessment.

Runs entirely offline; never claims equivalence with SQL Server or Spark until
representative source-system acceptance tests have been executed.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import Any, Iterable
import json
import re

KINDS = frozenset({'CAPTURE', 'REFUND'})
FIELDS = frozenset({'tenant_id','event_id','transaction_id','account_id','kind',
                    'amount_cents','occurred_at','source_updated_at','source_lsn','is_deleted'})
IDENT = re.compile(r'^[A-Za-z_][A-Za-z0-9_]{0,62}$')

class ModernizationError(ValueError):
    pass


def safe_uc_name(name: str) -> str:
    """Never interpolate an arbitrary caller-provided SQL identifier."""
    if not isinstance(name, str) or not IDENT.fullmatch(name):
        raise ModernizationError('Unsafe Unity Catalog identifier')
    return f'`{name}`'


def safe_lab_schema(name: str) -> str:
    if not (isinstance(name,str) and (name.startswith('lab_') or name.startswith('temporalx_lab'))):
        raise ModernizationError('Schema must start with lab_ or temporalx_lab')
    return safe_uc_name(name)


def strict_utc(value: Any) -> str:
    if not isinstance(value, str):
        raise ModernizationError('Timestamp must be a string with timezone')
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as e:
        raise ModernizationError('Invalid timestamp') from e
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ModernizationError('Timestamp must include UTC offset')
    return dt.astimezone(timezone.utc).isoformat(timespec='milliseconds')


def normalize_delivery(row: dict) -> dict:
    if not isinstance(row, dict) or set(row) != FIELDS:
        raise ModernizationError('Wrong delivery fields')
    out = dict(row)
    for key in ('tenant_id','event_id','transaction_id','account_id'):
        val = out[key]
        if not isinstance(val,str) or not val.strip() or len(val)>128:
            raise ModernizationError(f'Invalid {key}')
    if out['kind'] not in KINDS:
        raise ModernizationError('Unrecognized payment operation')
    if type(out['amount_cents']) is not int or not (0 < out['amount_cents'] < 2**63):
        raise ModernizationError('Money must be positive integer cents')
    if type(out['source_lsn']) is not int or not (0 <= out['source_lsn'] < 2**63):
        raise ModernizationError('Invalid source LSN')
    if type(out['is_deleted']) is not bool:
        raise ModernizationError('is_deleted must be bool')
    out['occurred_at'] = strict_utc(out['occurred_at'])
    out['source_updated_at'] = strict_utc(out['source_updated_at'])
    return out


def payload_digest(row: dict) -> str:
    return sha256(json.dumps(row,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def reconcile_deliveries(deliveries: Iterable[dict]) -> dict:
    """Silver last-LSN-wins; Gold sums non-deleted unique business events.

    A second payload at the same (tenant,event,LSN) is a conflict, not a tie.
    Source LSN must be monotonic per event; late physical arrival is accepted.
    """
    current: dict[tuple[str,str],dict] = {}
    fingerprints: dict[tuple[str,str,int],str] = {}
    physical = 0
    for item in deliveries:
        physical += 1
        row = normalize_delivery(item)
        key=(row['tenant_id'],row['event_id'])
        rev=(*key,row['source_lsn'])
        digest=payload_digest(row)
        if rev in fingerprints and fingerprints[rev] != digest:
            raise ModernizationError('Conflicting CDC records at identical event identity and LSN')
        fingerprints[rev]=digest
        if key not in current or row['source_lsn'] > current[key]['source_lsn']:
            current[key]=row
    balances=defaultdict(int)
    for x in current.values():
        if x['is_deleted']:
            continue
        balances[(x['tenant_id'],x['account_id'])] += (1 if x['kind']=='CAPTURE' else -1)*x['amount_cents']
    return {'physical_deliveries':physical,'silver_events':len(current),
            'gold': [{'tenant_id':t,'account_id':a,'net_cents':v}
                     for (t,a),v in sorted(balances.items())]}


_REQUIRED_STAGES = {'azure_sql_extract','ssis_transform','adf_orchestrate'}
_VALID_TARGET = {'autoloader_bronze','spark_sql_silver','pyspark_gold','lakeflow_jobs'}

def assess_legacy_manifest(doc: Any) -> dict:
    """Assessment only: migration blockers must not silently auto-convert."""
    if not isinstance(doc,dict) or set(doc) != {'project','mappings','known_blockers'}:
        raise ModernizationError('Invalid migration manifest')
    if not isinstance(doc['project'],str) or not doc['project']:
        raise ModernizationError('Missing project')
    if not isinstance(doc['mappings'],list) or len(doc['mappings']) < 3:
        raise ModernizationError('Missing migration mappings')
    if not isinstance(doc['known_blockers'],list) or not all(isinstance(v,str) for v in doc['known_blockers']):
        raise ModernizationError('Invalid blocker list')
    staged={}
    for item in doc['mappings']:
        if not isinstance(item,dict) or set(item) != {'source_stage','source_logic','target_component','validation'}:
            raise ModernizationError('Invalid mapping contract')
        stage=item['source_stage']
        if stage in staged or stage not in _REQUIRED_STAGES or item['target_component'] not in _VALID_TARGET:
            raise ModernizationError('Invalid or repeated migration stage')
        if not all(isinstance(item[x],str) and item[x].strip() for x in ('source_logic','validation')):
            raise ModernizationError('Missing migration acceptance criteria')
        staged[stage]=item
    missing=sorted(_REQUIRED_STAGES-set(staged))
    if missing:
        raise ModernizationError('Unmapped source stages: '+', '.join(missing))
    return {'project':doc['project'],'mapped_stages':len(staged),
            'unresolved_blockers':doc['known_blockers'],
            'decision':'REVIEW_REQUIRED' if doc['known_blockers'] else 'ACCEPTANCE_TESTS_REQUIRED',
            'production_cutover_authorized':False}
