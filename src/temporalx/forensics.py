"""Sprint 07: strict, read-only comparison of state-store and replay account aggregates.

These are *normalized logical account aggregates*, NOT arbitrary RocksDB KV bytes.
An exporter must map its actual operator-specific key/value schema to this contract.
Hashes provide reproducibility/tamper detection, not cryptographic attestation.
"""
from __future__ import annotations

from collections import Counter
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping
import json

from temporalx.engine import replay
from temporalx.model import Event

FORMAT = 'temporalx.account-state.v1'
ALIGN = ('state_kind', 'source_snapshot_id', 'source_sha256', 'policy',
         'operator_revision', 'checkpoint_batch_id', 'operator_id',
         'watermark_policy', 'tenant_scope', 'currency')
LIMIT = 100_000


class ForensicEvidenceError(ValueError):
    """Rejected rather than presenting a misleading state comparison."""


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def sha256_bytes(data: bytes) -> str:
    return sha256(data).hexdigest()


def _positive_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 512:
        raise ForensicEvidenceError(f'{label} must be a nonempty string (<=512 characters)')
    return value


def validate_metadata(metadata: Mapping[str, Any]) -> dict:
    if not isinstance(metadata, dict):
        raise ForensicEvidenceError('metadata must be an object')
    if set(metadata) != set(ALIGN) | {'origin'}:
        raise ForensicEvidenceError('Unexpected/missing provenance fields')
    out = dict(metadata)
    for k in ('state_kind', 'source_snapshot_id', 'source_sha256', 'policy',
              'operator_revision', 'watermark_policy', 'currency', 'origin'):
        _positive_string(out[k], k)
    if out['state_kind'] != 'account-net-cents-v1' or out['currency'] != 'USD':
        raise ForensicEvidenceError('Only account-net-cents-v1 / USD is comparable')
    if out['policy'] not in ('v1', 'v2'):
        raise ForensicEvidenceError('Unsupported replay policy')
    if out['origin'] not in ('replay', 'checkpoint', 'synthetic-checkpoint'):
        raise ForensicEvidenceError('Unrecognized evidence origin')
    if (len(out['source_sha256']) != 64 or
        any(c not in '0123456789abcdef' for c in out['source_sha256'])):
        raise ForensicEvidenceError('source_sha256 must be lowercase hex sha256')
    if type(out['checkpoint_batch_id']) is not int or out['checkpoint_batch_id'] < 0:
        raise ForensicEvidenceError('checkpoint_batch_id must be nonnegative int')
    if type(out['operator_id']) is not int or out['operator_id'] < 0:
        raise ForensicEvidenceError('operator_id must be nonnegative int')
    if (not isinstance(out['tenant_scope'], list) or not out['tenant_scope'] or
        len(out['tenant_scope']) > 1000 or
        any(not isinstance(s, str) or not s or len(s) > 128 for s in out['tenant_scope']) or
        sorted(set(out['tenant_scope'])) != out['tenant_scope']):
        raise ForensicEvidenceError('tenant_scope must be a nonempty sorted unique list')
    return out


def normalize_rows(rows: Iterable[dict], scope: list[str], *, max_rows: int = LIMIT) -> list[dict]:
    """Distinct tenant/account keys; zeros are materialized, never implicitly erased."""
    normalized: dict[tuple[str, str], int] = {}
    for index, row in enumerate(rows):
        if index >= max_rows:
            raise ForensicEvidenceError('Row limit exceeded; compare via distributed Spark instead')
        if not isinstance(row, dict) or set(row) != {'tenant_id', 'account_id', 'net_cents'}:
            raise ForensicEvidenceError('Rows require tenant_id, account_id, net_cents exactly')
        tenant = _positive_string(row['tenant_id'], 'tenant_id')
        account = _positive_string(row['account_id'], 'account_id')
        if tenant not in scope:
            raise ForensicEvidenceError('Tenant outside the declared evidence scope')
        if type(row['net_cents']) is not int or not (-2**63 <= row['net_cents'] < 2**63):
            raise ForensicEvidenceError('net_cents must be signed 64-bit integer')
        key = (tenant, account)
        if key in normalized:
            raise ForensicEvidenceError('Duplicate normalized state key: ' + str(key))
        normalized[key] = row['net_cents']
    return [{'tenant_id': t, 'account_id': a, 'net_cents': normalized[t, a]}
            for t, a in sorted(normalized)]


def make_bundle(metadata: dict, rows: Iterable[dict]) -> dict:
    meta = validate_metadata(metadata)
    data = normalize_rows(rows, meta['tenant_scope'])
    envelope = {'format': FORMAT, 'metadata': meta, 'rows': data}
    return {**envelope, 'sha256': sha256_bytes(canonical(envelope))}


def verify_bundle(bundle: Any) -> dict:
    if not isinstance(bundle, dict) or set(bundle) != {'format', 'metadata', 'rows', 'sha256'}:
        raise ForensicEvidenceError('Invalid bundle structure')
    if bundle['format'] != FORMAT or not isinstance(bundle['rows'], list):
        raise ForensicEvidenceError('Unsupported bundle format')
    if len(bundle['rows']) > LIMIT:
        raise ForensicEvidenceError('Too many rows for bounded local comparator')
    rebuilt = make_bundle(bundle['metadata'], bundle['rows'])
    if rebuilt != bundle:
        raise ForensicEvidenceError('Digest mismatch, unordered records or modified evidence')
    return rebuilt


def load_bundle(path: str | Path) -> dict:
    p = Path(path)
    if p.stat().st_size > 24 * 1024 * 1024:
        raise ForensicEvidenceError('Evidence file exceeds byte limit')
    try:
        data = json.loads(p.read_text(encoding='utf-8'))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ForensicEvidenceError('Invalid JSON evidence') from exc
    return verify_bundle(data)


def write_new(path: str | Path, payload: dict) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    # Never overwrite existing evidence, including after a rerun.
    with p.open('xb') as f:
        f.write(canonical(payload) + b'\n')
    return p


def reference_from_events(event_path: str | Path, metadata: dict) -> dict:
    meta = validate_metadata(metadata)
    if meta['origin'] != 'replay':
        raise ForensicEvidenceError('Reference must declare origin=replay')
    path = Path(event_path)
    raw = path.read_bytes()
    if sha256_bytes(raw) != meta['source_sha256']:
        raise ForensicEvidenceError('Source archive digest differs from declared snapshot')
    events = []
    for number, line in enumerate(raw.decode('utf-8').splitlines(), 1):
        if not line.strip():
            continue
        try:
            wrapper = json.loads(line)
            payload = wrapper['event'] if isinstance(wrapper, dict) and 'event' in wrapper else wrapper
            e = Event.from_dict(payload)
        except (ValueError, TypeError, KeyError) as exc:
            raise ForensicEvidenceError(f'Invalid archive event at line {number}: {exc}') from exc
        if e.tenant_id in meta['tenant_scope']:
            events.append(e)
    result = replay(events, policy=meta['policy'])
    records = []
    for key, net in result.account_balances.items():
        tenant, account = json.loads(key)
        records.append({'tenant_id': tenant, 'account_id': account, 'net_cents': net})
    return make_bundle(meta, records)


def compare_bundles(reference: dict, observed: dict, *, max_details: int = 5000) -> dict:
    ref = verify_bundle(reference)
    obs = verify_bundle(observed)
    if ref['metadata']['origin'] != 'replay' or obs['metadata']['origin'] not in ('checkpoint', 'synthetic-checkpoint'):
        raise ForensicEvidenceError('Expected replay reference and checkpoint observation')
    incompatible = [k for k in ALIGN if ref['metadata'][k] != obs['metadata'][k]]
    if incompatible:
        raise ForensicEvidenceError('INCOMPARABLE snapshots: ' + ', '.join(incompatible))
    left = {(r['tenant_id'], r['account_id']): r['net_cents'] for r in ref['rows']}
    right = {(r['tenant_id'], r['account_id']): r['net_cents'] for r in obs['rows']}
    counts = Counter()
    detail = []
    for key in sorted(set(left) | set(right)):
        if key not in right:
            status = 'MISSING_IN_CHECKPOINT'
        elif key not in left:
            status = 'UNEXPECTED_IN_CHECKPOINT'
        elif left[key] != right[key]:
            status = 'AMOUNT_MISMATCH'
        else:
            status = 'MATCH'
        counts[status] += 1
        if status != 'MATCH' and len(detail) < max_details:
            detail.append({'tenant_id': key[0], 'account_id': key[1], 'classification': status,
                           'expected_cents': left.get(key), 'observed_cents': right.get(key),
                           'delta_cents': right[key] - left[key] if key in right and key in left else None})
    total = sum(counts.values())
    divergences = total - counts['MATCH']
    report = {
        'format': 'temporalx.forensic-diff.v1',
        'decision': 'DIVERGENCE_FOUND' if divergences else 'CONSISTENT_FOR_REVIEW',
        'production_mutation_authorized': False,
        'evidence_level': 'CHECKPOINT_EXPORT_UNVERIFIED' if obs['metadata']['origin'] == 'checkpoint' else 'SYNTHETIC_SIMULATION',
        'reference_sha256': ref['sha256'], 'observed_sha256': obs['sha256'],
        'aligned_provenance': {k: ref['metadata'][k] for k in ALIGN},
        'counts': {k: counts[k] for k in ('MATCH','MISSING_IN_CHECKPOINT','UNEXPECTED_IN_CHECKPOINT','AMOUNT_MISMATCH')},
        'total_keys': total, 'divergent_keys': divergences, 'reported_details': len(detail),
        'details_truncated': divergences > len(detail), 'details': detail,
        'note': 'Logical aggregate reconciliation, not arbitrary RocksDB bytes; absence/TTL/window semantics must be part of the declared operator contract.',
    }
    report['sha256'] = sha256_bytes(canonical(report))
    return report


def _get_path(value: dict, path: str):
    node: Any = value
    for field in path.split('.'):
        if not isinstance(node, dict) or field not in node:
            raise ForensicEvidenceError(f'Operator schema missing field {path}')
        node = node[field]
    return node


def project_statestore(rows: Iterable[dict], *, tenant_field='key.tenant_id',
                       account_field='key.account_id', cents_field='value.net_cents',
                       tenant_scope: list[str], max_rows: int = LIMIT) -> list[dict]:
    """Operator-specific projection. Requires explicit semantic field mappings.

    Spark DataFrame reader uses `key`, whereas SQL TVF may return `id`.
    A non-aggregate state store cannot be cast into net_cents by this function.
    """
    projected = []
    for index, row in enumerate(rows):
        if index >= max_rows:
            raise ForensicEvidenceError('Physical checkpoint row limit exceeded')
        if not isinstance(row, dict):
            raise ForensicEvidenceError('Expected Spark row dict')
        projected.append({'tenant_id': _get_path(row, tenant_field),
                          'account_id': _get_path(row, account_field),
                          'net_cents': _get_path(row, cents_field)})
    return normalize_rows(projected, tenant_scope, max_rows=max_rows)
