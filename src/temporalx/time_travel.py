"""Sprint 08: temporal state-transition reconciliation (pure Python research oracle).

Track logical account aggregates, not raw RocksDB bytes. Each transition carries
its explicit before and after states; None means ABSENT and is distinct from 0.
The first divergence is located by scanning every covered committed microbatch,
never binary search (a discrepancy might subsequently heal).
"""
from __future__ import annotations

from collections import defaultdict
from hashlib import sha256
from typing import Any, Iterable
import json

FORMAT = 'temporalx.state-timeline.v1'
FIELDS = ('batch_id', 'tenant_id', 'account_id', 'before_cents', 'after_cents')
META_FIELDS = ('checkpoint_id', 'source_snapshot_id', 'source_sha256',
               'batch_lineage_sha256', 'operator_revision', 'operator_id',
               'state_kind', 'watermark_policy', 'tenant_scope', 'currency',
               'first_batch_id', 'last_batch_id', 'origin')
ALIGN = tuple(x for x in META_FIELDS if x != 'origin')
MAX_TRANSITIONS = 500_000
MAX_BATCHES = 2_000


class TimelineError(ValueError):
    """Evidence is incomplete, contradictory or not comparable."""


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode('utf-8')


def digest(value: Any) -> str:
    return sha256(canonical(value)).hexdigest()


def _int(x: Any, name: str, minimum: int = -(2**63)) -> int:
    if type(x) is not int or x < minimum or x >= 2**63:
        raise TimelineError(f'{name} must be a bounded integer')
    return x


def _string(x: Any, name: str, limit: int = 256) -> str:
    if not isinstance(x, str) or not x or len(x) > limit:
        raise TimelineError(f'{name} must be a nonempty string')
    return x


def validate_metadata(m: Any) -> dict:
    if not isinstance(m, dict) or set(m) != set(META_FIELDS):
        raise TimelineError('Incomplete/extra timeline provenance')
    out = dict(m)
    for k in ('checkpoint_id', 'source_snapshot_id', 'operator_revision', 'state_kind',
              'watermark_policy', 'currency', 'origin'):
        _string(out[k], k)
    for k in ('source_sha256', 'batch_lineage_sha256'):
        s = out[k]
        if not isinstance(s, str) or len(s) != 64 or any(c not in '0123456789abcdef' for c in s):
            raise TimelineError(f'{k} is not a SHA-256 hexadecimal digest')
    for k in ('operator_id', 'first_batch_id', 'last_batch_id'):
        _int(out[k], k, minimum=0)
    if out['last_batch_id'] < out['first_batch_id'] or out['last_batch_id'] - out['first_batch_id'] >= MAX_BATCHES:
        raise TimelineError('Invalid batch coverage interval')
    if out['origin'] not in ('reference', 'checkpoint', 'synthetic-checkpoint'):
        raise TimelineError('Unsupported evidence origin')
    if out['state_kind'] != 'account-net-cents-v1' or out['currency'] != 'USD':
        raise TimelineError('Unsupported state semantics')
    tenants = out['tenant_scope']
    if not isinstance(tenants, list) or not tenants or len(tenants) > 500 or not all(isinstance(v, str) and v and len(v) <= 128 for v in tenants) or sorted(set(tenants)) != tenants:
        raise TimelineError('Tenant scope must be sorted, nonempty and unique')
    return out


def normalize_transitions(rows: Iterable[dict], meta: dict) -> list[dict]:
    states: dict[tuple[str, str], int | None] = {}
    transitions = []
    last_batch: dict[tuple[str, str], int] = {}
    for i, row in enumerate(rows):
        if i >= MAX_TRANSITIONS:
            raise TimelineError('Local row cap exceeded; use the distributed Spark implementation')
        if not isinstance(row, dict) or set(row) != set(FIELDS):
            raise TimelineError('A transition needs exactly five expected fields')
        b = _int(row['batch_id'], 'batch_id', minimum=0)
        if not meta['first_batch_id'] <= b <= meta['last_batch_id']:
            raise TimelineError('Transition outside declared batch coverage')
        tenant = _string(row['tenant_id'], 'tenant_id', 128)
        account = _string(row['account_id'], 'account_id', 128)
        if tenant not in meta['tenant_scope']:
            raise TimelineError('Transition outside tenant scope')
        before, after = row['before_cents'], row['after_cents']
        if before is not None: _int(before, 'before_cents')
        if after is not None: _int(after, 'after_cents')
        key = (tenant, account)
        if b <= last_batch.get(key, -1):
            raise TimelineError('Transitions for a key must be strictly increasing by batch')
        # ABsENT is not 0; reject a stream whose 'before' value is invented.
        if before != states.get(key):
            raise TimelineError('Broken state transition: before does not match prior after')
        states[key] = after
        last_batch[key] = b
        transitions.append(dict(row))
    return sorted(transitions, key=lambda e: (e['batch_id'], e['tenant_id'], e['account_id']))


def make_timeline(metadata: dict, rows: Iterable[dict]) -> dict:
    m = validate_metadata(metadata)
    body = {'format': FORMAT, 'metadata': m, 'transitions': normalize_transitions(rows, m)}
    return {**body, 'sha256': digest(body)}


def verify_timeline(value: Any) -> dict:
    if not isinstance(value, dict) or set(value) != {'format', 'metadata', 'transitions', 'sha256'}:
        raise TimelineError('Invalid timeline envelope')
    if value['format'] != FORMAT or not isinstance(value['transitions'], list):
        raise TimelineError('Invalid timeline format')
    rebuilt = make_timeline(value['metadata'], value['transitions'])
    if rebuilt != value:
        raise TimelineError('Timeline order/digest mismatch: evidence may have changed')
    return rebuilt


def compare_timelines(reference: dict, observed: dict, *, max_details: int = 25) -> dict:
    r = verify_timeline(reference)
    o = verify_timeline(observed)
    if r['metadata']['origin'] != 'reference' or o['metadata']['origin'] not in ('checkpoint', 'synthetic-checkpoint'):
        raise TimelineError('Expected an independent reference and a checkpoint observation')
    mismatch = [k for k in ALIGN if r['metadata'][k] != o['metadata'][k]]
    if mismatch:
        raise TimelineError('INCOMPARABLE timeline provenance: ' + ', '.join(mismatch))
    if type(max_details) is not int or max_details < 0 or max_details > 10_000:
        raise TimelineError('Invalid max_details')
    # Scans EVERY batch in the declared range: divergence can heal and a binary
    # search over the final status would miss the actual earliest incident.
    indexed = {side: defaultdict(dict) for side in ('reference', 'observed')}
    for label, bundle in (('reference', r), ('observed', o)):
        for t in bundle['transitions']:
            indexed[label][t['batch_id']][(t['tenant_id'], t['account_id'])] = t
    state = {'reference': {}, 'observed': {}}
    active: set[tuple[str, str]] = set()
    first_batch = None
    details = []
    per_batch = []
    divergence_episodes = 0
    # Sets of active differences change only when a key transitions.
    for b in range(r['metadata']['first_batch_id'], r['metadata']['last_batch_id'] + 1):
        affected = set(indexed['reference'][b]) | set(indexed['observed'][b])
        new_divergences = []
        healed = 0
        for key in sorted(affected):
            was_bad = key in active
            for side in ('reference', 'observed'):
                t = indexed[side][b].get(key)
                if t is not None:
                    state[side][key] = t['after_cents']
            left = state['reference'].get(key)
            right = state['observed'].get(key)
            bad = left != right
            if bad:
                active.add(key)
            else:
                active.discard(key)
            if bad and not was_bad:
                divergence_episodes += 1
                new_divergences.append(key)
                if first_batch is None:
                    first_batch = b
                if len(details) < max_details:
                    details.append({'batch_id': b, 'tenant_id': key[0], 'account_id': key[1],
                                    'reference_before': indexed['reference'][b].get(key, {}).get('before_cents'),
                                    'reference_after': left,
                                    'observed_before': indexed['observed'][b].get(key, {}).get('before_cents'),
                                    'observed_after': right,
                                    'classification': ('MISSING_OBSERVED' if right is None else
                                                       'UNEXPECTED_OBSERVED' if left is None else 'AMOUNT_MISMATCH')})
            elif was_bad and not bad:
                healed += 1
        per_batch.append({'batch_id': b, 'newly_divergent_keys': len(new_divergences),
                          'healed_keys': healed, 'currently_divergent_keys': len(active)})
    report = {'format': 'temporalx.time-travel-diff.v1',
              'status': 'HISTORICAL_DIVERGENCE' if first_batch is not None else 'NO_DIVERGENCE_IN_COVERED_BATCHES',
              'first_divergent_batch_id': first_batch,
              'last_covered_batch_id': r['metadata']['last_batch_id'],
              'first_covered_batch_id': r['metadata']['first_batch_id'],
              'first_globally_divergent_batch_proven': first_batch is not None and r['metadata']['first_batch_id'] == 0,
              'historical_divergence_episodes': divergence_episodes,
              'divergent_keys_at_end': len(active),
              'reference_sha256': r['sha256'], 'observed_sha256': o['sha256'],
              'evidence_level': 'CHECKPOINT_EXPORT_UNVERIFIED' if o['metadata']['origin'] == 'checkpoint' else 'SYNTHETIC_SIMULATION',
              'production_mutation_authorized': False,
              'per_batch': per_batch,
              'details': details,
              'details_truncated': divergence_episodes > len(details),
              'note': 'First divergence is scoped to completely covered batches and declared semantics. Digest alone is not trusted attestation.'}
    return {**report, 'sha256': digest(report)}
