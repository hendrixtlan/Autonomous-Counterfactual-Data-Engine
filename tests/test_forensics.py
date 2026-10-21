from __future__ import annotations
from hashlib import sha256
import json
from pathlib import Path
import pytest
from temporalx.forensics import (ForensicEvidenceError, compare_bundles, make_bundle,
                                 load_bundle, reference_from_events, project_statestore,
                                 verify_bundle, write_new, normalize_rows)

EVENTS = Path(__file__).resolve().parents[1] / 'fixtures/causality/incident-001/events.jsonl'


def meta(**replace):
    base = dict(state_kind='account-net-cents-v1', source_snapshot_id='unit-input-001',
                source_sha256='a' * 64, policy='v2', operator_revision='s07-aggregate-v1',
                checkpoint_batch_id=3, operator_id=0, watermark_policy='unbounded-history',
                tenant_scope=['blue'], currency='USD', origin='replay')
    return {**base, **replace}


def records():
    return [{'tenant_id':'blue','account_id':'A','net_cents':12500},
            {'tenant_id':'blue','account_id':'B','net_cents':2500},
            {'tenant_id':'blue','account_id':'C','net_cents':0}]


def pair():
    return make_bundle(meta(), records()), make_bundle(meta(origin='synthetic-checkpoint'), records())


def test_identical_state_is_only_reviewable():
    ref, obs = pair()
    report = compare_bundles(ref, obs)
    assert report['decision'] == 'CONSISTENT_FOR_REVIEW'
    assert report['production_mutation_authorized'] is False
    assert report['divergent_keys'] == 0


def test_three_different_divergences_and_delta():
    ref, _ = pair()
    changed = [dict(records()[0], net_cents=13000),
               {'tenant_id':'blue','account_id':'C','net_cents':0},
               {'tenant_id':'blue','account_id':'ghost','net_cents':100}]
    report = compare_bundles(ref, make_bundle(meta(origin='synthetic-checkpoint'), changed))
    assert report['counts'] == dict(MATCH=1, MISSING_IN_CHECKPOINT=1,
                                    UNEXPECTED_IN_CHECKPOINT=1, AMOUNT_MISMATCH=1)
    assert report['divergent_keys'] == 3
    assert next(d['delta_cents'] for d in report['details'] if d['classification'] == 'AMOUNT_MISMATCH') == 500
    assert report['production_mutation_authorized'] is False


@pytest.mark.parametrize('changed', [
    {'source_snapshot_id':'other'}, {'source_sha256':'b'*64}, {'operator_id':1},
    {'checkpoint_batch_id':4}, {'policy':'v1'}, {'watermark_policy':'5m'},
    {'operator_revision':'v2'}, {'tenant_scope':['red']},
])
def test_incompatible_snapshot_fails_closed(changed):
    ref, _ = pair()
    observation = make_bundle(meta(origin='synthetic-checkpoint', **changed),
                              [dict(r, tenant_id='red') for r in records()] if changed.get('tenant_scope') else records())
    with pytest.raises(ForensicEvidenceError, match='INCOMPARABLE'):
        compare_bundles(ref, observation)


def test_determinism_sorting_and_hash():
    a = make_bundle(meta(), records())
    b = make_bundle(meta(), list(reversed(records())))
    assert a == b
    assert len(a['sha256']) == 64


def test_tampering_detected_and_no_overwrite(tmp_path):
    ref, _ = pair()
    path = tmp_path / 'evidence.json'
    write_new(path, ref)
    assert load_bundle(path) == ref
    with pytest.raises(FileExistsError):
        write_new(path, ref)
    ref['rows'][0]['net_cents'] += 3
    path.write_text(json.dumps(ref))
    with pytest.raises(ForensicEvidenceError, match='Digest mismatch'):
        load_bundle(path)


def test_scope_enforced_and_duplicate_key_rejected():
    with pytest.raises(ForensicEvidenceError, match='outside'):
        make_bundle(meta(), [{'tenant_id':'red','account_id':'A','net_cents':5}])
    with pytest.raises(ForensicEvidenceError, match='Duplicate'):
        make_bundle(meta(), records() + records()[:1])


def test_ambiguous_amounts_rejected():
    for value in [True, 1.0, '1', None, 2**80]:
        with pytest.raises(ForensicEvidenceError):
            make_bundle(meta(), [dict(records()[0], net_cents=value)])


def test_schema_projection_requires_expected_fields():
    rows = [{'key': {'tenant_id':'blue','account_id':'A'},
             'value': {'net_cents': 120}},
            {'key': {'tenant_id':'blue','account_id':'B'},
             'value': {'net_cents': -20}}]
    assert project_statestore(rows, tenant_scope=['blue'])[0]['net_cents'] == 120
    with pytest.raises(ForensicEvidenceError, match='missing field'):
        project_statestore([{'key': {'tenant_id':'blue','account_id':'A'}, 'value': {'sum':120}}],tenant_scope=['blue'])
    assert project_statestore([{'id': {'tenant_id':'blue','account_id':'A'},'value':{'sum':120}}],
                  tenant_scope=['blue'], tenant_field='id.tenant_id', account_field='id.account_id',
                  cents_field='value.sum')[0]['net_cents'] == 120


def test_null_not_equivalent_to_zero():
    a, b = pair()
    b = make_bundle(meta(origin='synthetic-checkpoint'), records()[:2])
    report = compare_bundles(a, b)
    assert report['counts']['MISSING_IN_CHECKPOINT'] == 1


def test_limit_and_report_truncation():
    with pytest.raises(ForensicEvidenceError, match='limit'):
        normalize_rows(records(), ['blue'], max_rows=2)
    a, b = pair()
    r = compare_bundles(a, make_bundle(meta(origin='synthetic-checkpoint'), []), max_details=1)
    assert r['divergent_keys'] == 3 and r['details_truncated'] and len(r['details']) == 1


def test_reference_oracle_is_bound_to_actual_input_bytes():
    actual = sha256(EVENTS.read_bytes()).hexdigest()
    m = meta(origin='replay', source_sha256=actual)
    b = reference_from_events(EVENTS, m)
    assert b['rows'] and {r['tenant_id'] for r in b['rows']} == {'blue'}
    with pytest.raises(ForensicEvidenceError, match='Source archive digest'):
        reference_from_events(EVENTS, meta())


def test_missing_expected_origin_is_rejected():
    a, b = pair()
    with pytest.raises(ForensicEvidenceError):
        compare_bundles(b, a)


def test_invalid_bundle_structure():
    a, _ = pair()
    a['unexpected'] = True
    with pytest.raises(ForensicEvidenceError):
        verify_bundle(a)
