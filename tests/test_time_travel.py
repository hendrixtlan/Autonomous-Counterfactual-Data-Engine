from copy import deepcopy
import pytest
from temporalx.time_travel import (TimelineError, make_timeline, verify_timeline, compare_timelines,
                                   normalize_transitions)
from scripts.time_travel_lab import fixture


def test_first_divergence_even_when_last_state_is_consistent():
    a, b = fixture()
    report = compare_timelines(a, b)
    assert report['status'] == 'HISTORICAL_DIVERGENCE'
    assert report['first_divergent_batch_id'] == 2
    assert report['last_covered_batch_id'] == 4
    assert report['historical_divergence_episodes'] == 2
    assert report['divergent_keys_at_end'] == 0
    assert [x['currently_divergent_keys'] for x in report['per_batch']] == [0, 0, 1, 2, 0]
    assert report['production_mutation_authorized'] is False


def test_no_false_divergence_for_identical_transitions():
    a, _ = fixture()
    b = make_timeline({**a['metadata'], 'origin':'synthetic-checkpoint'}, a['transitions'])
    report = compare_timelines(a, b)
    assert report['first_divergent_batch_id'] is None
    assert report['historical_divergence_episodes'] == 0


def test_absent_is_distinct_from_zero():
    a, b = fixture()
    observed = list(b['transitions'])
    # Create an unexpected $0 account: must be classified as phantom, not absent.
    observed.append(dict(batch_id=0, tenant_id='blue', account_id='zero', before_cents=None, after_cents=0))
    b = make_timeline(b['metadata'], observed)
    report = compare_timelines(a, b)
    assert report['first_divergent_batch_id'] == 0
    assert any(x['account_id']=='zero' and x['classification']=='UNEXPECTED_OBSERVED' for x in report['details'])


@pytest.mark.parametrize('key,value', [
    ('source_sha256', 'c'*64), ('checkpoint_id','other'), ('batch_lineage_sha256','c'*64),
    ('operator_revision','changed'), ('operator_id',1), ('first_batch_id',1),
    ('last_batch_id',5), ('watermark_policy','5m'), ('source_snapshot_id','changed'),
])
def test_strict_coverage_and_lineage(key, value):
    a, b = fixture()
    changed = dict(b['metadata'], **{key:value})
    if key in ('first_batch_id','last_batch_id'):
        # Make all rows legal under the modified interval before comparing.
        rows = [row for row in b['transitions'] if changed['first_batch_id'] <= row['batch_id'] <= changed['last_batch_id']]
        if key == 'first_batch_id':
            # Cannot just omit batch zero: the next state's before would be inconsistent.
            with pytest.raises(TimelineError):
                make_timeline(changed, rows)
            return
    else:
        rows = b['transitions']
    b2 = make_timeline(changed, rows)
    with pytest.raises(TimelineError, match='INCOMPARABLE'):
        compare_timelines(a,b2)


def test_broken_before_value_fails_closed():
    a, _ = fixture()
    rows = deepcopy(a['transitions'])
    next(x for x in rows if x['batch_id']==2)['before_cents'] = 70000
    with pytest.raises(TimelineError, match='Broken'):
        make_timeline(a['metadata'], rows)


def test_duplicate_key_batch_fails_closed():
    a, _ = fixture()
    with pytest.raises(TimelineError, match='strictly increasing'):
        make_timeline(a['metadata'], a['transitions']+[a['transitions'][0]])


def test_digest_and_evidence_tamper():
    a, _ = fixture()
    a['transitions'][0]['after_cents'] = 999
    with pytest.raises(TimelineError):
        verify_timeline(a)


def test_no_implicit_integer_coercion():
    a, _ = fixture()
    rows = deepcopy(a['transitions'])
    rows[0]['after_cents'] = True
    with pytest.raises(TimelineError):
        make_timeline(a['metadata'], rows)


def test_scoped_first_batch_not_global():
    a, b = fixture()
    def cut(evidence):
        # Keep batch 2+ and insert carried-forward baseline at the first retained batch.
        meta = {**evidence['metadata'], 'first_batch_id':2}
        rows=[]
        for acc, before in [('A', 12000),('B',5000)]:
            rows.append(dict(batch_id=2,tenant_id='blue',account_id=acc,
                             before_cents=None,after_cents=before))
        # next batch starts from initial snapshot; replace the original batch 2 A change
        # with a later one to ensure transition consistency.
        for row in evidence['transitions']:
            if row['batch_id'] == 2:
                row = dict(row)
                row['batch_id'] = 3
            elif row['batch_id'] < 3:
                continue
            else:
                row = dict(row, batch_id=4)
            rows.append(row)
        return meta, rows
    # Test coverage scope using a fresh independent scenario instead of treating partial
    # history as a globally complete statement.
    ma={**a['metadata'],'first_batch_id':2,'last_batch_id':3}
    mb={**b['metadata'],'first_batch_id':2,'last_batch_id':3}
    ra=[dict(batch_id=2,tenant_id='blue',account_id='A',before_cents=None,after_cents=100)]
    rb=[dict(batch_id=2,tenant_id='blue',account_id='A',before_cents=None,after_cents=101)]
    report=compare_timelines(make_timeline(ma,ra),make_timeline(mb,rb))
    assert report['first_divergent_batch_id']==2
    assert report['first_globally_divergent_batch_proven'] is False


def test_reject_foreign_tenant():
    a, _ = fixture()
    rows=deepcopy(a['transitions'])
    rows[0]['tenant_id']='red'
    with pytest.raises(TimelineError, match='scope'):
        make_timeline(a['metadata'],rows)


def test_limited_forensic_details():
    a,b=fixture()
    r=compare_timelines(a,b,max_details=1)
    assert r['details_truncated'] and len(r['details'])==1


def test_manifest_tenant_list_sorted():
    a,_=fixture()
    with pytest.raises(TimelineError,match='Tenant scope'):
        make_timeline({**a['metadata'],'tenant_scope':['red','blue']}, a['transitions'])
