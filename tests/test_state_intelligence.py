from dataclasses import replace
import json
from pathlib import Path
import pytest
from temporalx.state_intelligence import (
    Field, StateVariable, StateContract, load_contract, assess_upgrade,
    incident_diagnosis,
)
from temporalx.state_reference import Observation, EventTimeSessionEngine, ProcessingTimeDeduper, example

BASE = Path(__file__).resolve().parents[1] / 'fixtures' / 'state_intelligence'


def load(name):
    return load_contract(json.loads((BASE / name).read_text()))


def test_compatible_widen_and_add():
    report = assess_upgrade(load('contract-v1.json'), load('contract-v2-compatible.json'))
    assert report.decision == 'REVIEW_FOR_RESUME'
    assert any('null' in reason for reason in report.reasons)
    assert any('widened' in reason for reason in report.reasons)
    assert report.review_required


def test_changed_logic_requires_rebuild():
    report = assess_upgrade(load('contract-v1.json'), load('contract-v3-replay.json'))
    assert report.decision == 'REBUILD_REQUIRED'


@pytest.mark.parametrize('key,value', [
    ('group_keys', ('tenant_id',)), ('source_id', 'another.bronze'),
    ('sink_id', 'another.sink'), ('time_mode', 'ProcessingTime'),
    ('shuffle_partitions', 3), ('operator_id', 'other_operator'),
])
def test_identity_or_semantic_block(key, value):
    old = load('contract-v1.json')
    new = replace(old, **{key: value}, variables=tuple(
        replace(v, ttl_ms=None) for v in old.variables))
    assert assess_upgrade(old, new).decision == 'BLOCK'


def test_removed_field_requires_rebuild():
    old = load('contract-v1.json')
    v = old.variables[0]
    new = replace(old, variables=(replace(v, fields=v.fields[:-1]),))
    assert assess_upgrade(old, new).decision == 'REBUILD_REQUIRED'


def test_added_variable_can_be_reviewed():
    old = load('contract-v1.json')
    new = replace(old, variables=old.variables + (StateVariable('risk', 'value', (Field('score', 'double'),)),))
    assert assess_upgrade(old, new).decision == 'REVIEW_FOR_RESUME'


def test_nullable_to_required_requires_rebuild():
    old = load('contract-v2-compatible.json')
    new_variable = replace(old.variables[0], fields=tuple(replace(f, nullable=False) for f in old.variables[0].fields))
    assert assess_upgrade(old, replace(old, variables=(new_variable,))).decision == 'REBUILD_REQUIRED'


def test_added_field_without_avro_requires_rebuild():
    base = load('contract-v1.json')
    old = replace(base, encoding='unsaferow')
    new = replace(load('contract-v2-compatible.json'), encoding='unsaferow')
    assert assess_upgrade(old, new).decision == 'REBUILD_REQUIRED'


def test_contract_invalid_ttl_event_time():
    old = load('contract-v1.json')
    with pytest.raises(ValueError):
        replace(old, variables=(replace(old.variables[0], ttl_ms=1000),))


def test_invalid_contract_duplicate_fields():
    with pytest.raises(ValueError):
        StateVariable('session', 'value', (Field('dup', 'int'), Field('dup', 'int')))


def test_digest_stable_across_roundtrip():
    old = load('contract-v1.json')
    assert old.digest == load_contract(old.dict()).digest


def test_processing_ttl_expires_and_reaccepts():
    obs = Observation('a', 'acct', 'e1', 100, 100)
    checker = ProcessingTimeDeduper(1000)
    assert checker.accept(obs, 100) == 'ACCEPTED'
    assert checker.accept(obs, 999) == 'DUPLICATE'
    assert checker.visible_entries(1100) == 0
    assert checker.accept(obs, 1100) == 'ACCEPTED_AFTER_EXPIRY'


def test_tenant_isolation_and_identity_conflict():
    d = ProcessingTimeDeduper(1000)
    a = Observation('a', 'acct', 'id', 1, 1)
    b = Observation('b', 'acct', 'id', 1, 1)
    conflict = Observation('a', 'DIFFERENT', 'id', 1, 1)
    assert d.accept(a, 100) == 'ACCEPTED'
    assert d.accept(b, 100) == 'ACCEPTED'
    assert d.accept(conflict, 200) == 'IDENTITY_CONFLICT'


def test_session_watermark_timer_and_late_event():
    e = EventTimeSessionEngine(3000)
    assert e.ingest(Observation('a', 'acc', 'e1', 1000, 1100)) == 'ACCEPTED'
    assert e.ingest(Observation('a', 'acc', 'e2', 2000, 2100)) == 'ACCEPTED'
    assert e.advance_watermark(4999) == []
    result = e.advance_watermark(5000)
    assert result[0]['event_count'] == 2
    assert result[0]['timer_ms'] == 5000
    assert e.ingest(Observation('a', 'acc', 'e3', 2500, 8000)) == 'LATE'
    with pytest.raises(ValueError):
        e.advance_watermark(1)


def test_session_cross_tenant():
    e = EventTimeSessionEngine(1000)
    e.ingest(Observation('a', 'acc', 'e1', 10, 10))
    e.ingest(Observation('b', 'acc', 'e1', 20, 20))
    assert len(e.advance_watermark(1000)) == 0
    assert len(e.advance_watermark(1020)) == 2


def test_example_expected():
    x = example()
    assert x['ttl_decisions'] == ['ACCEPTED', 'DUPLICATE', 'ACCEPTED_AFTER_EXPIRY']
    assert x['late_decision'] == 'LATE'
    assert x['sessions_closed_before_deadline'] == 0
    assert x['closed_sessions'][0]['event_count'] == 2


def test_diagnosis_missing_data_is_not_zero():
    report = incident_diagnosis([{'batchId': 0, 'stateOperators': [{'operatorName': 'stateStoreSave', 'customMetrics': {}}]}])
    assert any(i['severity'] == 'unknown' for i in report['signals'])
    assert 'NO_AUTOMATIC' in report['authorization']


def test_diagnosis_slow_checkpoint_and_stall():
    fake = [{'batchId': 0, 'stateOperators': [{'customMetrics': {
        'rocksdbCommitCheckpointLatency': 4200,
        'rocksdbWriterStallLatencyMs': 900,
        'rocksdbSstFileSize': 1000,
    }}]}]
    report = incident_diagnosis(fake)
    assert len([x for x in report['signals'] if x['severity'] == 'warning']) == 2


def test_no_state_operator_diagnosis():
    report = incident_diagnosis([])
    assert report['observations'] == 0
    assert len(report['signals']) >= 1
