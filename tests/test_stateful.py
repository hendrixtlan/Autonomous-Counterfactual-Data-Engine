"""Executable acceptance criteria for Sprint 03 reference microbatch engine."""
from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from temporalx.engine import ReplayConflictError
from temporalx.model import Event
from temporalx.stateful import (CheckpointIncompatibleError, Delivery,
    SourceOffsetCollisionError, StateStore, demo, load_deliveries)

FIXTURES = Path(__file__).resolve().parents[1] / 'fixtures' / 'streaming'


def batches():
    return load_deliveries(FIXTURES / 'batch01.jsonl'), load_deliveries(FIXTURES / 'batch02.jsonl')


def test_demo_isolates_old_vs_new_and_offline_backfill(tmp_path):
    results = demo(tmp_path / 'test.db', FIXTURES)
    key = '["blue","acct-1"]'
    assert results['stream-v1']['totals'][key] == 25500  # duplicated +10000
    assert results['stream-v2']['totals'][key] == 15500  # timely 15500, late refund -3000 absent
    assert results['backfill-v2']['totals'][key] == 12500  # corrected capture - refund
    assert results['stream-v2']['counters']['late'] == 1
    assert results['backfill-v2']['counters']['late'] == 0
    assert results['stream-v2']['counters']['deduplicated'] == 1
    assert results['stream-v2']['totals']['["green","acct-1"]'] == 7000


def test_resume_same_state_with_identical_batch_is_idempotent(tmp_path):
    first, _ = batches()
    with StateStore(tmp_path / 's.db') as state:
        one = state.apply(branch='stream_v2', batch_id='one', deliveries=first)
        two = state.apply(branch='stream_v2', batch_id='one', deliveries=first)
        assert two['status'] == 'ALREADY_COMMITTED'
        assert two['state'] == one['state']
        assert two['state']['counters']['batches'] == 1


def test_reused_batch_id_with_changed_bytes_rejected(tmp_path):
    first, _ = batches()
    with StateStore(tmp_path / 's.db') as state:
        state.apply(branch='candidate', batch_id='one', deliveries=first)
        with pytest.raises(SourceOffsetCollisionError, match='batch_id'):
            state.apply(branch='candidate', batch_id='one', deliveries=first[:-1])
        assert state.snapshot('candidate')['counters']['batches'] == 1


def test_checkpoint_rejected_on_policy_or_schema_change(tmp_path):
    first, _ = batches()
    with StateStore(tmp_path / 's.db') as state:
        state.apply(branch='live', batch_id='one', deliveries=first, policy='v1')
        with pytest.raises(CheckpointIncompatibleError):
            state.apply(branch='live', batch_id='two', deliveries=first, policy='v2')
        with pytest.raises(CheckpointIncompatibleError):
            state.apply(branch='live', batch_id='two', deliveries=first, policy='v1', window_seconds=60)
        with pytest.raises(CheckpointIncompatibleError):
            state.apply(branch='live', batch_id='two', deliveries=first, policy='v1', operator_revision='r2')
        assert state.snapshot('live')['counters']['batches'] == 1


def test_checkpoint_persists_across_process_reopen(tmp_path):
    first, second = batches()
    with StateStore(tmp_path / 's.db') as state:
        state.apply(branch='persist', batch_id='one', deliveries=first)
    with StateStore(tmp_path / 's.db') as state:
        assert state.apply(branch='persist', batch_id='two', deliveries=second)['state']['counters']['batches'] == 2
        assert state.snapshot('persist')['counters']['late'] == 1


def test_atomic_rollback_on_conflicting_identity_inside_batch(tmp_path):
    first, _ = batches()
    original = first[0]
    altered = replace(original.event, amount_cents=99999)
    conflict = Delivery(partition=9, offset=9, event=altered)
    with StateStore(tmp_path / 's.db') as state:
        with pytest.raises(ReplayConflictError):
            state.apply(branch='live', batch_id='bad', deliveries=[original, conflict])
        assert state.db.execute('SELECT COUNT(*) FROM batches').fetchone()[0] == 0
        assert state.db.execute('SELECT COUNT(*) FROM source_offsets').fetchone()[0] == 0
        assert state.db.execute('SELECT COUNT(*) FROM checkpoints').fetchone()[0] == 0


def test_offset_collision_is_error_and_does_not_mutate_ledger(tmp_path):
    first, _ = batches()
    with StateStore(tmp_path / 's.db') as state:
        previous = state.apply(branch='live', batch_id='one', deliveries=[first[0]])
        evil = Delivery(topic=first[0].topic, partition=first[0].partition,
                        offset=first[0].offset, event=first[2].event)
        with pytest.raises(SourceOffsetCollisionError, match='Offset mutated'):
            state.apply(branch='live', batch_id='evil', deliveries=[evil])
        assert state.snapshot('live') == previous['state']


def test_repeated_offset_in_new_batch_does_not_double_count(tmp_path):
    first, _ = batches()
    with StateStore(tmp_path / 's.db') as state:
        state.apply(branch='live', batch_id='one', deliveries=[first[0]])
        r = state.apply(branch='live', batch_id='two', deliveries=[first[0]])
        assert r['repeated_offsets'] == 1
        assert r['applied'] == 0
        assert r['state']['totals']['["blue","acct-1"]'] == 10000


def test_backfill_enforces_branch_isolation_not_checkpoint_reuse(tmp_path):
    first, second = batches()
    with StateStore(tmp_path / 's.db') as state:
        state.apply(branch='stream', batch_id='one', deliveries=first)
        state.apply(branch='stream', batch_id='two', deliveries=second)
        checkpoint_before = state.snapshot('stream')
        with pytest.raises(CheckpointIncompatibleError):
            state.apply(branch='stream', batch_id='recovery', deliveries=first + second, replay_mode='backfill')
        state.apply(branch='recovery', batch_id='one', deliveries=first + second, replay_mode='backfill')
        assert state.snapshot('stream') == checkpoint_before
        assert state.snapshot('recovery')['counters']['late'] == 0


def test_watermark_only_moves_between_microbatches(tmp_path):
    first, second = batches()
    with StateStore(tmp_path / 's.db') as state:
        result = state.apply(branch='new', batch_id='one', deliveries=first + second)
        assert result['late'] == 0  # same batch: previous watermark is None
        assert result['state']['totals']['["blue","acct-1"]'] == 12500


def test_bad_deliveries_are_rejected_before_touching_state(tmp_path):
    first, _ = batches()
    bad = tmp_path / 'bad.jsonl'
    bad.write_text(json.dumps({**first[0].payload(), 'partition': -1}) + '\n')
    with pytest.raises(ValueError, match='partition and offset'):
        load_deliveries(bad)
    with StateStore(tmp_path / 's.db') as state:
        with pytest.raises(ValueError, match='safe non-empty'):
            state.apply(branch='../bad', batch_id='one', deliveries=first)


def test_branch_fingerprints_and_evidence_are_stable(tmp_path):
    first, _ = batches()
    with StateStore(tmp_path / 'a.db') as a:
        a.apply(branch='x', batch_id='one', deliveries=first)
        expected = a.snapshot('x')['evidence_sha256']
    with StateStore(tmp_path / 'b.db') as b:
        b.apply(branch='x', batch_id='one', deliveries=first)
        assert b.snapshot('x')['evidence_sha256'] == expected


def test_lateness_setting_is_part_of_immutable_branch_contract(tmp_path):
    first, second = batches()
    with StateStore(tmp_path / 's.db') as state:
        state.apply(branch='short', batch_id='one', deliveries=first, allowed_lateness_seconds=300)
        state.apply(branch='short', batch_id='two', deliveries=second, allowed_lateness_seconds=300)
        state.apply(branch='long', batch_id='one', deliveries=first, allowed_lateness_seconds=1800)
        state.apply(branch='long', batch_id='two', deliveries=second, allowed_lateness_seconds=1800)
        assert state.snapshot('short')['counters']['late'] == 1
        assert state.snapshot('long')['counters']['late'] == 0
        assert state.snapshot('long')['totals']['["blue","acct-1"]'] == 12500


def test_fault_injection_crash_rolls_back_every_state_write(tmp_path):
    first, _ = batches()
    with StateStore(tmp_path / 's.db') as state:
        with pytest.raises(RuntimeError, match='SIMULATED_CRASH'):
            state.apply(branch='crash_lab', batch_id='one', deliveries=first, crash_after_source_offsets=3)
        for table in ('checkpoints', 'batches', 'source_offsets', 'logical_events', 'windows'):
            assert state.db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] == 0
        recovered = state.apply(branch='crash_lab', batch_id='one', deliveries=first)
        assert recovered['status'] == 'COMMITTED'
        assert recovered['state']['counters']['applied'] == 3
