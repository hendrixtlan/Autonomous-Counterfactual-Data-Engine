"""End-to-end local persistence and replay gates; no Databricks account required."""
import json
import sqlite3

import pytest

from temporalx.archive import Archive
from temporalx.engine import ReplayConflictError
from temporalx.generator import generate_events
from temporalx.io import save_events


def fixture(tmp_path, count=10):
    events = generate_events(count, seed=42)
    path = tmp_path / 'events.jsonl'
    save_events(path, events)
    return path, events


def test_append_only_atomic_batch_and_no_double_ingestion(tmp_path):
    path, events = fixture(tmp_path)
    with Archive(tmp_path / 'data.db') as arc:
        first = arc.ingest(path, 'batch-001')
        second = arc.ingest(path, 'batch-001')
        assert first.accepted == len(events)
        assert not first.previously_ingested
        assert second.previously_ingested
        assert arc.status()['deliveries'] == len(events)
        with pytest.raises(sqlite3.IntegrityError, match='append-only'):
            with arc.db:
                arc.db.execute('DELETE FROM deliveries')
        assert arc.status()['deliveries'] == len(events)


def test_replay_manifest_fails_closed_on_reuse_after_new_data(tmp_path):
    path, events = fixture(tmp_path)
    with Archive(tmp_path / 'data.db') as arc:
        arc.ingest(path, 'batch-A')
        first = arc.compare('run-001')
        assert first['comparison']['reconciled']
        assert first == arc.compare('run-001')
        assert len(first['evidence_sha256']) == 64
        late = tmp_path / 'late.jsonl'
        save_events(late, [generate_events(20)[-1]])
        arc.ingest(late, 'batch-B')
        with pytest.raises(ReplayConflictError, match='run_id'):
            arc.compare('run-001')
        assert arc.compare('run-002')['evidence_sha256'] != first['evidence_sha256']


def test_historical_knowledge_cutoff_preserves_late_refund(tmp_path):
    path, _ = fixture(tmp_path)
    with Archive(tmp_path / 'data.db') as arc:
        arc.ingest(path, 'a')
        earlier = arc.compare('before', known_at='2026-10-02T00:00:00+00:00')
        later = arc.compare('after', known_at='2026-10-05T00:00:00+00:00')
        assert earlier['comparison']['candidate']['visible_events'] < later['comparison']['candidate']['visible_events']
        assert earlier['snapshot_sha256'] != later['snapshot_sha256']


def test_rejects_identity_conflicts_across_batches(tmp_path):
    path, events = fixture(tmp_path)
    bad = events[0].to_dict()
    bad['amount_cents'] += 1
    altered = tmp_path / 'altered.jsonl'
    altered.write_text(json.dumps(bad) + '\n')
    with Archive(tmp_path / 'data.db') as arc:
        arc.ingest(path, 'original')
        with pytest.raises(ReplayConflictError):
            arc.ingest(altered, 'modified')
        assert arc.status()['batches'] == 1
        with pytest.raises(ReplayConflictError, match='batch_id'):
            arc.ingest(altered, 'original')


def test_quarantines_bad_input_without_fake_financial_data(tmp_path):
    path, events = fixture(tmp_path)
    with path.open('a') as f:
        f.write('{bad json\n')
        f.write(json.dumps({'hello': 123}) + '\n')
    with Archive(tmp_path / 'data.db') as arc:
        receipt = arc.ingest(path, 'bad-batch')
        assert receipt.accepted == len(events)
        assert receipt.quarantined == 2
        assert len(arc.snapshot()) == len(events)
        assert arc.status()['quarantine'] == 2


def test_identical_physical_messages_retained_across_distinct_batches(tmp_path):
    path, events = fixture(tmp_path)
    with Archive(tmp_path / 'data.db') as arc:
        arc.ingest(path, 'batch-a')
        arc.ingest(path, 'batch-b')
        assert arc.status()['deliveries'] == 2 * len(events)
        compare = arc.compare('twice')['comparison']
        assert compare['candidate']['applied_events'] < compare['baseline']['applied_events']
        assert compare['candidate']['duplicate_messages'] > 0


def test_utc_validation_and_unknown_identity(tmp_path):
    path, events = fixture(tmp_path)
    row = events[0].to_dict()
    row['ingested_at'] = '2026-10-01T00:00:00'
    path.write_text(json.dumps(row) + '\n')
    with Archive(tmp_path / 'data.db') as arc:
        assert arc.ingest(path, 'bad-time').quarantined == 1
        assert arc.status()['deliveries'] == 0


def test_run_id_is_immutable_across_temporal_cutoffs(tmp_path):
    path, _ = fixture(tmp_path)
    with Archive(tmp_path / "arch.db") as arc:
        arc.ingest(path, "source")
        arc.compare("r1", known_at="2026-10-02T00:00:00Z")
        with pytest.raises(ReplayConflictError, match="run_id"):
            arc.compare("r1", known_at="2026-10-05T00:00:00Z")
        assert arc.status()["replay_runs"] == 1


def test_archive_rejects_ambiguous_batch_identifier(tmp_path):
    path, _ = fixture(tmp_path)
    with Archive(tmp_path / "arch.db") as arc:
        with pytest.raises(ValueError, match="batch_id"):
            arc.ingest(path, "bad/name")
        assert arc.status()["deliveries"] == 0
