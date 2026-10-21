"""Sprint04 independent unit tests; no simulated result is called Spark execution."""
from __future__ import annotations
import ast
from dataclasses import replace
import json
from pathlib import Path
import pytest

from temporalx.rocksdb_lab import (
    Experiment, CheckpointContractError, ensure_local_contract,
    parse_progress, summary, load_progress,
)

ROOT = Path(__file__).parents[1]


def test_contract_is_deterministic_and_idempotent(tmp_path):
    e = Experiment('rocks_a')
    first = ensure_local_contract(tmp_path, e)
    assert first == ensure_local_contract(tmp_path, e)
    assert json.loads(first.read_text())['branch'] == 'rocks_a'
    assert len(e.fingerprint) == 64


def test_cannot_reuse_checkpoint_across_strategy(tmp_path):
    ensure_local_contract(tmp_path, Experiment('rocks_a', strategy='snapshot'))
    with pytest.raises(CheckpointContractError):
        ensure_local_contract(tmp_path, Experiment('rocks_a', strategy='changelog'))


def test_cannot_change_state_partitioning_or_source(tmp_path):
    a = Experiment('rocks_a', shuffle_partitions=2, source_identity='src_a')
    ensure_local_contract(tmp_path, a)
    with pytest.raises(CheckpointContractError):
        ensure_local_contract(tmp_path, replace(a, shuffle_partitions=4))
    with pytest.raises(CheckpointContractError):
        ensure_local_contract(tmp_path, replace(a, source_identity='src_b'))


@pytest.mark.parametrize('kwargs', [
    {'branch': '../escape'}, {'branch': 'UPPER'}, {'branch': ''},
    {'branch': 'ok', 'strategy': 'unknown'},
    {'branch': 'ok', 'shuffle_partitions': 0},
    {'branch': 'ok', 'max_memory_mb': 10},
    {'branch': 'ok', 'operator_revision': 'r2'},
    {'branch': 'ok', 'watermark': '0 minutes'},
])
def test_bad_contracts_rejected(kwargs):
    with pytest.raises(ValueError):
        Experiment(**kwargs)


def test_spark_config_contains_correct_state_provider_and_bounds():
    snapshot = Experiment('rocks_a', strategy='snapshot', max_memory_mb=1024)
    config = snapshot.spark_config()
    assert config['spark.sql.streaming.stateStore.providerClass'].endswith('RocksDBStateStoreProvider')
    assert config['spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled'] == 'false'
    assert config['spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB'] == '1024'
    assert Experiment('rocks_b').spark_config()['spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled'] == 'true'


def test_metrics_include_state_cache_sst_and_latency():
    rows = parse_progress(load_progress(ROOT / 'fixtures/rocksdb/sample_progress.json'))
    assert len(rows) == 2
    assert rows[0]['block_cache_hit_ratio'] == .8
    assert rows[1]['block_cache_hit_ratio'] == .9
    report = summary(rows)
    assert report['metrics']['sst_bytes']['max'] == 550000
    assert report['metrics']['checkpoint_ms']['p50'] == 33.5
    assert report['metrics']['trigger_ms']['p95'] > 200


def test_unknown_metrics_are_not_misreported_as_zero():
    rows = parse_progress([{'batchId': 1, 'stateOperators': [{'operatorName': 'dedupe', 'customMetrics': {}}]}])
    assert rows[0]['sst_bytes'] is None
    assert rows[0]['block_cache_hit_ratio'] is None
    assert rows[0]['checkpoint_ms'] is None
    assert summary(rows)['warnings']


def test_empty_no_state_operator_is_a_warning():
    rows = parse_progress([{'batchId': 0, 'stateOperators': []}])
    assert rows == []
    assert summary(rows)['operator_observations'] == 0


def test_multiple_operators_are_distinct():
    rows = parse_progress([{'batchId': 0, 'stateOperators': [
        {'operatorName': 'dedupe', 'numRowsTotal': 8},
        {'operatorName': 'stateStoreSave', 'numRowsTotal': 12}
    ]}])
    assert [r['state_rows'] for r in rows] == [8.0, 12.0]
    assert [r['operator_index'] for r in rows] == [0, 1]


def test_malformed_metric_types_fail_closed():
    with pytest.raises(ValueError, match='stateOperators'):
        parse_progress([{'stateOperators': {'operator': 1}}])
    with pytest.raises(ValueError, match='customMetrics'):
        parse_progress([{'stateOperators': [{'customMetrics': 'wrong'}]}])


def test_fixture_generator_repeatability_and_archive_preservation(tmp_path):
    import sys
    sys.path.insert(0, str(ROOT / 'scripts'))
    from prepare_rocksdb_fixture import prepare
    a = prepare(tmp_path / 'a', accounts=7, chunks=3)
    b = prepare(tmp_path / 'b', accounts=7, chunks=3)
    assert a['physical_deliveries'] == 21
    assert a['logical_keys'] == 7
    assert a['expected_duplicate_deliveries'] == 14
    for i in range(3):
        assert (tmp_path / 'a' / f'{i:04d}.json').read_bytes() == (tmp_path / 'b' / f'{i:04d}.json').read_bytes()
    with pytest.raises(FileExistsError):
        prepare(tmp_path / 'a', accounts=7)


def test_spark_scripts_syntax_without_pyspark_runtime():
    for file in ('rocksdb/local_spark.py', 'rocksdb/databricks_experiment.py', 'rocksdb/inspect_checkpoint.py'):
        ast.parse((ROOT / file).read_text(), filename=file)


def test_sink_verifier_success_and_failures(tmp_path):
    import sys
    sys.path.insert(0, str(ROOT / 'scripts'))
    from prepare_rocksdb_fixture import prepare
    from verify_rocksdb_sink import verify
    from json import dumps
    src = tmp_path / 'input'
    tgt = tmp_path / 'accepted'
    prepare(src, accounts=4, chunks=2)
    tgt.mkdir()
    rows = [json.loads(x) for x in (src / '0000.json').read_text().splitlines()]
    (tgt / 'batch-000000000000.jsonl').write_text(''.join(dumps(r) + '\n' for r in rows))
    report = verify(src, tgt)
    assert report['result'] == 'PASS'
    assert (report['physical_source_deliveries'], report['unique_source_events'], report['accepted_events']) == (8, 4, 4)
    (tgt / 'batch-000000000001.jsonl').write_text(dumps(rows[0]) + '\n')
    with pytest.raises(ValueError, match='Duplicate event'):
        verify(src, tgt)
    (tgt / 'batch-000000000001.jsonl').unlink()
    (tgt / 'batch-000000000000.jsonl').write_text(dumps(rows[0]) + '\n')
    assert verify(src, tgt)['result'] == 'FAIL'


def test_sink_verifier_rejects_conflicting_source_identity(tmp_path):
    import sys
    sys.path.insert(0, str(ROOT / 'scripts'))
    from prepare_rocksdb_fixture import prepare
    from verify_rocksdb_sink import verify
    src, tgt = tmp_path / 'input', tmp_path / 'accepted'
    prepare(src, accounts=2, chunks=2)
    tgt.mkdir()
    first = json.loads((src / '0000.json').read_text().splitlines()[0])
    corrupted = {**first, 'amount_cents': 999999}
    with (src / '0001.json').open('a') as f:
        f.write(json.dumps(corrupted) + '\n')
    (tgt / 'batch-000000000000.jsonl').write_text(json.dumps(first) + '\n')
    with pytest.raises(ValueError, match='Conflicting'):
        verify(src, tgt)


def test_new_recovery_batch_is_append_only_and_not_repeatable(tmp_path):
    import sys
    sys.path.insert(0, str(ROOT / 'scripts'))
    from prepare_rocksdb_fixture import prepare
    from append_recovery_batch import append_batch
    src = tmp_path / 'input'
    prepare(src, accounts=3, chunks=2)
    original = (src / '0000.json').read_bytes()
    out = append_batch(src, 'new_restart1', 5)
    assert out['new_unique_events'] == 5
    assert len((src / 'new_restart1.json').read_text().splitlines()) == 5
    assert (src / '0000.json').read_bytes() == original
    with pytest.raises(FileExistsError):
        append_batch(src, 'new_restart1')
    with pytest.raises(ValueError):
        append_batch(src, '../bad')
