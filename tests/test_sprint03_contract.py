"""Static contract assertions; no false claim about Docker/Spark runtime tests."""
import ast
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_bundle_has_isolated_sprint03_jobs_and_existing_jobs():
    config = yaml.safe_load((ROOT / 'databricks.yml').read_text())
    jobs = config['resources']['jobs']
    for job in ('temporalx_sprint01', 'temporalx_sprint02',
                'temporalx_sprint03_capture', 'temporalx_sprint03_state',
                'temporalx_sprint03_recovery'):
        assert job in jobs
    assert jobs['temporalx_sprint03_state']['max_concurrent_runs'] == 1
    names = {task['task_key'] for task in jobs['temporalx_sprint03_state']['tasks']}
    assert names == {'state_baseline', 'state_candidate'}
    assert 'checkpoint_root' in config['variables']


def test_kafka_compose_is_development_only_and_binds_loopback():
    data = yaml.safe_load((ROOT / 'docker-compose.yml').read_text())
    broker = data['services']['redpanda']
    assert broker['image'].endswith(':v26.2.4')
    assert '127.0.0.1:19092:19092' in broker['ports']
    assert any('localhost:19092' in item for item in broker['command'])


def test_spark_script_is_parseable_and_has_an_explicit_branch_guard():
    file = ROOT / 'streaming/databricks_stream.py'
    tree = ast.parse(file.read_text())
    functions = {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}
    assert {'ingest_kafka', 'streaming_state', 'offline_backfill', 'register_branch'} <= functions
    code = file.read_text()
    assert 'register_branch(spark, names, args.branch' in code
    assert 'checkpoint_path(args.checkpoint_root' in code
    assert 'dropDuplicatesWithinWatermark' in code
    assert 'whenNotMatchedInsertAll' in code
    assert 'errorifexists' in code


def test_streaming_fixture_has_stable_transport_coordinates():
    from temporalx.stateful import load_deliveries
    first = load_deliveries(ROOT / 'fixtures/streaming/batch01.jsonl')
    second = load_deliveries(ROOT / 'fixtures/streaming/batch02.jsonl')
    coords = [(d.topic, d.partition, d.offset) for d in first + second]
    assert len(coords) == len(set(coords)) == 6
    assert len({(d.event.tenant_id, d.event.event_id) for d in first + second}) == 5
