"""Validate the authored Databricks orchestration without claiming a Spark runtime test."""
import importlib.util
from pathlib import Path
import pytest

HERE = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('s07_db', HERE/'s07/databricks_lab.py')
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)

class Args:
    checkpoint_root = '/Volumes/sandbox/temporalx/checkpoints'
    evidence_root = '/Volumes/sandbox/temporalx/evidence'
    branch = 'research_001'


def test_disjoint_explicit_roots():
    cp, evidence = module.paths(Args)
    assert cp.endswith('s07/account_research_001')
    assert evidence.endswith('s07/research_001')
    assert evidence not in cp


def test_forensic_path_rejects_placeholder_and_traversal():
    for invalid in ['dbfs:/Volumes/sandbox', '/Volumes/REPLACE_ME/x',
                    '/Volumes/a/../b', '/Volumes/a//b', '/Volumes/a b']:
        with pytest.raises(ValueError):
            module.validate_path(invalid, 'test')


def test_forensic_path_rejects_overlap():
    class Overlap:
        checkpoint_root = '/Volumes/a/b'
        evidence_root = '/Volumes/a/b/evidence'
        branch = 'ok'
    with pytest.raises(ValueError, match='nested'):
        module.paths(Overlap)


def test_forensic_path_branch_contract():
    class Invalid:
        checkpoint_root = '/Volumes/a/b/checkpoint'
        evidence_root = '/Volumes/a/b/evidence'
        branch = '../production'
    with pytest.raises(ValueError):
        module.paths(Invalid)


def test_s07_bundle_deployment_tasks_exist():
    text = (HERE/'databricks.yml').read_text()
    assert 'freeze_bronze_snapshot' in text
    assert 'materialize_rocksdb_state' in text
    assert 'read_checkpoint_and_reconcile' in text
    assert "--source-version, '0'" in text  # Explicit fixture version; must be set for workspace.
