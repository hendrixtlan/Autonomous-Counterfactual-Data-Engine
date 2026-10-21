"""Sprint 06 regression: deterministic causal investigation and fail-closed gating."""
from __future__ import annotations

from copy import deepcopy
import json
import shutil
from pathlib import Path

import pytest

from temporalx.causality import (
    EvidenceRejected, collect, investigate, experiment, decide, assemble,
    run_pipeline, verify_report, guarded_read, checksum, canonical
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / 'fixtures/causality/incident-001'


def sandbox(tmp_path: Path) -> Path:
    folder = tmp_path / 'incident'
    shutil.copytree(FIXTURE, folder)
    return folder


def test_reproducible_research_and_report_hash():
    report = run_pipeline(FIXTURE)
    assert report == run_pipeline(FIXTURE)
    assert verify_report(report, FIXTURE)
    assert report['report_sha256'] == checksum(canonical({k: v for k,v in report.items() if k != 'report_sha256'}))


def test_incident_v1_v2_counterfactual():
    report = run_pipeline(FIXTURE)
    trial = report['experiment']
    assert trial['status'] == 'PASSED'
    assert trial['deterministic_order_reversal'] is True
    assert trial['tenant_isolated'] is True
    assert trial['candidate_total_cents'] == 12500
    assert trial['baseline_total_cents'] == 22500
    assert trial['delta_total_cents'] == -10000
    assert trial['stream_total_match_v1'] is False  # Late refund was omitted online


def test_agent_scoped_and_no_auto_apply():
    report = run_pipeline(FIXTURE)
    assert report['tenant_id'] == 'blue'
    assert report['observations']['duplicate_deliveries'] == 1
    assert report['observations']['lag_over_5m'] == 1
    assert report['recovery_plan']['recommendation'] == 'PROPOSE_ISOLATED_REBUILD'
    assert report['recovery_plan']['requires_human_approval']
    assert report['recovery_plan']['production_mutation_allowed'] is False
    assert report['recovery_plan']['checkpoint_reuse_approved'] is False
    for key in report['experiment']['account_delta_cents']:
        assert json.loads(key)[0] == 'blue'


def test_manifest_tamper_fails_verification(tmp_path):
    folder = sandbox(tmp_path)
    report = run_pipeline(folder)
    with (folder / 'events.jsonl').open('a', encoding='utf8') as f:
        f.write('\n')
    assert verify_report(report, folder) is False


def test_report_tamper_fails_verification():
    report = run_pipeline(FIXTURE)
    report['experiment']['delta_total_cents'] = 0
    assert verify_report(report, FIXTURE) is False


@pytest.mark.parametrize('bad_path', ['../batch01.jsonl', '/etc/passwd', 'events.jsonl/../../foo', 'C:\\windows\\system.ini'])
def test_rejects_unsafe_paths(tmp_path, bad_path):
    folder = sandbox(tmp_path)
    obj = json.loads((folder / 'incident.json').read_text())
    obj['events_file'] = bad_path
    (folder / 'incident.json').write_text(json.dumps(obj))
    with pytest.raises(EvidenceRejected):
        collect(folder)


def test_rejects_symlink(tmp_path):
    folder = sandbox(tmp_path)
    (folder / 'events.jsonl').unlink()
    (folder / 'events.jsonl').symlink_to(FIXTURE / 'events.jsonl')
    with pytest.raises(EvidenceRejected):
        collect(folder)


def test_rejects_cross_tenant_only(tmp_path):
    folder = sandbox(tmp_path)
    obj = json.loads((folder / 'incident.json').read_text())
    obj['tenant_id'] = 'nonexistent'
    (folder / 'incident.json').write_text(json.dumps(obj))
    with pytest.raises(EvidenceRejected, match='No events'):
        collect(folder)


def test_contract_mismatch_requires_rebuild():
    evidence = collect(FIXTURE)
    observations = investigate(evidence)
    assert observations['upgrade']['decision'] == 'REBUILD_REQUIRED'


def test_blocked_contract_is_never_reused(tmp_path):
    folder = sandbox(tmp_path)
    proposed = json.loads((folder / 'proposed.json').read_text())
    proposed['shuffle_partitions'] = 3
    (folder / 'proposed.json').write_text(json.dumps(proposed))
    report = run_pipeline(folder)
    assert report['recovery_plan']['recommendation'] == 'BLOCK'
    assert report['recovery_plan']['checkpoint_reuse_approved'] is False


def test_corrupt_archive_blocks_recommendation(tmp_path):
    folder = sandbox(tmp_path)
    with (folder / 'events.jsonl').open('a', encoding='utf8') as out:
        out.write('not-json\n')
    with pytest.raises(EvidenceRejected, match='Invalid archived event'):
        run_pipeline(folder)


def test_conflicting_duplicate_fails_invariant(tmp_path):
    folder = sandbox(tmp_path)
    rows = (folder / 'events.jsonl').read_text().splitlines()
    duplicate = json.loads(rows[0])
    duplicate['event']['amount_cents'] = 123
    rows.append(json.dumps(duplicate))
    (folder / 'events.jsonl').write_text('\n'.join(rows) + '\n')
    report = run_pipeline(folder)
    assert report['experiment']['status'] == 'FAILED_INVARIANT'
    assert report['recovery_plan']['recommendation'] == 'BLOCK'


def test_empty_progress_evidence_is_not_called_healthy(tmp_path):
    folder = sandbox(tmp_path)
    (folder / 'progress.json').write_text('[]')
    report = run_pipeline(folder)
    signals = report['observations']['diagnosis']['signals']
    assert any(s['severity'] == 'unknown' for s in signals)


def test_unsafe_id_rejected(tmp_path):
    folder = sandbox(tmp_path)
    obj = json.loads((folder / 'incident.json').read_text())
    obj['incident_id'] = '../../bad'
    (folder / 'incident.json').write_text(json.dumps(obj))
    with pytest.raises(EvidenceRejected):
        collect(folder)


def test_report_store_is_exclusive(tmp_path):
    from scripts.causality_investigate import store_report
    folder = sandbox(tmp_path)
    report = run_pipeline(folder)
    dest = tmp_path / 'out' / 'report.json'
    store_report(report, folder, dest)
    store_report(report, folder, dest)  # idempotent
    with pytest.raises(EvidenceRejected):
        store_report({**report, 'incident_id': 'tampered'}, folder, dest)
    with pytest.raises(EvidenceRejected):
        store_report(report, folder, folder / 'forbidden.json')


def test_graph_dependency_is_explicit():
    from temporalx.causality_graph import build_graph
    try:
        import langgraph  # noqa
    except ImportError:
        with pytest.raises(RuntimeError, match='Install'):
            build_graph()
    else:
        assert build_graph() is not None


def test_graph_parity_if_installed():
    pytest.importorskip('langgraph')
    from temporalx.causality_graph import run_graph
    assert run_graph(FIXTURE) == run_pipeline(FIXTURE)


def test_synthetic_fault_injection_signal_detection():
    from scripts.benchmark_causality import cases, assess
    rows = cases()
    assert len(rows) == 8
    for name in ('duplicate', 'late_lag', 'semantic_change'):
        scores = assess([r['fault_injection'][name] for r in rows],
                        [r['observed'][name] for r in rows], name)
        assert (scores.tp, scores.fp, scores.tn, scores.fn) == (4, 0, 4, 0)
