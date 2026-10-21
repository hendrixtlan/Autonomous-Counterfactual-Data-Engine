"""TemporalX Sprint 06: evidence-first, read-only causal incident investigator.

This module deliberately cannot write to Spark, checkpoint, Delta or Kafka.
It returns verifiable hypotheses and sandbox replay evidence, NOT causal proof
nor permission for production repair. All conclusions are bounded by source inputs.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any
import json
import re

from temporalx.engine import counterfactual, ReplayConflictError, InvariantViolation
from temporalx.io import load_events
from temporalx.model import Event
from temporalx.state_intelligence import load_contract, assess_upgrade, incident_diagnosis

MAX_INPUT_BYTES = 16 * 1024 * 1024
ID_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}$')


class EvidenceRejected(ValueError):
    """Evidence missing, invalid, out of scope, or changed during processing."""


def canonical(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')


def checksum(data: bytes) -> str:
    return sha256(data).hexdigest()


def guarded_read(base: Path, relative: str) -> tuple[Path, bytes]:
    """No symlinks, absolute paths, traversal or files outside the incident directory."""
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise EvidenceRejected('Evidence paths must be nonempty relative paths')
    if '..' in Path(relative).parts or '\\' in relative:
        raise EvidenceRejected('Evidence path traversal rejected')
    cursor = base
    for part in Path(relative).parts:
        cursor = cursor / part
        if cursor.is_symlink():
            raise EvidenceRejected('Symlinked evidence rejected')
    resolved = cursor.resolve()
    if not resolved.is_relative_to(base.resolve()) or not resolved.is_file():
        raise EvidenceRejected('Evidence file missing or outside incident folder')
    if resolved.stat().st_size > MAX_INPUT_BYTES:
        raise EvidenceRejected('Evidence file exceeds size limit')
    return resolved, resolved.read_bytes()


@dataclass(frozen=True)
class InputArtifact:
    name: str
    path: str
    sha256: str
    bytes: int


@dataclass(frozen=True)
class Incident:
    incident_id: str
    tenant_id: str
    events_file: str
    progress_file: str
    original_contract: str
    proposed_contract: str
    expected_stream_total_cents: int | None = None

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> Incident:
        required = {'incident_id', 'tenant_id', 'events_file', 'progress_file',
                    'original_contract', 'proposed_contract'}
        if not required <= obj.keys() or set(obj) - (required | {'expected_stream_total_cents'}):
            raise EvidenceRejected('Incident manifest has missing/unknown keys')
        result = cls(**obj)
        if not ID_RE.fullmatch(result.incident_id) or not ID_RE.fullmatch(result.tenant_id):
            raise EvidenceRejected('Incident and tenant IDs must be safe identifiers')
        if result.expected_stream_total_cents is not None and type(result.expected_stream_total_cents) is not int:
            raise EvidenceRejected('Expected total must be integer cents')
        return result


def collect(folder: Path) -> dict[str, Any]:
    """Strictly source-scoped: independent of wall-clock time and networking."""
    folder = Path(folder).resolve()
    _, raw_manifest = guarded_read(folder, 'incident.json')
    incident = Incident.from_dict(json.loads(raw_manifest))
    paths = {'events': incident.events_file, 'progress': incident.progress_file,
             'original_contract': incident.original_contract,
             'proposed_contract': incident.proposed_contract}
    artifacts = [InputArtifact('manifest', 'incident.json', checksum(raw_manifest), len(raw_manifest))]
    content: dict[str, bytes] = {}
    for name, path in paths.items():
        _, raw = guarded_read(folder, path)
        content[name] = raw
        artifacts.append(InputArtifact(name, path, checksum(raw), len(raw)))
    # Parse bytes already hashed: do not reopen mutable source paths here.
    events = []
    for n, line in enumerate(content['events'].decode('utf-8').splitlines(), 1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and 'event' in obj:
                obj = obj['event']  # Kafka envelope in Sprint 03
            event = Event.from_dict(obj)
        except (ValueError, KeyError, TypeError) as exc:
            raise EvidenceRejected(f'Invalid archived event at line {n}: {exc}') from exc
        if event.tenant_id == incident.tenant_id:
            events.append(event)
    if not events:
        raise EvidenceRejected('No events for requested tenant; refusing cross-tenant inference')
    try:
        progress = json.loads(content['progress'])
        old = load_contract(json.loads(content['original_contract']))
        new = load_contract(json.loads(content['proposed_contract']))
    except (ValueError, TypeError, KeyError) as exc:
        raise EvidenceRejected(f'Invalid progress/contract evidence: {exc}') from exc
    if not isinstance(progress, list):
        raise EvidenceRejected('Progress evidence must be a list of query progress records')
    # Restrict tenant inference to the tenant's own events, never mix balances.
    return {'incident': incident, 'artifacts': artifacts, 'events': tuple(events),
            'progress': progress, 'old': old, 'new': new,
            'evidence_digest': checksum(canonical([asdict(a) for a in artifacts]))}


def investigate(evidence: dict[str, Any]) -> dict[str, Any]:
    incident = evidence['incident']
    events = evidence['events']
    key_counts: dict[str, int] = {}
    for ev in events:
        key_counts[ev.event_id] = key_counts.get(ev.event_id, 0) + 1
    duplicate_deliveries = sum(x - 1 for x in key_counts.values())
    max_event_time = max(e.occurred_at for e in events)
    late_by_arrival = sum(ev.ingested_at > ev.occurred_at + timedelta(minutes=5) for ev in events)
    # Arrival lag is an observation, NOT proof that Spark dropped the event.
    upgrade = assess_upgrade(evidence['old'], evidence['new'])
    diagnosis = incident_diagnosis(evidence['progress'])
    hypotheses = [
        {'id': 'H_DUPLICATE', 'claim': 'Duplicate delivery may have amplified v1 balances',
         'evidence': ['event identities', 'policy v1 vs v2'],
         'observed_deliveries': duplicate_deliveries,
         'status': 'TESTABLE' if duplicate_deliveries else 'NOT_SUPPORTED_BY_INPUT'},
        {'id': 'H_LATE', 'claim': 'Arrival lag may cause watermark exclusion',
         'evidence': ['occurred_at vs ingested_at'], 'observed_lag_over_5m': late_by_arrival,
         'status': 'NEEDS_STREAM_WATERMARK_EVIDENCE' if late_by_arrival else 'NOT_SUPPORTED_BY_INPUT'},
        {'id': 'H_CONTRACT', 'claim': 'Resume checkpoint compatibility requires assessment',
         'evidence': ['original_contract', 'proposed_contract'],
         'status': upgrade.decision},
        {'id': 'H_ROCKSDB', 'claim': 'RocksDB performance metrics may indicate recovery risk',
         'evidence': ['progress'], 'status': 'SIGNALS_FOUND' if diagnosis['signals'] else 'NO_SIGNALS'},
    ]
    return {'hypotheses': hypotheses, 'upgrade': upgrade.dict(),
            'diagnosis': diagnosis, 'event_count': len(events),
            'max_event_time': max_event_time.isoformat(),
            'duplicate_deliveries': duplicate_deliveries,
            'lag_over_5m': late_by_arrival}


def experiment(evidence: dict[str, Any]) -> dict[str, Any]:
    """Both policies run against one frozen, tenant-isolated source snapshot."""
    try:
        result = counterfactual(evidence['events'])
    except (ReplayConflictError, InvariantViolation) as exc:
        return {'status': 'FAILED_INVARIANT', 'reason': str(exc), 'safe_to_propose': False}
    baseline = result['baseline']['account_balances']
    candidate = result['candidate']['account_balances']
    # Re-run with reversed delivery order to check canonical replay determinism.
    swapped = counterfactual(reversed(evidence['events']))
    deterministic = result == swapped
    same_tenant = all(json.loads(key)[0] == evidence['incident'].tenant_id for key in (set(baseline) | set(candidate)))
    expected = evidence['incident'].expected_stream_total_cents
    published_match = None if expected is None else sum(baseline.values()) == expected
    return {'status': 'PASSED' if deterministic and same_tenant and result['reconciled'] else 'FAILED_INVARIANT',
            'deterministic_order_reversal': deterministic,
            'tenant_isolated': same_tenant, 'stream_total_match_v1': published_match,
            'observed_stream_total_cents': expected,
            'baseline_total_cents': sum(baseline.values()),
            'candidate_total_cents': sum(candidate.values()),
            'delta_total_cents': result['total_delta_cents'],
            'account_delta_cents': result['account_delta_cents'],
            'snapshot_sha256': checksum(canonical([ev.to_dict() for ev in evidence['events']])),
            'safe_to_propose': deterministic and same_tenant and result['reconciled']}


def decide(evidence: dict[str, Any], diagnosis: dict[str, Any], trial: dict[str, Any]) -> dict[str, Any]:
    """Conservative policy: no autonomous mutation, even when replay matches."""
    if trial['status'] != 'PASSED':
        outcome, why = 'BLOCK', 'Reconciliation or replay invariants failed'
    elif diagnosis['upgrade']['decision'] == 'BLOCK':
        outcome, why = 'BLOCK', 'State contract change is not eligible for checkpoint reuse'
    elif diagnosis['upgrade']['decision'] == 'REBUILD_REQUIRED':
        outcome, why = 'PROPOSE_ISOLATED_REBUILD', 'Changed state semantics require a new isolated checkpoint'
    elif trial['delta_total_cents'] != 0:
        outcome, why = 'PROPOSE_ISOLATED_REBUILD', 'Replay demonstrates financial divergence'
    else:
        outcome, why = 'INVESTIGATE_FURTHER', 'No financial divergence demonstrated by replay'
    return {'recommendation': outcome, 'rationale': why,
            'requires_human_approval': True, 'production_mutation_allowed': False,
            'checkpoint_reuse_approved': False,
            'required_gates': ['independent Delta/Spark reconciliation', 'source retention and coverage audit',
                               'checkpoint compatibility review', 'peer review and formal change approval'],
            'scope': {'tenant_id': evidence['incident'].tenant_id,
                      'source': 'read-only local evidence', 'target': 'sandbox only'}}


def assemble(evidence: dict[str, Any], diagnosis: dict[str, Any], trial: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    payload = {'schema_version': 's06.v1', 'incident_id': evidence['incident'].incident_id,
               'tenant_id': evidence['incident'].tenant_id,
               'source_artifacts': [asdict(a) for a in evidence['artifacts']],
               'evidence_digest': evidence['evidence_digest'],
               'observations': diagnosis, 'experiment': trial, 'recovery_plan': policy,
               'limitations': [
                   'Local Python replay is not evidence of successful Spark/RocksDB recovery.',
                   'Arrival lag does not establish actual watermark drops.',
                   'RocksDB metrics alone do not demonstrate a root cause.',
                   'Human review and independent Spark validation are mandatory.'
               ]}
    return {**payload, 'report_sha256': checksum(canonical(payload))}


def verify_report(report: dict[str, Any], folder: Path) -> bool:
    """Detect changes in evidence or report. Does not infer authenticity of origin."""
    copied = dict(report)
    claimed = copied.pop('report_sha256', None)
    if claimed != checksum(canonical(copied)):
        return False
    base = Path(folder).resolve()
    try:
        for artifact in report['source_artifacts']:
            _, data = guarded_read(base, artifact['path'])
            if len(data) != artifact['bytes'] or checksum(data) != artifact['sha256']:
                return False
    except (EvidenceRejected, OSError, ValueError, KeyError):
        return False
    return True


def run_pipeline(folder: Path) -> dict[str, Any]:
    evidence = collect(folder)
    observations = investigate(evidence)
    trial = experiment(evidence)
    policy = decide(evidence, observations, trial)
    report = assemble(evidence, observations, trial, policy)
    if not verify_report(report, folder):
        raise EvidenceRejected('Source files changed during investigation')
    return report
