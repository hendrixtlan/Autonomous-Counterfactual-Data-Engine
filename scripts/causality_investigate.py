"""Incident investigation CLI: read-only sources; report destination must be separate."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from temporalx.causality import run_pipeline, verify_report, EvidenceRejected, canonical


def store_report(report: dict, incident_folder: Path, output: Path) -> None:
    output = Path(output).resolve()
    incident_folder = Path(incident_folder).resolve()
    if output.is_relative_to(incident_folder):
        raise EvidenceRejected('Report destination must be outside the incident evidence directory')
    if output.exists():
        if output.is_symlink() or output.read_bytes() != canonical(report) + b'\n':
            raise EvidenceRejected('Refusing to overwrite an existing different report')
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    # Exclusive file creation prevents accidental overwrite or symlink replacement.
    with output.open('x', encoding='utf-8') as dest:
        dest.write(canonical(report).decode('utf-8') + '\n')


def main() -> None:
    parser = argparse.ArgumentParser(description='Evidence-first TemporalX causal incident investigation')
    parser.add_argument('--incident-dir', type=Path, default=Path('fixtures/causality/incident-001'))
    parser.add_argument('--out', type=Path, default=Path('reports/incident-001.json'))
    parser.add_argument('--engine', choices=['reference', 'langgraph'], default='reference')
    parser.add_argument('--mlflow', action='store_true', help='Enable optional LangGraph MLflow tracing')
    parser.add_argument('--verify', type=Path, help='Verify an existing report against the evidence directory')
    args = parser.parse_args()
    if args.verify:
        payload = json.loads(args.verify.read_text(encoding='utf-8'))
        if not verify_report(payload, args.incident_dir):
            raise SystemExit('FAILED: report or input evidence changed')
        print('VERIFIED: evidence bytes and report digest match')
        return
    if args.mlflow and args.engine != 'langgraph':
        parser.error('--mlflow requires --engine langgraph')
    if args.engine == 'langgraph':
        from temporalx.causality_graph import run_graph
        report = run_graph(args.incident_dir, enable_mlflow=args.mlflow)
    else:
        report = run_pipeline(args.incident_dir)
    store_report(report, args.incident_dir, args.out)
    print(json.dumps({'incident_id': report['incident_id'],
                      'recommendation': report['recovery_plan']['recommendation'],
                      'delta_total_cents': report['experiment'].get('delta_total_cents'),
                      'report_sha256': report['report_sha256'],
                      'output': str(args.out)}, indent=2))


if __name__ == '__main__':
    main()
