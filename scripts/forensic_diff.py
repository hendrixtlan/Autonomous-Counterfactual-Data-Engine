"""Offline Sprint 07 forensic diff CLI; no Spark dependencies or writes to production."""
from __future__ import annotations
import argparse
import json
from hashlib import sha256
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from temporalx.forensics import (compare_bundles, load_bundle, make_bundle,
                                 reference_from_events, write_new, ForensicEvidenceError)


def metadata_for_source(path: Path, scope: list[str], *, batch: int, operator: int,
                        source_id: str | None = None, policy: str = 'v2') -> dict:
    digest = sha256(path.read_bytes()).hexdigest()
    return dict(state_kind='account-net-cents-v1', source_snapshot_id=source_id or f'archive-sha256:{digest}',
                source_sha256=digest, policy=policy, operator_revision='s07-aggregate-v1',
                checkpoint_batch_id=batch, operator_id=operator, watermark_policy='unbounded-history',
                tenant_scope=sorted(set(scope)), currency='USD', origin='replay')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='cmd', required=True)
    base = sub.add_parser('reference', help='construct oracle bundle from immutable event archive')
    base.add_argument('--events', required=True)
    base.add_argument('--tenant', action='append', required=True)
    base.add_argument('--batch-id', type=int, required=True)
    base.add_argument('--operator-id', type=int, default=0)
    base.add_argument('--source-id')
    base.add_argument('--policy', choices=('v1','v2'), default='v2')
    base.add_argument('--out', required=True)
    cmp = sub.add_parser('compare', help='strictly align two evidence bundles and diff')
    cmp.add_argument('--reference', required=True)
    cmp.add_argument('--observed', required=True)
    cmp.add_argument('--out', required=True)
    demo = sub.add_parser('demo', help='create synthetic missing/extra/mismatch cases')
    demo.add_argument('--events', default='examples/demo_events.jsonl')
    demo.add_argument('--out-dir', default='artifacts/s07-demo')
    demo.add_argument('--tenant', default='tenant-1')
    verify = sub.add_parser('verify', help='verify SHA-256 and row contracts of evidence')
    verify.add_argument('--bundle', required=True)
    args = parser.parse_args()
    try:
        if args.cmd == 'reference':
            p = Path(args.events)
            ref = reference_from_events(p, metadata_for_source(p, args.tenant, batch=args.batch_id,
                                        operator=args.operator_id, source_id=args.source_id, policy=args.policy))
            write_new(args.out, ref)
            print(json.dumps({'reference_file': args.out, 'sha256': ref['sha256'], 'rows': len(ref['rows'])}))
        elif args.cmd == 'compare':
            report = compare_bundles(load_bundle(args.reference), load_bundle(args.observed))
            write_new(args.out, report)
            print(json.dumps({'report': args.out, 'decision': report['decision'], 'counts': report['counts'],
                              'report_sha256': report['sha256']}))
        elif args.cmd == 'verify':
            b = load_bundle(args.bundle)
            print(json.dumps({'status': 'VERIFIED', 'sha256': b['sha256']}))
        elif args.cmd == 'demo':
            out = Path(args.out_dir)
            path = Path(args.events)
            meta = metadata_for_source(path, [args.tenant], batch=3, operator=0)
            reference = reference_from_events(path, meta)
            if not reference['rows']:
                raise ForensicEvidenceError('Demo tenant has no reference events')
            # Construct an explicitly synthetic checkpoint observation. Inject one
            # wrong amount, one absent account, one extra ghost account.
            rows = [dict(r) for r in reference['rows']]
            rows[0]['net_cents'] += 10000
            if len(rows) < 2:
                raise ForensicEvidenceError('Demo requires at least two accounts to inject missing-key incident')
            rows.pop(-1)  # drop an actual account from the synthetic observation
            rows.append({'tenant_id': args.tenant, 'account_id': '__unexpected_s07', 'net_cents': 42})
            observed = make_bundle({**meta, 'origin': 'synthetic-checkpoint'}, rows)
            report = compare_bundles(reference, observed)
            out.mkdir(parents=True, exist_ok=True)
            write_new(out / 'reference.json', reference)
            write_new(out / 'observed-synthetic.json', observed)
            write_new(out / 'report.json', report)
            print(json.dumps({'decision': report['decision'], 'counts': report['counts'],
                              'output_dir': str(out), 'synthetic_only': True}))
    except (ForensicEvidenceError, ValueError, FileNotFoundError, FileExistsError) as exc:
        print(f'FAIL_CLOSED: {exc}', file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
