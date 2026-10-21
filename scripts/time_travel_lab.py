"""Sprint 08 local temporal investigations: fixture, hash check and reconciliation."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from temporalx.time_travel import make_timeline, verify_timeline, compare_timelines, canonical
from temporalx.forensics import write_new


def fixture():
    """Fault inserted at batch 2, healed at batch 4: endpoint checks miss it."""
    base = dict(checkpoint_id='s08-study-001', source_snapshot_id='synthetic-ledger@42',
                source_sha256='a'*64, batch_lineage_sha256='b'*64,
                operator_revision='account-sum-v1', operator_id=0,
                state_kind='account-net-cents-v1', watermark_policy='unbounded-history',
                tenant_scope=['blue'], currency='USD', first_batch_id=0, last_batch_id=4)
    ref = [
        (0, 'A', None, 10000), (0, 'B', None, 5000),
        (1, 'A', 10000, 12000), (2, 'A', 12000, 12500),
        (3, 'B', 5000, 4000), (4, 'A', 12500, 15000),
    ]
    obs = [
        (0, 'A', None, 10000), (0, 'B', None, 5000),
        (1, 'A', 10000, 12000), (2, 'A', 12000, 22500),
        (3, 'B', 5000, 4000), (3, 'ghost', None, 900),
        (4, 'A', 22500, 15000), (4, 'ghost', 900, None),
    ]
    as_rows = lambda x: [dict(batch_id=b, tenant_id='blue', account_id=a,
                               before_cents=before, after_cents=after) for b,a,before,after in x]
    return (make_timeline({**base,'origin':'reference'},as_rows(ref)),
            make_timeline({**base,'origin':'synthetic-checkpoint'},as_rows(obs)))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='command', required=True)
    demo = sub.add_parser('demo')
    demo.add_argument('--out-dir', default='artifacts/s08-demo')
    compare = sub.add_parser('compare')
    compare.add_argument('--reference', required=True)
    compare.add_argument('--observed', required=True)
    compare.add_argument('--out', required=True)
    args = p.parse_args()
    if args.command == 'demo':
        folder = Path(args.out_dir)
        ref, observed = fixture()
        write_new(folder/'reference.json', ref)
        write_new(folder/'observed-synthetic.json', observed)
        report = compare_timelines(ref, observed)
        write_new(folder/'time-travel-diff.json', report)
    else:
        ref = verify_timeline(json.loads(Path(args.reference).read_text()))
        obs = verify_timeline(json.loads(Path(args.observed).read_text()))
        report = compare_timelines(ref, obs)
        write_new(args.out, report)
    print(json.dumps({'status': report['status'],
                      'first_divergent_batch_id': report['first_divergent_batch_id'],
                      'divergence_episodes': report['historical_divergence_episodes'],
                      'divergent_keys_at_end': report['divergent_keys_at_end'],
                      'evidence_level': report['evidence_level'],
                      'production_mutation_authorized': False}, indent=2))

if __name__ == '__main__':
    main()
