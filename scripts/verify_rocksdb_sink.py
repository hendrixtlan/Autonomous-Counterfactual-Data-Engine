"""Verify the exact set of unique logical events in an atomic Spark lab sink.

A PASS certifies only the synthetic input and local accepted output, not any
real payment/ledger system or checkpoint failure mechanism.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path


def _jsonl(path: Path):
    for line in path.read_text(encoding='utf-8').splitlines():
        if line.strip():
            yield json.loads(line)


def verify(source: str | Path, accepted: str | Path) -> dict:
    source = Path(source)
    accepted = Path(accepted)
    files = sorted(source.glob('*.json'))
    published = sorted(accepted.glob('batch-*.jsonl'))
    if not files:
        raise ValueError('No source JSON files')
    if not published:
        raise ValueError('No durable accepted batch files')
    expected = {}
    deliveries = 0
    for file in files:
        for row in _jsonl(file):
            deliveries += 1
            key = (row['tenant_id'], row['event_id'])
            business = (row['account_id'], row['kind'], int(row['amount_cents']))
            if key in expected and expected[key] != business:
                raise ValueError('Conflicting event business identity in source')
            expected[key] = business
    actual = {}
    for file in published:
        for row in _jsonl(file):
            key = (row['tenant_id'], row['event_id'])
            business = (row['account_id'], row['kind'], int(row['amount_cents']))
            if key in actual:
                raise ValueError(f'Duplicate event published: {key}')
            actual[key] = business
    missing = expected.keys() - actual.keys()
    extra = actual.keys() - expected.keys()
    altered = {k for k in expected.keys() & actual.keys() if expected[k] != actual[k]}
    report = {'result': 'PASS' if not (missing or extra or altered) else 'FAIL',
              'physical_source_deliveries': deliveries,
              'unique_source_events': len(expected),
              'accepted_events': len(actual),
              'missing': len(missing), 'extra': len(extra), 'altered': len(altered)}
    return report


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input', required=True)
    p.add_argument('--accepted', required=True)
    args = p.parse_args()
    report = verify(args.input, args.accepted)
    print(json.dumps(report, indent=2))
    if report['result'] != 'PASS':
        raise SystemExit(1)

if __name__ == '__main__':
    main()
