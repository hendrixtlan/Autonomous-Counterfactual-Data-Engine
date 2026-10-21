"""Append NEW synthetic file to Spark JSON source to force a checkpoint resume.

Never mutates existing stream source files: Spark file source assumes append-only.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path


def append_batch(directory: Path, batch_name: str, count: int = 20) -> dict:
    if not batch_name.startswith('new_') or not batch_name.removeprefix('new_').isalnum():
        raise ValueError('batch-name must be new_ followed by letters or digits')
    if not (1 <= count <= 200000):
        raise ValueError('count must be 1..200k')
    if not directory.is_dir() or not list(directory.glob('*.json')):
        raise ValueError('Source directory must be initialized by prepare_rocksdb_fixture')
    target = directory / (batch_name + '.json')
    with target.open('x', encoding='utf-8') as file:
        for i in range(count):
            row = {'tenant_id': 'blue', 'event_id': f'{batch_name}_event_{i:010d}',
                   'account_id': f'{batch_name}_acct_{i:010d}', 'kind': 'CAPTURE',
                   'amount_cents': 101, 'occurred_at': '2026-10-08T00:09:00+00:00',
                   'source_batch': 999}
            file.write(json.dumps(row, sort_keys=True) + '\n')
    return {'file': str(target), 'new_unique_events': count}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input', required=True)
    p.add_argument('--batch-name', required=True)
    p.add_argument('--count', type=int, default=20)
    args = p.parse_args()
    print(json.dumps(append_batch(Path(args.input), args.batch_name, args.count), indent=2))


if __name__ == '__main__':
    main()
