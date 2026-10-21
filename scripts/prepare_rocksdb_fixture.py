"""Create event files for a reproducible RocksDB streaming demonstration.

Writes each source batch to a separate JSON file, in deterministic file order.
Input file processing still depends on streaming source admission and timing.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path


def prepare(output: Path, *, accounts: int = 5, chunks: int = 2) -> dict:
    if not (1 <= accounts <= 2000000) or not (2 <= chunks <= 100):
        raise ValueError('accounts must be 1..2M and chunks 2..100')
    output.mkdir(parents=True, exist_ok=True)
    if list(output.glob('*.json')):
        raise FileExistsError('Input directory must be empty. Create a new isolated experiment')
    counts = []
    start = datetime(2026, 10, 8, tzinfo=timezone.utc)
    for part in range(chunks):
        written = 0
        file = output / f'{part:04d}.json'
        with file.open('x', encoding='utf-8') as fd:
            for i in range(accounts):
                # First half will form unique keys; second half repeats the
                # same event identities and tests the deduplicating state store.
                event_key = f'{i:09d}'
                occurred = start + timedelta(minutes=part)
                row = {
                    'tenant_id': 'blue' if i % 2 == 0 else 'green',
                    'event_id': f'purchase_{event_key}',
                    'account_id': f'acct_{event_key}',
                    'kind': 'CAPTURE', 'amount_cents': 100 + i % 900,
                    'occurred_at': occurred.isoformat(),
                    'source_batch': part,
                }
                fd.write(json.dumps(row, sort_keys=True) + '\n')
                written += 1
        counts.append(written)
    return {'input_dir': str(output), 'files': chunks, 'events_per_file': accounts,
            'physical_deliveries': accounts * chunks, 'logical_keys': accounts,
            'expected_duplicate_deliveries': accounts * (chunks - 1), 'file_counts': counts}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='artifacts/rocksdb-input')
    parser.add_argument('--accounts', type=int, default=20)
    parser.add_argument('--chunks', type=int, default=2)
    args = parser.parse_args()
    print(json.dumps(prepare(Path(args.output), accounts=args.accounts, chunks=args.chunks), indent=2))


if __name__ == '__main__':
    main()
