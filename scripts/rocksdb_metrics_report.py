"""Parse Spark StreamingQueryProgress without requiring Spark locally."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from temporalx.rocksdb_lab import load_progress, parse_progress, summary


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--input', required=True, help='Spark progress JSON/JSONL')
    p.add_argument('--output', help='Optional summary JSON')
    args = p.parse_args()
    report = summary(parse_progress(load_progress(args.input)))
    payload = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload + '\n', encoding='utf-8')
    print(payload)

if __name__ == '__main__':
    main()
