"""Local reproducible Sprint 05 CLI. Does not require Spark or a LLM."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from temporalx.state_intelligence import load_contract, assess_upgrade, incident_diagnosis
from temporalx.state_reference import example
from temporalx.rocksdb_lab import load_progress


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='command', required=True)
    upgrade = sub.add_parser('upgrade')
    upgrade.add_argument('--old', required=True)
    upgrade.add_argument('--new', required=True)
    diagnose = sub.add_parser('diagnose')
    diagnose.add_argument('--progress', required=True)
    sub.add_parser('simulate')
    args = parser.parse_args()
    if args.command == 'simulate':
        result = example()
    elif args.command == 'upgrade':
        original = load_contract(json.loads(Path(args.old).read_text(encoding='utf-8')))
        candidate = load_contract(json.loads(Path(args.new).read_text(encoding='utf-8')))
        result = assess_upgrade(original, candidate).dict()
    else:
        result = incident_diagnosis(load_progress(args.progress))
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
