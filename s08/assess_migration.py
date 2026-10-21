"""Offline migration inventory assessment and synthetic Bronze/Silver/Gold oracle."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from temporalx.lakehouse_modernization import assess_legacy_manifest,reconcile_deliveries


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--manifest',default='s08/fixtures/legacy_migration.json')
    p.add_argument('--deliveries',default='s08/fixtures/azure_sql_cdc.jsonl')
    p.add_argument('--out')
    args=p.parse_args()
    a=assess_legacy_manifest(json.loads(Path(args.manifest).read_text(encoding='utf-8')))
    b=reconcile_deliveries(json.loads(line) for line in Path(args.deliveries).read_text(encoding='utf-8').splitlines() if line.strip())
    report={'migration':a,'synthetic_reconciliation':b,'spark_test_executed':False}
    if args.out:
        dst=Path(args.out)
        if dst.exists(): raise FileExistsError('Evidence file exists; use a fresh output')
        dst.parent.mkdir(parents=True,exist_ok=True)
        dst.write_text(json.dumps(report,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    print(json.dumps(report,indent=2,sort_keys=True))

if __name__=='__main__':main()
