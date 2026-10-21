"""Read-only checkpoint introspection. Requires compatible Databricks runtime.

State content may be sensitive in real workloads: values are OFF by default.
Inspect checkpoint copies or stop the writer if consistency is critical.
"""
from __future__ import annotations
import argparse
import json
import re
from pyspark.sql import SparkSession


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--checkpoint-root', required=True)
    p.add_argument('--branch', required=True)
    p.add_argument('--limit', type=int, default=10)
    p.add_argument('--show-values', action='store_true', help='Only for SYNTHETIC state')
    args = p.parse_args()
    if not args.checkpoint_root.startswith('/Volumes/') or '..' in args.checkpoint_root:
        raise ValueError('Only read isolated UC Volume checkpoints')
    if not re.fullmatch(r'[a-z][a-z0-9_]{0,39}', args.branch):
        raise ValueError('Use a safe research branch name')
    if not 1 <= args.limit <= 100:
        raise ValueError('Limit to 1..100 rows')
    path = args.checkpoint_root.rstrip('/') + '/s04/rocksdb_' + args.branch
    spark = SparkSession.builder.getOrCreate()
    metadata = spark.read.format('state-metadata').load(path)
    print('STATE_METADATA_SCHEMA', metadata.schema.simpleString())
    metadata.orderBy('operatorId').show(args.limit, truncate=False)
    state = spark.read.format('statestore').load(path)
    print('STATE_SCHEMA', state.schema.simpleString())
    if args.show_values:
        state.limit(args.limit).show(args.limit, truncate=False)
    else:
        print(json.dumps({'notice': 'State key/value contents withheld; use --show-values only on synthetic data',
                          'checkpoint': path}))

if __name__ == '__main__':
    main()
