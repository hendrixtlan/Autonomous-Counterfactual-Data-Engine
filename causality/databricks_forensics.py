"""Read-only Databricks checkpoint metadata and state-inspection experiment.

Intended for a user-controlled isolated lab. Never modifies stream/checkpoint.
This experiment is NOT run in local CI and outputs only metadata by default.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import PurePosixPath


def check_checkpoint_path(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r'/Volumes/[A-Za-z0-9_]+/[A-Za-z0-9_]+/[A-Za-z0-9_]+/[A-Za-z0-9_./-]+', value):
        raise ValueError('Only explicitly specified Unity Catalog Volume checkpoint paths are permitted')
    if '..' in PurePosixPath(value).parts or '//' in value:
        raise ValueError('Checkpoint traversal is not permitted')
    return value.rstrip('/')


def main() -> None:
    parser = argparse.ArgumentParser(description='Read-only RocksDB forensic metadata collection')
    parser.add_argument('--checkpoint', required=True)
    parser.add_argument('--state-var', default='session')
    parser.add_argument('--operator-id', type=int, default=0)
    parser.add_argument('--sample-synthetic-values', action='store_true')
    args = parser.parse_args()
    path = check_checkpoint_path(args.checkpoint)
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,63}', args.state_var):
        parser.error('Invalid state variable identifier')
    if not 0 <= args.operator_id <= 100:
        parser.error('Operator out of range')
    from pyspark.sql import SparkSession  # only needed inside Databricks
    spark = SparkSession.builder.getOrCreate()
    metadata = spark.read.format('state-metadata').load(path)
    print('METADATA_SCHEMA', metadata.schema.simpleString())
    metadata.show(10, truncate=True)
    reader = spark.read.format('statestore').option('operatorId', args.operator_id)
    state = reader.option('stateVarName', args.state_var).load(path)
    print('STATE_SCHEMA', state.schema.simpleString())
    if args.sample_synthetic_values:
        print('WARNING: enable only for synthetic tenant-isolated research checkpoints')
        state.limit(5).show(5, truncate=True)
    print(json.dumps({'checkpoint': path, 'operator_id': args.operator_id,
                      'state_variable': args.state_var,
                      'state_values_exposed': bool(args.sample_synthetic_values),
                      'checkpoint_modified': False}))


if __name__ == '__main__':
    main()
