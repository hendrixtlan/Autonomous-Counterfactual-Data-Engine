"""Read-only forensic inspection for transformWithState variables and timers.

Access to checkpoint state may expose sensitive data. Default prints schema and
operator metadata only; raw values require explicit opt-in for synthetic labs.
"""
import argparse
import re
from pyspark.sql import SparkSession


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--checkpoint-root', required=True)
    p.add_argument('--branch', required=True)
    p.add_argument('--mode', required=True, choices=['ttl', 'timer'])
    p.add_argument('--operator-id', type=int, default=0)
    p.add_argument('--show-synthetic-values', action='store_true')
    p.add_argument('--limit', type=int, default=10)
    args = p.parse_args()
    if not args.checkpoint_root.startswith('/Volumes/') or '..' in args.checkpoint_root:
        raise ValueError('Only checkpoint paths in UC Volumes allowed')
    if not re.fullmatch('[a-z][a-z0-9_]{0,39}', args.branch) or not (0 <= args.operator_id <= 100):
        raise ValueError('Unsafe branch or operator ID')
    if not 1 <= args.limit <= 50:
        raise ValueError('Invalid limit')
    checkpoint = args.checkpoint_root.rstrip('/') + '/s05/' + args.mode + '_' + args.branch
    spark = SparkSession.builder.getOrCreate()
    meta = spark.read.format('state-metadata').load(checkpoint)
    meta.show(args.limit, truncate=False)
    state_var = 'seen_event_ids' if args.mode == 'ttl' else 'session'
    reader = spark.read.format('statestore').option('operatorId', args.operator_id)
    state = reader.option('stateVarName', state_var).load(checkpoint)
    print('STATE_VARIABLE', state_var, 'SCHEMA', state.schema.simpleString())
    if args.show_synthetic_values:
        state.limit(args.limit).show(args.limit, truncate=False)
    if args.mode == 'timer':
        timers = reader.option('readRegisteredTimers', True).load(checkpoint)
        print('TIMERS_SCHEMA', timers.schema.simpleString())
        if args.show_synthetic_values:
            timers.limit(args.limit).show(args.limit, truncate=False)
    print('No checkpoint mutation was performed.')


if __name__ == '__main__':
    main()
