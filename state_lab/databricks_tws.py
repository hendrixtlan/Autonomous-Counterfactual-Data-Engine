"""Databricks Runtime >= 17.3 LTS row-based transformWithState lab.

Requires actual workspace compute; not executed in offline local test suite.
Synthetic rate stream, research-only isolated Delta tables and checkpoints.
Two INDEPENDENT examples: processing-time TTL MapState, event-time session timers.
Do NOT repurpose checkpoints across processors or semantic revisions.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import re
from pyspark.sql import Row, SparkSession, functions as F
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, TimestampType


class TTLProbe(StatefulProcessor):
    """Demonstrate MapState TTL; it is NOT a payment deduplication guarantee."""
    def __init__(self, ttl_ms: int):
        self.ttl_ms = ttl_ms

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.seen = handle.getMapState(
            stateName='seen_event_ids',
            userKeySchema='event_id long',
            valueSchema='first_seen_ms long',
            ttlDurationMs=self.ttl_ms,
        )

    def handleInputRows(self, key, rows, timerValues):
        batch_ms = int(timerValues.getCurrentProcessingTimeInMs())
        for row in rows:
            event_id = (int(row.event_id),)
            was_seen = self.seen.containsKey(event_id)
            if not was_seen:
                self.seen.updateValue(event_id, (batch_ms,))
            yield Row(tenant_id=key[0], account_id=key[1], event_id=event_id[0],
                      accepted=not was_seen, processing_time_ms=batch_ms)

    def close(self):
        pass


class SessionTimerProbe(StatefulProcessor):
    """Event-time session close with explicit timer; NO TTL in EventTime mode."""
    def __init__(self, inactivity_ms: int):
        self.inactivity_ms = inactivity_ms

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.handle = handle
        self.session = handle.getValueState(
            stateName='session', schema='first_event_ms long, last_event_ms long, event_count long, deadline_ms long'
        )

    def handleInputRows(self, key, rows, timerValues):
        current = self.session.get() if self.session.exists() else None
        first = int(current[0]) if current is not None else None
        last = int(current[1]) if current is not None else None
        count = int(current[2]) if current is not None else 0
        old_deadline = int(current[3]) if current is not None else None
        watermark = int(timerValues.getCurrentWatermarkInMs())
        for row in rows:
            event_ms = int(row.event_ms)
            # Defensive drop of already-late rows; upstream watermark behavior
            # must still be validated with a real Databricks execution.
            if event_ms <= watermark:
                continue
            first = event_ms if first is None else min(first, event_ms)
            last = event_ms if last is None else max(last, event_ms)
            count += 1
        if count == 0:
            return
        deadline = int(last) + self.inactivity_ms
        if old_deadline is not None and old_deadline != deadline:
            self.handle.deleteTimer(old_deadline)
        self.handle.registerTimer(deadline)
        self.session.update((first, last, count, deadline))
        return
        yield  # make generator; output rows are emitted by the timer callback

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo):
        state = self.session.get() if self.session.exists() else None
        if state is None:
            return
        first, last, count, deadline = map(int, state)
        if int(expiredTimerInfo.getExpiryTimeInMs()) != deadline:
            return
        self.session.clear()
        yield Row(tenant_id=key[0], account_id=key[1],
                  first_event_ms=first, last_event_ms=last, event_count=count,
                  event_ts=datetime.fromtimestamp(deadline / 1000, tz=timezone.utc),
                  action='SESSION_CLOSED_BY_EVENT_TIME_TIMER')

    def close(self):
        pass


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--mode', required=True, choices=['ttl', 'timer'])
    p.add_argument('--catalog', required=True)
    p.add_argument('--schema', required=True)
    p.add_argument('--checkpoint-root', required=True)
    p.add_argument('--branch', required=True)
    p.add_argument('--run-seconds', type=int, default=60)
    p.add_argument('--ttl-ms', type=int, default=8000)
    p.add_argument('--inactivity-ms', type=int, default=5000)
    args = p.parse_args()
    ident = re.compile(r'^[A-Za-z_][A-Za-z0-9_]{0,63}$')
    branch = re.compile(r'^[a-z][a-z0-9_]{0,39}$')
    if not ident.fullmatch(args.catalog) or not ident.fullmatch(args.schema) or not branch.fullmatch(args.branch):
        raise ValueError('Unsafe catalog, schema or branch name')
    if not args.checkpoint_root.startswith('/Volumes/') or '..' in args.checkpoint_root:
        raise ValueError('Only separate UC Volume checkpoints are accepted')
    if not (10 <= args.run_seconds <= 3600 and 1000 <= args.ttl_ms <= 3600000 and 1000 <= args.inactivity_ms <= 3600000):
        raise ValueError('Out-of-range experiment duration')

    spark = SparkSession.builder.getOrCreate()
    # Prefer a compatible Databricks Runtime >=17.3 LTS. These are explicit
    # research settings; state schemas use Avro to support a follow-up trial.
    spark.conf.set('spark.sql.streaming.stateStore.providerClass',
                   'org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider')
    spark.conf.set('spark.sql.streaming.stateStore.encodingFormat', 'avro')
    spark.conf.set('spark.sql.shuffle.partitions', '2')
    source = (spark.readStream.format('rate').option('rowsPerSecond', 2).option('numPartitions', 1).load()
              .select(F.lit('sandbox').alias('tenant_id'),
                      F.concat(F.lit('acct-'), (F.floor(F.col('value') / F.lit(20)) % 2).cast('string')).alias('account_id'),
                      (F.col('value') % 4).cast('long').alias('event_id'),
                      F.col('timestamp').alias('event_ts'),
                      F.unix_millis('timestamp').cast('long').alias('event_ms')))
    checkpoint = args.checkpoint_root.rstrip('/') + '/s05/' + args.mode + '_' + args.branch
    destination = f'{args.catalog}.{args.schema}.tx_s05_{args.mode}_{args.branch}'
    if args.mode == 'ttl':
        output_schema = StructType([
            StructField('tenant_id', StringType()), StructField('account_id', StringType()),
            StructField('event_id', LongType()), StructField('accepted', BooleanType()),
            StructField('processing_time_ms', LongType())])
        output = (source.groupBy('tenant_id', 'account_id')
                  .transformWithState(TTLProbe(args.ttl_ms), output_schema, outputMode='Append', timeMode='ProcessingTime'))
    else:
        output_schema = StructType([
            StructField('tenant_id', StringType()), StructField('account_id', StringType()),
            StructField('first_event_ms', LongType()), StructField('last_event_ms', LongType()),
            StructField('event_count', LongType()), StructField('event_ts', TimestampType()),
            StructField('action', StringType())])
        output = (source.withWatermark('event_ts', '2 seconds').groupBy('tenant_id', 'account_id')
                  .transformWithState(SessionTimerProbe(args.inactivity_ms), output_schema,
                                      outputMode='Append', timeMode='EventTime', eventTimeColumnName='event_ts'))
    query = (output.writeStream.format('delta').outputMode('append')
             .option('checkpointLocation', checkpoint)
             .trigger(processingTime='3 seconds').toTable(destination))
    try:
        query.awaitTermination(args.run_seconds)
    finally:
        if query.isActive:
            query.stop()
    if query.exception() is not None:
        raise RuntimeError('Stateful streaming query failed') from query.exception()
    print(f'RESEARCH_RESULT sink={destination} checkpoint={checkpoint}; inspect with state_lab/inspect_state.py')


if __name__ == '__main__':
    main()
