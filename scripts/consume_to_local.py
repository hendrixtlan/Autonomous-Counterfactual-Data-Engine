"""Exercise actual Kafka offsets against atomic SQLite microbatch state.

Broker offset commit happens *after* local durable commit. Broker and SQLite do
not share a transaction; retry of the same Kafka offsets is safe for the lab.
The consumer group must not be shared with other consumers in this experiment.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from temporalx.model import Event  # noqa: E402
from temporalx.stateful import Delivery, StateStore  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap', default='localhost:19092')
    parser.add_argument('--topic', default='temporalx.transactions')
    parser.add_argument('--group', default='temporalx-state-s03-lab')
    parser.add_argument('--db', default='artifacts/kafka-checkpoint.db')
    parser.add_argument('--branch', default='kafka_v2')
    parser.add_argument('--batch-id', required=True)
    parser.add_argument('--policy', choices=['v1', 'v2'], default='v2')
    parser.add_argument('--max-messages', type=int, default=10000)
    parser.add_argument('--idle-polls', type=int, default=3)
    args = parser.parse_args()
    if args.bootstrap != 'localhost:19092' and not args.bootstrap.startswith('127.0.0.1:'):
        raise SystemExit('This reference consumer only permits local plaintext Kafka')
    if args.max_messages <= 0 or args.idle_polls <= 0:
        raise SystemExit('Limits must be positive')
    try:
        from confluent_kafka import Consumer, KafkaException
    except ImportError as exc:
        raise SystemExit('Install optional Kafka client: python -m pip install -e ".[kafka]"') from exc
    consumer = Consumer({'bootstrap.servers': args.bootstrap, 'group.id': args.group,
                         'auto.offset.reset': 'earliest', 'enable.auto.commit': False})
    consumer.subscribe([args.topic])
    received = []
    idle = 0
    try:
        while len(received) < args.max_messages and idle < args.idle_polls:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                idle += 1
                continue
            if msg.error():
                raise KafkaException(msg.error())
            idle = 0
            received.append(Delivery(topic=msg.topic(), partition=msg.partition(), offset=msg.offset(),
                                     event=Event.from_dict(json.loads(msg.value()))))
        if not received:
            print(json.dumps({'status': 'NO_MESSAGES'}))
            return
        with StateStore(args.db) as store:
            outcome = store.apply(branch=args.branch, batch_id=args.batch_id,
                                  deliveries=received, policy=args.policy)
        consumer.commit(asynchronous=False)
        print(json.dumps(outcome, indent=2, sort_keys=True))
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
