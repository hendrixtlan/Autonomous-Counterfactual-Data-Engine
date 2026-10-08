"""Publish one synthetic fixture batch into a disposable Kafka-compatible topic.

Only designed for plaintext, localhost research use. Kafka assigns REAL offsets;
the JSONL synthetic offsets are reference coordinates, not sent as source offsets.
"""
import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap', default='localhost:19092')
    parser.add_argument('--topic', default='temporalx.transactions')
    parser.add_argument('--batch', choices=['01', '02'], required=True)
    parser.add_argument('--fixture-root', default=str(Path(__file__).resolve().parents[1] / 'fixtures/streaming'))
    args = parser.parse_args()
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise SystemExit('Install optional Kafka client: python -m pip install -e ".[kafka]"') from exc
    if args.bootstrap != 'localhost:19092' and not args.bootstrap.startswith('127.0.0.1:'):
        raise SystemExit('Synthetic producer is intentionally locked to local plaintext Kafka')
    producer = Producer({'bootstrap.servers': args.bootstrap, 'enable.idempotence': True,
                         'acks': 'all', 'message.timeout.ms': 10000})
    messages = [json.loads(line) for line in (Path(args.fixture_root) / f'batch{args.batch}.jsonl').read_text().splitlines() if line.strip()]
    failures = []
    def delivered(err, msg):
        if err:
            failures.append(str(err))
    for envelope in messages:
        e = envelope['event']
        producer.produce(args.topic, key=f"{e['tenant_id']}:{e['transaction_id']}".encode(),
                         value=json.dumps(e, sort_keys=True).encode(),
                         headers={'fixture_batch': args.batch}, callback=delivered)
        producer.poll(0)
    remaining = producer.flush(20)
    if remaining or failures:
        raise SystemExit(f'Kafka delivery incomplete: outstanding={remaining}, failures={failures}')
    print(json.dumps({'batch': args.batch, 'produced': len(messages), 'topic': args.topic}))


if __name__ == '__main__':
    main()
