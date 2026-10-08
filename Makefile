.PHONY: install test demo benchmark
install:
	python -m pip install -e '.[test]'
test:
	python -m pytest -q
demo:
	python -m temporalx generate --count 100 --output artifacts/events.jsonl
	python -m temporalx compare --input artifacts/events.jsonl --output artifacts/comparison.json
benchmark:
	python -m temporalx benchmark --count 10000

archive-demo:
	python -m temporalx archive-ingest --db artifacts/temporalx.db --input examples/demo_events.jsonl --batch-id demo-001
	python -m temporalx archive-compare --db artifacts/temporalx.db --run-id replay-001

sprint03-demo:
	PYTHONPATH=src python scripts/local_stream_demo.py --db artifacts/stateful-s03.db

kafka-up:
	docker compose up -d

kafka-down:
	docker compose down

kafka-topic:
	docker compose exec redpanda rpk topic create temporalx.transactions -p 2

kafka-batch01:
	python scripts/produce_kafka.py --batch 01

kafka-batch02:
	python scripts/produce_kafka.py --batch 02
