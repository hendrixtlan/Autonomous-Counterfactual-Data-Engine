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
