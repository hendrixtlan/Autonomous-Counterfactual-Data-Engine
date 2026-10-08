"""Local entry point: generate, replay, compare, benchmark."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from time import perf_counter

from temporalx.engine import counterfactual, replay
from temporalx.archive import Archive
from temporalx.generator import generate_events
from temporalx.io import load_events, save_events


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TemporalX Sprint 01 reference oracle")
    subs = parser.add_subparsers(dest="action", required=True)
    gen = subs.add_parser("generate", help="Create fault-injected JSONL events")
    gen.add_argument("--count", type=int, default=100)
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--output", default="artifacts/events.jsonl")
    for action in ("replay", "compare"):
        p = subs.add_parser(action)
        p.add_argument("--input", default="artifacts/events.jsonl")
        p.add_argument("--valid-at", default=None, help="Event-time upper bound, ISO-8601 UTC-offset")
        p.add_argument("--known-at", default=None, help="Knowledge-time upper bound, ISO-8601 UTC-offset")
        p.add_argument("--output", default=None, help="Optional JSON result artifact path")
        if action == "replay":
            p.add_argument("--policy", choices=("v1", "v2"), default="v2")
    bench = subs.add_parser("benchmark")
    bench.add_argument("--count", type=int, default=10000)
    bench.add_argument("--seed", type=int, default=42)
    ing = subs.add_parser("archive-ingest", help="Atomically append a source batch to local SQLite archive")
    ing.add_argument("--db", default="artifacts/temporalx.db")
    ing.add_argument("--input", default="artifacts/events.jsonl")
    ing.add_argument("--batch-id", required=True)
    comp = subs.add_parser("archive-compare", help="Write or verify an immutable replay manifest")
    comp.add_argument("--db", default="artifacts/temporalx.db")
    comp.add_argument("--run-id", required=True)
    comp.add_argument("--known-at")
    comp.add_argument("--valid-at")
    status = subs.add_parser("archive-status", help="Print archive row counts")
    status.add_argument("--db", default="artifacts/temporalx.db")
    args = parser.parse_args(argv)
    if args.action.startswith("archive-"):
        with Archive(args.db) as archive:
            if args.action == "archive-ingest":
                answer = archive.ingest(args.input, args.batch_id).to_dict()
            elif args.action == "archive-compare":
                answer = archive.compare(args.run_id, valid_at=args.valid_at, known_at=args.known_at)
            else:
                answer = archive.status()
        print(json.dumps(answer, indent=2, sort_keys=True))
        return 0
    if args.action == "generate":
        events = generate_events(args.count, args.seed)
        save_events(args.output, events)
        print(json.dumps({"path": args.output, "records": len(events), "seed": args.seed}))
        return 0
    if args.action == "benchmark":
        events = generate_events(args.count, args.seed)
        start = perf_counter()
        result = counterfactual(events)
        elapsed = perf_counter() - start
        print(json.dumps({"transactions_requested": args.count,
                          "physical_messages": len(events),
                          "elapsed_seconds": round(elapsed, 6),
                          "total_delta_cents": result["total_delta_cents"]}))
        return 0
    events = load_events(args.input)
    bounds = {"valid_at": args.valid_at, "known_at": args.known_at}
    if args.action == "replay":
        payload = replay(events, policy=args.policy, **bounds).to_dict()
    else:
        payload = counterfactual(events, **bounds)
    output = json.dumps(payload, indent=2, sort_keys=True)
    if args.output:
        target = Path(args.output)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(output + "\n", encoding="utf-8")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
