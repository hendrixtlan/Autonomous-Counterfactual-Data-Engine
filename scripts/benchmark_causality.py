"""Small, deterministic fault-injection benchmark of incident SIGNAL DETECTION.

The label denotes synthetic fault injection, NOT proven real-world root cause.
This benchmark does not exercise Spark, Kafka, RocksDB or LLMs.
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import product
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import shutil

from temporalx.causality import run_pipeline

BASE = Path(__file__).resolve().parents[1] / 'fixtures/causality/incident-001'


@dataclass(frozen=True)
class Score:
    name: str
    accuracy: float
    tp: int
    fp: int
    tn: int
    fn: int

    def dict(self) -> dict:
        return vars(self).copy()


def assess(truth: list[bool], detected: list[bool], label: str) -> Score:
    if len(truth) != len(detected) or not truth:
        raise ValueError('Mismatched/empty labels')
    tp = sum(a and b for a, b in zip(truth, detected))
    tn = sum(not a and not b for a, b in zip(truth, detected))
    fp = sum(not a and b for a, b in zip(truth, detected))
    fn = sum(a and not b for a, b in zip(truth, detected))
    return Score(label, (tp + tn)/len(truth), tp, fp, tn, fn)


def cases() -> list[dict]:
    results = []
    for dup, late, semantic_change in product((False, True), repeat=3):
        with TemporaryDirectory(prefix='temporalx-causal-bench-') as root:
            folder = Path(root) / 'incident'
            shutil.copytree(BASE, folder)
            rows = [json.loads(s) for s in (folder/'events.jsonl').read_text().splitlines() if s]
            if not dup:
                first = next(x for x in rows if x['event']['event_id'] == 'capture-001')
                removed = False
                filtered = []
                for x in rows:
                    if x['event']['event_id'] == 'capture-001' and x is not first and not removed:
                        removed = True
                        continue
                    filtered.append(x)
                rows = filtered
            if not late:
                for x in rows:
                    if x['event']['event_id'] == 'refund-001':
                        x['event']['ingested_at'] = x['event']['occurred_at']
            (folder/'events.jsonl').write_text(''.join(json.dumps(x, sort_keys=True)+'\n' for x in rows))
            if not semantic_change:
                shutil.copyfile(folder/'original.json', folder/'proposed.json')
            manifest = json.loads((folder/'incident.json').read_text())
            manifest['expected_stream_total_cents'] = None
            (folder/'incident.json').write_text(json.dumps(manifest))
            report = run_pipeline(folder)
            observations = report['observations']
            found = {'duplicate': observations['duplicate_deliveries'] > 0,
                     'late_lag': observations['lag_over_5m'] > 0,
                     'semantic_change': observations['upgrade']['decision'] == 'REBUILD_REQUIRED'}
            results.append({'fault_injection': {'duplicate': dup, 'late_lag': late,
                                               'semantic_change': semantic_change},
                            'observed': found,
                            'recommendation': report['recovery_plan']['recommendation']})
    return results


def main() -> None:
    rows = cases()
    metrics = [assess([r['fault_injection'][name] for r in rows],
                      [r['observed'][name] for r in rows], name).dict()
               for name in ('duplicate', 'late_lag', 'semantic_change')]
    print(json.dumps({'bench_type': 'synthetic-signal-detection-only',
                      'cases': len(rows), 'metrics': metrics}, indent=2))


if __name__ == '__main__':
    main()
