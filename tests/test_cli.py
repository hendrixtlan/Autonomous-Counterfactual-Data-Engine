import json

from temporalx.cli import main
from temporalx.io import load_events


def test_generate_compare_cli(tmp_path, capsys):
    events = tmp_path / "events.jsonl"
    summary = tmp_path / "summary.json"
    assert main(["generate", "--count", "21", "--output", str(events)]) == 0
    assert len(load_events(events)) > 21
    assert main(["compare", "--input", str(events), "--output", str(summary)]) == 0
    comparison = json.loads(summary.read_text())
    assert comparison["reconciled"] is True
    assert comparison["candidate"]["applied_events"] < comparison["baseline"]["applied_events"]
    assert comparison["total_delta_cents"] < 0
    assert '"candidate"' in capsys.readouterr().out
