"""JSONL persistence with canonical deterministic representation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from temporalx.model import Event


def save_events(path: str | Path, events: Iterable[Event]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="\n") as handle:
        for event in events:
            handle.write(json.dumps(event.to_dict(), sort_keys=True, separators=(",", ":")) + "\n")


def load_events(path: str | Path) -> list[Event]:
    events = []
    with Path(path).open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                events.append(Event.from_dict(json.loads(line)))
            except (ValueError, KeyError, TypeError) as exc:
                raise ValueError(f"Invalid JSONL event at line {line_number}: {exc}") from exc
    return events
