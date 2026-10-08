"""SQLite reference for atomic microbatch replay with explicit event-time watermark.

This deliberately models a bounded research subset, NOT Spark's entire watermark
and state-store semantics. Batches advance max(event_time) after processing using
only the watermark established by previously committed microbatches.

No cross-branch state is ever reused. This module has no Spark/Kafka dependency.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from temporalx.engine import ReplayConflictError
from temporalx.model import Event


class CheckpointIncompatibleError(ValueError):
    """Attempted restart would reinterpret persisted state under a new contract."""


class SourceOffsetCollisionError(ValueError):
    """One physical source offset has incompatible recorded bytes."""


@dataclass(frozen=True)
class Delivery:
    partition: int
    offset: int
    event: Event
    topic: str = "temporalx.transactions"

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "Delivery":
        partition, offset, topic = obj["partition"], obj["offset"], obj.get("topic", "temporalx.transactions")
        if type(partition) is not int or partition < 0 or type(offset) is not int or offset < 0:
            raise ValueError("partition and offset must be nonnegative integers")
        if not isinstance(topic, str) or not topic.strip():
            raise ValueError("topic must be a nonempty string")
        return cls(partition=partition, offset=offset, topic=topic, event=Event.from_dict(obj["event"]))

    def payload(self) -> dict[str, Any]:
        return {"topic": self.topic, "partition": self.partition, "offset": self.offset, "event": self.event.to_dict()}


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def load_deliveries(path: str | Path) -> list[Delivery]:
    result = []
    for n, line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            if not isinstance(obj, dict):
                raise ValueError("not a JSON object")
            result.append(Delivery.from_dict(obj))
        except (ValueError, TypeError, KeyError) as exc:
            raise ValueError(f"Invalid delivery on line {n}: {exc}") from exc
    return result


class StateStore:
    """ACID checkpoint of a toy operator. Single-writer, non-production prototype."""

    def __init__(self, path: str | Path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA busy_timeout = 5000")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS checkpoints (
            branch TEXT PRIMARY KEY, signature TEXT NOT NULL,
            max_event_time TEXT, created_at TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS batches (
            branch TEXT NOT NULL, batch_id TEXT NOT NULL, input_sha TEXT NOT NULL,
            source_count INTEGER NOT NULL, applied INTEGER NOT NULL,
            deduplicated INTEGER NOT NULL, late INTEGER NOT NULL,
            repeated_offsets INTEGER NOT NULL,
            PRIMARY KEY(branch,batch_id));
          CREATE TABLE IF NOT EXISTS source_offsets (
            branch TEXT NOT NULL, topic TEXT NOT NULL, partition_id INTEGER NOT NULL,
            offset_id INTEGER NOT NULL, delivery_sha TEXT NOT NULL,
            PRIMARY KEY(branch,topic,partition_id,offset_id));
          CREATE TABLE IF NOT EXISTS logical_events (
            branch TEXT NOT NULL, tenant TEXT NOT NULL, event_id TEXT NOT NULL,
            event_sha TEXT NOT NULL,
            PRIMARY KEY(branch,tenant,event_id));
          CREATE TABLE IF NOT EXISTS windows (
            branch TEXT NOT NULL, tenant TEXT NOT NULL, account TEXT NOT NULL,
            start_utc TEXT NOT NULL, net_cents INTEGER NOT NULL,
            PRIMARY KEY(branch,tenant,account,start_utc));
          CREATE TABLE IF NOT EXISTS late_events (
            branch TEXT NOT NULL, topic TEXT NOT NULL, partition_id INTEGER NOT NULL,
            offset_id INTEGER NOT NULL, tenant TEXT NOT NULL, event_id TEXT NOT NULL,
            occurred_at TEXT NOT NULL, watermark_at TEXT NOT NULL,
            PRIMARY KEY(branch,topic,partition_id,offset_id));
        """)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.db.close()

    def apply(
        self,
        *,
        branch: str,
        batch_id: str,
        deliveries: Iterable[Delivery],
        policy: str = "v2",
        allowed_lateness_seconds: int = 300,
        window_seconds: int = 300,
        replay_mode: str = "watermarked",
        operator_revision: str = "window-aggregator-r1",
        crash_after_source_offsets: int | None = None,  # test-only fault injection
    ) -> dict[str, Any]:
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", branch) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", batch_id):
            raise ValueError("branch and batch_id require safe non-empty identifiers")
        if policy not in {"v1", "v2"} or replay_mode not in {"watermarked", "backfill"}:
            raise ValueError("invalid policy or replay_mode")
        if type(allowed_lateness_seconds) is not int or allowed_lateness_seconds < 0:
            raise ValueError("allowed_lateness_seconds must be a nonnegative integer")
        if type(window_seconds) is not int or window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if not operator_revision.strip():
            raise ValueError("operator_revision must not be empty")
        if crash_after_source_offsets is not None and (
            type(crash_after_source_offsets) is not int or crash_after_source_offsets <= 0
        ):
            raise ValueError("fault injection offset must be positive")
        incoming = tuple(deliveries)
        content_hash = digest([d.payload() for d in incoming])
        signature = digest({"policy": policy, "allowed_lateness_seconds": allowed_lateness_seconds,
                            "window_seconds": window_seconds, "replay_mode": replay_mode,
                            "operator_revision": operator_revision, "state_keys": ["tenant", "account", "window_start"]})
        cur = self.db.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            old = cur.execute("SELECT signature,max_event_time FROM checkpoints WHERE branch=?", (branch,)).fetchone()
            if old is not None and old["signature"] != signature:
                raise CheckpointIncompatibleError("Checkpoint contract changed; use a NEW branch, never reuse the old checkpoint")
            if old is None:
                cur.execute("INSERT INTO checkpoints VALUES (?,?,?,?)", (branch, signature, None, datetime.now(timezone.utc).isoformat()))
            earlier = cur.execute("SELECT input_sha FROM batches WHERE branch=? AND batch_id=?", (branch, batch_id)).fetchone()
            if earlier is not None:
                if earlier["input_sha"] != content_hash:
                    raise SourceOffsetCollisionError("batch_id reused with different source content")
                cur.execute("COMMIT")
                return {"branch": branch, "batch_id": batch_id, "status": "ALREADY_COMMITTED", "state": self.snapshot(branch)}
            max_before = datetime.fromisoformat(old["max_event_time"]) if old and old["max_event_time"] else None
            watermark = max_before - timedelta(seconds=allowed_lateness_seconds) if max_before else None
            applied = duplicates = late = repeated = 0
            max_seen = max_before
            for delivery_index, d in enumerate(incoming, 1):
                event = d.event
                # This physical offset can be replayed in a new branch but not
                # rewritten under the same branch; offsets are source coordinates.
                physical_sha = digest(d.payload())
                prior = cur.execute("SELECT delivery_sha FROM source_offsets WHERE branch=? AND topic=? AND partition_id=? AND offset_id=?",
                                    (branch, d.topic, d.partition, d.offset)).fetchone()
                if prior:
                    if prior["delivery_sha"] != physical_sha:
                        raise SourceOffsetCollisionError(f"Offset mutated: {d.topic}/{d.partition}/{d.offset}")
                    repeated += 1
                    continue
                cur.execute("INSERT INTO source_offsets VALUES (?,?,?,?,?)", (branch, d.topic, d.partition, d.offset, physical_sha))
                if delivery_index == crash_after_source_offsets:
                    raise RuntimeError("SIMULATED_CRASH_AFTER_SOURCE_OFFSET")
                max_seen = max(event.occurred_at, max_seen) if max_seen else event.occurred_at
                key = (branch, event.tenant_id, event.event_id)
                logical_sha = digest({k: v for k, v in event.to_dict().items() if k not in ("ingested_at", "schema_version")})
                prev_event = cur.execute("SELECT event_sha FROM logical_events WHERE branch=? AND tenant=? AND event_id=?", key).fetchone()
                if prev_event:
                    if prev_event["event_sha"] != logical_sha:
                        raise ReplayConflictError(f"Conflicting logical identity {key}")
                    duplicates += 1
                    if policy == "v2":
                        continue
                if replay_mode == "watermarked" and watermark is not None and event.occurred_at < watermark:
                    late += 1
                    cur.execute("INSERT INTO late_events VALUES (?,?,?,?,?,?,?,?)",
                                (branch, d.topic, d.partition, d.offset, event.tenant_id, event.event_id,
                                 event.occurred_at.isoformat(), watermark.isoformat()))
                    continue
                if prev_event is None:
                    cur.execute("INSERT INTO logical_events VALUES (?,?,?,?)", (*key, logical_sha))
                start_epoch = int(event.occurred_at.timestamp()) // window_seconds * window_seconds
                window_start = datetime.fromtimestamp(start_epoch, timezone.utc).isoformat()
                change = event.amount_cents if event.kind == "CAPTURE" else -event.amount_cents
                cur.execute("""INSERT INTO windows VALUES (?,?,?,?,?)
                               ON CONFLICT(branch,tenant,account,start_utc)
                               DO UPDATE SET net_cents=net_cents+excluded.net_cents""",
                            (branch, event.tenant_id, event.account_id, window_start, change))
                applied += 1
            cur.execute("UPDATE checkpoints SET max_event_time=? WHERE branch=?", (max_seen.isoformat() if max_seen else None, branch))
            cur.execute("INSERT INTO batches VALUES (?,?,?,?,?,?,?,?)",
                        (branch, batch_id, content_hash, len(incoming), applied, duplicates, late, repeated))
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
        return {"branch": branch, "batch_id": batch_id, "status": "COMMITTED", "watermark_before": watermark.isoformat() if watermark else None,
                "applied": applied, "duplicates": duplicates, "late": late, "repeated_offsets": repeated,
                "state": self.snapshot(branch)}

    def snapshot(self, branch: str) -> dict[str, Any]:
        record = self.db.execute("SELECT signature,max_event_time FROM checkpoints WHERE branch=?", (branch,)).fetchone()
        if record is None:
            raise ValueError(f"Branch does not exist: {branch}")
        windows = [dict(row) for row in self.db.execute(
            "SELECT tenant,account,start_utc,net_cents FROM windows WHERE branch=? ORDER BY tenant,account,start_utc", (branch,))]
        metrics = self.db.execute("SELECT COALESCE(SUM(applied),0) applied, COALESCE(SUM(deduplicated),0) deduplicated, "
                                  "COALESCE(SUM(late),0) late, COALESCE(SUM(repeated_offsets),0) repeated_offsets, COUNT(*) batches "
                                  "FROM batches WHERE branch=?", (branch,)).fetchone()
        counts = dict(metrics)
        receipts = [dict(row) for row in self.db.execute(
            "SELECT batch_id,input_sha,source_count FROM batches WHERE branch=? ORDER BY batch_id", (branch,))]
        offsets = [dict(row) for row in self.db.execute(
            "SELECT topic,partition_id,offset_id,delivery_sha FROM source_offsets WHERE branch=? ORDER BY topic,partition_id,offset_id", (branch,))]
        counts["logical_identity_count"] = self.db.execute("SELECT COUNT(*) FROM logical_events WHERE branch=?", (branch,)).fetchone()[0]
        totals: dict[str, int] = {}
        for w in windows:
            key = json.dumps([w["tenant"], w["account"]], separators=(",", ":"))
            totals[key] = totals.get(key, 0) + w["net_cents"]
        evidence = {"branch": branch, "signature": record["signature"], "max_event_time": record["max_event_time"],
                    "windows": windows, "totals": totals, "counters": counts, "batch_receipts": receipts, "source_sha256": digest(offsets)}
        return {**evidence, "evidence_sha256": digest(evidence)}


def demo(db: str | Path, folder: str | Path) -> dict[str, Any]:
    folder = Path(folder)
    first = load_deliveries(folder / "batch01.jsonl")
    second = load_deliveries(folder / "batch02.jsonl")
    with StateStore(db) as state:
        for policy in ("v1", "v2"):
            state.apply(branch=f"stream-{policy}", batch_id="01", deliveries=first, policy=policy)
            state.apply(branch=f"stream-{policy}", batch_id="02", deliveries=second, policy=policy)
        # Branch off from immutable deliveries; this is NOT checkpoint reuse.
        state.apply(branch="backfill-v2", batch_id="rebuild-all", deliveries=[*first, *second],
                    policy="v2", replay_mode="backfill")
        return {name: state.snapshot(name) for name in ("stream-v1", "stream-v2", "backfill-v2")}
