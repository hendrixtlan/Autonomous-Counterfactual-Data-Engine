"""Durable local counterpart of the Databricks Delta archive.

SQLite is deliberately used for the *local contract tests*, not described as
Delta Lake or a replacement for Unity Catalog. Every ingest transaction is
atomic, every delivery is preserved, and replay results are independently
anchored to batch and visible-event digests.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from temporalx import __version__
from temporalx.engine import ReplayConflictError, counterfactual
from temporalx.model import Event, parse_instant


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


@dataclass(frozen=True)
class BatchReceipt:
    batch_id: str
    input_sha256: str
    accepted: int
    quarantined: int
    previously_ingested: bool

    def to_dict(self) -> dict:
        return vars(self).copy()


class Archive:
    """Append-only event delivery archive with an auditable replay ledger."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA busy_timeout=5000")
        self._initialize()

    def __enter__(self) -> Archive:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        self.db.close()

    def _initialize(self) -> None:
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS batches (
                batch_id TEXT PRIMARY KEY,
                input_sha256 TEXT NOT NULL,
                accepted INTEGER NOT NULL,
                quarantined INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS deliveries (
                batch_id TEXT NOT NULL REFERENCES batches(batch_id),
                ordinal INTEGER NOT NULL,
                tenant_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                semantic_hash TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                ingested_at TEXT NOT NULL,
                canonical_event TEXT NOT NULL,
                PRIMARY KEY (batch_id, ordinal)
            );
            CREATE INDEX IF NOT EXISTS delivery_identity
              ON deliveries(tenant_id, event_id);
            CREATE TABLE IF NOT EXISTS quarantine (
                batch_id TEXT NOT NULL REFERENCES batches(batch_id),
                ordinal INTEGER NOT NULL,
                raw_line TEXT NOT NULL,
                reason TEXT NOT NULL,
                PRIMARY KEY (batch_id, ordinal)
            );
            CREATE TABLE IF NOT EXISTS replay_runs (
                run_id TEXT PRIMARY KEY,
                evidence_sha256 TEXT NOT NULL,
                evidence_json TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS deliveries_no_update
              BEFORE UPDATE ON deliveries BEGIN SELECT RAISE(ABORT, 'Bronze is append-only'); END;
            CREATE TRIGGER IF NOT EXISTS deliveries_no_delete
              BEFORE DELETE ON deliveries BEGIN SELECT RAISE(ABORT, 'Bronze is append-only'); END;
            CREATE TRIGGER IF NOT EXISTS batches_no_update
              BEFORE UPDATE ON batches BEGIN SELECT RAISE(ABORT, 'Batch evidence is append-only'); END;
            CREATE TRIGGER IF NOT EXISTS batches_no_delete
              BEFORE DELETE ON batches BEGIN SELECT RAISE(ABORT, 'Batch evidence is append-only'); END;
            CREATE TRIGGER IF NOT EXISTS runs_no_update
              BEFORE UPDATE ON replay_runs BEGIN SELECT RAISE(ABORT, 'Replay evidence is append-only'); END;
            CREATE TRIGGER IF NOT EXISTS runs_no_delete
              BEFORE DELETE ON replay_runs BEGIN SELECT RAISE(ABORT, 'Replay evidence is append-only'); END;
        """)

    def ingest(self, path: str | Path, batch_id: str) -> BatchReceipt:
        if not batch_id or len(batch_id) > 128 or not all(
            c.isascii() and (c.isalnum() or c in '-_.') for c in batch_id
        ):
            raise ValueError("batch_id must use 1-128 ASCII letters/digits/-_. characters")
        payload = Path(path).read_bytes()
        source_sha = digest(payload)
        parsed: list[tuple[int, Event]] = []
        rejected: list[tuple[int, str, str]] = []
        for ordinal, raw in enumerate(payload.decode("utf-8").splitlines(), start=1):
            if not raw.strip():
                continue
            try:
                record = json.loads(raw)
                if not isinstance(record, dict):
                    raise ValueError("event must be a JSON object")
                parsed.append((ordinal, Event.from_dict(record)))
            except (ValueError, TypeError) as exc:
                rejected.append((ordinal, raw, str(exc)))
        new_hashes: dict[tuple[str, str], str] = {}
        for _, ev in parsed:
            key = (ev.tenant_id, ev.event_id)
            # Schema version and arrival time are not business identity fields.
            semantic = {
                "tenant_id": ev.tenant_id, "event_id": ev.event_id,
                "transaction_id": ev.transaction_id, "account_id": ev.account_id,
                "kind": ev.kind, "amount_cents": ev.amount_cents,
                "occurred_at": ev.occurred_at.isoformat(), "currency": ev.currency,
            }
            fp = digest(canonical(semantic).encode("utf-8"))
            if key in new_hashes and new_hashes[key] != fp:
                raise ReplayConflictError(f"Conflicting event identity inside batch: {key}")
            new_hashes[key] = fp
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            old = self.db.execute(
                "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
            ).fetchone()
            if old:
                if old["input_sha256"] != source_sha:
                    raise ReplayConflictError("batch_id reused with different source bytes")
                return BatchReceipt(batch_id, source_sha, old["accepted"], old["quarantined"], True)
            if new_hashes:
                for row in self.db.execute(
                    "SELECT DISTINCT tenant_id,event_id,semantic_hash FROM deliveries"
                ):
                    key = (row["tenant_id"], row["event_id"])
                    if key in new_hashes and new_hashes[key] != row["semantic_hash"]:
                        raise ReplayConflictError(f"Conflicting archived identity: {key}")
            self.db.execute(
                "INSERT INTO batches VALUES (?,?,?,?)",
                (batch_id, source_sha, len(parsed), len(rejected)),
            )
            self.db.executemany(
                "INSERT INTO deliveries VALUES (?,?,?,?,?,?,?,?)",
                [(batch_id, ordinal, ev.tenant_id, ev.event_id,
                  new_hashes[(ev.tenant_id, ev.event_id)],
                  ev.occurred_at.isoformat(), ev.ingested_at.isoformat(),
                  canonical(ev.to_dict())) for ordinal, ev in parsed],
            )
            self.db.executemany(
                "INSERT INTO quarantine VALUES (?,?,?,?)",
                [(batch_id, *item) for item in rejected],
            )
        return BatchReceipt(batch_id, source_sha, len(parsed), len(rejected), False)

    def status(self) -> dict:
        def count(table: str) -> int:
            return self.db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        return {"batches": count("batches"), "deliveries": count("deliveries"),
                "quarantine": count("quarantine"), "replay_runs": count("replay_runs")}

    def snapshot(self, *, valid_at: str | None = None, known_at: str | None = None) -> list[Event]:
        valid = parse_instant(valid_at) if valid_at else None
        known = parse_instant(known_at) if known_at else None
        events = (Event.from_dict(json.loads(row["canonical_event"])) for row in
                  self.db.execute("SELECT canonical_event FROM deliveries ORDER BY batch_id, ordinal"))
        return [ev for ev in events if (valid is None or ev.occurred_at <= valid)
                and (known is None or ev.ingested_at <= known)]

    def compare(self, run_id: str, *, valid_at: str | None = None, known_at: str | None = None) -> dict:
        if not run_id or len(run_id) > 128:
            raise ValueError("run_id must have 1-128 characters")
        # Snapshot is materialized before comparison. Read-only within a single
        # SQLite transaction, so an ingest cannot change it mid-evaluation.
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            visible = self.snapshot(valid_at=valid_at, known_at=known_at)
            comparison = counterfactual(visible, valid_at=valid_at, known_at=known_at)
            batches = [dict(row) for row in self.db.execute(
                "SELECT batch_id,input_sha256,accepted,quarantined FROM batches ORDER BY batch_id"
            )]
            delivered = sorted(canonical(ev.to_dict()) for ev in visible)
            evidence = {
                "run_id": run_id,
                "engine_version": __version__,
                "source_batches": batches,
                "snapshot_sha256": digest("\n".join(delivered).encode("utf-8")),
                "comparison": comparison,
            }
            evidence_json = canonical(evidence)
            evidence_hash = digest(evidence_json.encode("utf-8"))
            row = self.db.execute(
                "SELECT evidence_sha256 FROM replay_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if row and row["evidence_sha256"] != evidence_hash:
                raise ReplayConflictError("run_id reused with changed replay evidence")
            if not row:
                self.db.execute(
                    "INSERT INTO replay_runs VALUES (?,?,?)", (run_id, evidence_hash, evidence_json)
                )
        return {**evidence, "evidence_sha256": evidence_hash}
