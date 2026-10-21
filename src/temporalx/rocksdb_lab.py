"""Portable RocksDB experiment contracts and Spark progress analytics.

No native RocksDB operations are claimed by this module. Spark is the owner of
RocksDB; this module validates experiments and analyzes real Spark progress JSON.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
import os
from pathlib import Path
import re
from statistics import mean
from typing import Any, Iterable

_BRANCH = re.compile(r"^[a-z][a-z0-9_]{0,39}$")
_PROVIDER = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"


class CheckpointContractError(ValueError):
    """Preflight protection against incompatible reuse of a streaming checkpoint."""


@dataclass(frozen=True)
class Experiment:
    branch: str
    strategy: str = "changelog"
    watermark: str = "10 minutes"
    operator_revision: str = "s04-dedup-v1"
    schema_revision: int = 1
    shuffle_partitions: int = 2
    max_memory_mb: int = 512
    block_cache_mb: int = 32
    write_buffer_mb: int = 32
    compact_on_commit: bool = False
    track_total_rows: bool = True
    source_identity: str = "synthetic_json_v1"
    sink_identity: str = "atomic_batch_files_v1"
    provider: str = _PROVIDER

    def __post_init__(self) -> None:
        if not _BRANCH.fullmatch(self.branch):
            raise ValueError("Unsafe branch name; use lowercase letters, digits or underscore")
        if self.strategy not in {"changelog", "snapshot"}:
            raise ValueError("strategy must be changelog or snapshot")
        if self.watermark != "10 minutes":
            raise ValueError("This experiment uses a fixed 10-minute watermark")
        if self.schema_revision != 1 or self.operator_revision != "s04-dedup-v1":
            raise ValueError("Unsupported state schema/operator revision; create another experiment")
        if not (1 <= self.shuffle_partitions <= 128):
            raise ValueError("shuffle_partitions must be between 1 and 128")
        if not (128 <= self.max_memory_mb <= 65536):
            raise ValueError("max_memory_mb must be between 128 and 65536")
        if not (4 <= self.block_cache_mb <= 4096 and 4 <= self.write_buffer_mb <= 4096):
            raise ValueError("cache and write buffer must be 4..4096 MB")
        if not self.source_identity or not self.sink_identity:
            raise ValueError("source and sink identities are required")
        if self.provider != _PROVIDER:
            raise ValueError("RocksDB state store provider is fixed")

    def canonical(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))

    @property
    def fingerprint(self) -> str:
        return sha256(self.canonical().encode()).hexdigest()

    def spark_config(self) -> dict[str, str]:
        return {
            "spark.sql.streaming.stateStore.providerClass": self.provider,
            "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled": str(self.strategy == "changelog").lower(),
            "spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage": "true",
            "spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB": str(self.max_memory_mb),
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.sql.streaming.stateStore.rocksdb.blockCacheSizeMB": str(self.block_cache_mb),
            "spark.sql.streaming.stateStore.rocksdb.writeBufferSizeMB": str(self.write_buffer_mb),
            "spark.sql.streaming.stateStore.rocksdb.compactOnCommit": str(self.compact_on_commit).lower(),
            "spark.sql.streaming.stateStore.rocksdb.trackTotalNumberOfRows": str(self.track_total_rows).lower(),
        }


def ensure_local_contract(root: str | Path, experiment: Experiment) -> Path:
    """Create immutable local manifest atomically, or reject drift before Spark starts.

    For a shared cloud checkpoint, use a transactional registry with single-writer
    enforcement; this local filesystem primitive does not supply distributed CAS.
    """
    path = Path(root).resolve() / "_contracts" / f"{experiment.branch}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (experiment.canonical() + "\n").encode()
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if path.read_bytes() != payload:
            raise CheckpointContractError(f"Incompatible experiment contract for {experiment.branch}; use a NEW branch")
        return path
    with os.fdopen(fd, "wb") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    return path


def _number(v: Any) -> float | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    return None


def parse_progress(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize state operator custom metrics from multiple Spark versions.

    Some RocksDB metrics change names across Databricks Runtime releases.
    Missing measurements remain null, never silently zeroed.
    """
    out = []
    for entry in records:
        if not isinstance(entry, dict):
            raise ValueError("Spark query progress must be JSON objects")
        state = entry.get("stateOperators") or []
        if not isinstance(state, list):
            raise ValueError("stateOperators must be an array")
        for index, operator in enumerate(state):
            custom = operator.get("customMetrics") or {}
            if not isinstance(custom, dict):
                raise ValueError("customMetrics must be an object")
            hit = _number(custom.get("rocksdbReadBlockCacheHitCount"))
            miss = _number(custom.get("rocksdbReadBlockCacheMissCount"))
            ratio = hit / (hit + miss) if hit is not None and miss is not None and hit + miss > 0 else None
            out.append({
                "batch_id": entry.get("batchId"),
                "run_id": entry.get("runId"),
                "operator_index": index,
                "operator": operator.get("operatorName"),
                "input_rows": entry.get("numInputRows"),
                "processed_rows_per_second": _number(entry.get("processedRowsPerSecond")),
                "trigger_ms": _number((entry.get("durationMs") or {}).get("triggerExecution")),
                "state_rows": _number(operator.get("numRowsTotal")),
                "state_updated": _number(operator.get("numRowsUpdated")),
                "state_removed": _number(operator.get("numRowsRemoved")),
                "dropped_by_watermark": _number(operator.get("numRowsDroppedByWatermark")),
                "state_memory_bytes": _number(operator.get("memoryUsedBytes")),
                "dedup_dropped": _number(custom.get("numDroppedDuplicateRows")),
                "sst_bytes": _number(custom.get("rocksdbSstFileSize")),
                "checkpoint_ms": _number(custom.get("rocksdbCommitCheckpointLatency")),
                "flush_ms": _number(custom.get("rocksdbCommitFlushLatency")),
                "compaction_ms": _number(custom.get("rocksdbTotalCompactionLatencyMs")),
                "writer_stall_ms": _number(custom.get("rocksdbWriterStallLatencyMs")),
                "block_cache_hit_ratio": ratio,
                "bytes_read_by_compaction": _number(custom.get("rocksdbTotalBytesReadByCompaction")),
                "bytes_written_by_compaction": _number(custom.get("rocksdbTotalBytesWrittenByCompaction")),
            })
    return out


def _quantile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    vals = sorted(values)
    pos = (len(vals) - 1) * quantile
    low, high = int(pos), min(int(pos) + 1, len(vals) - 1)
    return round(vals[low] + (vals[high] - vals[low]) * (pos - low), 3)


def summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    fields = ["trigger_ms", "checkpoint_ms", "writer_stall_ms", "sst_bytes", "state_rows", "block_cache_hit_ratio"]
    result = {"operator_observations": len(rows), "metrics": {}}
    for field in fields:
        values = [float(r[field]) for r in rows if r.get(field) is not None]
        result["metrics"][field] = {"samples": len(values), "mean": round(mean(values), 3) if values else None,
                                    "p50": _quantile(values, .5), "p95": _quantile(values, .95), "max": max(values) if values else None}
    result["warnings"] = []
    if not rows:
        result["warnings"].append("No stateOperators observations: query may not have a stateful operator")
    if rows and not any(r.get("sst_bytes") is not None for r in rows):
        result["warnings"].append("No RocksDB SST metric: inspect state store provider and runtime support")
    return result


def load_progress(path: str | Path) -> list[dict[str, Any]]:
    """Accept a JSON array, single object or JSONL file."""
    raw = Path(path).read_text(encoding="utf-8").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return [parsed]
        if isinstance(parsed, list):
            return parsed
        raise ValueError("Expected JSON objects")
    except json.JSONDecodeError:
        return [json.loads(line) for line in raw.splitlines() if line.strip()]
