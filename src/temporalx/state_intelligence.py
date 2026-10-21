"""Sprint 05: conservative state schema/semantic preflight; never mutates checkpoints.

The gate is intentionally *more conservative* than Spark's native schema evolution:
compatible physical schemas can still change business semantics.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from hashlib import sha256
import json
from typing import Any

VALID_TYPES = {'int', 'long', 'float', 'double', 'string', 'bytes', 'boolean', 'timestamp'}
WIDEN = {
    ('int', 'long'), ('int', 'float'), ('int', 'double'),
    ('long', 'float'), ('long', 'double'), ('float', 'double'),
    ('string', 'bytes'), ('bytes', 'string'),
}


@dataclass(frozen=True)
class Field:
    name: str
    dtype: str
    nullable: bool = True

    def __post_init__(self) -> None:
        if not self.name or self.dtype not in VALID_TYPES:
            raise ValueError('Invalid state field')


@dataclass(frozen=True)
class StateVariable:
    name: str
    kind: str
    fields: tuple[Field, ...]
    ttl_ms: int | None = None

    def __post_init__(self) -> None:
        if self.kind not in {'value', 'map', 'list'} or not self.name:
            raise ValueError('Invalid state variable')
        if len({f.name for f in self.fields}) != len(self.fields) or not self.fields:
            raise ValueError('Duplicate/empty state fields')
        if self.ttl_ms is not None and self.ttl_ms <= 0:
            raise ValueError('TTL must be positive')


@dataclass(frozen=True)
class StateContract:
    source_id: str
    sink_id: str
    group_keys: tuple[str, ...]
    operator_id: str
    logic_revision: str
    time_mode: str
    encoding: str
    variables: tuple[StateVariable, ...]
    shuffle_partitions: int = 2
    watermark_ms: int | None = None

    def __post_init__(self) -> None:
        if not all([self.source_id, self.sink_id, self.operator_id, self.logic_revision]):
            raise ValueError('Missing contract identity')
        if not self.group_keys or len(set(self.group_keys)) != len(self.group_keys):
            raise ValueError('Missing/duplicate group keys')
        if self.time_mode not in {'None', 'ProcessingTime', 'EventTime'}:
            raise ValueError('Unknown time mode')
        if self.encoding not in {'avro', 'unsaferow'}:
            raise ValueError('Unknown state encoding')
        if not 1 <= self.shuffle_partitions <= 1024:
            raise ValueError('Invalid shuffle partitions')
        if self.watermark_ms is not None and self.watermark_ms < 0:
            raise ValueError('Invalid watermark')
        if len({v.name for v in self.variables}) != len(self.variables):
            raise ValueError('Duplicate state variables')
        if self.time_mode != 'ProcessingTime' and any(v.ttl_ms is not None for v in self.variables):
            raise ValueError('TTL requires ProcessingTime')

    def dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def digest(self) -> str:
        return sha256(json.dumps(self.dict(), sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def load_contract(payload: dict[str, Any]) -> StateContract:
    vars_ = tuple(StateVariable(
        name=v['name'], kind=v['kind'],
        fields=tuple(Field(**f) for f in v['fields']), ttl_ms=v.get('ttl_ms'),
    ) for v in payload['variables'])
    return StateContract(**{**payload, 'variables': vars_, 'group_keys': tuple(payload['group_keys'])})


@dataclass(frozen=True)
class UpgradeReport:
    decision: str
    reasons: tuple[str, ...]
    old_digest: str
    new_digest: str
    review_required: bool = True

    def dict(self) -> dict[str, Any]:
        return asdict(self)


def assess_upgrade(old: StateContract, new: StateContract) -> UpgradeReport:
    """Never authorizes deployment; PASS means eligible for explicit review only.

    BLOCK: cannot safely treat as same workload. REBUILD: new checkpoint and full
    archive reconciliation. REVIEW: candidate for resumed checkpoint after testing.
    """
    block: list[str] = []
    rebuild: list[str] = []
    review: list[str] = []
    for name in ('source_id', 'sink_id', 'group_keys', 'operator_id', 'time_mode', 'shuffle_partitions'):
        if getattr(old, name) != getattr(new, name):
            block.append(f'{name} changed: never reuse the old checkpoint')
    if old.watermark_ms != new.watermark_ms:
        rebuild.append('watermark policy changed: historical semantics require replay')
    if old.logic_revision != new.logic_revision:
        rebuild.append('business logic revision changed: schema compatibility is insufficient')
    if old.encoding != new.encoding:
        block.append('state encoding changed: checkpoint cannot be assumed compatible')
    a, b = {v.name: v for v in old.variables}, {v.name: v for v in new.variables}
    for name in sorted(a.keys() - b.keys()):
        rebuild.append(f'state variable {name} removed: prove no historical dependency')
    for name in sorted(b.keys() - a.keys()):
        review.append(f'state variable {name} added; initialize existing groups defensively')
    for name in sorted(a.keys() & b.keys()):
        before, after = a[name], b[name]
        if before.kind != after.kind:
            rebuild.append(f'{name}: state type changed')
        if before.ttl_ms != after.ttl_ms:
            rebuild.append(f'{name}: TTL duration changed; dedup/session behavior may change')
        prev, nxt = {f.name: f for f in before.fields}, {f.name: f for f in after.fields}
        for field in sorted(prev.keys() - nxt.keys()):
            rebuild.append(f'{name}.{field} removed: replay unless migration independently proven')
        for field in sorted(nxt.keys() - prev.keys()):
            if new.encoding != 'avro':
                rebuild.append(f'{name}.{field} added without Avro state encoding')
            else:
                review.append(f'{name}.{field} added: old values are null (None)')
        for field in sorted(prev.keys() & nxt.keys()):
            x, y = prev[field], nxt[field]
            if x.dtype != y.dtype:
                if (x.dtype, y.dtype) in WIDEN and new.encoding == 'avro':
                    review.append(f'{name}.{field} type widened {x.dtype}->{y.dtype}')
                else:
                    rebuild.append(f'{name}.{field} incompatible type {x.dtype}->{y.dtype}')
            if x.nullable and not y.nullable:
                rebuild.append(f'{name}.{field} became nonnullable')
            if not x.nullable and y.nullable:
                review.append(f'{name}.{field} became nullable')
    if block:
        return UpgradeReport('BLOCK', tuple(block + rebuild + review), old.digest, new.digest)
    if rebuild:
        return UpgradeReport('REBUILD_REQUIRED', tuple(rebuild + review), old.digest, new.digest)
    return UpgradeReport('REVIEW_FOR_RESUME', tuple(review or ['identical contract; validate deployment/metadata externally']), old.digest, new.digest)


def incident_diagnosis(progress: list[dict[str, Any]], *, checkpoint_p95_budget_ms: float = 3000,
                       max_stall_ms: float = 500) -> dict[str, Any]:
    """Rule-based advisor based only on observed metrics, without invented zeros.

    Does not invoke an LLM or take corrective actions.
    """
    from temporalx.rocksdb_lab import parse_progress, summary
    rows = parse_progress(progress)
    stats = summary(rows)
    signals: list[dict[str, str]] = []
    if not rows:
        signals.append({'severity': 'unknown', 'signal': 'no state operator observations', 'action': 'collect query progress'})
    p95 = stats['metrics']['checkpoint_ms']['p95']
    if p95 is None:
        signals.append({'severity': 'unknown', 'signal': 'checkpoint latency missing', 'action': 'verify RocksDB metrics enabled'})
    elif p95 > checkpoint_p95_budget_ms:
        signals.append({'severity': 'warning', 'signal': 'checkpoint latency above budget', 'action': 'compare changelog and snapshot recovery in isolated branches'})
    stalls = [r['writer_stall_ms'] for r in rows if r['writer_stall_ms'] is not None]
    if stalls and max(stalls) > max_stall_ms:
        signals.append({'severity': 'warning', 'signal': 'writer stalls observed', 'action': 'inspect SST, compaction, caches and state cardinality'})
    if rows and not any(r['sst_bytes'] is not None for r in rows):
        signals.append({'severity': 'unknown', 'signal': 'SST size absent', 'action': 'verify provider/runtime and custom metrics'})
    return {'observations': len(rows), 'signals': signals, 'summary': stats,
            'authorization': 'NO_AUTOMATIC_RECOVERY_OR_PRODUCTION_CHANGE'}
