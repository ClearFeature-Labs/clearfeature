"""Batch job status store: operational per-job / per-chunk state.

Holds the lifecycle of a batch job and each of its chunks so
``GET /v1/batch/jobs/{job_id}`` can report progress. Like ``RequestStatusStore`` this is
**operational observability state, not the system of record** — best-effort, short-lived
(TTL), and it never holds feature values or raw payloads (only counts + bounded error
strings). Derived counts (completed/failed chunks + failed items) are computed from the
**per-chunk state**, so replaying the same chunk sets its state again rather than
incrementing a counter (no double-counting).

``InMemoryBatchJobStatusStore`` is for memory mode / tests; ``ValkeyBatchJobStatusStore``
mirrors ``ValkeyRequestStatusStore`` for the multi-process topology.
"""

from __future__ import annotations

import dataclasses
import json
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

_TERMINAL_CHUNK = ("completed", "completed_with_errors", "failed")


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"{name} must be timezone-aware")


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


@dataclass(frozen=True)
class BatchChunkStatus:
    chunk_id: str
    chunk_index: int
    status: str  # accepted | running | completed | completed_with_errors | failed
    item_count: int
    updated_at: datetime
    ok_items: int = 0
    failed_items: int = 0
    first_errors: list[str] = field(default_factory=list)
    finished_at: datetime | None = None

    def __post_init__(self) -> None:
        _require_aware(self.updated_at, "updated_at")
        if self.finished_at is not None:
            _require_aware(self.finished_at, "finished_at")

    def to_dict(self) -> dict:
        return {
            "chunk_id": self.chunk_id,
            "chunk_index": self.chunk_index,
            "status": self.status,
            "item_count": self.item_count,
            "ok_items": self.ok_items,
            "failed_items": self.failed_items,
            "first_errors": list(self.first_errors),
            "updated_at": _iso(self.updated_at),
            "finished_at": _iso(self.finished_at),
        }

    @classmethod
    def from_dict(cls, data: dict) -> BatchChunkStatus:
        return cls(
            chunk_id=data["chunk_id"],
            chunk_index=data["chunk_index"],
            status=data["status"],
            item_count=data["item_count"],
            updated_at=_parse_dt(data["updated_at"]),
            ok_items=data.get("ok_items", 0),
            failed_items=data.get("failed_items", 0),
            first_errors=list(data.get("first_errors", [])),
            finished_at=_parse_dt(data.get("finished_at")),
        )


@dataclass(frozen=True)
class BatchJobStatus:
    job_id: str
    status: str  # accepted|running|completed|completed_with_errors|failed|publish_failed
    view: str
    view_version: int
    created_at: datetime
    updated_at: datetime
    total_items: int
    chunk_count: int
    write_online: bool
    requested_features: list[str] = field(default_factory=list)
    requested_feature_groups: list[str] = field(default_factory=list)
    chunks: dict[str, BatchChunkStatus] = field(default_factory=dict)
    finished_at: datetime | None = None
    error_summary: dict = field(default_factory=dict)
    manifest_id: str | None = None  # set for dataset-scoped jobs

    def __post_init__(self) -> None:
        _require_aware(self.created_at, "created_at")
        _require_aware(self.updated_at, "updated_at")
        if self.finished_at is not None:
            _require_aware(self.finished_at, "finished_at")

    # Derived-from-per-chunk-state counts (replay-safe; never raw counters).
    def completed_chunks(self) -> int:
        return sum(
            1 for c in self.chunks.values()
            if c.status in ("completed", "completed_with_errors")
        )

    def failed_chunks(self) -> int:
        return sum(1 for c in self.chunks.values() if c.status == "failed")

    def failed_items(self) -> int:
        return sum(c.failed_items for c in self.chunks.values())

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "view": self.view,
            "view_version": self.view_version,
            "requested_features": list(self.requested_features),
            "requested_feature_groups": list(self.requested_feature_groups),
            "total_items": self.total_items,
            "chunk_count": self.chunk_count,
            "write_online": self.write_online,
            "completed_chunks": self.completed_chunks(),
            "failed_chunks": self.failed_chunks(),
            "failed_items": self.failed_items(),
            "chunks": {cid: c.to_dict() for cid, c in self.chunks.items()},
            "created_at": _iso(self.created_at),
            "updated_at": _iso(self.updated_at),
            "finished_at": _iso(self.finished_at),
            "error_summary": dict(self.error_summary),
            "manifest_id": self.manifest_id,
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> BatchJobStatus:
        return cls(
            job_id=data["job_id"],
            status=data["status"],
            view=data["view"],
            view_version=data["view_version"],
            created_at=_parse_dt(data["created_at"]),
            updated_at=_parse_dt(data["updated_at"]),
            total_items=data["total_items"],
            chunk_count=data["chunk_count"],
            write_online=data["write_online"],
            requested_features=list(data.get("requested_features", [])),
            requested_feature_groups=list(data.get("requested_feature_groups", [])),
            chunks={
                cid: BatchChunkStatus.from_dict(c)
                for cid, c in data.get("chunks", {}).items()
            },
            finished_at=_parse_dt(data.get("finished_at")),
            error_summary=dict(data.get("error_summary", {})),
            manifest_id=data.get("manifest_id"),
        )

    @classmethod
    def from_json(cls, raw: bytes | str) -> BatchJobStatus:
        return cls.from_dict(json.loads(raw))


def _derive_job_status(job: BatchJobStatus) -> str:
    states = [c.status for c in job.chunks.values()]
    if len(states) < job.chunk_count or any(
        s in ("accepted", "running") for s in states
    ):
        if states and all(s == "accepted" for s in states) and len(states) == job.chunk_count:
            return "accepted"
        return "running"
    if all(s == "failed" for s in states):
        return "failed"
    if any(s in ("failed", "completed_with_errors") for s in states):
        return "completed_with_errors"
    return "completed"


def merge_chunk(job: BatchJobStatus, chunk: BatchChunkStatus) -> BatchJobStatus:
    """Set a chunk's state (by chunk_id) and re-derive the job status. Idempotent.

    Job progress derives from unique chunk terminal states, never delivery counts, so a
    duplicate delivery cannot double-count. A terminal chunk never regresses to a
    non-terminal state: with chunk-keyed events  a redelivered chunk
    (crash-before-commit, rebalance window) is reprocessed starting with a late
    ``running`` update, which must not un-finish an already finished chunk.
    """
    existing = job.chunks.get(chunk.chunk_id)
    if (
        existing is not None
        and existing.status in _TERMINAL_CHUNK
        and chunk.status not in _TERMINAL_CHUNK
    ):
        return job
    now = datetime.now(tz=UTC)
    chunks = dict(job.chunks)
    chunks[chunk.chunk_id] = chunk
    merged = dataclasses.replace(job, chunks=chunks, updated_at=now)
    status = _derive_job_status(merged)
    finished_at = now if status in _TERMINAL_CHUNK else None
    return dataclasses.replace(merged, status=status, finished_at=finished_at)


class BatchJobStatusStore(Protocol):
    def put(self, status: BatchJobStatus) -> None: ...

    def get(self, job_id: str) -> BatchJobStatus | None: ...

    def set_chunk(
        self, job_id: str, chunk: BatchChunkStatus
    ) -> BatchJobStatus | None: ...


class InMemoryBatchJobStatusStore:
    """Dict-backed batch status store for memory mode / tests (single process).

    ``set_chunk`` holds a lock so concurrent chunk completions (threads) cannot lose
    each other's updates — the same guarantee the Valkey store gets from WATCH/MULTI.
    """

    def __init__(self) -> None:
        self._items: dict[str, BatchJobStatus] = {}
        self._lock = threading.Lock()

    def put(self, status: BatchJobStatus) -> None:
        self._items[status.job_id] = status

    def get(self, job_id: str) -> BatchJobStatus | None:
        return self._items.get(job_id)

    def set_chunk(self, job_id: str, chunk: BatchChunkStatus) -> BatchJobStatus | None:
        with self._lock:
            existing = self._items.get(job_id)
            if existing is None:
                return None
            merged = merge_chunk(existing, chunk)
            self._items[job_id] = merged
            return merged


def batch_job_key(job_id: str) -> str:
    return f"fs:batch-job:{job_id}"


class ValkeyBatchJobStatusStore:
    """Valkey-backed batch status store (shared across processes), TTL-bounded.

    ``set_chunk`` is an optimistic WATCH/MULTI transaction : with
    chunk-keyed batch events, several workers finish chunks of the SAME job
    concurrently, and a plain read-modify-write would lose updates (a chunk stuck at
    ``accepted`` forever). ``redis``'s ``transaction()`` retries the merge when another
    writer touches the job key between read and write; contention is per-job and brief
    (chunks finish at human timescales), so retries are rare and bounded in practice.
    """

    def __init__(self, client, ttl_s: int) -> None:
        self._client = client
        self._ttl_s = ttl_s

    def put(self, status: BatchJobStatus) -> None:
        self._client.set(batch_job_key(status.job_id), status.to_json(), ex=self._ttl_s)

    def get(self, job_id: str) -> BatchJobStatus | None:
        raw = self._client.get(batch_job_key(job_id))
        if raw is None:
            return None
        return BatchJobStatus.from_json(raw)

    def set_chunk(self, job_id: str, chunk: BatchChunkStatus) -> BatchJobStatus | None:
        key = batch_job_key(job_id)
        result: list[BatchJobStatus | None] = [None]

        def _merge_in_txn(pipe) -> None:
            raw = pipe.get(key)  # runs while WATCHing key (immediate mode)
            if raw is None:
                result[0] = None
                pipe.multi()  # empty transaction; nothing to write
                return
            merged = merge_chunk(BatchJobStatus.from_json(raw), chunk)
            result[0] = merged
            pipe.multi()
            pipe.set(key, merged.to_json(), ex=self._ttl_s)

        # Retries _merge_in_txn (re-reading the job) whenever a concurrent writer
        # invalidates the WATCH, so no chunk update can be lost.
        self._client.transaction(_merge_in_txn, key)
        return result[0]
