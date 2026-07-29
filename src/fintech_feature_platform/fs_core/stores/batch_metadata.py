"""Durable batch job/chunk metadata store (async Postgres audit projection,).

The Metadata Writer projects ``BatchChunkRequested`` (accepted/requested) and
``BatchChunkProcessed`` (completion) into ``batch_jobs`` / ``batch_chunks``. This is a
durable audit projection — **not** the operational status path (that stays in
``BatchJobStatusStore``). It stores counts + bounded error strings only: never feature
values, ``inline_sources``, raw payloads, or object keys.

Rules: idempotent + replay-safe + order-independent + non-regressing.
* A requested replay never downgrades a processed chunk or a terminal job.
* A processed event is authoritative for a chunk's result.
* An identical processed replay is a no-op; a **different** terminal processed result for
  the same chunk_id raises ``BatchMetadataConflictError`` (no last-write-wins).
Job status is derived from the durable chunk rows.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

_TERMINAL = ("completed", "completed_with_errors", "failed")


class BatchMetadataConflictError(Exception):
    """Raised when a chunk_id is re-processed with a different terminal result."""


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"{name} must be timezone-aware")


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


@dataclass(frozen=True)
class BatchChunkRecord:
    chunk_id: str
    job_id: str
    chunk_index: int
    chunk_count: int
    status: str
    item_count: int
    created_at: datetime
    updated_at: datetime
    ok_items: int = 0
    failed_items: int = 0
    first_errors: list[str] = field(default_factory=list)
    finished_at: datetime | None = None

    def __post_init__(self) -> None:
        _require_aware(self.created_at, "created_at")
        _require_aware(self.updated_at, "updated_at")
        if self.finished_at is not None:
            _require_aware(self.finished_at, "finished_at")

    def to_dict(self) -> dict:
        return {
            "chunk_id": self.chunk_id,
            "job_id": self.job_id,
            "chunk_index": self.chunk_index,
            "chunk_count": self.chunk_count,
            "status": self.status,
            "item_count": self.item_count,
            "ok_items": self.ok_items,
            "failed_items": self.failed_items,
            "first_errors": list(self.first_errors),
            "created_at": _iso(self.created_at),
            "updated_at": _iso(self.updated_at),
            "finished_at": _iso(self.finished_at),
        }

    @classmethod
    def from_dict(cls, data: dict) -> BatchChunkRecord:
        return cls(
            chunk_id=data["chunk_id"],
            job_id=data["job_id"],
            chunk_index=data["chunk_index"],
            chunk_count=data["chunk_count"],
            status=data["status"],
            item_count=data["item_count"],
            created_at=_parse_dt(data["created_at"]),
            updated_at=_parse_dt(data["updated_at"]),
            ok_items=data.get("ok_items", 0),
            failed_items=data.get("failed_items", 0),
            first_errors=list(data.get("first_errors", [])),
            finished_at=_parse_dt(data.get("finished_at")),
        )


@dataclass(frozen=True)
class BatchJobRecord:
    job_id: str
    status: str
    created_at: datetime
    updated_at: datetime
    view: str | None = None
    view_version: int | None = None
    requested_features: list[str] = field(default_factory=list)
    requested_feature_groups: list[str] = field(default_factory=list)
    total_items: int = 0
    chunk_count: int = 0
    write_online: bool = False
    finished_at: datetime | None = None
    error_summary: dict = field(default_factory=dict)
    manifest_id: str | None = None  # set for dataset-scoped jobs

    def __post_init__(self) -> None:
        _require_aware(self.created_at, "created_at")
        _require_aware(self.updated_at, "updated_at")
        if self.finished_at is not None:
            _require_aware(self.finished_at, "finished_at")

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
            "created_at": _iso(self.created_at),
            "updated_at": _iso(self.updated_at),
            "finished_at": _iso(self.finished_at),
            "error_summary": dict(self.error_summary),
            "manifest_id": self.manifest_id,
        }

    @classmethod
    def from_dict(cls, data: dict) -> BatchJobRecord:
        return cls(
            job_id=data["job_id"],
            status=data["status"],
            created_at=_parse_dt(data["created_at"]),
            updated_at=_parse_dt(data["updated_at"]),
            view=data.get("view"),
            view_version=data.get("view_version"),
            requested_features=list(data.get("requested_features", [])),
            requested_feature_groups=list(data.get("requested_feature_groups", [])),
            total_items=data.get("total_items", 0),
            chunk_count=data.get("chunk_count", 0),
            write_online=data.get("write_online", False),
            finished_at=_parse_dt(data.get("finished_at")),
            error_summary=dict(data.get("error_summary", {})),
            manifest_id=data.get("manifest_id"),
        )


def _processed_identity(chunk: BatchChunkRecord) -> tuple:
    return (
        chunk.status, chunk.item_count, chunk.ok_items, chunk.failed_items,
        tuple(chunk.first_errors),
    )


def check_processed_conflict(
    existing: BatchChunkRecord | None, incoming: BatchChunkRecord
) -> bool:
    """Return True if the incoming processed result is an identical replay (no-op);
    raise ``BatchMetadataConflictError`` on a conflicting terminal result."""
    if existing is None or existing.status not in _TERMINAL:
        return False
    if _processed_identity(existing) == _processed_identity(incoming):
        return True
    raise BatchMetadataConflictError(
        f"chunk {incoming.chunk_id!r} re-processed with a different result"
    )


def derive_job_status(chunks: list[BatchChunkRecord], chunk_count: int) -> str:
    if not chunks:
        return "accepted"
    statuses = [c.status for c in chunks]
    all_terminal = all(s in _TERMINAL for s in statuses)
    if not (len(chunks) >= chunk_count and all_terminal):
        if all(s in ("accepted", "requested") for s in statuses):
            return "accepted"
        return "running"
    if all(s == "failed" for s in statuses):
        return "failed"
    if any(s in ("failed", "completed_with_errors") for s in statuses) or any(
        c.failed_items > 0 for c in chunks
    ):
        return "completed_with_errors"
    return "completed"


def merge_job_snapshot(
    existing: BatchJobRecord | None, incoming: BatchJobRecord
) -> BatchJobRecord:
    """Fill snapshot fields from ``incoming`` without regressing; keep created_at."""
    now = datetime.now(tz=UTC)
    if existing is None:
        return dataclasses.replace(incoming, updated_at=now)

    def pick(inc: Any, exi: Any) -> Any:
        return inc if inc else exi

    return dataclasses.replace(
        existing,
        view=pick(incoming.view, existing.view),
        view_version=pick(incoming.view_version, existing.view_version),
        requested_features=incoming.requested_features or existing.requested_features,
        requested_feature_groups=(
            incoming.requested_feature_groups or existing.requested_feature_groups
        ),
        total_items=pick(incoming.total_items, existing.total_items),
        chunk_count=pick(incoming.chunk_count, existing.chunk_count),
        write_online=incoming.write_online or existing.write_online,
        manifest_id=pick(incoming.manifest_id, existing.manifest_id),
        updated_at=now,
    )


class BatchMetadataStore(Protocol):
    def upsert_job(self, job: BatchJobRecord) -> None: ...

    def upsert_chunk_requested(self, chunk: BatchChunkRecord) -> None: ...

    def upsert_chunk_processed(self, chunk: BatchChunkRecord) -> None: ...

    def get_job(self, job_id: str) -> BatchJobRecord | None: ...

    def list_chunks(self, job_id: str) -> list[BatchChunkRecord]: ...


class InMemoryBatchMetadataStore:
    """Dict-backed durable-projection store for memory mode / tests."""

    def __init__(self) -> None:
        self._jobs: dict[str, BatchJobRecord] = {}
        self._chunks: dict[str, BatchChunkRecord] = {}

    def get_job(self, job_id: str) -> BatchJobRecord | None:
        return self._jobs.get(job_id)

    def list_chunks(self, job_id: str) -> list[BatchChunkRecord]:
        return [c for c in self._chunks.values() if c.job_id == job_id]

    def _recompute_job_status(self, job_id: str, chunk_count: int) -> None:
        now = datetime.now(tz=UTC)
        chunks = self.list_chunks(job_id)
        existing = self._jobs.get(job_id)
        # The job row's chunk_count is authoritative once known (a chunk event's own
        # chunk_count is only a fallback for processed-before-requested).
        effective = existing.chunk_count if existing and existing.chunk_count else chunk_count
        status = derive_job_status(chunks, effective)
        finished_at = now if status in _TERMINAL else None
        if existing is None:
            self._jobs[job_id] = BatchJobRecord(
                job_id=job_id, status=status, created_at=now, updated_at=now,
                chunk_count=chunk_count, finished_at=finished_at,
            )
        else:
            self._jobs[job_id] = dataclasses.replace(
                existing, status=status, updated_at=now, finished_at=finished_at,
            )

    def upsert_job(self, job: BatchJobRecord) -> None:
        merged = merge_job_snapshot(self._jobs.get(job.job_id), job)
        self._jobs[job.job_id] = merged
        self._recompute_job_status(job.job_id, merged.chunk_count)

    def upsert_chunk_requested(self, chunk: BatchChunkRecord) -> None:
        existing = self._chunks.get(chunk.chunk_id)
        if existing is not None and existing.status in _TERMINAL:
            return  # do not downgrade a processed chunk
        created_at = existing.created_at if existing else chunk.created_at
        self._chunks[chunk.chunk_id] = dataclasses.replace(chunk, created_at=created_at)
        self._recompute_job_status(chunk.job_id, chunk.chunk_count)

    def upsert_chunk_processed(self, chunk: BatchChunkRecord) -> None:
        existing = self._chunks.get(chunk.chunk_id)
        if check_processed_conflict(existing, chunk):
            return  # identical replay -> no-op
        created_at = existing.created_at if existing else chunk.created_at
        self._chunks[chunk.chunk_id] = dataclasses.replace(chunk, created_at=created_at)
        self._recompute_job_status(chunk.job_id, chunk.chunk_count)


# --- Postgres (conn-based; read-modify-write, mirrors PostgresMetadataStore) --

_JOB_COLUMNS = (
    "job_id", "status", "feature_view", "view_version", "requested_features_json",
    "requested_feature_groups_json", "total_items", "chunk_count", "write_online",
    "created_at", "updated_at", "finished_at", "error_summary_json", "manifest_id",
)
_CHUNK_COLUMNS = (
    "chunk_id", "job_id", "chunk_index", "chunk_count", "status", "item_count",
    "ok_items", "failed_items", "first_errors_json", "created_at", "updated_at",
    "finished_at",
)

_UPSERT_JOB_SQL = """
INSERT INTO batch_jobs (
    job_id, status, feature_view, view_version, requested_features_json,
    requested_feature_groups_json, total_items, chunk_count, write_online,
    created_at, updated_at, finished_at, error_summary_json, manifest_id
) VALUES (
    %(job_id)s, %(status)s, %(feature_view)s, %(view_version)s,
    %(requested_features_json)s::jsonb,
    %(requested_feature_groups_json)s::jsonb, %(total_items)s, %(chunk_count)s,
    %(write_online)s, %(created_at)s, %(updated_at)s, %(finished_at)s,
    %(error_summary_json)s::jsonb, %(manifest_id)s
)
ON CONFLICT (job_id) DO UPDATE SET
    status = EXCLUDED.status, feature_view = EXCLUDED.feature_view,
    view_version = EXCLUDED.view_version,
    requested_features_json = EXCLUDED.requested_features_json,
    requested_feature_groups_json = EXCLUDED.requested_feature_groups_json,
    total_items = EXCLUDED.total_items, chunk_count = EXCLUDED.chunk_count,
    write_online = EXCLUDED.write_online, updated_at = EXCLUDED.updated_at,
    finished_at = EXCLUDED.finished_at, error_summary_json = EXCLUDED.error_summary_json,
    manifest_id = EXCLUDED.manifest_id
"""

_UPSERT_CHUNK_SQL = """
INSERT INTO batch_chunks (
    chunk_id, job_id, chunk_index, chunk_count, status, item_count, ok_items,
    failed_items, first_errors_json, created_at, updated_at, finished_at
) VALUES (
    %(chunk_id)s, %(job_id)s, %(chunk_index)s, %(chunk_count)s, %(status)s,
    %(item_count)s, %(ok_items)s, %(failed_items)s, %(first_errors_json)s::jsonb,
    %(created_at)s, %(updated_at)s, %(finished_at)s
)
ON CONFLICT (chunk_id) DO UPDATE SET
    status = EXCLUDED.status, item_count = EXCLUDED.item_count,
    ok_items = EXCLUDED.ok_items, failed_items = EXCLUDED.failed_items,
    first_errors_json = EXCLUDED.first_errors_json, updated_at = EXCLUDED.updated_at,
    finished_at = EXCLUDED.finished_at
"""

_SELECT_JOB_SQL = f"SELECT {', '.join(_JOB_COLUMNS)} FROM batch_jobs WHERE job_id = %(job_id)s"
_SELECT_CHUNK_SQL = (
    f"SELECT {', '.join(_CHUNK_COLUMNS)} FROM batch_chunks WHERE chunk_id = %(chunk_id)s"
)
_SELECT_CHUNKS_BY_JOB_SQL = (
    f"SELECT {', '.join(_CHUNK_COLUMNS)} FROM batch_chunks WHERE job_id = %(job_id)s"
)


def _job_to_params(job: BatchJobRecord) -> dict[str, Any]:
    return {
        "job_id": job.job_id, "status": job.status, "feature_view": job.view,
        "view_version": job.view_version,
        "requested_features_json": json.dumps(list(job.requested_features)),
        "requested_feature_groups_json": json.dumps(list(job.requested_feature_groups)),
        "total_items": job.total_items, "chunk_count": job.chunk_count,
        "write_online": job.write_online, "created_at": job.created_at,
        "updated_at": job.updated_at, "finished_at": job.finished_at,
        "error_summary_json": json.dumps(job.error_summary, sort_keys=True),
        "manifest_id": job.manifest_id,
    }


def _chunk_to_params(chunk: BatchChunkRecord) -> dict[str, Any]:
    return {
        "chunk_id": chunk.chunk_id, "job_id": chunk.job_id,
        "chunk_index": chunk.chunk_index, "chunk_count": chunk.chunk_count,
        "status": chunk.status, "item_count": chunk.item_count,
        "ok_items": chunk.ok_items, "failed_items": chunk.failed_items,
        "first_errors_json": json.dumps(list(chunk.first_errors)),
        "created_at": chunk.created_at, "updated_at": chunk.updated_at,
        "finished_at": chunk.finished_at,
    }


def _loads(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _row_to_job(row: Mapping[str, Any]) -> BatchJobRecord:
    return BatchJobRecord(
        job_id=row["job_id"], status=row["status"], created_at=row["created_at"],
        updated_at=row["updated_at"], view=row["feature_view"],
        view_version=row["view_version"],
        requested_features=list(_loads(row["requested_features_json"]) or []),
        requested_feature_groups=list(_loads(row["requested_feature_groups_json"]) or []),
        total_items=row["total_items"], chunk_count=row["chunk_count"],
        write_online=row["write_online"], finished_at=row["finished_at"],
        error_summary=dict(_loads(row["error_summary_json"]) or {}),
        manifest_id=row.get("manifest_id"),
    )


def _row_to_chunk(row: Mapping[str, Any]) -> BatchChunkRecord:
    return BatchChunkRecord(
        chunk_id=row["chunk_id"], job_id=row["job_id"], chunk_index=row["chunk_index"],
        chunk_count=row["chunk_count"], status=row["status"], item_count=row["item_count"],
        created_at=row["created_at"], updated_at=row["updated_at"],
        ok_items=row["ok_items"], failed_items=row["failed_items"],
        first_errors=list(_loads(row["first_errors_json"]) or []),
        finished_at=row["finished_at"],
    )


class PostgresBatchMetadataStore:
    """Postgres-backed durable batch projection (caller owns the connection)."""

    def __init__(self, connection) -> None:
        self._connection = connection

    def get_job(self, job_id: str) -> BatchJobRecord | None:
        with self._connection.cursor() as cur:
            cur.execute(_SELECT_JOB_SQL, {"job_id": job_id})
            row = cur.fetchone()
        return _row_to_job(row) if row is not None else None

    def _get_chunk(self, chunk_id: str) -> BatchChunkRecord | None:
        with self._connection.cursor() as cur:
            cur.execute(_SELECT_CHUNK_SQL, {"chunk_id": chunk_id})
            row = cur.fetchone()
        return _row_to_chunk(row) if row is not None else None

    def list_chunks(self, job_id: str) -> list[BatchChunkRecord]:
        with self._connection.cursor() as cur:
            cur.execute(_SELECT_CHUNKS_BY_JOB_SQL, {"job_id": job_id})
            rows = cur.fetchall()
        return [_row_to_chunk(r) for r in rows]

    def _write_chunk(self, chunk: BatchChunkRecord) -> None:
        with self._connection.cursor() as cur:
            cur.execute(_UPSERT_CHUNK_SQL, _chunk_to_params(chunk))
        self._connection.commit()

    def _recompute_job_status(self, job_id: str, chunk_count: int) -> None:
        now = datetime.now(tz=UTC)
        chunks = self.list_chunks(job_id)
        existing = self.get_job(job_id)
        effective = existing.chunk_count if existing and existing.chunk_count else chunk_count
        status = derive_job_status(chunks, effective)
        finished_at = now if status in _TERMINAL else None
        if existing is None:
            job = BatchJobRecord(
                job_id=job_id, status=status, created_at=now, updated_at=now,
                chunk_count=chunk_count, finished_at=finished_at,
            )
        else:
            job = dataclasses.replace(
                existing, status=status, updated_at=now, finished_at=finished_at
            )
        with self._connection.cursor() as cur:
            cur.execute(_UPSERT_JOB_SQL, _job_to_params(job))
        self._connection.commit()

    def upsert_job(self, job: BatchJobRecord) -> None:
        merged = merge_job_snapshot(self.get_job(job.job_id), job)
        with self._connection.cursor() as cur:
            cur.execute(_UPSERT_JOB_SQL, _job_to_params(merged))
        self._connection.commit()
        self._recompute_job_status(job.job_id, merged.chunk_count)

    def upsert_chunk_requested(self, chunk: BatchChunkRecord) -> None:
        existing = self._get_chunk(chunk.chunk_id)
        if existing is not None and existing.status in _TERMINAL:
            return
        created_at = existing.created_at if existing else chunk.created_at
        self._write_chunk(dataclasses.replace(chunk, created_at=created_at))
        self._recompute_job_status(chunk.job_id, chunk.chunk_count)

    def upsert_chunk_processed(self, chunk: BatchChunkRecord) -> None:
        existing = self._get_chunk(chunk.chunk_id)
        if check_processed_conflict(existing, chunk):
            return
        created_at = existing.created_at if existing else chunk.created_at
        self._write_chunk(dataclasses.replace(chunk, created_at=created_at))
        self._recompute_job_status(chunk.job_id, chunk.chunk_count)
