"""Durable request metadata store (async Postgres projection target).

Holds two projections written by the Metadata Writer (a separate Kafka consumer group,
**not** the online critical path):

* ``RequestMetadata`` — a durable per-request snapshot (``feature_requests``);
* ``RequestEvent`` — an append-only audit log keyed by ``event_hash`` (``request_events``).

It stores **no feature values, no raw payloads, and no DLQ ``source_payload_b64``** — only
request/entity/view metadata and small event summaries. ``InMemoryMetadataStore`` is for
memory mode / tests; ``PostgresMetadataStore`` is conn-based (lazy ``psycopg`` via the
caller). Snapshot upserts are idempotent and non-regressing (``completed``/``failed`` is
never overwritten back to ``accepted``); ``append_event`` dedups by ``event_hash``.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

_TERMINAL = ("completed", "failed")


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"{name} must be timezone-aware")


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


def event_hash(raw_value: bytes) -> str:
    """sha256 of the raw consumed Kafka message bytes (exact replay dedup)."""
    return hashlib.sha256(raw_value).hexdigest()


@dataclass(frozen=True)
class RequestMetadata:
    request_id: str
    updated_at: datetime
    job_id: str | None = None
    status: str | None = None
    entity_type: str | None = None
    entity_key: dict[str, str] | None = None
    view: str | None = None
    view_version: int | None = None
    requested_features: list[str] = field(default_factory=list)
    requested_feature_groups: list[str] = field(default_factory=list)
    online_write_status: str | None = None
    offline_write_status: str | None = None
    created_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        _require_aware(self.updated_at, "updated_at")
        if self.created_at is not None:
            _require_aware(self.created_at, "created_at")
        if self.finished_at is not None:
            _require_aware(self.finished_at, "finished_at")

    def to_dict(self) -> dict:
        return {
            "request_id": self.request_id,
            "job_id": self.job_id,
            "status": self.status,
            "entity_type": self.entity_type,
            "entity_key": dict(self.entity_key) if self.entity_key is not None else None,
            "view": self.view,
            "view_version": self.view_version,
            "requested_features": list(self.requested_features),
            "requested_feature_groups": list(self.requested_feature_groups),
            "online_write_status": self.online_write_status,
            "offline_write_status": self.offline_write_status,
            "created_at": _iso(self.created_at),
            "updated_at": _iso(self.updated_at),
            "finished_at": _iso(self.finished_at),
            "error": self.error,
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> RequestMetadata:
        entity_key = data.get("entity_key")
        return cls(
            request_id=data["request_id"],
            updated_at=_parse_dt(data["updated_at"]),
            job_id=data.get("job_id"),
            status=data.get("status"),
            entity_type=data.get("entity_type"),
            entity_key=dict(entity_key) if entity_key is not None else None,
            view=data.get("view"),
            view_version=data.get("view_version"),
            requested_features=list(data.get("requested_features", [])),
            requested_feature_groups=list(data.get("requested_feature_groups", [])),
            online_write_status=data.get("online_write_status"),
            offline_write_status=data.get("offline_write_status"),
            created_at=_parse_dt(data.get("created_at")),
            finished_at=_parse_dt(data.get("finished_at")),
            error=data.get("error"),
        )

    @classmethod
    def from_json(cls, raw: bytes | str) -> RequestMetadata:
        return cls.from_dict(json.loads(raw))


@dataclass(frozen=True)
class RequestEvent:
    event_hash: str
    event_type: str
    occurred_at: datetime
    created_at: datetime
    request_id: str | None = None
    summary: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_aware(self.occurred_at, "occurred_at")
        _require_aware(self.created_at, "created_at")

    def to_dict(self) -> dict:
        return {
            "event_hash": self.event_hash,
            "event_type": self.event_type,
            "occurred_at": _iso(self.occurred_at),
            "created_at": _iso(self.created_at),
            "request_id": self.request_id,
            "summary": dict(self.summary),
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> RequestEvent:
        return cls(
            event_hash=data["event_hash"],
            event_type=data["event_type"],
            occurred_at=_parse_dt(data["occurred_at"]),
            created_at=_parse_dt(data["created_at"]),
            request_id=data.get("request_id"),
            summary=dict(data.get("summary", {})),
        )

    @classmethod
    def from_json(cls, raw: bytes | str) -> RequestEvent:
        return cls.from_dict(json.loads(raw))


def merge_request(existing: RequestMetadata, incoming: RequestMetadata) -> RequestMetadata:
    """Idempotent, non-regressing merge of an incoming partial snapshot onto existing.

    Non-None scalars and non-empty lists from ``incoming`` win; ``status`` never regresses
    from a terminal (``completed``/``failed``) back to ``accepted``/``running``.
    """
    status = incoming.status
    if status is None:
        status = existing.status
    elif existing.status in _TERMINAL and status in (None, "accepted", "running"):
        status = existing.status

    def pick(inc: Any, exi: Any) -> Any:
        return inc if inc is not None else exi

    return RequestMetadata(
        request_id=existing.request_id,
        updated_at=datetime.now(tz=UTC),
        job_id=pick(incoming.job_id, existing.job_id),
        status=status,
        entity_type=pick(incoming.entity_type, existing.entity_type),
        entity_key=pick(incoming.entity_key, existing.entity_key),
        view=pick(incoming.view, existing.view),
        view_version=pick(incoming.view_version, existing.view_version),
        requested_features=incoming.requested_features or existing.requested_features,
        requested_feature_groups=(
            incoming.requested_feature_groups or existing.requested_feature_groups
        ),
        online_write_status=pick(
            incoming.online_write_status, existing.online_write_status
        ),
        offline_write_status=pick(
            incoming.offline_write_status, existing.offline_write_status
        ),
        created_at=existing.created_at or incoming.created_at,
        finished_at=pick(incoming.finished_at, existing.finished_at),
        error=pick(incoming.error, existing.error),
    )


class MetadataStore(Protocol):
    def upsert_request(self, metadata: RequestMetadata) -> None: ...

    def append_event(self, event: RequestEvent) -> bool: ...

    def get_request(self, request_id: str) -> RequestMetadata | None: ...


class InMemoryMetadataStore:
    """Dict-backed metadata store for memory mode / tests."""

    def __init__(self) -> None:
        self._requests: dict[str, RequestMetadata] = {}
        self._event_hashes: set[str] = set()

    def upsert_request(self, metadata: RequestMetadata) -> None:
        existing = self._requests.get(metadata.request_id)
        if existing is None:
            now = datetime.now(tz=UTC)
            self._requests[metadata.request_id] = dataclasses.replace(
                metadata, updated_at=now, created_at=metadata.created_at or now
            )
        else:
            self._requests[metadata.request_id] = merge_request(existing, metadata)

    def append_event(self, event: RequestEvent) -> bool:
        if event.event_hash in self._event_hashes:
            return False
        self._event_hashes.add(event.event_hash)
        return True

    def get_request(self, request_id: str) -> RequestMetadata | None:
        return self._requests.get(request_id)


# --- Postgres (conn-based; lazy psycopg via the caller) ----------------------

_REQUEST_COLUMNS = (
    "request_id",
    "job_id",
    "status",
    "entity_type",
    "entity_key_json",
    "feature_view",
    "view_version",
    "requested_features_json",
    "requested_feature_groups_json",
    "online_write_status",
    "offline_write_status",
    "created_at",
    "updated_at",
    "finished_at",
    "error",
)

_UPSERT_REQUEST_SQL = """
INSERT INTO feature_requests (
    request_id, job_id, status, entity_type, entity_key_json, feature_view,
    view_version, requested_features_json, requested_feature_groups_json,
    online_write_status, offline_write_status, created_at, updated_at, finished_at, error
) VALUES (
    %(request_id)s, %(job_id)s, %(status)s, %(entity_type)s, %(entity_key_json)s::jsonb,
    %(feature_view)s, %(view_version)s, %(requested_features_json)s::jsonb,
    %(requested_feature_groups_json)s::jsonb, %(online_write_status)s,
    %(offline_write_status)s, %(created_at)s, %(updated_at)s, %(finished_at)s, %(error)s
)
ON CONFLICT (request_id) DO UPDATE SET
    job_id = EXCLUDED.job_id, status = EXCLUDED.status,
    entity_type = EXCLUDED.entity_type, entity_key_json = EXCLUDED.entity_key_json,
    feature_view = EXCLUDED.feature_view, view_version = EXCLUDED.view_version,
    requested_features_json = EXCLUDED.requested_features_json,
    requested_feature_groups_json = EXCLUDED.requested_feature_groups_json,
    online_write_status = EXCLUDED.online_write_status,
    offline_write_status = EXCLUDED.offline_write_status,
    created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
    finished_at = EXCLUDED.finished_at, error = EXCLUDED.error
"""

_INSERT_EVENT_SQL = """
INSERT INTO request_events (
    event_hash, request_id, event_type, occurred_at, summary_json, created_at
) VALUES (
    %(event_hash)s, %(request_id)s, %(event_type)s, %(occurred_at)s,
    %(summary_json)s::jsonb, %(created_at)s
)
ON CONFLICT (event_hash) DO NOTHING
"""

_SELECT_REQUEST_SQL = (
    "SELECT request_id, job_id, status, entity_type, entity_key_json, feature_view, "
    "view_version, requested_features_json, requested_feature_groups_json, "
    "online_write_status, offline_write_status, created_at, updated_at, finished_at, error "
    "FROM feature_requests WHERE request_id = %(request_id)s"
)


def _request_to_params(metadata: RequestMetadata) -> dict[str, Any]:
    return {
        "request_id": metadata.request_id,
        "job_id": metadata.job_id,
        "status": metadata.status,
        "entity_type": metadata.entity_type,
        "entity_key_json": json.dumps(metadata.entity_key)
        if metadata.entity_key is not None
        else None,
        "feature_view": metadata.view,
        "view_version": metadata.view_version,
        "requested_features_json": json.dumps(list(metadata.requested_features)),
        "requested_feature_groups_json": json.dumps(
            list(metadata.requested_feature_groups)
        ),
        "online_write_status": metadata.online_write_status,
        "offline_write_status": metadata.offline_write_status,
        "created_at": metadata.created_at,
        "updated_at": metadata.updated_at,
        "finished_at": metadata.finished_at,
        "error": metadata.error,
    }


def _row_to_request(row: Mapping[str, Any]) -> RequestMetadata:
    def _loads(value: Any) -> Any:
        return json.loads(value) if isinstance(value, str) else value

    entity_key = _loads(row["entity_key_json"])
    return RequestMetadata(
        request_id=row["request_id"],
        updated_at=row["updated_at"],
        job_id=row["job_id"],
        status=row["status"],
        entity_type=row["entity_type"],
        entity_key=dict(entity_key) if entity_key is not None else None,
        view=row["feature_view"],
        view_version=row["view_version"],
        requested_features=list(_loads(row["requested_features_json"]) or []),
        requested_feature_groups=list(_loads(row["requested_feature_groups_json"]) or []),
        online_write_status=row["online_write_status"],
        offline_write_status=row["offline_write_status"],
        created_at=row["created_at"],
        finished_at=row["finished_at"],
        error=row["error"],
    )


def _event_to_params(event: RequestEvent) -> dict[str, Any]:
    return {
        "event_hash": event.event_hash,
        "request_id": event.request_id,
        "event_type": event.event_type,
        "occurred_at": event.occurred_at,
        "summary_json": json.dumps(event.summary, sort_keys=True),
        "created_at": event.created_at,
    }


class PostgresMetadataStore:
    """Postgres-backed metadata projection (the caller owns the connection lifetime)."""

    def __init__(self, connection) -> None:
        self._connection = connection

    def upsert_request(self, metadata: RequestMetadata) -> None:
        existing = self.get_request(metadata.request_id)
        if existing is None:
            now = datetime.now(tz=UTC)
            merged = dataclasses.replace(
                metadata, updated_at=now, created_at=metadata.created_at or now
            )
        else:
            merged = merge_request(existing, metadata)
        with self._connection.cursor() as cur:
            cur.execute(_UPSERT_REQUEST_SQL, _request_to_params(merged))
        self._connection.commit()

    def append_event(self, event: RequestEvent) -> bool:
        with self._connection.cursor() as cur:
            cur.execute(_INSERT_EVENT_SQL, _event_to_params(event))
            inserted = cur.rowcount == 1
        self._connection.commit()
        return inserted

    def get_request(self, request_id: str) -> RequestMetadata | None:
        with self._connection.cursor() as cur:
            cur.execute(_SELECT_REQUEST_SQL, {"request_id": request_id})
            row = cur.fetchone()
        return _row_to_request(row) if row is not None else None
