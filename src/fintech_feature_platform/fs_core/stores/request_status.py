"""Request status store: short-lived operational state for feature requests.

Holds the lifecycle of a Kafka-first feature request (``accepted`` -> ``running`` ->
``completed`` / ``failed``, plus online/offline/metadata write sub-statuses) so
``GET /v1/feature-requests/{request_id}`` can report it. It is **observability state,
not the system of record** — best-effort, short-lived (TTL), and it never holds feature
values, raw payloads, ``object_key``/``storage_uri``, or DLQ payloads.

``InMemoryRequestStatusStore`` is for memory mode / tests (single process).
``ValkeyRequestStatusStore`` is required for the real multi-process topology so the API
and the worker processes share one store. ``redis`` is imported lazily via the existing
``connect_valkey`` helper, so default tests stay Docker-free.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

# Fields required to upsert a status from ``update`` when no entry exists yet.
_CREATE_REQUIRED = ("job_id", "status", "entity_type", "entity_key", "view", "view_version")


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"{name} must be timezone-aware")


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


@dataclass(frozen=True)
class RequestStatus:
    request_id: str
    job_id: str
    status: str
    entity_type: str
    entity_key: dict[str, str]
    view: str
    view_version: int
    created_at: datetime
    updated_at: datetime
    requested_features: list[str] = field(default_factory=list)
    requested_feature_groups: list[str] = field(default_factory=list)
    online_write_status: str | None = None
    offline_write_status: str | None = None
    metadata_write_status: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        _require_aware(self.created_at, "created_at")
        _require_aware(self.updated_at, "updated_at")
        if self.started_at is not None:
            _require_aware(self.started_at, "started_at")
        if self.finished_at is not None:
            _require_aware(self.finished_at, "finished_at")

    def to_dict(self) -> dict:
        return {
            "request_id": self.request_id,
            "job_id": self.job_id,
            "status": self.status,
            "entity_type": self.entity_type,
            "entity_key": dict(self.entity_key),
            "view": self.view,
            "view_version": self.view_version,
            "requested_features": list(self.requested_features),
            "requested_feature_groups": list(self.requested_feature_groups),
            "online_write_status": self.online_write_status,
            "offline_write_status": self.offline_write_status,
            "metadata_write_status": self.metadata_write_status,
            "created_at": _iso(self.created_at),
            "updated_at": _iso(self.updated_at),
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at),
            "error": self.error,
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> RequestStatus:
        return cls(
            request_id=data["request_id"],
            job_id=data["job_id"],
            status=data["status"],
            entity_type=data["entity_type"],
            entity_key=dict(data["entity_key"]),
            view=data["view"],
            view_version=data["view_version"],
            requested_features=list(data.get("requested_features", [])),
            requested_feature_groups=list(data.get("requested_feature_groups", [])),
            online_write_status=data.get("online_write_status"),
            offline_write_status=data.get("offline_write_status"),
            metadata_write_status=data.get("metadata_write_status"),
            created_at=_parse_dt(data["created_at"]),
            updated_at=_parse_dt(data["updated_at"]),
            started_at=_parse_dt(data.get("started_at")),
            finished_at=_parse_dt(data.get("finished_at")),
            error=data.get("error"),
        )

    @classmethod
    def from_json(cls, raw: bytes | str) -> RequestStatus:
        return cls.from_dict(json.loads(raw))


_FIELD_NAMES = frozenset(f.name for f in dataclasses.fields(RequestStatus))


def _apply_changes(existing: RequestStatus, changes: Mapping[str, object]) -> RequestStatus:
    allowed = {k: v for k, v in changes.items() if k in _FIELD_NAMES and k != "request_id"}
    allowed.setdefault("updated_at", datetime.now(tz=UTC))
    return dataclasses.replace(existing, **allowed)


def _maybe_create(request_id: str, changes: Mapping[str, object]) -> RequestStatus | None:
    if not all(k in changes for k in _CREATE_REQUIRED):
        return None
    now = datetime.now(tz=UTC)
    kwargs = {k: v for k, v in changes.items() if k in _FIELD_NAMES}
    kwargs.setdefault("created_at", now)
    kwargs.setdefault("updated_at", now)
    return RequestStatus(request_id=request_id, **kwargs)


class RequestStatusStore(Protocol):
    def put(self, status: RequestStatus) -> None: ...

    def get(self, request_id: str) -> RequestStatus | None: ...

    def update(self, request_id: str, **changes: object) -> RequestStatus | None: ...


class InMemoryRequestStatusStore:
    """Dict-backed status store for memory mode / tests (single process)."""

    def __init__(self) -> None:
        self._items: dict[str, RequestStatus] = {}

    def put(self, status: RequestStatus) -> None:
        self._items[status.request_id] = status

    def get(self, request_id: str) -> RequestStatus | None:
        return self._items.get(request_id)

    def update(self, request_id: str, **changes: object) -> RequestStatus | None:
        existing = self._items.get(request_id)
        if existing is None:
            created = _maybe_create(request_id, changes)
            if created is None:
                return None
            self._items[request_id] = created
            return created
        merged = _apply_changes(existing, changes)
        self._items[request_id] = merged
        return merged


def status_key(request_id: str) -> str:
    return f"fs:request-status:{request_id}"


class ValkeyRequestStatusStore:
    """Valkey-backed status store (shared across API + worker processes), TTL-bounded."""

    def __init__(self, client, ttl_s: int) -> None:
        self._client = client
        self._ttl_s = ttl_s

    def put(self, status: RequestStatus) -> None:
        self._client.set(status_key(status.request_id), status.to_json(), ex=self._ttl_s)

    def get(self, request_id: str) -> RequestStatus | None:
        raw = self._client.get(status_key(request_id))
        if raw is None:
            return None
        return RequestStatus.from_json(raw)

    def update(self, request_id: str, **changes: object) -> RequestStatus | None:
        existing = self.get(request_id)
        if existing is None:
            created = _maybe_create(request_id, changes)
            if created is None:
                return None
            self.put(created)
            return created
        merged = _apply_changes(existing, changes)
        self.put(merged)
        return merged
