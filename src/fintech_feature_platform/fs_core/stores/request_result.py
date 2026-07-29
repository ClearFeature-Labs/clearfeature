"""Request result store: the request's OWN computed values for hybrid responses.

Holds, per ``request_id``, the values the request itself computed (its
``FeatureWriteSet``) plus each feature's D9 write outcome, so
``POST /v1/feature-requests/compute`` can return request-scoped results instead of
re-reading Valkey ``/latest``. Like the status store
it is short-lived (TTL) and bounded: computed feature values only — never raw payloads,
``object_key``/``storage_uri``, or source reports.

Unlike the status store, a failed result write is NOT best-effort: the worker runner
must treat it as a retriable infrastructure failure (no offset commit, replay), because
``status=completed`` must imply the request-scoped result is readable.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from fintech_feature_platform.fs_core.models import FeatureWriteSet


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"{name} must be timezone-aware")


@dataclass(frozen=True)
class RequestResult:
    """Computed values of one request. ``features`` maps feature name to a dict with
    ``feature_version``, ``value``, ``data_ts``/``max_input_data_ts``/``calc_ts`` (iso
    strings, max may be null), ``input_fingerprint``, ``value_hash``, and the
    per-feature ``online_write_status`` D9 outcome."""

    request_id: str
    view: str
    view_version: int
    entity_key: dict[str, str]
    features: dict[str, dict[str, Any]]
    created_at: datetime

    def __post_init__(self) -> None:
        _require_aware(self.created_at, "created_at")

    @classmethod
    def from_write_set(
        cls,
        request_id: str,
        write_set: FeatureWriteSet,
        online_written: dict[str, str],
        created_at: datetime | None = None,
    ) -> RequestResult:
        features: dict[str, dict[str, Any]] = {}
        for name, result in write_set.results.items():
            features[name] = {
                "feature_version": result.ref.version,
                "value": result.value,
                "data_ts": result.data_ts.isoformat(),
                "max_input_data_ts": (
                    result.max_input_data_ts.isoformat()
                    if result.max_input_data_ts is not None
                    else None
                ),
                "calc_ts": result.calc_ts.isoformat(),
                "input_fingerprint": result.input_fingerprint,
                "value_hash": result.value_hash,
                "online_write_status": online_written.get(result.ref.encode()),
            }
        return cls(
            request_id=request_id,
            view=write_set.view,
            view_version=write_set.view_version,
            entity_key=dict(write_set.entity_key.parts),
            features=features,
            created_at=created_at if created_at is not None else datetime.now(tz=UTC),
        )

    def to_dict(self) -> dict:
        return {
            "request_id": self.request_id,
            "view": self.view,
            "view_version": self.view_version,
            "entity_key": dict(self.entity_key),
            "features": self.features,
            "created_at": self.created_at.isoformat(),
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> RequestResult:
        return cls(
            request_id=data["request_id"],
            view=data["view"],
            view_version=data["view_version"],
            entity_key=dict(data["entity_key"]),
            features={name: dict(item) for name, item in data["features"].items()},
            created_at=datetime.fromisoformat(data["created_at"]),
        )

    @classmethod
    def from_json(cls, raw: bytes | str) -> RequestResult:
        return cls.from_dict(json.loads(raw))


class RequestResultStore(Protocol):
    def put(self, result: RequestResult) -> None: ...

    def get(self, request_id: str) -> RequestResult | None: ...


class InMemoryRequestResultStore:
    """Dict-backed result store for memory mode / tests (single process)."""

    def __init__(self) -> None:
        self._items: dict[str, RequestResult] = {}

    def put(self, result: RequestResult) -> None:
        self._items[result.request_id] = result

    def get(self, request_id: str) -> RequestResult | None:
        return self._items.get(request_id)


def result_key(request_id: str) -> str:
    return f"fs:request-result:{request_id}"


class ValkeyRequestResultStore:
    """Valkey-backed result store (shared across API + worker processes), TTL-bounded."""

    def __init__(self, client, ttl_s: int) -> None:
        self._client = client
        self._ttl_s = ttl_s

    def put(self, result: RequestResult) -> None:
        self._client.set(result_key(result.request_id), result.to_json(), ex=self._ttl_s)

    def get(self, request_id: str) -> RequestResult | None:
        raw = self._client.get(result_key(request_id))
        if raw is None:
            return None
        return RequestResult.from_json(raw)
