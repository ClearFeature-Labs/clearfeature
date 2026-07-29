"""Source-dataset ingestion manifest + item index.

A ``SourceDatasetManifest`` is the durable run record of one ingestion into landing
form (a) — raw reports in object storage + ``raw_reports_meta``. It stores run
accounting (read/written/duplicate/rejected counts), watermarks, and a dataset
content fingerprint; it never stores raw payloads. Each ``SourceDatasetItem`` records
one JSONL row's outcome as a **reference** (``report_ref`` + ``content_hash`` +
metadata), so a later batch job  can operate on ``report_refs`` without
touching payloads. Payloads live only in the raw payload store; refs/metadata live here.

``InMemorySourceDatasetStore`` is for memory mode / tests; ``PostgresSourceDatasetStore``
is the durable local-backend store (schema ``infra/postgres/006_source_dataset_manifest.sql``).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

SOURCE_KIND_OBJECT_STORAGE_JSONL = "object_storage_jsonl"
SOURCE_KIND_DWH_JSON_REPORTS = "dwh_json_reports"
SOURCE_KIND_DWH_TABLE = "dwh_table"
COPY = "copy"
REGISTER_IN_PLACE = "register_in_place"

# Landing form : (a) raw reports, (b) precomputed feature rows.
LANDING_RAW_REPORTS = "raw_reports"
LANDING_FEATURE_ROWS = "feature_rows"

STATUS_PENDING = "pending"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"

ITEM_WRITTEN = "written"
ITEM_DUPLICATE = "duplicate"
ITEM_REJECTED = "rejected"


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"{name} must be timezone-aware")


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


@dataclass(frozen=True)
class SourceDatasetManifest:
    """Durable record of one ingestion run (counts + watermarks + fingerprint).

    Serves both landing forms : ``landing_form="raw_reports"`` (a) uses
    ``report_type`` and event_ts watermarks; ``landing_form="feature_rows"`` (b) uses
    ``view``/``view_version`` and data_ts watermarks and leaves ``report_type`` unset.
    """

    manifest_id: str
    dataset_id: str
    source_kind: str
    entity_type: str
    source_name: str
    copy_mode: str
    created_at: datetime
    report_type: str | None = None
    landing_form: str = LANDING_RAW_REPORTS
    view: str | None = None
    view_version: int | None = None
    status: str = STATUS_PENDING
    input_uri: str | None = None
    created_by: str | None = None
    item_count_read: int = 0
    item_count_written: int = 0
    item_count_duplicate: int = 0
    item_count_rejected: int = 0
    watermark_min_event_ts: datetime | None = None
    watermark_max_event_ts: datetime | None = None
    content_hash: str | None = None

    def __post_init__(self) -> None:
        _require_aware(self.created_at, "created_at")
        if self.watermark_min_event_ts is not None:
            _require_aware(self.watermark_min_event_ts, "watermark_min_event_ts")
        if self.watermark_max_event_ts is not None:
            _require_aware(self.watermark_max_event_ts, "watermark_max_event_ts")
        if self.copy_mode not in (COPY, REGISTER_IN_PLACE):
            raise ValueError(f"unknown copy_mode {self.copy_mode!r}")
        if self.landing_form not in (LANDING_RAW_REPORTS, LANDING_FEATURE_ROWS):
            raise ValueError(f"unknown landing_form {self.landing_form!r}")

    def to_dict(self) -> dict:
        return {
            "manifest_id": self.manifest_id,
            "dataset_id": self.dataset_id,
            "source_kind": self.source_kind,
            "entity_type": self.entity_type,
            "source_name": self.source_name,
            "report_type": self.report_type,
            "landing_form": self.landing_form,
            "view": self.view,
            "view_version": self.view_version,
            "copy_mode": self.copy_mode,
            "created_at": _iso(self.created_at),
            "status": self.status,
            "input_uri": self.input_uri,
            "created_by": self.created_by,
            "item_count_read": self.item_count_read,
            "item_count_written": self.item_count_written,
            "item_count_duplicate": self.item_count_duplicate,
            "item_count_rejected": self.item_count_rejected,
            "watermark_min_event_ts": _iso(self.watermark_min_event_ts),
            "watermark_max_event_ts": _iso(self.watermark_max_event_ts),
            "content_hash": self.content_hash,
        }

    @classmethod
    def from_dict(cls, data: dict) -> SourceDatasetManifest:
        return cls(
            manifest_id=data["manifest_id"],
            dataset_id=data["dataset_id"],
            source_kind=data["source_kind"],
            entity_type=data["entity_type"],
            source_name=data["source_name"],
            report_type=data.get("report_type"),
            landing_form=data.get("landing_form", LANDING_RAW_REPORTS),
            view=data.get("view"),
            view_version=data.get("view_version"),
            copy_mode=data["copy_mode"],
            created_at=_parse_dt(data["created_at"]),
            status=data.get("status", STATUS_PENDING),
            input_uri=data.get("input_uri"),
            created_by=data.get("created_by"),
            item_count_read=data.get("item_count_read", 0),
            item_count_written=data.get("item_count_written", 0),
            item_count_duplicate=data.get("item_count_duplicate", 0),
            item_count_rejected=data.get("item_count_rejected", 0),
            watermark_min_event_ts=_parse_dt(data.get("watermark_min_event_ts")),
            watermark_max_event_ts=_parse_dt(data.get("watermark_max_event_ts")),
            content_hash=data.get("content_hash"),
        )


@dataclass(frozen=True)
class SourceDatasetItem:
    """One ingested row's outcome as a reference — never a payload."""

    manifest_id: str
    item_index: int
    status: str
    source_name: str
    report_type: str
    entity_key: dict[str, str] | None = None
    report_ref: str | None = None
    event_ts: datetime | None = None
    content_hash: str | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        if self.event_ts is not None:
            _require_aware(self.event_ts, "event_ts")

    def to_dict(self) -> dict:
        return {
            "manifest_id": self.manifest_id,
            "item_index": self.item_index,
            "status": self.status,
            "source_name": self.source_name,
            "report_type": self.report_type,
            "entity_key": dict(self.entity_key) if self.entity_key is not None else None,
            "report_ref": self.report_ref,
            "event_ts": _iso(self.event_ts),
            "content_hash": self.content_hash,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: dict) -> SourceDatasetItem:
        entity_key = data.get("entity_key")
        return cls(
            manifest_id=data["manifest_id"],
            item_index=data["item_index"],
            status=data["status"],
            source_name=data["source_name"],
            report_type=data["report_type"],
            entity_key=dict(entity_key) if entity_key is not None else None,
            report_ref=data.get("report_ref"),
            event_ts=_parse_dt(data.get("event_ts")),
            content_hash=data.get("content_hash"),
            error=data.get("error"),
        )


class SourceDatasetStore(Protocol):
    def upsert_manifest(self, manifest: SourceDatasetManifest) -> None: ...

    def add_items(self, items: list[SourceDatasetItem]) -> None: ...

    def get_manifest(self, manifest_id: str) -> SourceDatasetManifest | None: ...

    def list_items(self, manifest_id: str) -> list[SourceDatasetItem]: ...


class InMemorySourceDatasetStore:
    """Dict-backed store for memory mode / tests (single process)."""

    def __init__(self) -> None:
        self._manifests: dict[str, SourceDatasetManifest] = {}
        self._items: dict[str, list[SourceDatasetItem]] = {}

    def upsert_manifest(self, manifest: SourceDatasetManifest) -> None:
        self._manifests[manifest.manifest_id] = manifest

    def add_items(self, items: list[SourceDatasetItem]) -> None:
        for item in items:
            self._items.setdefault(item.manifest_id, []).append(item)

    def get_manifest(self, manifest_id: str) -> SourceDatasetManifest | None:
        return self._manifests.get(manifest_id)

    def list_items(self, manifest_id: str) -> list[SourceDatasetItem]:
        return list(self._items.get(manifest_id, []))


# --- Postgres (durable local backend) ----------------------------------------

_UPSERT_MANIFEST_SQL = """
INSERT INTO source_dataset_manifests (
    manifest_id, dataset_id, source_kind, entity_type, source_name, report_type,
    landing_form, view, view_version, copy_mode, created_at, status, input_uri,
    created_by, item_count_read, item_count_written, item_count_duplicate,
    item_count_rejected, watermark_min_event_ts, watermark_max_event_ts, content_hash
) VALUES (
    %(manifest_id)s, %(dataset_id)s, %(source_kind)s, %(entity_type)s, %(source_name)s,
    %(report_type)s, %(landing_form)s, %(view)s, %(view_version)s, %(copy_mode)s,
    %(created_at)s, %(status)s, %(input_uri)s, %(created_by)s, %(item_count_read)s,
    %(item_count_written)s, %(item_count_duplicate)s, %(item_count_rejected)s,
    %(watermark_min_event_ts)s, %(watermark_max_event_ts)s, %(content_hash)s
)
ON CONFLICT (manifest_id) DO UPDATE SET
    status = EXCLUDED.status,
    item_count_read = EXCLUDED.item_count_read,
    item_count_written = EXCLUDED.item_count_written,
    item_count_duplicate = EXCLUDED.item_count_duplicate,
    item_count_rejected = EXCLUDED.item_count_rejected,
    watermark_min_event_ts = EXCLUDED.watermark_min_event_ts,
    watermark_max_event_ts = EXCLUDED.watermark_max_event_ts,
    content_hash = EXCLUDED.content_hash
"""

_INSERT_ITEM_SQL = """
INSERT INTO source_dataset_items (
    manifest_id, item_index, status, source_name, report_type,
    entity_key, report_ref, event_ts, content_hash, error
) VALUES (
    %(manifest_id)s, %(item_index)s, %(status)s, %(source_name)s, %(report_type)s,
    %(entity_key)s::jsonb, %(report_ref)s, %(event_ts)s, %(content_hash)s, %(error)s
)
ON CONFLICT (manifest_id, item_index) DO NOTHING
"""

_MANIFEST_COLUMNS = (
    "manifest_id", "dataset_id", "source_kind", "entity_type", "source_name",
    "report_type", "landing_form", "view", "view_version", "copy_mode", "created_at",
    "status", "input_uri", "created_by", "item_count_read", "item_count_written",
    "item_count_duplicate", "item_count_rejected", "watermark_min_event_ts",
    "watermark_max_event_ts", "content_hash",
)

_ITEM_COLUMNS = (
    "manifest_id", "item_index", "status", "source_name", "report_type",
    "entity_key", "report_ref", "event_ts", "content_hash", "error",
)


def _manifest_to_params(m: SourceDatasetManifest) -> dict[str, Any]:
    d = m.to_dict()
    d["created_at"] = m.created_at
    d["watermark_min_event_ts"] = m.watermark_min_event_ts
    d["watermark_max_event_ts"] = m.watermark_max_event_ts
    return d


def _item_to_params(item: SourceDatasetItem) -> dict[str, Any]:
    return {
        "manifest_id": item.manifest_id,
        "item_index": item.item_index,
        "status": item.status,
        "source_name": item.source_name,
        "report_type": item.report_type,
        "entity_key": json.dumps(item.entity_key) if item.entity_key is not None else None,
        "report_ref": item.report_ref,
        "event_ts": item.event_ts,
        "content_hash": item.content_hash,
        "error": item.error,
    }


def _row_to_manifest(row: dict[str, Any]) -> SourceDatasetManifest:
    return SourceDatasetManifest(
        manifest_id=row["manifest_id"],
        dataset_id=row["dataset_id"],
        source_kind=row["source_kind"],
        entity_type=row["entity_type"],
        source_name=row["source_name"],
        report_type=row["report_type"],
        landing_form=row["landing_form"],
        view=row["view"],
        view_version=row["view_version"],
        copy_mode=row["copy_mode"],
        created_at=row["created_at"],
        status=row["status"],
        input_uri=row["input_uri"],
        created_by=row["created_by"],
        item_count_read=row["item_count_read"],
        item_count_written=row["item_count_written"],
        item_count_duplicate=row["item_count_duplicate"],
        item_count_rejected=row["item_count_rejected"],
        watermark_min_event_ts=row["watermark_min_event_ts"],
        watermark_max_event_ts=row["watermark_max_event_ts"],
        content_hash=row["content_hash"],
    )


def _row_to_item(row: dict[str, Any]) -> SourceDatasetItem:
    entity_key = row["entity_key"]
    if isinstance(entity_key, str):
        entity_key = json.loads(entity_key)
    return SourceDatasetItem(
        manifest_id=row["manifest_id"],
        item_index=row["item_index"],
        status=row["status"],
        source_name=row["source_name"],
        report_type=row["report_type"],
        entity_key=entity_key,
        report_ref=row["report_ref"],
        event_ts=row["event_ts"],
        content_hash=row["content_hash"],
        error=row["error"],
    )


class PostgresSourceDatasetStore:
    """Postgres-backed manifest + item store (caller owns the connection)."""

    def __init__(self, connection) -> None:
        self._connection = connection

    def upsert_manifest(self, manifest: SourceDatasetManifest) -> None:
        with self._connection.cursor() as cur:
            cur.execute(_UPSERT_MANIFEST_SQL, _manifest_to_params(manifest))
        self._connection.commit()

    def add_items(self, items: list[SourceDatasetItem]) -> None:
        if not items:
            return
        params = [_item_to_params(item) for item in items]
        with self._connection.cursor() as cur:
            cur.executemany(_INSERT_ITEM_SQL, params)
        self._connection.commit()

    def get_manifest(self, manifest_id: str) -> SourceDatasetManifest | None:
        sql = (
            f"SELECT {', '.join(_MANIFEST_COLUMNS)} FROM source_dataset_manifests "
            "WHERE manifest_id = %(manifest_id)s"
        )
        with self._connection.cursor() as cur:
            cur.execute(sql, {"manifest_id": manifest_id})
            row = cur.fetchone()
        return _row_to_manifest(row) if row is not None else None

    def list_items(self, manifest_id: str) -> list[SourceDatasetItem]:
        sql = (
            f"SELECT {', '.join(_ITEM_COLUMNS)} FROM source_dataset_items "
            "WHERE manifest_id = %(manifest_id)s ORDER BY item_index ASC"
        )
        with self._connection.cursor() as cur:
            cur.execute(sql, {"manifest_id": manifest_id})
            rows = cur.fetchall()
        return [_row_to_item(row) for row in rows]
