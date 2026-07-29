"""Postgres-backed implementation of the raw report metadata repository.

Implements ``RawReportMetaRepository.get_meta(report_ref)`` plus ``add(meta)``
against a ``raw_reports_meta`` table (see ``infra/postgres/001_raw_reports_meta.sql``).
``EntityKey`` is stored as an ordered JSONB array of ``[name, value]`` pairs and
datetimes as ``TIMESTAMPTZ``.

``psycopg`` (v3) is imported lazily (only in ``connect_postgres``), so this module
imports — and its unit tests run — without the optional ``postgres`` extra. A missing
``report_ref`` raises ``KeyError`` (uniform with ``InMemoryMetaRepository`` so
``ReportResolver`` is unchanged); a malformed row raises ``ValueError``. ``report_ref``
is the public lookup key; ``storage_uri`` is the internal payload pointer in the row.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from fintech_feature_platform.fs_core.models import EntityKey, RawReportMeta
from fintech_feature_platform.fs_core.raw.meta_repository import (
    CorrectionContext,
    RawReportMetaConflictError,
    build_availability_change,
    is_availability_correction,
    raw_meta_conflicts,
)

_AUDIT_INSERT_SQL = """
INSERT INTO raw_report_availability_changes (
    report_ref, old_available_at, new_available_at,
    old_availability_source, new_availability_source,
    changed_at, change_origin, manifest_id, request_id, actor_key_id
) VALUES (
    %(report_ref)s, %(old_available_at)s, %(new_available_at)s,
    %(old_availability_source)s, %(new_availability_source)s,
    %(changed_at)s, %(change_origin)s, %(manifest_id)s, %(request_id)s,
    %(actor_key_id)s
)
"""

_COLUMNS = (
    "report_ref",
    "report_type",
    "entity_type",
    "entity_key",
    "report_ts",
    "payload_size_bytes",
    "content_hash",
    "storage_uri",
    "format",
    "compression",
    "created_at",
    "available_at",
    "availability_source",
)

_SELECT_SQL = (
    f"SELECT {', '.join(_COLUMNS)} "
    "FROM raw_reports_meta WHERE report_ref = %(report_ref)s"
)

_UPSERT_SQL = """
INSERT INTO raw_reports_meta (
    report_ref, report_type, entity_type, entity_key, report_ts,
    payload_size_bytes, content_hash, storage_uri, format, compression, created_at,
    available_at, availability_source
) VALUES (
    %(report_ref)s, %(report_type)s, %(entity_type)s, %(entity_key)s::jsonb, %(report_ts)s,
    %(payload_size_bytes)s, %(content_hash)s, %(storage_uri)s, %(format)s,
    %(compression)s, %(created_at)s, %(available_at)s, %(availability_source)s
)
ON CONFLICT (report_ref) DO UPDATE SET
    report_type = EXCLUDED.report_type,
    entity_type = EXCLUDED.entity_type,
    entity_key = EXCLUDED.entity_key,
    report_ts = EXCLUDED.report_ts,
    payload_size_bytes = EXCLUDED.payload_size_bytes,
    content_hash = EXCLUDED.content_hash,
    storage_uri = EXCLUDED.storage_uri,
    format = EXCLUDED.format,
    compression = EXCLUDED.compression,
    created_at = EXCLUDED.created_at,
    available_at = EXCLUDED.available_at,
    availability_source = EXCLUDED.availability_source
"""

_INSERT_SQL = """
INSERT INTO raw_reports_meta (
    report_ref, report_type, entity_type, entity_key, report_ts,
    payload_size_bytes, content_hash, storage_uri, format, compression, created_at,
    available_at, availability_source
) VALUES (
    %(report_ref)s, %(report_type)s, %(entity_type)s, %(entity_key)s::jsonb, %(report_ts)s,
    %(payload_size_bytes)s, %(content_hash)s, %(storage_uri)s, %(format)s,
    %(compression)s, %(created_at)s, %(available_at)s, %(availability_source)s
)
"""


def connect_postgres(conninfo: str = ""):
    """Connect to Postgres. Imports the ``psycopg`` SDK lazily (``postgres`` extra)."""
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(conninfo, row_factory=dict_row)


class PostgresRawReportMetaRepository:
    """Reads/writes ``RawReportMeta`` rows; ``add`` is an upsert on ``report_ref``."""

    def __init__(self, connection) -> None:
        self._connection = connection

    def get_meta(self, report_ref: str) -> RawReportMeta:
        with self._connection.cursor() as cur:
            cur.execute(_SELECT_SQL, {"report_ref": report_ref})
            row = cur.fetchone()
        if row is None:
            raise KeyError(report_ref)
        return _row_to_meta(row)

    def add(self, meta: RawReportMeta) -> None:
        with self._connection.cursor() as cur:
            cur.execute(_UPSERT_SQL, _meta_to_params(meta))
        self._connection.commit()

    def upsert(
        self,
        meta: RawReportMeta,
        *,
        correction_context: CorrectionContext | None = None,
    ) -> bool:
        """Idempotent projection write: insert if absent, no-op if identical, raise on
        a conflicting ``report_ref`` (immutable identity)."""
        try:
            existing = self.get_meta(meta.report_ref)
        except KeyError:
            existing = None
        if existing is None:
            with self._connection.cursor() as cur:
                cur.execute(_INSERT_SQL, _meta_to_params(meta))
            self._connection.commit()
            return True
        if raw_meta_conflicts(existing, meta):
            raise RawReportMetaConflictError(meta.report_ref)
        if is_availability_correction(existing, meta):
            # Availability correction : identity unchanged,
            # trusted available_at differs -> update the availability fields in
            # place (first-ingestion created_at preserved) AND write exactly one
            # append-only audit row — atomically, in ONE transaction (a failure
            # before commit leaves neither the update nor the audit row).
            change = build_availability_change(existing, meta, correction_context)
            with self._connection.cursor() as cur:
                cur.execute(
                    "UPDATE raw_reports_meta SET available_at = %(available_at)s, "
                    "availability_source = %(availability_source)s "
                    "WHERE report_ref = %(report_ref)s",
                    {
                        "available_at": meta.available_at,
                        "availability_source": meta.availability_source,
                        "report_ref": meta.report_ref,
                    },
                )
                cur.execute(
                    _AUDIT_INSERT_SQL,
                    {
                        "report_ref": change.report_ref,
                        "old_available_at": change.old_available_at,
                        "new_available_at": change.new_available_at,
                        "old_availability_source": change.old_availability_source,
                        "new_availability_source": change.new_availability_source,
                        "changed_at": change.changed_at,
                        "change_origin": change.change_origin,
                        "manifest_id": change.manifest_id,
                        "request_id": change.request_id,
                        "actor_key_id": change.actor_key_id,
                    },
                )
            self._connection.commit()
            return True
        return False


def _meta_to_params(meta: RawReportMeta) -> dict[str, Any]:
    return {
        "report_ref": meta.report_ref,
        "report_type": meta.report_type,
        "entity_type": meta.entity_type,
        "entity_key": json.dumps([[name, value] for name, value in meta.entity_key.parts]),
        "report_ts": meta.report_ts,
        "payload_size_bytes": meta.payload_size_bytes,
        "content_hash": meta.content_hash,
        "storage_uri": meta.storage_uri,
        "format": meta.format,
        "compression": meta.compression,
        "created_at": meta.created_at,
        "available_at": meta.available_at,
        "availability_source": meta.availability_source,
    }


def _row_to_meta(row: Mapping[str, Any]) -> RawReportMeta:
    try:
        parts = row["entity_key"]
        if isinstance(parts, str):
            parts = json.loads(parts)
        entity_key = EntityKey(tuple((str(p[0]), str(p[1])) for p in parts))
        return RawReportMeta(
            report_ref=row["report_ref"],
            report_type=row["report_type"],
            entity_type=row["entity_type"],
            entity_key=entity_key,
            report_ts=row["report_ts"],
            payload_size_bytes=row["payload_size_bytes"],
            content_hash=row["content_hash"],
            storage_uri=row["storage_uri"],
            created_at=row["created_at"],
            format=row["format"],
            compression=row["compression"],
            # Nullable availability clock (legacy rows have none).
            available_at=row.get("available_at"),
            availability_source=row.get("availability_source"),
        )
    except (KeyError, TypeError, ValueError, IndexError) as exc:
        raise ValueError(f"malformed raw_reports_meta row: {exc}") from exc
