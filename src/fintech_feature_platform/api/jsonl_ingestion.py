"""JSONL raw-report ingestion into landing form (a), the platform design rule.

Reads raw reports one JSON object per line and lands each into the canonical form:
the payload goes to the raw payload store, its metadata to ``raw_reports_meta``, and a
reference (``report_ref``) into a ``SourceDatasetManifest`` + item index. It **never**
computes features and **never** sends payloads through Kafka — a later batch job
 operates on the ``report_refs`` this run records.

Idempotency is content-addressed: a row's ``report_ref`` (when not supplied) is derived
from ``(entity_key, source_name, report_type, content_hash)``, so an identical rerun
re-derives the same ref, the ``raw_reports_meta`` upsert is a no-op (counted duplicate),
and no new report/meta row appears. A supplied ``report_ref`` re-used with different
content is a structural conflict and the row is rejected — never silently overwritten.
Only ``copy_mode="copy"`` is supported in this slice.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, field_validator

from fintech_feature_platform.api.backend import AppBackend
from fintech_feature_platform.fs_core.models import EntityKey, RawReportMeta
from fintech_feature_platform.fs_core.raw.meta_repository import (
    CORRECTION_ORIGIN_DWH,
    CORRECTION_ORIGIN_JSONL,
    CorrectionContext,
    RawReportMetaConflictError,
)
from fintech_feature_platform.fs_core.stores.source_dataset import (
    COPY,
    ITEM_DUPLICATE,
    ITEM_REJECTED,
    ITEM_WRITTEN,
    LANDING_RAW_REPORTS,
    REGISTER_IN_PLACE,
    SOURCE_KIND_OBJECT_STORAGE_JSONL,
    STATUS_COMPLETED,
    SourceDatasetItem,
    SourceDatasetManifest,
)


class JsonlReportRow(BaseModel):
    """One JSONL ingestion row. ``source_name``/``report_type`` default from the run.

    ``available_at`` : when this fact became available to the
    bank — TRUSTED input on this operator-controlled path. Absent -> ingestion time,
    recorded as such. It is never defaulted from ``event_ts`` (business time), must be
    timezone-aware, and must not precede ``event_ts`` (a fact cannot be available
    before its as-of time).
    """

    entity_key: dict[str, str]
    event_ts: datetime
    payload: Any
    report_ref: str | None = None
    source_name: str | None = None
    report_type: str | None = None
    available_at: datetime | None = None

    @field_validator("event_ts", "available_at")
    @classmethod
    def _require_timezone_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and (
            value.tzinfo is None or value.tzinfo.utcoffset(value) is None
        ):
            raise ValueError("must be timezone-aware")
        return value

    @field_validator("entity_key")
    @classmethod
    def _require_non_empty_key(cls, value: dict[str, str]) -> dict[str, str]:
        if not value:
            raise ValueError("entity_key must be non-empty")
        return value


def _content_hash(serialized: bytes) -> str:
    return "sha256:" + hashlib.sha256(serialized).hexdigest()


def _derive_report_ref(
    entity_encoded: str, source_name: str, report_type: str, content_hash: str
) -> str:
    # Deterministic, content-addressed: identical logical reports -> identical ref, so a
    # rerun dedups via the raw_reports_meta upsert instead of creating a new report.
    material = "|".join([entity_encoded, source_name, report_type, content_hash])
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return f"rep_{digest[:32]}"


@dataclass(frozen=True)
class _PreparedRow:
    """One input row prepared for landing: a parsed report OR a parse error."""

    row: JsonlReportRow | None
    error: str | None


def _iter_jsonl(lines: Iterable[str]) -> Iterator[_PreparedRow]:
    """Prepare JSONL lines: skip blanks, parse each into a row or a rejection reason."""
    for line in lines:
        if not line.strip():
            continue
        try:
            yield _PreparedRow(JsonlReportRow.model_validate(json.loads(line)), None)
        except Exception as exc:  # noqa: BLE001 - malformed row -> rejected, not fatal
            yield _PreparedRow(None, str(exc))


def land_report_rows(
    *,
    backend: AppBackend,
    prepared: Iterable[_PreparedRow],
    source_kind: str,
    entity_type: str,
    source_name: str,
    report_type: str,
    copy_mode: str = COPY,
    dataset_id: str | None = None,
    input_uri: str | None = None,
    created_by: str | None = None,
    fail_fast: bool = False,
) -> SourceDatasetManifest:
    """Land prepared report rows into form (a); return the run manifest.

    Shared by JSONL  and DWH-JSON  ingestion — the only difference
    between them is how the input rows are prepared. A prepared row carrying a parse
    ``error`` is recorded as a rejected item; a valid row is deduped/landed. Bad rows do
    not abort the run unless ``fail_fast``. Raises ``ValueError`` for an unsupported
    ``copy_mode``.
    """
    if copy_mode == REGISTER_IN_PLACE:
        raise ValueError(
            "copy_mode='register_in_place' is not supported yet (copy-only)"
        )
    if copy_mode != COPY:
        raise ValueError(f"unknown copy_mode {copy_mode!r}")

    manifest_id = f"sdm_{uuid4().hex}"
    dataset_id = dataset_id or f"ds_{uuid4().hex}"
    created_at = datetime.now(tz=UTC)
    # Origin evidence for availability corrections : the only
    # correction-capable paths are the content-addressed ingestion runs.
    correction_context = CorrectionContext(
        change_origin=(
            CORRECTION_ORIGIN_JSONL
            if source_kind == SOURCE_KIND_OBJECT_STORAGE_JSONL
            else CORRECTION_ORIGIN_DWH
        ),
        manifest_id=manifest_id,
    )

    items: list[SourceDatasetItem] = []
    read = written = duplicate = rejected = 0
    min_ts: datetime | None = None
    max_ts: datetime | None = None
    written_hashes: list[str] = []

    def _reject(item_index: int, reason: str) -> None:
        nonlocal rejected
        rejected += 1
        items.append(
            SourceDatasetItem(
                manifest_id=manifest_id, item_index=item_index, status=ITEM_REJECTED,
                source_name=source_name, report_type=report_type, error=reason,
            )
        )

    for prepared_row in prepared:
        item_index = read
        read += 1
        if prepared_row.error is not None:
            _reject(item_index, prepared_row.error)
            if fail_fast:
                raise ValueError(prepared_row.error)
            continue
        row = prepared_row.row
        try:
            src = row.source_name or source_name
            rtype = row.report_type or report_type
            entity_key = EntityKey.from_mapping(row.entity_key)
            serialized = backend.serialize_payload(row.payload)
            content_hash = _content_hash(serialized)
            report_ref = row.report_ref or _derive_report_ref(
                entity_key.encode(), src, rtype, content_hash
            )
            storage_uri = backend.make_storage_uri(rtype, report_ref, row.event_ts)
            # Availability : trusted source-provided value on this
            # operator path, else the ingestion time — NEVER event_ts. RawReportMeta
            # validation rejects available_at < report_ts (impossible contract).
            if row.available_at is not None:
                effective_available_at = row.available_at
                availability_source = "source_provided"
            else:
                effective_available_at = created_at
                availability_source = "ingestion_time"
            meta = RawReportMeta(
                report_ref=report_ref,
                report_type=rtype,
                entity_type=entity_type,
                entity_key=entity_key,
                report_ts=row.event_ts,
                payload_size_bytes=len(serialized),
                content_hash=content_hash,
                storage_uri=storage_uri,
                created_at=created_at,
                format=backend.raw_format,
                compression=backend.raw_compression,
                available_at=effective_available_at,
                availability_source=availability_source,
            )
            # Meta first: a report_ref reused with different content raises here, BEFORE
            # any payload write, so a conflicting payload can never overwrite the stored
            # one. Payload PUT is content-addressed (idempotent) for new and duplicate.
            is_new = backend.metas.upsert(meta, correction_context=correction_context)
            backend.payloads.put(storage_uri, row.payload)
        except RawReportMetaConflictError as exc:
            _reject(item_index, f"report_ref conflict: {exc}")
            if fail_fast:
                raise
            continue
        except Exception as exc:  # noqa: BLE001 - bad row: reject, do not abort the run
            _reject(item_index, str(exc))
            if fail_fast:
                raise
            continue

        status = ITEM_WRITTEN if is_new else ITEM_DUPLICATE
        if is_new:
            written += 1
        else:
            duplicate += 1
        written_hashes.append(content_hash)
        min_ts = row.event_ts if min_ts is None else min(min_ts, row.event_ts)
        max_ts = row.event_ts if max_ts is None else max(max_ts, row.event_ts)
        items.append(
            SourceDatasetItem(
                manifest_id=manifest_id, item_index=item_index, status=status,
                source_name=src, report_type=rtype, entity_key=dict(row.entity_key),
                report_ref=report_ref, event_ts=row.event_ts, content_hash=content_hash,
            )
        )

    # Dataset fingerprint: identical inputs -> identical hash across reruns.
    fingerprint = hashlib.sha256(
        json.dumps(sorted(written_hashes)).encode("utf-8")
    ).hexdigest()

    manifest = SourceDatasetManifest(
        manifest_id=manifest_id,
        dataset_id=dataset_id,
        source_kind=source_kind,
        entity_type=entity_type,
        source_name=source_name,
        report_type=report_type,
        landing_form=LANDING_RAW_REPORTS,
        copy_mode=copy_mode,
        created_at=created_at,
        status=STATUS_COMPLETED,
        input_uri=input_uri,
        created_by=created_by,
        item_count_read=read,
        item_count_written=written,
        item_count_duplicate=duplicate,
        item_count_rejected=rejected,
        watermark_min_event_ts=min_ts,
        watermark_max_event_ts=max_ts,
        content_hash="sha256:" + fingerprint,
    )
    backend.source_datasets.upsert_manifest(manifest)
    backend.source_datasets.add_items(items)
    return manifest


def run_jsonl_ingestion(
    *,
    backend: AppBackend,
    lines: Iterable[str],
    entity_type: str,
    source_name: str,
    report_type: str,
    dataset_id: str | None = None,
    copy_mode: str = COPY,
    input_uri: str | None = None,
    created_by: str | None = None,
    fail_fast: bool = False,
) -> SourceDatasetManifest:
    """Ingest raw reports from JSONL into landing form (a); return the run manifest."""
    return land_report_rows(
        backend=backend,
        prepared=_iter_jsonl(lines),
        source_kind=SOURCE_KIND_OBJECT_STORAGE_JSONL,
        entity_type=entity_type,
        source_name=source_name,
        report_type=report_type,
        dataset_id=dataset_id,
        copy_mode=copy_mode,
        input_uri=input_uri,
        created_by=created_by,
        fail_fast=fail_fast,
    )
