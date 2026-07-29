"""DWH extraction / materialization into the two canonical landing forms.

A DWH is an **ingestion boundary, not a compute runtime** (invariant I5): this module
reads already-extracted DWH rows (via the ``DwhReader`` seam) and materializes them into
platform storage. Online and batch workers never call a ``DwhReader`` and never run SQL.

Two paths:

- ``run_dwh_json_extraction`` — DWH rows with a JSON column -> landing form (a): reuses
  the raw-report landing (raw payload store + ``raw_reports_meta`` +
  ``SourceDatasetManifest`` + ``report_refs`` + content_hash dedup).
- ``run_dwh_feature_import`` — DWH rows of precomputed feature values -> landing form
  (b): validates + dedups into offline feature history, recording a
  ``SourceDatasetManifest`` (landing_form=feature_rows) for run accounting. Imported rows
  carry real ``data_ts``/``calc_ts`` so the PIT builder's safety_gap/availability
   applies unchanged.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, field_validator

from fintech_feature_platform.api.backend import AppBackend
from fintech_feature_platform.api.jsonl_ingestion import (
    JsonlReportRow,
    _PreparedRow,
    land_report_rows,
)
from fintech_feature_platform.fs_core.dedup import partition_new_results
from fintech_feature_platform.fs_core.dwh.reader import DwhReader
from fintech_feature_platform.fs_core.events.models import (
    FEATURE_UPDATE_SOURCE_DWH_IMPORT,
)
from fintech_feature_platform.fs_core.hashing import value_hash
from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult
from fintech_feature_platform.fs_core.propagation import emit_feature_updates
from fintech_feature_platform.fs_core.stores.source_dataset import (
    COPY,
    LANDING_FEATURE_ROWS,
    SOURCE_KIND_DWH_JSON_REPORTS,
    SOURCE_KIND_DWH_TABLE,
    STATUS_COMPLETED,
    SourceDatasetManifest,
)

# --- DWH JSON reports -> landing form (a) ------------------------------------

class DwhJsonConfig(BaseModel):
    """Config for extracting DWH rows with a JSON column into raw reports (form a)."""

    entity_type: str
    source_name: str
    report_type: str
    query_name: str  # named source query/table; also recorded as input_uri
    dataset_id: str | None = None
    entity_key_column: str = "entity_key"
    event_ts_column: str = "event_ts"
    payload_column: str = "payload_json"
    # Optional column carrying trusted historical availability (this is an
    # operator-controlled extraction path). Absent column/value -> ingestion time.
    available_at_column: str = "available_at"


def _prepare_dwh_json_rows(reader: DwhReader, config: DwhJsonConfig):
    """Map DWH JSON-column rows to prepared report rows (a parsed row or a reason)."""
    for raw in reader.read_rows(config.query_name):
        try:
            normalized = {
                "entity_key": raw[config.entity_key_column],
                "event_ts": raw[config.event_ts_column],
                "payload": raw[config.payload_column],
            }
            if raw.get(config.available_at_column) is not None:
                normalized["available_at"] = raw[config.available_at_column]
            yield _PreparedRow(JsonlReportRow.model_validate(normalized), None)
        except Exception as exc:  # noqa: BLE001 - bad DWH row -> rejected, not fatal
            yield _PreparedRow(None, str(exc))


def run_dwh_json_extraction(
    *,
    backend: AppBackend,
    reader: DwhReader,
    config: DwhJsonConfig,
    fail_fast: bool = False,
) -> SourceDatasetManifest:
    """Extract DWH JSON-column rows into landing form (a); return the run manifest."""
    return land_report_rows(
        backend=backend,
        prepared=_prepare_dwh_json_rows(reader, config),
        source_kind=SOURCE_KIND_DWH_JSON_REPORTS,
        entity_type=config.entity_type,
        source_name=config.source_name,
        report_type=config.report_type,
        copy_mode=COPY,
        dataset_id=config.dataset_id,
        input_uri=config.query_name,
        fail_fast=fail_fast,
    )


# --- DWH feature rows -> landing form (b) ------------------------------------

class DwhFeatureRow(BaseModel):
    """One tall DWH feature row: (entity, feature, value, data_ts, calc_ts)."""

    entity_key: dict[str, str]
    feature_name: str
    feature_version: int
    value: Any
    data_ts: datetime
    calc_ts: datetime

    @field_validator("data_ts", "calc_ts")
    @classmethod
    def _require_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise ValueError("data_ts/calc_ts must be timezone-aware")
        return value

    @field_validator("entity_key")
    @classmethod
    def _require_non_empty_key(cls, value: dict[str, str]) -> dict[str, str]:
        if not value:
            raise ValueError("entity_key must be non-empty")
        return value


class DwhFeatureConfig(BaseModel):
    """Config for importing DWH tall feature rows into offline history (form b)."""

    entity_type: str
    view: str
    view_version: int
    query_name: str
    dataset_id: str | None = None
    source_name: str | None = None
    entity_key_column: str = "entity_key"
    feature_name_column: str = "feature_name"
    feature_version_column: str = "feature_version"
    value_column: str = "value"
    data_ts_column: str = "data_ts"
    calc_ts_column: str = "calc_ts"


def run_dwh_feature_import(
    *,
    backend: AppBackend,
    reader: DwhReader,
    config: DwhFeatureConfig,
    fail_fast: bool = False,
) -> SourceDatasetManifest:
    """Import DWH tall feature rows into offline history (form b); return the manifest.

    Rows are validated (tz-aware ``data_ts``/``calc_ts``, feature registered in the view
    at the given version) and deduped against offline history, so an identical rerun is a
    no-op. Bad rows are rejected (counted) and never abort the run unless ``fail_fast``.
    """
    view_def = _resolve_view(backend, config.view, config.view_version)
    declared = {f.name: f.feature_version for f in view_def.features}
    created_at = datetime.now(tz=UTC)

    read = rejected = 0
    valid_results: list[FeatureResult] = []
    written_material: list[str] = []
    min_ts: datetime | None = None
    max_ts: datetime | None = None

    for raw in reader.read_rows(config.query_name):
        read += 1
        try:
            row = DwhFeatureRow.model_validate(
                {
                    "entity_key": raw[config.entity_key_column],
                    "feature_name": raw[config.feature_name_column],
                    "feature_version": raw[config.feature_version_column],
                    "value": raw[config.value_column],
                    "data_ts": raw[config.data_ts_column],
                    "calc_ts": raw[config.calc_ts_column],
                }
            )
            if row.feature_name not in declared:
                raise ValueError(f"unknown feature {row.feature_name!r} in view")
            if declared[row.feature_name] != row.feature_version:
                raise ValueError(
                    f"feature {row.feature_name!r} version {row.feature_version} != "
                    f"registry version {declared[row.feature_name]}"
                )
            entity_key = EntityKey.from_mapping(
                {k: row.entity_key[k] for k in view_def.key_fields},
                key_order=view_def.key_fields,
            )
            result = FeatureResult(
                ref=FeatureRef(row.feature_name, row.feature_version),
                entity_key=entity_key,
                value=row.value,
                data_ts=row.data_ts,
                calc_ts=row.calc_ts,
                value_hash=value_hash(row.value),
            )
        except Exception as exc:  # noqa: BLE001 - bad row: reject, do not abort
            rejected += 1
            if fail_fast:
                raise ValueError(str(exc)) from exc
            continue
        valid_results.append(result)
        written_material.append("|".join([
            entity_key.encode(), result.ref.encode(),
            row.data_ts.isoformat(), result.value_hash,
        ]))
        min_ts = row.data_ts if min_ts is None else min(min_ts, row.data_ts)
        max_ts = row.data_ts if max_ts is None else max(max_ts, row.data_ts)

    # Dedup against offline history (and within this run): idempotent rerun.
    new_results, duplicates = partition_new_results(
        backend.offline, config.view, config.view_version, valid_results
    )
    backend.offline.append_many(config.view, config.view_version, new_results)

    # Landing form (b) imports may have dependents : emit
    # values-free FeatureUpdated for imported features with reactive dependents, AFTER the
    # durable offline append. This is a synchronous path with no per-run status object, so a
    # publish failure fails the run clearly rather than silently losing propagation (the
    # rows are already durable; an idempotent rerun re-emits and completes).
    try:
        emit_feature_updates(
            publisher=backend.events,
            view_def=view_def,
            results=new_results,
            entity_type=config.entity_type,
            source=FEATURE_UPDATE_SOURCE_DWH_IMPORT,
        )
    except Exception as exc:  # noqa: BLE001 - do not claim propagation on publish failure
        raise ValueError(f"feature_updates publish failed: {exc}") from exc

    fingerprint = hashlib.sha256(
        json.dumps(sorted(written_material)).encode("utf-8")
    ).hexdigest()
    manifest = SourceDatasetManifest(
        manifest_id=f"sdm_{uuid4().hex}",
        dataset_id=config.dataset_id or f"ds_{uuid4().hex}",
        source_kind=SOURCE_KIND_DWH_TABLE,
        entity_type=config.entity_type,
        source_name=config.source_name or config.query_name,
        report_type=None,
        landing_form=LANDING_FEATURE_ROWS,
        view=config.view,
        view_version=config.view_version,
        copy_mode=COPY,
        created_at=created_at,
        status=STATUS_COMPLETED,
        input_uri=config.query_name,
        item_count_read=read,
        item_count_written=len(new_results),
        item_count_duplicate=duplicates,
        item_count_rejected=rejected,
        watermark_min_event_ts=min_ts,
        watermark_max_event_ts=max_ts,
        content_hash="sha256:" + fingerprint,
    )
    backend.source_datasets.upsert_manifest(manifest)
    return manifest


def _resolve_view(backend: AppBackend, view: str, view_version: int):
    for candidate in backend.registry.feature_views:
        if candidate.name == view and candidate.view_version == view_version:
            return candidate
    raise ValueError(f"unknown view {view!r} version {view_version}")
