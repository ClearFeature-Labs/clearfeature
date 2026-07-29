"""Table feature import: load precomputed feature values from exported rows.

For migration / new-source cases where features are already computed in a DWH/SQL
system and exported to JSONL/CSV/(optional) Parquet. Each configured column is imported
as a feature *value* (already computed) — the runner never calls ComputeCore and never
recomputes. Every imported value gets a ``data_ts`` (per-row column or a default).

Milestone 1 has **no target/label/role semantics**: the platform imports configured
columns as feature values only. If a column happens to be a batch model score or a
target-like field, that is the caller's responsibility — it is just a feature value
here, and only columns registered as features in the view may be imported.
"""

from __future__ import annotations

import csv
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, field_validator

from fintech_feature_platform.api.backend import AppBackend
from fintech_feature_platform.api.backfill import (
    OFFLINE_ONLY,
    resolve_view,
    write_online_for_mode,
)
from fintech_feature_platform.fs_core.dedup import partition_new_results
from fintech_feature_platform.fs_core.events.models import (
    FEATURE_UPDATE_SOURCE_TABLE_IMPORT,
)
from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult
from fintech_feature_platform.fs_core.propagation import emit_feature_updates
from fintech_feature_platform.fs_core.write_guard import WRITE_OUTCOMES


class TableFeatureColumn(BaseModel):
    column: str
    dtype: str  # string | int | float | bool | json


class TableFeatureImportConfig(BaseModel):
    view: str
    view_version: int
    entity_columns: list[str]
    data_ts_column: str | None = None
    default_data_ts: datetime | None = None
    # Availability clock : when the platform "knew" the imported value. Lets
    # historical imports carry their true calc_ts so PIT strict availability
    # (calc_ts <= observation_ts) is meaningful for backtesting. Defaults to import time.
    calc_ts_column: str | None = None
    default_calc_ts: datetime | None = None
    features: dict[str, TableFeatureColumn]

    @field_validator("default_data_ts", "default_calc_ts")
    @classmethod
    def _require_timezone_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and (
            value.tzinfo is None or value.tzinfo.utcoffset(value) is None
        ):
            raise ValueError("timestamp must be timezone-aware")
        return value


@dataclass(frozen=True)
class TableFeatureImportRowResult:
    row_index: int
    status: str  # "ok" | "error"
    features_written: int
    error: str | None = None


@dataclass(frozen=True)
class TableFeatureImportSummary:
    run_id: str
    write_mode: str
    rows_read: int
    rows_skipped_blank: int
    rows_total: int
    rows_succeeded: int
    rows_failed: int
    features_candidate: int
    features_written: int
    duplicates_skipped: int
    missing_values: int
    online_written: int
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    # Propagation : values-free FeatureUpdated emitted for imported features with
    # reactive dependents. ``propagation_enqueue_status`` is "enqueued" | "none" | "failed"
    # so a publish failure is visible in run accounting (never silently lost).
    propagation_updates_emitted: int = 0
    propagation_enqueue_status: str = "none"
    expected_rows: int | None = None
    source_name: str | None = None
    source_file: str | None = None
    source_checksum: str | None = None
    errors: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    results: list[TableFeatureImportRowResult] = field(default_factory=list)


_VALID_DTYPES = ("string", "int", "float", "bool", "json")
_TRUE = {"true", "1", "yes"}
_FALSE = {"false", "0", "no"}


def _is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _convert(value: Any, dtype: str) -> Any:
    if dtype == "string":
        return str(value)
    if dtype == "int":
        return int(value)
    if dtype == "float":
        return float(value)
    if dtype == "bool":
        if isinstance(value, bool):
            return value
        token = str(value).strip().lower()
        if token in _TRUE:
            return True
        if token in _FALSE:
            return False
        raise ValueError(f"invalid bool value {value!r}")
    if dtype == "json":
        return json.loads(value) if isinstance(value, str) else value
    raise ValueError(f"unknown dtype {dtype!r}")


def _parse_data_ts(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        raise ValueError("data_ts must be timezone-aware")
    return parsed


def read_rows(path: str, fmt: str) -> list[dict[str, Any]]:
    """Read exported rows from JSONL/CSV/(optional) Parquet into a list of dicts."""
    if fmt == "jsonl":
        with open(path, encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]
    if fmt == "csv":
        with open(path, encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))
    if fmt == "parquet":
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:  # pragma: no cover - exercised only without pyarrow
            raise RuntimeError(
                "Parquet support requires optional dependency pyarrow"
            ) from exc
        return pq.read_table(path).to_pylist()
    raise ValueError(f"unknown format {fmt!r}")


def run_table_feature_import(
    *,
    backend: AppBackend,
    config: TableFeatureImportConfig,
    rows: Iterable[Mapping[str, Any]],
    write_mode: str = OFFLINE_ONLY,
    fail_fast: bool = False,
    run_id: str | None = None,
    expected_rows: int | None = None,
    source_name: str | None = None,
    source_file: str | None = None,
    source_checksum: str | None = None,
) -> TableFeatureImportSummary:
    """Import precomputed feature values from rows into offline (and optionally online).

    Exact-duplicate offline rows are skipped (idempotent rerun). Config errors (unknown
    feature, entity-column mismatch, no data_ts source, bad dtype) raise ``ValueError``
    up front. Row-level errors honour ``fail_fast``. ``expected_rows`` (if given) is
    compared to ``rows_total`` and yields an ``expected_rows_mismatch`` warning.
    """
    run_id = run_id or uuid4().hex
    started_at = datetime.now(tz=UTC)
    rows = list(rows)
    rows_read = len(rows)
    write_online = write_online_for_mode(write_mode)
    view = resolve_view(backend, config.view, config.view_version)
    features_by_name = {feature.name: feature for feature in view.features}

    # --- config validation (always raises; not per-row) ---
    if set(config.entity_columns) != set(view.key_fields):
        raise ValueError(
            f"entity_columns {config.entity_columns} must match view key_fields "
            f"{list(view.key_fields)}"
        )
    if not config.data_ts_column and config.default_data_ts is None:
        raise ValueError("data_ts_column or default_data_ts must be provided")
    versions: dict[str, int] = {}
    for name, column in config.features.items():
        feature = features_by_name.get(name)
        if feature is None:
            raise ValueError(f"unknown feature {name!r}")
        if column.dtype not in _VALID_DTYPES:
            raise ValueError(f"unknown dtype {column.dtype!r} for feature {name!r}")
        versions[name] = feature.feature_version

    results: list[TableFeatureImportRowResult] = []
    errors: list[dict[str, Any]] = []
    succeeded = 0
    failed = 0
    features_candidate = 0
    features_written = 0
    duplicates_skipped = 0
    missing_values = 0
    online_written = 0
    all_written: list[FeatureResult] = []

    for index, row in enumerate(rows):
        try:
            entity_key = EntityKey.from_mapping(
                {name: str(row[name]) for name in view.key_fields},
                key_order=view.key_fields,
            )
            data_ts = _row_data_ts(row, config)
            calc_ts = _row_calc_ts(row, config)

            row_results: list[FeatureResult] = []
            row_missing = 0
            for name, column in config.features.items():
                raw = row.get(column.column)
                if _is_missing(raw):
                    row_missing += 1
                    continue
                row_results.append(
                    FeatureResult(
                        ref=FeatureRef(name, versions[name]),
                        entity_key=entity_key,
                        value=_convert(raw, column.dtype),
                        data_ts=data_ts,
                        calc_ts=calc_ts,
                    )
                )
        except Exception as exc:  # noqa: BLE001 - collect row-level errors for migration
            if fail_fast:
                raise
            failed += 1
            errors.append({"row_index": index, "error": str(exc)})
            results.append(
                TableFeatureImportRowResult(
                    row_index=index, status="error", features_written=0, error=str(exc)
                )
            )
            continue

        features_candidate += len(row_results)
        # Skip exact-duplicate offline rows (idempotent rerun); write online only for new.
        new_results, row_dups = partition_new_results(
            backend.offline, view.name, view.view_version, row_results
        )
        duplicates_skipped += row_dups
        if new_results:
            backend.offline.append_many(view.name, view.view_version, new_results)
            all_written.extend(new_results)
            if write_online:
                written = backend.online.write_many(
                    view.name, view.view_version, new_results
                )
                online_written += sum(
                    1 for value in written.values() if value in WRITE_OUTCOMES
                )
        missing_values += row_missing
        features_written += len(new_results)
        succeeded += 1
        results.append(
            TableFeatureImportRowResult(
                row_index=index, status="ok", features_written=len(new_results)
            )
        )

    rows_total = succeeded + failed
    warnings: list[str] = []
    if expected_rows is not None and expected_rows != rows_total:
        warnings.append("expected_rows_mismatch")

    # Propagation : emit values-free FeatureUpdated for imported features with
    # reactive dependents, AFTER the durable offline append. This is a synchronous path, so
    # a publish failure is recorded visibly (propagation_enqueue_status=failed) rather than
    # silently lost — the imported rows are already durable and the wave can be replayed.
    propagation_updates_emitted = 0
    propagation_enqueue_status = "none"
    if all_written:
        try:
            propagation_updates_emitted = emit_feature_updates(
                publisher=backend.events,
                view_def=view,
                results=all_written,
                entity_type=view.entity,
                source=FEATURE_UPDATE_SOURCE_TABLE_IMPORT,
                run_id=run_id,
            )
            propagation_enqueue_status = (
                "enqueued" if propagation_updates_emitted else "none"
            )
        except Exception as exc:  # noqa: BLE001 - record visibly, do not claim success
            propagation_enqueue_status = "failed"
            warnings.append(f"propagation_enqueue_failed: {exc}")

    finished_at = datetime.now(tz=UTC)

    return TableFeatureImportSummary(
        run_id=run_id,
        write_mode=write_mode,
        rows_read=rows_read,
        rows_skipped_blank=0,
        rows_total=rows_total,
        rows_succeeded=succeeded,
        rows_failed=failed,
        features_candidate=features_candidate,
        features_written=features_written,
        duplicates_skipped=duplicates_skipped,
        missing_values=missing_values,
        online_written=online_written,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=int((finished_at - started_at).total_seconds() * 1000),
        propagation_updates_emitted=propagation_updates_emitted,
        propagation_enqueue_status=propagation_enqueue_status,
        expected_rows=expected_rows,
        source_name=source_name,
        source_file=source_file,
        source_checksum=source_checksum,
        errors=errors,
        warnings=warnings,
        results=results,
    )


def _row_data_ts(row: Mapping[str, Any], config: TableFeatureImportConfig) -> datetime:
    # Prefer the per-row column; fall back to the configured default.
    if config.data_ts_column and not _is_missing(row.get(config.data_ts_column)):
        return _parse_data_ts(row[config.data_ts_column])
    if config.default_data_ts is not None:
        return config.default_data_ts
    raise ValueError("missing data_ts for row (no column value and no default)")


def _row_calc_ts(row: Mapping[str, Any], config: TableFeatureImportConfig) -> datetime:
    # Availability clock: per-row column, then configured default, then import time.
    # Falling back to now() preserves legacy behavior for imports that do not set it.
    if config.calc_ts_column and not _is_missing(row.get(config.calc_ts_column)):
        return _parse_data_ts(row[config.calc_ts_column])
    if config.default_calc_ts is not None:
        return config.default_calc_ts
    return datetime.now(tz=UTC)
