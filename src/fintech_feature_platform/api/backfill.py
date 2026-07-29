"""Offline-only / offline+online raw JSON backfill runner (idempotent reruns).

Loads historical raw reports (one JSON object per line) and, for each row, persists the
raw payload + metadata and appends offline feature history via the shared
``persist_and_compute`` helper. Exact-duplicate offline rows are skipped
(``skip_duplicate_offline=True``), so reruns are safe; corrections (same data_ts, new
value) and new data_ts values are still appended. ``write_mode`` controls whether online
latest is also updated (default offline-only).

Each run produces a ``RawBackfillSummary`` with a ``run_id`` plus run + input accounting
(rows_read/blank/total, features_candidate/written, duplicates_skipped, timings,
warnings). It is a migration helper, not a scheduler: no retries, no state machine. Note:
reruns may still write new raw payload objects + metadata rows — only offline feature
rows are deduplicated. Backend-agnostic (memory or local) via ``AppBackend``.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import BaseModel

from fintech_feature_platform.api.backend import AppBackend
from fintech_feature_platform.api.direct_compute import InlineSource, persist_and_compute
from fintech_feature_platform.fs_core.models import EntityKey
from fintech_feature_platform.fs_core.registry.models import FeatureViewDef
from fintech_feature_platform.fs_core.write_guard import WRITE_OUTCOMES

OFFLINE_ONLY = "offline_only"
OFFLINE_AND_ONLINE = "offline_and_online"
WRITE_MODES = (OFFLINE_ONLY, OFFLINE_AND_ONLINE)


def write_online_for_mode(write_mode: str) -> bool:
    """Map a write_mode to the ``write_online`` flag; raise on an unknown mode."""
    if write_mode not in WRITE_MODES:
        raise ValueError(f"unknown write_mode {write_mode!r}")
    return write_mode == OFFLINE_AND_ONLINE


def resolve_view(backend: AppBackend, view: str, view_version: int) -> FeatureViewDef:
    for candidate in backend.registry.feature_views:
        if candidate.name == view and candidate.view_version == view_version:
            return candidate
    raise ValueError(f"unknown view {view!r} version {view_version}")


class RawBackfillItem(BaseModel):
    """One parsed backfill row: an entity with one or more inline source payloads."""

    entity: dict[str, str]
    inline_sources: dict[str, InlineSource]
    context: dict[str, Any] | None = None


@dataclass(frozen=True)
class RawBackfillResult:
    row_index: int
    status: str  # "ok" | "error"
    report_refs: dict[str, str]
    features_written: int
    error: str | None = None


@dataclass(frozen=True)
class RawBackfillSummary:
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
    online_written: int
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    expected_rows: int | None = None
    source_name: str | None = None
    source_file: str | None = None
    source_checksum: str | None = None
    errors: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    results: list[RawBackfillResult] = field(default_factory=list)


def _parse_row(line: str) -> RawBackfillItem:
    # json.loads raises ValueError on malformed JSON; model_validate raises on bad rows
    # (e.g. naive report_ts). Both are caught per row by the runner.
    return RawBackfillItem.model_validate(json.loads(line))


def run_raw_json_backfill(
    *,
    backend: AppBackend,
    view: str,
    view_version: int,
    requested_features: list[str],
    lines: Iterable[str],
    write_mode: str = OFFLINE_ONLY,
    fail_fast: bool = False,
    run_id: str | None = None,
    expected_rows: int | None = None,
    source_name: str | None = None,
    source_file: str | None = None,
    source_checksum: str | None = None,
) -> RawBackfillSummary:
    """Backfill offline feature history from raw JSONL rows (idempotent on rerun).

    ``view``/``view_version``/``requested_features``/``write_mode`` are global. Exact
    duplicate offline rows are skipped. ``expected_rows`` (if given) is compared to
    ``rows_total`` (non-blank records) and yields an ``expected_rows_mismatch`` warning.
    """
    run_id = run_id or uuid4().hex
    write_online = write_online_for_mode(write_mode)
    resolved_view = resolve_view(backend, view, view_version)
    started_at = datetime.now(tz=UTC)

    results: list[RawBackfillResult] = []
    errors: list[dict[str, Any]] = []
    rows_read = 0
    rows_skipped_blank = 0
    succeeded = 0
    failed = 0
    features_candidate = 0
    features_written = 0
    duplicates_skipped = 0
    online_written = 0

    for line in lines:
        rows_read += 1
        if not line.strip():
            rows_skipped_blank += 1
            continue
        index = succeeded + failed
        try:
            item = _parse_row(line)
            entity_key = EntityKey.from_mapping(
                {name: item.entity[name] for name in resolved_view.key_fields},
                key_order=resolved_view.key_fields,
            )
            outcome, report_refs = persist_and_compute(
                backend,
                resolved_view,
                entity_key,
                item.inline_sources,
                requested_features,
                write_online=write_online,
                skip_duplicate_offline=True,
            )
        except Exception as exc:  # noqa: BLE001 - collect row-level errors for migration
            if fail_fast:
                raise
            failed += 1
            errors.append({"row_index": index, "error": str(exc)})
            results.append(
                RawBackfillResult(
                    row_index=index, status="error", report_refs={},
                    features_written=0, error=str(exc),
                )
            )
            continue

        computed = len(outcome.results)
        written = computed - outcome.duplicates_skipped
        features_candidate += computed
        features_written += written
        duplicates_skipped += outcome.duplicates_skipped
        online_written += sum(
            1 for w in outcome.online_written.values() if w in WRITE_OUTCOMES
        )
        succeeded += 1
        results.append(
            RawBackfillResult(
                row_index=index, status="ok", report_refs=report_refs,
                features_written=written,
            )
        )

    rows_total = succeeded + failed
    warnings: list[str] = []
    if expected_rows is not None and expected_rows != rows_total:
        warnings.append("expected_rows_mismatch")
    finished_at = datetime.now(tz=UTC)

    return RawBackfillSummary(
        run_id=run_id,
        write_mode=write_mode,
        rows_read=rows_read,
        rows_skipped_blank=rows_skipped_blank,
        rows_total=rows_total,
        rows_succeeded=succeeded,
        rows_failed=failed,
        features_candidate=features_candidate,
        features_written=features_written,
        duplicates_skipped=duplicates_skipped,
        online_written=online_written,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=int((finished_at - started_at).total_seconds() * 1000),
        expected_rows=expected_rows,
        source_name=source_name,
        source_file=source_file,
        source_checksum=source_checksum,
        errors=errors,
        warnings=warnings,
        results=results,
    )
