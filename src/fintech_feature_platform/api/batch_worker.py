"""Pure batch chunk worker (Kafka-first, reuses the shared compute path).

``handle_batch_chunk`` consumes one ``BatchChunkRequested``, plans the output features
once, then computes each item via the shared ``persist_and_compute`` (offline-first, with
optional online CAS). It records **per-item** success/failure — one bad item does not fail
the chunk — and updates the batch status best-effort. Infra store failures propagate so the
runner can avoid committing (replay is safe: offline dedup + online CAS). It never calls
``ComputeCore`` directly, never publishes ``FeatureOfflineWriteRequested`` (offline is done
by ``persist_and_compute``), and stores no feature values.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import perf_counter

from fintech_feature_platform.api.backend import AppBackend
from fintech_feature_platform.api.batch_controls import NoopBatchRuntimeControls
from fintech_feature_platform.api.direct_compute import InlineSource, persist_and_compute
from fintech_feature_platform.fs_core.dedup import partition_new_results
from fintech_feature_platform.fs_core.events.models import (
    FEATURE_UPDATE_SOURCE_BATCH_WORKER,
    BatchChunkProcessed,
    BatchChunkRequested,
)
from fintech_feature_platform.fs_core.events.topics import BATCH_JOB_EVENTS
from fintech_feature_platform.fs_core.models import EntityKey, SourceStamp
from fintech_feature_platform.fs_core.observability.metrics import recorder_of
from fintech_feature_platform.fs_core.planner import plan_features
from fintech_feature_platform.fs_core.propagation import emit_feature_updates
from fintech_feature_platform.fs_core.registry.models import FeatureViewDef, Registry
from fintech_feature_platform.fs_core.stores.batch_status import BatchChunkStatus

_MAX_FIRST_ERRORS = 10


@dataclass(frozen=True)
class BatchWorkerResult:
    # status: ok | invalid_event | publish_failed
    status: str
    chunk_id: str
    ok_items: int = 0
    failed_items: int = 0
    first_errors: list[str] = field(default_factory=list)
    error: str | None = None
    # Bounded counts of the guarded Mode-2 online refresh; None if not run.
    online_refresh_counts: dict[str, int] | None = None


def _find_view(registry: Registry, name: str, version: int) -> FeatureViewDef:
    return registry.find_view(name, version)  # ONE authoritative lookup


def _emit_feature_updates(
    backend: AppBackend,
    view_def: FeatureViewDef,
    results: list,
    batch_job_id: str,
    manifest_id: str | None,
) -> None:
    """Shared emitter seam for the batch worker (offline-first FeatureUpdated,)."""
    emit_feature_updates(
        publisher=backend.events,
        view_def=view_def,
        results=results,
        entity_type=view_def.entity,
        source=FEATURE_UPDATE_SOURCE_BATCH_WORKER,
        job_id=batch_job_id,
        manifest_id=manifest_id,
    )


@dataclass(frozen=True)
class _ChunkOutcome:
    ok_items: int
    failed_items: int
    first_errors: list[str]
    online_refresh_counts: dict[str, int] | None = None


_REFRESH_KEYS = (
    "online_refresh_attempted",
    "online_refresh_written",
    "online_refresh_written_recompute",
    "online_refresh_skipped_stale",
    "online_refresh_noop",
    "online_refresh_rate_limited",
    "online_refresh_failed",
)

# D9 outcome (from OnlineStore.write) -> refresh count key.
_OUTCOME_KEY = {
    "written": "online_refresh_written",
    "written_recompute": "online_refresh_written_recompute",
    "skipped_stale": "online_refresh_skipped_stale",
    "noop": "online_refresh_noop",
}


def _guarded_online_refresh(
    backend: AppBackend,
    view: str,
    view_version: int,
    results: list,
    controls,
) -> dict[str, int]:
    """Mode-2 guarded online refresh : secondary to offline, budgeted, D9-safe.

    Runs AFTER the offline append. Each computed value is pushed online via
    ``OnlineStore.write`` (the existing D9 write guard), bounded by a per-chunk feature
    cap and a token bucket; over-budget features are counted ``rate_limited``, online
    store failures ``failed`` — neither aborts the chunk. Returns bounded counts only
    (never values). Online refresh must never make batch depend on online infra.
    """
    counts = dict.fromkeys(_REFRESH_KEYS, 0)
    metrics = recorder_of(backend)
    refresh_started = perf_counter()
    max_features = getattr(controls, "online_refresh_max_features", 0)
    limiter = controls.online_refresh_limiter
    for index, result in enumerate(results):
        if max_features and index >= max_features:
            counts["online_refresh_rate_limited"] += 1
            continue
        if limiter.try_acquire(1) < 1:
            counts["online_refresh_rate_limited"] += 1
            continue
        counts["online_refresh_attempted"] += 1
        try:
            outcome = backend.online.write(view, view_version, result)
        except Exception:  # noqa: BLE001 - refresh is secondary; never fails the chunk
            counts["online_refresh_failed"] += 1
            continue
        counts[_OUTCOME_KEY.get(outcome, "online_refresh_noop")] += 1
        metrics.incr(
            "fsp_online_write_outcomes_total",
            {"execution_mode": "batch", "outcome": outcome},
        )
    metrics.observe(
        "fsp_pipeline_stage_duration_seconds",
        perf_counter() - refresh_started,
        {"execution_mode": "batch", "stage": "online_write"},
    )
    return counts


def _compute_ref_chunk(
    backend: AppBackend,
    view_def: FeatureViewDef,
    compute_features: list[str],
    items: list,
    *,
    batch_job_id: str,
    manifest_id: str | None = None,
    write_online: bool = False,
    controls=None,
) -> _ChunkOutcome:
    """Dataset-scoped  chunk: compute all items, then ONE bulk append.

    Each report is already landed, so items carry refs only. Per item we compute (no
    store write) resolving the payload via ``raw_reports_meta`` + payload store; results
    are collected, deduped once, and written offline in a single
    ``append_many`` — the chunk-level write shape that lets ref-scoped jobs scale.
    Offline is primary; when ``write_online`` a guarded Mode-2 online refresh runs AFTER
    the append. A bad/missing ref is a per-item deterministic error; a
    bulk-append infra failure propagates so the runner replays.
    """
    now = datetime.now(tz=UTC)
    results = []
    ok_items = 0
    failed_items = 0
    first_errors: list[str] = []
    for item in items:
        try:
            entity_key = EntityKey.from_mapping(
                {name: item.entity_key[name] for name in view_def.key_fields},
                key_order=view_def.key_fields,
            )
            source_stamps = {}
            for source_name, report_ref in item.source_refs.items():
                meta = backend.metas.get_meta(report_ref)  # KeyError -> per-item error
                source_stamps[source_name] = SourceStamp(
                    report_ts=meta.report_ts,
                    content_hash=meta.content_hash,
                    # Availability : trusted/ingestion-time value from the
                    # meta row; legacy rows fall back to their ingestion created_at.
                    available_at=meta.available_at or meta.created_at,
                    availability_source=meta.availability_source or "ingestion_time",
                )
            write_set = backend.store.compute_write_set(
                view=view_def.name,
                view_version=view_def.view_version,
                entity_key=entity_key,
                requested_features=compute_features,
                source_refs=dict(item.source_refs),
                source_stamps=source_stamps,
                calc_ts=now,
                execution_mode="batch",
            )
            results.extend(write_set.results.values())
            ok_items += 1
        except (KeyError, TypeError, ValueError) as exc:  # per-item deterministic error
            failed_items += 1
            if len(first_errors) < _MAX_FIRST_ERRORS:
                first_errors.append(str(exc)[:200])

    # Chunk-level: dedupe once (fingerprint-aware), then one bulk append. An infra
    # failure here propagates -> no BatchChunkProcessed publish -> runner replays.
    new_results, _ = partition_new_results(
        backend.offline, view_def.name, view_def.view_version, results
    )
    offline_started = perf_counter()
    backend.offline.append_many(view_def.name, view_def.view_version, new_results)
    recorder_of(backend).observe(
        "fsp_pipeline_stage_duration_seconds",
        perf_counter() - offline_started,
        {"execution_mode": "batch", "stage": "offline_write"},
    )

    # Emit values-free FeatureUpdated AFTER the durable bulk append and BEFORE the completion
    # event / commit. A publish failure propagates -> no BatchChunkProcessed ->
    # runner does not commit -> replay (offline dedup keeps the recompute idempotent).
    _emit_feature_updates(backend, view_def, list(results), batch_job_id, manifest_id)

    # Guarded online refresh runs ONLY after a successful offline append (offline wins).
    refresh_counts = None
    if write_online:
        refresh_counts = _guarded_online_refresh(
            backend, view_def.name, view_def.view_version, results, controls
        )
    return _ChunkOutcome(ok_items, failed_items, first_errors, refresh_counts)


def _compute_inline_chunk(
    backend: AppBackend,
    view_def: FeatureViewDef,
    compute_features: list[str],
    items: list,
    write_online: bool,
    *,
    batch_job_id: str,
    manifest_id: str | None = None,
) -> _ChunkOutcome:
    """Inline chunk : persist each item's payload + compute + offline append.

    Kept per-item because inline items carry payloads that must be stored; inline scope
    stays conservatively capped. ``persist_and_compute`` writes offline history, so this
    path also emits values-free FeatureUpdated  once, after all items append.
    """
    ok_items = 0
    failed_items = 0
    first_errors: list[str] = []
    written: list = []
    for item in items:
        try:
            entity_key = EntityKey.from_mapping(
                {name: item.entity_key[name] for name in view_def.key_fields},
                key_order=view_def.key_fields,
            )
            inline_sources = {
                name: InlineSource(**source)
                for name, source in item.inline_sources.items()
            }
            outcome, _ = persist_and_compute(
                backend, view_def, entity_key, inline_sources, compute_features,
                write_online=write_online, skip_duplicate_offline=True,
            )
            written.extend(outcome.results.values())
            ok_items += 1
        except (KeyError, TypeError, ValueError) as exc:  # per-item deterministic error
            failed_items += 1
            if len(first_errors) < _MAX_FIRST_ERRORS:
                first_errors.append(str(exc)[:200])

    # Emit AFTER all items durably appended offline (publish failure propagates -> replay).
    _emit_feature_updates(backend, view_def, written, batch_job_id, manifest_id)
    return _ChunkOutcome(ok_items, failed_items, first_errors)


def _safe_set_chunk(backend: AppBackend, job_id: str, chunk: BatchChunkStatus) -> None:
    """Best-effort per-chunk status update; never raises, never affects commit."""
    try:
        backend.batch_status.set_chunk(job_id, chunk)
    except Exception:  # noqa: BLE001 - status is observability only
        return


def handle_batch_chunk(
    backend: AppBackend, event: BatchChunkRequested, controls=None
) -> BatchWorkerResult:
    """Compute one chunk item-by-item; per-item errors, infra errors propagate.

    ``controls`` (a ``BatchRuntimeControls``; default Noop) supplies the guarded online
    refresh budget for ref chunks with ``write_online``.
    """
    if controls is None:
        controls = NoopBatchRuntimeControls()
    # Deterministic validation (bad view/features) -> invalid_event (poison -> DLQ).
    try:
        view_def = _find_view(backend.registry, event.view, event.view_version)
        plan = plan_features(
            view_def, event.requested_features, event.requested_feature_groups
        )
    except (KeyError, TypeError, ValueError) as exc:
        return BatchWorkerResult(
            status="invalid_event", chunk_id=event.chunk_id, error=str(exc)
        )

    chunk_started = perf_counter()
    now = datetime.now(tz=UTC)
    _safe_set_chunk(
        backend, event.batch_job_id,
        BatchChunkStatus(
            chunk_id=event.chunk_id, chunk_index=event.chunk_index, status="running",
            item_count=len(event.items), updated_at=now,
        ),
    )

    compute_features = list(plan.compute_features)
    # A job is one scope, so a chunk is homogeneous: ref items (dataset-scoped, bulk
    # append) or inline items (per-item persist). An infra failure inside either
    # propagates -> the runner does not commit -> replay (offline dedup keeps it safe).
    is_ref_chunk = bool(event.items) and bool(event.items[0].source_refs)
    if is_ref_chunk:
        outcome = _compute_ref_chunk(
            backend, view_def, compute_features, event.items,
            batch_job_id=event.batch_job_id, manifest_id=event.manifest_id,
            write_online=event.write_online, controls=controls,
        )
    else:
        outcome = _compute_inline_chunk(
            backend, view_def, compute_features, event.items, event.write_online,
            batch_job_id=event.batch_job_id, manifest_id=event.manifest_id,
        )
    ok_items = outcome.ok_items
    failed_items = outcome.failed_items
    first_errors = outcome.first_errors

    chunk_status = "completed" if failed_items == 0 else "completed_with_errors"
    finished = datetime.now(tz=UTC)
    _safe_set_chunk(
        backend, event.batch_job_id,
        BatchChunkStatus(
            chunk_id=event.chunk_id, chunk_index=event.chunk_index, status=chunk_status,
            item_count=len(event.items), ok_items=ok_items, failed_items=failed_items,
            first_errors=first_errors, updated_at=finished, finished_at=finished,
        ),
    )

    # Publish the durable processed event BEFORE the source offset is committed (the
    # Metadata Writer projects it into batch_jobs/batch_chunks). On failure -> no commit.
    processed = BatchChunkProcessed(
        batch_job_id=event.batch_job_id,
        chunk_id=event.chunk_id,
        chunk_index=event.chunk_index,
        chunk_count=event.chunk_count,
        correlation_id=event.correlation_id,
        processed_at=finished,
        status=chunk_status,
        item_count=len(event.items),
        ok_items=ok_items,
        failed_items=failed_items,
        first_errors=first_errors,
        manifest_id=event.manifest_id,
        online_refresh_counts=outcome.online_refresh_counts,
    )
    try:
        publish_started = perf_counter()
        backend.events.publish(BATCH_JOB_EVENTS, event.batch_job_id, processed)
        recorder_of(backend).observe(
            "fsp_pipeline_stage_duration_seconds",
            perf_counter() - publish_started,
            {"execution_mode": "batch", "stage": "publish"},
        )
    except Exception as exc:  # noqa: BLE001 - processed publish failed: do not commit
        return BatchWorkerResult(
            status="publish_failed", chunk_id=event.chunk_id, ok_items=ok_items,
            failed_items=failed_items, first_errors=first_errors, error=str(exc),
            online_refresh_counts=outcome.online_refresh_counts,
        )

    metrics = recorder_of(backend)
    # LEVEL 1 whole-chunk duration : decode-to-published wall clock.
    metrics.observe(
        "fsp_batch_chunk_duration_seconds", perf_counter() - chunk_started
    )
    metrics.incr("batch_chunks_total", {"status": chunk_status})
    metrics.incr("batch_items_total", {"outcome": "ok"}, value=ok_items)
    metrics.incr("batch_items_total", {"outcome": "failed"}, value=failed_items)
    metrics.incr("batch_rows_written_total", value=ok_items)
    return BatchWorkerResult(
        status="ok", chunk_id=event.chunk_id, ok_items=ok_items,
        failed_items=failed_items, first_errors=first_errors,
        online_refresh_counts=outcome.online_refresh_counts,
    )
