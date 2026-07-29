"""Pure online feature worker handler (Kafka-first, Valkey-first).

``handle_feature_compute_requested`` is the unit-testable core of the online worker:
it consumes one ``FeatureComputeRequested``, loads raw reports **directly from the
event's report descriptors** (no Postgres metadata lookup), computes via
``FeatureStore.compute_write_set`` (never ``compute``), writes the online store
**first**, then publishes a small ``FeatureComputeCompleted`` status event.

This module intentionally contains **no Kafka consumer loop and no offset commit** —
those (and the Offline Writer) come in later tasks. It also never appends offline
history; offline history still comes from the legacy synchronous path until the
Offline Writer exists.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import perf_counter
from typing import Any

from fintech_feature_platform.api.backend import AppBackend
from fintech_feature_platform.fs_core.events.models import (
    FeatureComputeCompleted,
    FeatureComputeRequested,
    FeatureOfflineWriteRequested,
)
from fintech_feature_platform.fs_core.events.topics import (
    FEATURE_COMPUTE_COMPLETED,
    FEATURE_WRITE_OFFLINE,
)
from fintech_feature_platform.fs_core.models import (
    EntityKey,
    FeatureWriteSet,
    SourceStamp,
)
from fintech_feature_platform.fs_core.observability.metrics import recorder_of
from fintech_feature_platform.fs_core.planner import plan_features
from fintech_feature_platform.fs_core.registry.models import FeatureViewDef, Registry
from fintech_feature_platform.fs_core.write_guard import (
    WRITE_OUTCOMES,
    aggregate_online_status,
)

# Online write status reported when a request expires before its online write.
DEADLINE_EXPIRED = "deadline_expired"


@dataclass(frozen=True)
class WorkerResult:
    # "ok" | "deadline_expired" | "compute_failed" | "online_write_failed" | "publish_failed"
    status: str
    view: str
    view_version: int
    entity_key: str
    # Per-feature D9 outcome ("written" | "written_recompute" | "skipped_stale" |
    # "noop"), keyed by FeatureRef.encode(). Never rely on truthiness.
    online_written: dict[str, str] = field(default_factory=dict)
    downstream_published: bool = False
    error: str | None = None
    # The request's own computed values; the runner stores them (request-scoped
    # hybrid result) before writing the terminal status. None for deadline_expired.
    write_set: FeatureWriteSet | None = None


def _find_view(registry: Registry, name: str, version: int) -> FeatureViewDef:
    return registry.find_view(name, version)  # ONE authoritative lookup


def handle_feature_compute_requested(
    backend: AppBackend, event: FeatureComputeRequested
) -> WorkerResult:
    """Process one online compute event: compute -> online-first -> publish status.

    Returns a ``WorkerResult``; a future Kafka consumer should commit the source
    offset only when ``status == "ok"`` or ``"deadline_expired"`` (both terminal).
    """
    entity_encoded = event.entity.encoded()

    # --- deadline check FIRST  -----------------------------------
    # An event that reaches the worker past its absolute deadline must not write Valkey
    # or publish an offline-write event. It is a deadline outcome, not a compute failure:
    # publish a deadline_expired completion (no values) and let the runner commit so it
    # does not replay. Compute is skipped — the outcome does not depend on any value.
    if event.expires_at is not None and datetime.now(tz=UTC) >= event.expires_at:
        completed = FeatureComputeCompleted(
            request_id=event.request_id,
            job_id=event.job_id,
            entity=event.entity,
            view=event.view,
            view_version=event.view_version,
            correlation_id=event.correlation_id,
            occurred_at=datetime.now(tz=UTC),
            written_features=[],
            online_write_status=DEADLINE_EXPIRED,
        )
        try:
            backend.events.publish(
                FEATURE_COMPUTE_COMPLETED, entity_encoded, completed
            )
        except Exception as exc:  # noqa: BLE001 - completion publish failed: replay
            return WorkerResult(
                status="publish_failed",
                view=event.view,
                view_version=event.view_version,
                entity_key=entity_encoded,
                error=str(exc),
            )
        return WorkerResult(
            status="deadline_expired",
            view=event.view,
            view_version=event.view_version,
            entity_key=entity_encoded,
        )

    # --- compute (no store writes) ------------------------------------------
    try:
        view_def = _find_view(backend.registry, event.view, event.view_version)
        entity_key = EntityKey.from_mapping(
            {field_name: event.entity.entity_key[field_name] for field_name in view_def.key_fields},
            key_order=view_def.key_fields,
        )
        # Expand requested groups + explicit features into the output set. Planner errors
        # are deterministic (bad request/registry) -> treated as compute_failed below.
        plan = plan_features(
            view_def, event.requested_features, event.requested_feature_groups
        )
        loader = _make_descriptor_loader(backend, event)
        # Per-source stamps: each feature derives its OWN data_ts from its inputs
        # (D3/D9) — never a request-level max(report_ts).
        source_stamps = {
            r.source_name: SourceStamp(
                report_ts=r.report_ts, content_hash=r.content_hash,
                # API accept time; never a client claim -> ingestion_time trust.
                available_at=r.available_at,
                availability_source="ingestion_time" if r.available_at else None,
            )
            for r in event.reports
        }
        write_set = backend.store.compute_write_set(
            view=event.view,
            view_version=event.view_version,
            entity_key=entity_key,
            requested_features=plan.compute_features,
            source_refs={r.source_name: r.report_ref for r in event.reports},
            source_stamps=source_stamps,
            calc_ts=datetime.now(tz=UTC),
            request_id=event.request_id,
            job_id=event.job_id,
            correlation_id=event.correlation_id,
            write_policy=event.write_policy,
            source_loader=loader,
        )
    except (KeyError, TypeError, ValueError) as exc:
        return WorkerResult(
            status="compute_failed",
            view=event.view,
            view_version=event.view_version,
            entity_key=entity_encoded,
            error=str(exc),
        )

    # --- online first -------------------------------------------------------
    metrics = recorder_of(backend)
    try:
        online_write_started = perf_counter()
        online_written = backend.online.write_many(
            event.view, event.view_version, list(write_set.results.values())
        )
        metrics.observe(
            "fsp_pipeline_stage_duration_seconds",
            perf_counter() - online_write_started,
            {"execution_mode": "online", "stage": "online_write"},
        )
        for outcome in online_written.values():
            metrics.incr(
                "fsp_online_write_outcomes_total",
                {"execution_mode": "online", "outcome": outcome},
            )
    except Exception as exc:  # noqa: BLE001 - report any store failure as retryable
        return WorkerResult(
            status="online_write_failed",
            view=event.view,
            view_version=event.view_version,
            entity_key=entity_key.encode(),
            error=str(exc),
        )

    # --- publish the durable values-bearing offline-write event FIRST -------
    # This must be acknowledged before the source offset can be committed,
    # so the computed values are replayable into offline history. The worker itself
    # never appends offline; the Offline Writer consumes this event.
    offline_event = FeatureOfflineWriteRequested(
        request_id=event.request_id,
        job_id=event.job_id,
        correlation_id=event.correlation_id,
        occurred_at=datetime.now(tz=UTC),
        write_set=write_set,
    )
    try:
        publish_started = perf_counter()
        backend.events.publish(
            FEATURE_WRITE_OFFLINE, entity_key.encode(), offline_event
        )
        metrics.observe(
            "fsp_pipeline_stage_duration_seconds",
            perf_counter() - publish_started,
            {"execution_mode": "online", "stage": "publish"},
        )
    except Exception as exc:  # noqa: BLE001 - online written; do not commit (replay-safe)
        return WorkerResult(
            status="publish_failed",
            view=event.view,
            view_version=event.view_version,
            entity_key=entity_key.encode(),
            online_written=online_written,
            downstream_published=False,
            error=str(exc),
        )

    # --- publish completion AFTER the offline-write event -------------------
    # Only features whose D9 outcome actually stored a value; skipped_stale/noop
    # features are still part of the request-scoped result.
    written_features = [
        name
        for name, result in write_set.results.items()
        if online_written.get(result.ref.encode()) in WRITE_OUTCOMES
    ]
    completed = FeatureComputeCompleted(
        request_id=event.request_id,
        job_id=event.job_id,
        entity=event.entity,
        view=event.view,
        view_version=event.view_version,
        correlation_id=event.correlation_id,
        occurred_at=datetime.now(tz=UTC),
        written_features=written_features,
        online_write_status=aggregate_online_status(online_written.values()),
    )
    try:
        backend.events.publish(
            FEATURE_COMPUTE_COMPLETED, entity_key.encode(), completed
        )
    except Exception as exc:  # noqa: BLE001 - online already written; do not commit offset
        return WorkerResult(
            status="publish_failed",
            view=event.view,
            view_version=event.view_version,
            entity_key=entity_key.encode(),
            online_written=online_written,
            downstream_published=False,
            error=str(exc),
        )

    return WorkerResult(
        status="ok",
        view=event.view,
        view_version=event.view_version,
        entity_key=entity_key.encode(),
        online_written=online_written,
        downstream_published=True,
        write_set=write_set,
    )


def _make_descriptor_loader(backend: AppBackend, event: FeatureComputeRequested):
    """Build a ``load(source_name) -> payload`` reading by descriptor ``object_key``.

    This is the self-contained path: payloads are read straight from object storage
    using the event descriptors, with no Postgres metadata lookup.
    """
    by_source = {report.source_name: report for report in event.reports}

    def load(source_name: str) -> Any:
        descriptor = by_source.get(source_name)
        if descriptor is None:
            raise ValueError(f"no report descriptor for source {source_name!r}")
        return backend.payloads.get_payload(descriptor.object_key)

    return load
