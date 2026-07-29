"""Pure event-to-metadata projection for the Metadata Writer.

``project_event`` dispatches a raw Kafka message by ``event_type`` to a handler that
upserts the ``feature_requests`` snapshot and appends a ``request_events`` audit row.
It stores **no feature values, no raw payloads, and no DLQ ``source_payload_b64``** — only
references and small summaries.

Failure classification (; the architecture documentation poison discipline):
deterministic audit conflicts (``RawReportMetaConflictError`` /
``BatchMetadataConflictError``) surface as ``structural_conflict`` — the runner
dead-letters them and commits, so they can never replay-loop and halt the partition.
Any other store exception surfaces as ``store_failed`` (transient: the runner does not
commit and the message replays). Malformed / unknown event types are structural poison
the runner dead-letters as well — never silently dropped.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

from fintech_feature_platform.fs_core.events.models import (
    EVENT_TYPE_BATCH_CHUNK_PROCESSED,
    EVENT_TYPE_BATCH_CHUNK_REQUESTED,
    EVENT_TYPE_DLQ,
    EVENT_TYPE_FEATURE_COMPUTE_COMPLETED,
    EVENT_TYPE_FEATURE_COMPUTE_REQUESTED,
    EVENT_TYPE_FEATURE_OFFLINE_WRITE_REQUESTED,
    EVENT_TYPE_MODEL_SCORE_WRITE,
    BatchChunkProcessed,
    BatchChunkRequested,
    DeadLetterEvent,
    FeatureComputeCompleted,
    FeatureComputeRequested,
    FeatureOfflineWriteRequested,
    ModelScoreWriteRequested,
)
from fintech_feature_platform.fs_core.models import EntityKey, RawReportMeta
from fintech_feature_platform.fs_core.raw.meta_repository import (
    RawReportMetaConflictError,
    RawReportMetaRepository,
)
from fintech_feature_platform.fs_core.stores.batch_metadata import (
    BatchChunkRecord,
    BatchJobRecord,
    BatchMetadataConflictError,
    BatchMetadataStore,
)
from fintech_feature_platform.fs_core.stores.metadata import (
    MetadataStore,
    RequestEvent,
    RequestMetadata,
    event_hash,
)


@dataclass(frozen=True)
class ProjectionResult:
    # ok | unknown_event_type | deserialization_failed | structural_conflict | store_failed
    status: str
    event_type: str | None = None
    request_id: str | None = None
    error: str | None = None


# Deterministic audit conflicts: the same immutable key re-projected with different data.
# These can never succeed on replay — the runner must DLQ them, not replay them.
_CONFLICT_ERRORS = (RawReportMetaConflictError, BatchMetadataConflictError)


def _project_requested(
    store: MetadataStore, raw_meta: RawReportMetaRepository,
    batch_meta: BatchMetadataStore, data: dict, ehash: str
) -> str:
    event = FeatureComputeRequested.from_dict(data)
    now = datetime.now(tz=UTC)
    store.upsert_request(
        RequestMetadata(
            request_id=event.request_id,
            updated_at=now,
            job_id=event.job_id,
            status="accepted",
            entity_type=event.entity.entity_type,
            entity_key=dict(event.entity.entity_key),
            view=event.view,
            view_version=event.view_version,
            requested_features=list(event.requested_features),
            requested_feature_groups=list(event.requested_feature_groups),
            created_at=event.occurred_at,
        )
    )
    store.append_event(
        RequestEvent(
            event_hash=ehash,
            event_type=event.event_type,
            occurred_at=event.occurred_at,
            created_at=now,
            request_id=event.request_id,
            summary={
                "request_id": event.request_id,
                "job_id": event.job_id,
                "entity": event.entity.to_dict(),
                "view": event.view,
                "view_version": event.view_version,
                "report_refs": [report.report_ref for report in event.reports],
            },
        )
    )
    # Async projection of raw report metadata (moved out of the submit critical path).
    # storage_uri = object_key is internal; never surfaced by the public API.
    for descriptor in event.reports:
        raw_meta.upsert(
            RawReportMeta(
                report_ref=descriptor.report_ref,
                report_type=descriptor.report_type,
                entity_type=event.entity.entity_type,
                entity_key=EntityKey.from_mapping(dict(event.entity.entity_key)),
                report_ts=descriptor.report_ts,
                payload_size_bytes=descriptor.size_bytes,
                content_hash=descriptor.content_hash,
                storage_uri=descriptor.object_key,
                created_at=now,
                format=descriptor.format,
                compression=descriptor.compression,
                # Online descriptors carry the API accept time — platform
                # acceptance, never a client-supplied historical claim.
                available_at=descriptor.available_at,
                availability_source=(
                    "ingestion_time" if descriptor.available_at is not None else None
                ),
            )
        )
    return event.request_id


def _project_completed(
    store: MetadataStore, raw_meta: RawReportMetaRepository,
    batch_meta: BatchMetadataStore, data: dict, ehash: str
) -> str:
    event = FeatureComputeCompleted.from_dict(data)
    now = datetime.now(tz=UTC)
    # Project the event's own online_write_status (written / skipped_stale / noop /
    # deadline_expired); legacy events (legacy) omit it and mean a successful write.
    online_write_status = event.online_write_status or "written"
    store.upsert_request(
        RequestMetadata(
            request_id=event.request_id,
            updated_at=now,
            job_id=event.job_id,
            status="completed",
            entity_type=event.entity.entity_type,
            entity_key=dict(event.entity.entity_key),
            view=event.view,
            view_version=event.view_version,
            online_write_status=online_write_status,
            finished_at=now,
        )
    )
    store.append_event(
        RequestEvent(
            event_hash=ehash,
            event_type=event.event_type,
            occurred_at=event.occurred_at,
            created_at=now,
            request_id=event.request_id,
            summary={
                "request_id": event.request_id,
                "job_id": event.job_id,
                "view": event.view,
                "view_version": event.view_version,
                "written_features": list(event.written_features),
                "online_write_status": online_write_status,
            },
        )
    )
    return event.request_id


def _project_offline_requested(
    store: MetadataStore, raw_meta: RawReportMetaRepository,
    batch_meta: BatchMetadataStore, data: dict, ehash: str
) -> str:
    event = FeatureOfflineWriteRequested.from_dict(data)
    now = datetime.now(tz=UTC)
    write_set = event.write_set
    store.upsert_request(
        RequestMetadata(
            request_id=event.request_id,
            updated_at=now,
            job_id=event.job_id,
            view=write_set.view,
            view_version=write_set.view_version,
            offline_write_status="pending",
        )
    )
    store.append_event(
        RequestEvent(
            event_hash=ehash,
            event_type=event.event_type,
            occurred_at=event.occurred_at,
            created_at=now,
            request_id=event.request_id,
            summary={
                "request_id": event.request_id,
                "job_id": event.job_id,
                "view": write_set.view,
                "view_version": write_set.view_version,
                "source_refs": dict(write_set.source_refs),
            },
        )
    )
    return event.request_id


def _project_dlq(
    store: MetadataStore, raw_meta: RawReportMetaRepository,
    batch_meta: BatchMetadataStore, data: dict, ehash: str
) -> str | None:
    event = DeadLetterEvent.from_dict(data)
    now = datetime.now(tz=UTC)
    store.append_event(
        RequestEvent(
            event_hash=ehash,
            event_type=event.event_type,
            occurred_at=event.occurred_at,
            created_at=now,
            request_id=event.original_request_id,
            summary={
                "failure_stage": event.failure_stage,
                "failure_status": event.failure_status,
                "error": event.error,
                "original_request_id": event.original_request_id,
                "original_job_id": event.original_job_id,
                "original_correlation_id": event.original_correlation_id,
                "attempt_count": event.attempt_count,
                "max_attempts": event.max_attempts,
            },
        )
    )
    if event.original_request_id:
        if event.failure_stage == "metadata_writer":
            # A metadata PROJECTION failure is not a request failure: the original
            # request may have completed normally. Audit row only (appended above);
            # never flip the request snapshot to failed/failed_dlq.
            pass
        elif event.failure_stage == "offline_writer":
            store.upsert_request(
                RequestMetadata(
                    request_id=event.original_request_id,
                    updated_at=now,
                    offline_write_status="failed_dlq",
                    error=event.error,
                )
            )
        else:
            store.upsert_request(
                RequestMetadata(
                    request_id=event.original_request_id,
                    updated_at=now,
                    status="failed",
                    error=event.error,
                )
            )
    return event.original_request_id


def _project_model_score(
    store: MetadataStore, raw_meta: RawReportMetaRepository,
    batch_meta: BatchMetadataStore, data: dict, ehash: str
) -> str:
    # Audit only: append a request_events row with lineage; NO feature values, and no
    # feature_requests upsert (score writes are not feature-compute requests).
    event = ModelScoreWriteRequested.from_dict(data)
    now = datetime.now(tz=UTC)
    store.append_event(
        RequestEvent(
            event_hash=ehash,
            event_type=event.event_type,
            occurred_at=event.occurred_at,
            created_at=now,
            request_id=event.score_write_id,
            summary={
                "score_write_id": event.score_write_id,
                "entity": event.entity.to_dict(),
                "view": event.view,
                "view_version": event.view_version,
                "features": [s.feature for s in event.scores],
                "models": [
                    {
                        "feature": s.feature,
                        "model_name": s.model_name,
                        "model_version": s.model_version,
                        "source_request_id": s.source_request_id,
                        "decision_request_id": s.decision_request_id,
                    }
                    for s in event.scores
                ],
                "write_online": event.write_online,
                "write_offline": event.write_offline,
            },
        )
    )
    return event.score_write_id


def _project_batch_chunk_requested(
    store: MetadataStore, raw_meta: RawReportMetaRepository,
    batch_meta: BatchMetadataStore, data: dict, ehash: str
) -> str:
    event = BatchChunkRequested.from_dict(data)
    now = datetime.now(tz=UTC)
    batch_meta.upsert_job(
        BatchJobRecord(
            job_id=event.batch_job_id, status="accepted", created_at=now, updated_at=now,
            view=event.view, view_version=event.view_version,
            requested_features=list(event.requested_features),
            requested_feature_groups=list(event.requested_feature_groups),
            total_items=event.total_items, chunk_count=event.chunk_count,
            write_online=event.write_online, manifest_id=event.manifest_id,
        )
    )
    batch_meta.upsert_chunk_requested(
        BatchChunkRecord(
            chunk_id=event.chunk_id, job_id=event.batch_job_id,
            chunk_index=event.chunk_index, chunk_count=event.chunk_count,
            status="requested", item_count=len(event.items),
            created_at=now, updated_at=now,
        )
    )
    store.append_event(
        RequestEvent(
            event_hash=ehash, event_type=event.event_type, occurred_at=event.occurred_at,
            created_at=now, request_id=event.batch_job_id,
            summary={
                "batch_job_id": event.batch_job_id, "chunk_id": event.chunk_id,
                "view": event.view, "view_version": event.view_version,
                "chunk_index": event.chunk_index, "chunk_count": event.chunk_count,
                "item_count": len(event.items),
            },
        )
    )
    return event.batch_job_id


def _project_batch_chunk_processed(
    store: MetadataStore, raw_meta: RawReportMetaRepository,
    batch_meta: BatchMetadataStore, data: dict, ehash: str
) -> str:
    event = BatchChunkProcessed.from_dict(data)
    now = datetime.now(tz=UTC)
    batch_meta.upsert_chunk_processed(
        BatchChunkRecord(
            chunk_id=event.chunk_id, job_id=event.batch_job_id,
            chunk_index=event.chunk_index, chunk_count=event.chunk_count,
            status=event.status, item_count=event.item_count, ok_items=event.ok_items,
            failed_items=event.failed_items, first_errors=list(event.first_errors),
            created_at=now, updated_at=now, finished_at=event.processed_at,
        )
    )
    store.append_event(
        RequestEvent(
            event_hash=ehash, event_type=event.event_type, occurred_at=event.processed_at,
            created_at=now, request_id=event.batch_job_id,
            summary={
                "batch_job_id": event.batch_job_id, "chunk_id": event.chunk_id,
                "status": event.status, "item_count": event.item_count,
                "ok_items": event.ok_items, "failed_items": event.failed_items,
                "first_errors": list(event.first_errors),
            },
        )
    )
    return event.batch_job_id


_HANDLERS = {
    EVENT_TYPE_FEATURE_COMPUTE_REQUESTED: _project_requested,
    EVENT_TYPE_FEATURE_COMPUTE_COMPLETED: _project_completed,
    EVENT_TYPE_FEATURE_OFFLINE_WRITE_REQUESTED: _project_offline_requested,
    EVENT_TYPE_MODEL_SCORE_WRITE: _project_model_score,
    EVENT_TYPE_BATCH_CHUNK_REQUESTED: _project_batch_chunk_requested,
    EVENT_TYPE_BATCH_CHUNK_PROCESSED: _project_batch_chunk_processed,
    EVENT_TYPE_DLQ: _project_dlq,
}


def project_event(
    store: MetadataStore, raw_meta: RawReportMetaRepository,
    batch_meta: BatchMetadataStore, raw_value: bytes
) -> ProjectionResult:
    """Project one raw Kafka message into the metadata stores (idempotent)."""
    ehash = event_hash(raw_value)
    try:
        data = json.loads(raw_value)
    except Exception as exc:  # noqa: BLE001 - malformed bytes
        return ProjectionResult(status="deserialization_failed", error=str(exc))

    event_type = data.get("event_type") if isinstance(data, dict) else None
    handler = _HANDLERS.get(event_type)
    if handler is None:
        return ProjectionResult(status="unknown_event_type", event_type=event_type)

    try:
        request_id = handler(store, raw_meta, batch_meta, data, ehash)
    except (KeyError, TypeError, ValueError) as exc:  # malformed/incomplete event
        return ProjectionResult(
            status="deserialization_failed", event_type=event_type, error=str(exc)
        )
    except _CONFLICT_ERRORS as exc:  # deterministic audit conflict -> DLQ, never replay
        return ProjectionResult(
            status="structural_conflict",
            event_type=event_type,
            error=f"{type(exc).__name__}: {exc}",
        )
    except Exception as exc:  # noqa: BLE001 - transient store failure -> do not commit
        return ProjectionResult(
            status="store_failed", event_type=event_type, error=str(exc)
        )

    return ProjectionResult(status="ok", event_type=event_type, request_id=request_id)
