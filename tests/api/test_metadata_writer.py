"""Tests for the Metadata Writer projection + runner."""

import dataclasses
import json
from datetime import UTC, datetime

import pytest

from fintech_feature_platform.api.metadata_writer import project_event
from fintech_feature_platform.api.metadata_writer_runner import process_next
from fintech_feature_platform.fs_core.events.consumer import (
    InMemoryEventConsumer,
    InMemoryMessage,
)
from fintech_feature_platform.fs_core.events.models import (
    DeadLetterEvent,
    EntityRef,
    FeatureComputeCompleted,
    FeatureComputeRequested,
    FeatureOfflineWriteRequested,
    ModelScoreWriteRequested,
    ReportDescriptor,
    ScoreItem,
)
from fintech_feature_platform.fs_core.events.publisher import InMemoryEventPublisher
from fintech_feature_platform.fs_core.models import (
    EntityKey,
    FeatureRef,
    FeatureResult,
    FeatureWriteSet,
    RawReportMeta,
)
from fintech_feature_platform.fs_core.raw.meta_repository import (
    InMemoryMetaRepository,
    RawReportMetaConflictError,
)
from fintech_feature_platform.fs_core.stores.batch_metadata import (
    InMemoryBatchMetadataStore,
)
from fintech_feature_platform.fs_core.stores.metadata import (
    InMemoryMetadataStore,
    merge_request,
)

_TS = datetime(2026, 6, 27, 10, tzinfo=UTC)
_ENTITY = {"user_id": "1", "application_id": "A1"}


class _RecordingStore:
    def __init__(self):
        self.requests = {}
        self.upserts = []
        self.events = []
        self._hashes = set()

    def upsert_request(self, metadata):
        self.upserts.append(metadata)
        existing = self.requests.get(metadata.request_id)
        self.requests[metadata.request_id] = (
            merge_request(existing, metadata) if existing else metadata
        )

    def append_event(self, event):
        if event.event_hash in self._hashes:
            return False
        self._hashes.add(event.event_hash)
        self.events.append(event)
        return True

    def get_request(self, request_id):
        return self.requests.get(request_id)


def _requested(request_id="freq_1") -> FeatureComputeRequested:
    return FeatureComputeRequested(
        request_id=request_id,
        job_id="job_1",
        priority="online",
        deadline_ms=1000,
        entity=EntityRef("application", dict(_ENTITY)),
        view="user_credit_risk",
        view_version=1,
        reports=[
            ReportDescriptor(
                report_ref="rep_1",
                source_name="credit_report",
                report_type="credit_report",
                schema_version="v1",
                report_ts=_TS,
                object_key="mem://secret_object",
                content_hash="sha256:x",
                size_bytes=10,
                compression="none",
                format="json",
            )
        ],
        write_policy="online_first",
        idempotency_key="idem_1",
        correlation_id="corr_1",
        occurred_at=_TS,
        requested_features=["declared_income"],
    )


def _completed(request_id="freq_1") -> FeatureComputeCompleted:
    return FeatureComputeCompleted(
        request_id=request_id,
        job_id="job_1",
        entity=EntityRef("application", dict(_ENTITY)),
        view="user_credit_risk",
        view_version=1,
        correlation_id="corr_1",
        occurred_at=_TS,
        written_features=["declared_income"],
    )


def _offline_requested(request_id="freq_1") -> FeatureOfflineWriteRequested:
    entity_key = EntityKey.from_mapping(_ENTITY, key_order=["user_id", "application_id"])
    write_set = FeatureWriteSet(
        view="user_credit_risk",
        view_version=1,
        entity_key=entity_key,
        results={
            "declared_income": FeatureResult(
                ref=FeatureRef("declared_income", 1),
                entity_key=entity_key,
                value=4_200_000,
                data_ts=_TS,
                calc_ts=_TS,
            )
        },
        source_refs={"credit_report": "rep_1"},
        request_id=request_id,
        job_id="job_1",
    )
    return FeatureOfflineWriteRequested(
        request_id=request_id,
        job_id="job_1",
        correlation_id="corr_1",
        occurred_at=_TS,
        write_set=write_set,
    )


def _dlq(stage="offline_writer", request_id="freq_1") -> DeadLetterEvent:
    return DeadLetterEvent(
        source_topic="fp.feature-write.offline",
        failure_stage=stage,
        failure_status="append_failed",
        error="boom",
        occurred_at=_TS,
        source_payload_b64="c2VjcmV0LXBheWxvYWQ=",  # must NOT be stored
        original_request_id=request_id,
        attempt_count=5,
        max_attempts=5,
    )


def _conflicting_meta() -> RawReportMeta:
    return RawReportMeta(
        report_ref="rep_1",
        report_type="credit_report",
        entity_type="application",
        entity_key=EntityKey.from_mapping(_ENTITY, key_order=["user_id", "application_id"]),
        report_ts=_TS,
        payload_size_bytes=10,
        content_hash="sha256:DIFFERENT",  # conflicts with the event descriptor
        storage_uri="mem://secret_object",
        created_at=_TS,
        format="json",
        compression="none",
    )


# --- projection -------------------------------------------------------------

def test_project_requested_creates_snapshot_and_event_without_object_key():
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    result = project_event(store, raw_meta, batch_meta, _requested().to_json())
    assert result.status == "ok"
    assert store.requests["freq_1"].status == "accepted"
    event = store.events[-1]
    assert event.event_type == "feature_compute.requested"
    assert event.summary["report_refs"] == ["rep_1"]
    flat = json.dumps(event.summary)
    assert "object_key" not in flat
    assert "secret_object" not in flat


def test_project_requested_projects_raw_reports_meta():
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    project_event(store, raw_meta, batch_meta, _requested().to_json())
    meta = raw_meta.get_meta("rep_1")
    assert meta.report_type == "credit_report"
    assert meta.entity_type == "application"
    assert meta.storage_uri == "mem://secret_object"  # = descriptor.object_key (internal)
    assert meta.content_hash == "sha256:x"
    assert meta.payload_size_bytes == 10


def test_project_raw_reports_meta_idempotent_on_replay():
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    raw = _requested().to_json()
    assert project_event(store, raw_meta, batch_meta, raw).status == "ok"
    assert project_event(store, raw_meta, batch_meta, raw).status == "ok"  # replay, no conflict


def test_project_raw_reports_meta_conflict_returns_structural_conflict():
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    raw_meta.upsert(_conflicting_meta())  # pre-existing different metadata for rep_1
    result = project_event(store, raw_meta, batch_meta, _requested().to_json())
    # Deterministic audit conflict is structural (DLQ-able), NOT a transient store_failed.
    assert result.status == "structural_conflict"
    assert "RawReportMetaConflictError" in result.error
    assert "rep_1" in result.error


def test_raw_meta_repository_upsert_conflict_raises():
    raw_meta = InMemoryMetaRepository()
    raw_meta.upsert(_conflicting_meta())
    with pytest.raises(RawReportMetaConflictError):
        # same report_ref, different content_hash via a fresh meta
        raw_meta.upsert(
            RawReportMeta(
                report_ref="rep_1",
                report_type="credit_report",
                entity_type="application",
                entity_key=EntityKey.from_mapping(
                    _ENTITY, key_order=["user_id", "application_id"]
                ),
                report_ts=_TS,
                payload_size_bytes=10,
                content_hash="sha256:x",  # differs from the seeded one
                storage_uri="mem://secret_object",
                created_at=_TS,
                format="json",
                compression="none",
            )
        )


def _model_score_event() -> ModelScoreWriteRequested:
    return ModelScoreWriteRequested(
        score_write_id="sw1",
        correlation_id="corr_1",
        occurred_at=_TS,
        entity=EntityRef("application", dict(_ENTITY)),
        view="user_credit_risk",
        view_version=1,
        scores=[
            ScoreItem(
                feature="pd_score",
                value=0.037,
                data_ts=_TS,
                calc_ts=_TS,
                model_name="pd_model",
                model_version="v4",
                source_request_id="freq_abc",
            )
        ],
        idempotency_key="sw1",
    )


def test_project_model_score_audits_without_values_or_feature_request_row():
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    result = project_event(store, raw_meta, batch_meta, _model_score_event().to_json())
    assert result.status == "ok"
    event = store.events[-1]
    assert event.event_type == "model_score.write.requested"
    assert event.summary["features"] == ["pd_score"]
    assert event.summary["models"][0]["model_name"] == "pd_model"
    flat = json.dumps(event.summary)
    assert "0.037" not in flat  # no score values in the audit
    # score writes are not feature-compute requests: no feature_requests row created
    assert store.requests.get("sw1") is None


def test_project_completed_updates_completed_no_values():
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    project_event(store, raw_meta, batch_meta, _completed().to_json())
    got = store.requests["freq_1"]
    assert got.status == "completed"
    assert got.online_write_status == "written"
    assert store.events[-1].summary["written_features"] == ["declared_income"]


def test_project_completed_deadline_expired_audits_without_values():
    #: a deadline_expired completion projects the deadline outcome — the
    # audit shows "expired before online write" with no feature values.
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    expired = dataclasses.replace(
        _completed(), written_features=[], online_write_status="deadline_expired"
    )
    project_event(store, raw_meta, batch_meta, expired.to_json())
    got = store.requests["freq_1"]
    assert got.status == "completed"
    assert got.online_write_status == "deadline_expired"
    summary = store.events[-1].summary
    assert summary["online_write_status"] == "deadline_expired"
    assert summary["written_features"] == []
    assert "4200000" not in json.dumps(summary)  # never any values


def test_project_offline_requested_sets_pending_no_values():
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    project_event(store, raw_meta, batch_meta, _offline_requested().to_json())
    assert store.requests["freq_1"].offline_write_status == "pending"
    flat = json.dumps(store.events[-1].summary)
    assert "4200000" not in flat  # no FeatureWriteSet values


def test_project_dlq_offline_sets_failed_dlq_no_source_payload():
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    project_event(store, raw_meta, batch_meta, _dlq(stage="offline_writer").to_json())
    assert store.requests["freq_1"].offline_write_status == "failed_dlq"
    flat = json.dumps(store.events[-1].summary)
    assert "source_payload_b64" not in flat
    assert "c2VjcmV0" not in flat


def test_project_dlq_online_sets_failed():
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    project_event(store, raw_meta, batch_meta, _dlq(stage="online_worker").to_json())
    assert store.requests["freq_1"].status == "failed"


def test_project_dlq_metadata_writer_stage_does_not_fail_request():
    # A metadata PROJECTION failure is not a request failure: the audit row is
    # appended but the request snapshot is never flipped to failed/failed_dlq.
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    project_event(store, raw_meta, batch_meta, _completed().to_json())
    assert store.requests["freq_1"].status == "completed"

    result = project_event(
        store, raw_meta, batch_meta, _dlq(stage="metadata_writer").to_json()
    )

    assert result.status == "ok"
    assert store.events[-1].summary["failure_stage"] == "metadata_writer"
    got = store.requests["freq_1"]
    assert got.status == "completed"  # unchanged
    assert got.offline_write_status != "failed_dlq"


def test_project_replay_is_idempotent():
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    raw = _requested().to_json()
    project_event(store, raw_meta, batch_meta, raw)
    project_event(store, raw_meta, batch_meta, raw)
    # request_events deduped (same raw bytes -> same event_hash)
    assert len(store.events) == 1
    assert store.requests["freq_1"].status == "accepted"


def test_project_unknown_event_type_skipped():
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    result = project_event(store, raw_meta, batch_meta, json.dumps({"event_type": "nope"}).encode())
    assert result.status == "unknown_event_type"


def test_project_deserialization_failure():
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    result = project_event(store, raw_meta, batch_meta, b"not-json")
    assert result.status == "deserialization_failed"


# --- batch projection  -------------------------------------------

def _batch_requested(chunk_index=0, chunk_count=2, total_items=2):
    from fintech_feature_platform.fs_core.events.models import (
        BatchChunkRequested,
        BatchItem,
    )

    return BatchChunkRequested(
        batch_job_id="batch-1",
        chunk_id=f"batch-1:{chunk_index}",
        chunk_index=chunk_index,
        chunk_count=chunk_count,
        correlation_id="batch-1",
        occurred_at=_TS,
        view="user_credit_risk",
        view_version=1,
        items=[
            BatchItem(
                entity_type="application",
                entity_key={"user_id": "u1", "application_id": "A1"},
                inline_sources={
                    "credit_report": {
                        "report_type": "credit_report",
                        "report_ts": "2026-01-01T00:00:00Z",
                        "payload": {"declared_income": 100000},
                    }
                },
            )
        ],
        requested_features=["declared_income"],
        total_items=total_items,
    )


def _batch_processed(chunk_index=0, chunk_count=2, status="completed", ok=1, failed=0):
    from fintech_feature_platform.fs_core.events.models import BatchChunkProcessed

    return BatchChunkProcessed(
        batch_job_id="batch-1",
        chunk_id=f"batch-1:{chunk_index}",
        chunk_index=chunk_index,
        chunk_count=chunk_count,
        correlation_id="batch-1",
        processed_at=_TS,
        status=status,
        item_count=ok + failed,
        ok_items=ok,
        failed_items=failed,
    )


def test_project_batch_requested_into_batch_tables_without_payloads():
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    result = project_event(store, raw_meta, batch_meta, _batch_requested().to_json())
    assert result.status == "ok"
    job = batch_meta.get_job("batch-1")
    assert job.view == "user_credit_risk"
    assert job.total_items == 2
    assert job.chunk_count == 2
    assert batch_meta.list_chunks("batch-1")[0].status == "requested"
    flat = json.dumps(store.events[-1].summary)
    assert "100000" not in flat  # no payload values in the audit
    assert "inline_sources" not in flat


def test_project_batch_processed_derives_job_status():
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    project_event(store, raw_meta, batch_meta, _batch_requested(0).to_json())
    project_event(store, raw_meta, batch_meta, _batch_requested(1).to_json())
    project_event(store, raw_meta, batch_meta, _batch_processed(0).to_json())
    result = project_event(store, raw_meta, batch_meta, _batch_processed(1).to_json())
    assert result.status == "ok"
    assert batch_meta.get_job("batch-1").status == "completed"


def test_project_batch_conflict_returns_structural_conflict():
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    project_event(store, raw_meta, batch_meta, _batch_processed(0, chunk_count=1).to_json())
    conflicting = _batch_processed(0, chunk_count=1, status="completed_with_errors", ok=0, failed=1)
    result = project_event(store, raw_meta, batch_meta, conflicting.to_json())
    assert result.status == "structural_conflict"
    assert "BatchMetadataConflictError" in result.error


def test_project_batch_processed_identical_replay_is_ok():
    store = _RecordingStore()
    raw_meta = InMemoryMetaRepository()
    batch_meta = InMemoryBatchMetadataStore()
    raw = _batch_processed(0, chunk_count=1).to_json()
    assert project_event(store, raw_meta, batch_meta, raw).status == "ok"
    assert project_event(store, raw_meta, batch_meta, raw).status == "ok"  # no-op replay


# --- runner -----------------------------------------------------------------

class _FailingPublisher:
    """Publisher whose publish always raises (DLQ broker down)."""

    def publish(self, topic, key, event, *, headers=None):
        raise RuntimeError("dlq broker down")


def _runner(messages, *, raw_meta=None, batch_meta=None, store=None, publisher=None):
    consumer = InMemoryEventConsumer([InMemoryMessage(m) for m in messages])
    publisher = publisher if publisher is not None else InMemoryEventPublisher()
    result = process_next(
        consumer,
        store if store is not None else InMemoryMetadataStore(),
        raw_meta if raw_meta is not None else InMemoryMetaRepository(),
        batch_meta if batch_meta is not None else InMemoryBatchMetadataStore(),
        publisher,
    )
    return result, consumer, publisher


def test_runner_commits_after_successful_projection():
    store = InMemoryMetadataStore()
    raw_meta = InMemoryMetaRepository()
    result, consumer, publisher = _runner(
        [_requested().to_json()], store=store, raw_meta=raw_meta
    )
    assert result.status == "ok"
    assert result.committed is True
    assert len(consumer.committed) == 1
    assert publisher.published == []  # no DLQ on success
    assert store.get_request("freq_1").status == "accepted"
    assert raw_meta.get_meta("rep_1").report_type == "credit_report"


def test_runner_identical_replay_commits_without_dlq():
    store = InMemoryMetadataStore()
    raw_meta = InMemoryMetaRepository()
    raw = _requested().to_json()
    _runner([raw], store=store, raw_meta=raw_meta)
    result, consumer, publisher = _runner([raw], store=store, raw_meta=raw_meta)
    assert result.status == "ok"
    assert result.committed is True
    assert publisher.published == []


def test_runner_identical_batch_replay_commits_without_dlq():
    batch_meta = InMemoryBatchMetadataStore()
    raw = _batch_processed(0, chunk_count=1).to_json()
    _runner([raw], batch_meta=batch_meta)
    result, consumer, publisher = _runner([raw], batch_meta=batch_meta)
    assert result.status == "ok"
    assert result.committed is True
    assert publisher.published == []


def test_runner_does_not_commit_on_store_failure():
    class _FailingStore:
        def upsert_request(self, metadata):
            raise RuntimeError("postgres down")

        def append_event(self, event):
            raise RuntimeError("postgres down")

        def get_request(self, request_id):
            return None

    result, consumer, publisher = _runner(
        [_requested().to_json()], store=_FailingStore()
    )
    # Transient infra: replay, and never dead-letter a retriable failure.
    assert result.status == "store_failed"
    assert result.committed is False
    assert consumer.committed == []
    assert publisher.published == []


def test_runner_dead_letters_and_commits_raw_meta_conflict():
    raw_meta = InMemoryMetaRepository()
    raw_meta.upsert(_conflicting_meta())
    result, consumer, publisher = _runner([_requested().to_json()], raw_meta=raw_meta)
    # Structural conflict: DLQ first, commit after the ack — never replay-loop.
    assert result.status == "dead_lettered"
    assert result.committed is True
    assert len(consumer.committed) == 1
    assert len(publisher.published) == 1
    record = publisher.published[0]
    assert record.topic == "fp.dlq"
    assert record.event.failure_stage == "metadata_writer"
    assert record.event.failure_status == "structural_conflict"


def test_runner_raw_meta_conflict_dlq_publish_failure_no_commit():
    raw_meta = InMemoryMetaRepository()
    raw_meta.upsert(_conflicting_meta())
    result, consumer, _ = _runner(
        [_requested().to_json()], raw_meta=raw_meta, publisher=_FailingPublisher()
    )
    # Non-lossy: without a DLQ ack the offset stays uncommitted (replay retries both).
    assert result.status == "dlq_publish_failed"
    assert result.committed is False
    assert consumer.committed == []


def test_runner_dead_letters_and_commits_batch_conflict():
    batch_meta = InMemoryBatchMetadataStore()
    _runner([_batch_processed(0, chunk_count=1).to_json()], batch_meta=batch_meta)
    conflicting = _batch_processed(
        0, chunk_count=1, status="completed_with_errors", ok=0, failed=1
    )
    result, consumer, publisher = _runner(
        [conflicting.to_json()], batch_meta=batch_meta
    )
    assert result.status == "dead_lettered"
    assert result.committed is True
    event = publisher.published[0].event
    assert event.failure_status == "structural_conflict"
    assert "BatchMetadataConflictError" in event.error
    # source topic mapped from the event type; batch ids extracted via fallback
    assert event.source_topic == "fp.batch.events"
    assert event.original_job_id == "batch-1"


def test_runner_batch_conflict_dlq_publish_failure_no_commit():
    batch_meta = InMemoryBatchMetadataStore()
    _runner([_batch_processed(0, chunk_count=1).to_json()], batch_meta=batch_meta)
    conflicting = _batch_processed(
        0, chunk_count=1, status="completed_with_errors", ok=0, failed=1
    )
    result, consumer, _ = _runner(
        [conflicting.to_json()], batch_meta=batch_meta, publisher=_FailingPublisher()
    )
    assert result.status == "dlq_publish_failed"
    assert result.committed is False
    assert consumer.committed == []


def test_runner_dead_letters_unknown_event_type():
    result, consumer, publisher = _runner(
        [json.dumps({"event_type": "nope"}).encode()]
    )
    # Never silently dropped: DLQ'd, then committed.
    assert result.status == "dead_lettered"
    assert result.committed is True
    event = publisher.published[0].event
    assert event.failure_status == "unknown_event_type"
    assert event.original_event_type == "nope"
    assert event.source_topic == "unknown"  # type not in the topic map


def test_runner_dead_letters_deserialization_poison():
    result, consumer, publisher = _runner([b"not-json"])
    assert result.status == "dead_lettered"
    assert result.committed is True
    event = publisher.published[0].event
    assert event.failure_status == "deserialization_failed"
    assert event.source_topic == "unknown"  # unparseable: topic unattributable
    assert event.original_event_type is None


def test_runner_deserialization_poison_dlq_publish_failure_no_commit():
    result, consumer, _ = _runner([b"not-json"], publisher=_FailingPublisher())
    assert result.status == "dlq_publish_failed"
    assert result.committed is False
    assert consumer.committed == []


def test_dlq_event_contents_for_raw_meta_conflict():
    import base64

    raw_meta = InMemoryMetaRepository()
    raw_meta.upsert(_conflicting_meta())
    raw = _requested().to_json()
    _, _, publisher = _runner([raw], raw_meta=raw_meta)

    event = publisher.published[0].event
    assert event.source_topic == "fp.feature-compute.online"  # mapped from event_type
    assert event.failure_stage == "metadata_writer"
    assert event.failure_status == "structural_conflict"
    assert "RawReportMetaConflictError" in event.error
    assert "rep_1" in event.error
    assert event.original_request_id == "freq_1"
    assert event.original_job_id == "job_1"
    assert event.original_event_type == "feature_compute.requested"
    assert event.source_payload_b64 == base64.b64encode(raw).decode("ascii")


def test_dlq_event_expands_no_payload_outside_b64():
    # Force a structural poison on an offline-write event (it embeds feature values):
    # strip a required field so the handler fails deterministically.
    data = json.loads(_offline_requested().to_json())
    del data["write_set"]["view"]
    _, _, publisher = _runner([json.dumps(data).encode()])

    event = publisher.published[0].event
    assert event.failure_status == "deserialization_failed"
    # Values may exist ONLY inside the opaque original-bytes copy; every other
    # field of the DLQ event must not expand them.
    visible = event.to_dict()
    visible.pop("source_payload_b64")
    assert "4200000" not in json.dumps(visible)


# --- metadata_write_status truthfulness  ----------


def _request_status(request_id="freq_1", metadata_status="pending"):
    from fintech_feature_platform.fs_core.stores.request_status import (
        InMemoryRequestStatusStore,
        RequestStatus,
    )

    store = InMemoryRequestStatusStore()
    now = datetime.now(tz=UTC)
    store.put(RequestStatus(
        request_id=request_id, job_id="job_1", status="completed",
        entity_type="application", entity_key=dict(_ENTITY),
        view="user_credit_risk", view_version=1, created_at=now, updated_at=now,
        metadata_write_status=metadata_status,
    ))
    return store


def _runner_with_status(messages, status_store):
    consumer = InMemoryEventConsumer([InMemoryMessage(m) for m in messages])
    result = process_next(
        consumer,
        InMemoryMetadataStore(),
        InMemoryMetaRepository(),
        InMemoryBatchMetadataStore(),
        InMemoryEventPublisher(),
        request_status_store=status_store,
    )
    return result, consumer


def test_completed_projection_marks_metadata_written_after_commit():
    status_store = _request_status()
    result, consumer = _runner_with_status([_completed().to_json()], status_store)
    assert result.status == "ok" and result.committed
    assert status_store.get("freq_1").metadata_write_status == "written"


def test_metadata_written_transition_is_idempotent_and_never_regresses():
    status_store = _request_status()
    raw = _completed().to_json()
    _runner_with_status([raw], status_store)
    first = status_store.get("freq_1")
    # Replay of the same event (at-least-once): still written, still committed.
    result, _ = _runner_with_status([raw], status_store)
    assert result.committed
    after = status_store.get("freq_1")
    assert after.metadata_write_status == "written"
    assert after.updated_at >= first.updated_at
    # A stray earlier event type never touches the field.
    result, _ = _runner_with_status([_requested().to_json()], status_store)
    assert status_store.get("freq_1").metadata_write_status == "written"


def test_requested_projection_leaves_metadata_status_pending():
    status_store = _request_status()
    _runner_with_status([_requested().to_json()], status_store)
    assert status_store.get("freq_1").metadata_write_status == "pending"


def test_status_store_failure_never_blocks_projection_commit():
    class _BrokenStatusStore:
        def get(self, request_id):
            raise RuntimeError("status store down")

        def put(self, status):
            raise RuntimeError("status store down")

    result, consumer = _runner_with_status(
        [_completed().to_json()], _BrokenStatusStore()
    )
    assert result.status == "ok" and result.committed  # status is observability only
