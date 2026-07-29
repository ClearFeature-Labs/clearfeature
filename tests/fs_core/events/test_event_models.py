"""Tests for the self-contained event models."""

import base64
import dataclasses
import json
from datetime import UTC, datetime

import pytest

from fintech_feature_platform.fs_core.events.models import (
    BatchChunkProcessed,
    BatchChunkRequested,
    BatchItem,
    DeadLetterEvent,
    EntityRef,
    FeatureComputeCompleted,
    FeatureComputeRequested,
    FeatureOfflineWriteRequested,
    ModelScoreWriteRequested,
    ReportDescriptor,
    ScoreItem,
    build_idempotency_key,
)
from fintech_feature_platform.fs_core.models import (
    EntityKey,
    FeatureRef,
    FeatureResult,
    FeatureWriteSet,
)

_TS = datetime(2026, 6, 27, 10, tzinfo=UTC)


def test_model_score_write_requested_round_trips():
    event = ModelScoreWriteRequested(
        score_write_id="sw1",
        correlation_id="corr_1",
        occurred_at=_TS,
        entity=EntityRef("application", {"user_id": "1", "application_id": "A1"}),
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
    restored = ModelScoreWriteRequested.from_json(event.to_json())
    assert restored == event
    assert restored.event_type == "model_score.write.requested"
    assert restored.scores[0].value == 0.037


def test_batch_chunk_requested_round_trips():
    event = BatchChunkRequested(
        batch_job_id="batch-1",
        chunk_id="batch-1:0",
        chunk_index=0,
        chunk_count=2,
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
        requested_feature_groups=["pd_model_input_v1"],
        write_online=True,
    )
    restored = BatchChunkRequested.from_json(event.to_json())
    assert restored == event
    assert restored.event_type == "batch.chunk.requested"
    assert restored.chunk_id == "batch-1:0"
    assert restored.total_items == 0  # default when omitted
    assert restored.items[0].inline_sources["credit_report"]["payload"] == {
        "declared_income": 100000
    }


def test_batch_chunk_processed_round_trips():
    event = BatchChunkProcessed(
        batch_job_id="batch-1",
        chunk_id="batch-1:0",
        chunk_index=0,
        chunk_count=2,
        correlation_id="batch-1",
        processed_at=_TS,
        status="completed_with_errors",
        item_count=3,
        ok_items=2,
        failed_items=1,
        first_errors=["bad row"],
    )
    restored = BatchChunkProcessed.from_json(event.to_json())
    assert restored == event
    assert restored.event_type == "batch.chunk.processed"
    assert restored.failed_items == 1


def _descriptor() -> ReportDescriptor:
    return ReportDescriptor(
        report_ref="rep_bureau_123",
        source_name="bureau",
        report_type="credit_report",
        schema_version="v1",
        report_ts=_TS,
        object_key="raw-reports/bureau/2026/06/27/rep_bureau_123.json.gz",
        content_hash="sha256:abc",
        size_bytes=1430000,
        compression="gzip",
        format="json",
    )


def _event() -> FeatureComputeRequested:
    return FeatureComputeRequested(
        request_id="freq_1",
        job_id="job_1",
        priority="online",
        deadline_ms=1000,
        entity=EntityRef("application", {"user_id": "u1", "application_id": "a1"}),
        view="user_credit_risk",
        view_version=1,
        reports=[_descriptor()],
        write_policy="online_first",
        idempotency_key="application:a1:pd:hash",
        correlation_id="corr_1",
        occurred_at=_TS,
        requested_feature_groups=["pd_model_input_v1"],
        requested_features=[],
    )


def test_report_descriptor_json_round_trip():
    d = _descriptor()
    assert ReportDescriptor.from_dict(d.to_dict()) == d


def test_feature_compute_requested_json_round_trip():
    event = _event()
    assert FeatureComputeRequested.from_dict(event.to_dict()) == event
    assert FeatureComputeRequested.from_json(event.to_json()) == event


def test_feature_compute_requested_round_trips_deadline_fields():
    event_ts = datetime(2026, 6, 27, 10, 0, tzinfo=UTC)
    expires_at = datetime(2026, 6, 27, 10, 0, 5, tzinfo=UTC)
    event = dataclasses.replace(_event(), event_ts=event_ts, expires_at=expires_at)
    data = event.to_dict()
    assert data["event_ts"] == event_ts.isoformat()
    assert data["expires_at"] == expires_at.isoformat()
    restored = FeatureComputeRequested.from_dict(data)
    assert restored.event_ts == event_ts
    assert restored.expires_at == expires_at
    assert restored == event


def test_feature_compute_requested_deadline_fields_default_none():
    # Backward compatibility: legacy events omit the fields; parsing keeps them None.
    event = _event()
    assert event.event_ts is None and event.expires_at is None
    assert event.to_dict()["expires_at"] is None
    legacy = {k: v for k, v in event.to_dict().items() if k not in ("event_ts", "expires_at")}
    assert FeatureComputeRequested.from_dict(legacy).expires_at is None


def test_expires_at_must_be_after_event_ts():
    ts = datetime(2026, 6, 27, 10, 0, tzinfo=UTC)
    with pytest.raises(ValueError, match="expires_at"):
        dataclasses.replace(_event(), event_ts=ts, expires_at=ts)


def test_deadline_fields_reject_naive_datetimes():
    with pytest.raises(ValueError, match="timezone-aware"):
        dataclasses.replace(_event(), expires_at=datetime(2026, 6, 27, 10, 0))  # noqa: DTZ001


def test_event_has_explicit_type_and_version():
    data = _event().to_dict()
    assert data["event_type"] == "feature_compute.requested"
    assert data["event_version"] == 1


def test_event_carries_view_and_view_version():
    data = _event().to_dict()
    assert data["view"] == "user_credit_risk"
    assert data["view_version"] == 1
    # round-trip preserves them
    assert FeatureComputeRequested.from_dict(data).view == "user_credit_risk"
    assert FeatureComputeRequested.from_dict(data).view_version == 1


def test_feature_compute_completed_round_trip():
    completed = FeatureComputeCompleted(
        request_id="freq_1",
        job_id="job_1",
        entity=EntityRef("application", {"user_id": "u1"}),
        view="user_credit_risk",
        view_version=1,
        correlation_id="corr_1",
        occurred_at=_TS,
        written_features=["declared_income"],
        online_write_status="written",
    )
    assert FeatureComputeCompleted.from_dict(completed.to_dict()) == completed
    assert FeatureComputeCompleted.from_json(completed.to_json()) == completed
    assert completed.to_dict()["event_type"] == "feature_compute.completed"


def test_feature_compute_completed_deadline_expired_round_trip():
    completed = FeatureComputeCompleted(
        request_id="freq_1",
        job_id="job_1",
        entity=EntityRef("application", {"user_id": "u1"}),
        view="user_credit_risk",
        view_version=1,
        correlation_id="corr_1",
        occurred_at=_TS,
        written_features=[],
        online_write_status="deadline_expired",
    )
    restored = FeatureComputeCompleted.from_dict(completed.to_dict())
    assert restored.online_write_status == "deadline_expired"
    assert restored.written_features == []


def test_serialized_event_contains_no_raw_payload():
    # An event built from a report whose payload had a secret must not carry it.
    raw = json.loads(_event().to_json())
    flat = json.dumps(raw)
    assert "payload" not in flat
    assert "declared_income" not in flat


def test_event_contains_report_ref_and_object_key():
    report = _event().to_dict()["reports"][0]
    assert report["report_ref"] == "rep_bureau_123"
    assert report["object_key"].endswith("rep_bureau_123.json.gz")


def _write_set() -> FeatureWriteSet:
    entity_key = EntityKey.from_mapping({"user_id": "u1", "application_id": "a1"})
    result = FeatureResult(
        ref=FeatureRef("declared_income", 1),
        entity_key=entity_key,
        value=4_200_000,
        data_ts=_TS,
        calc_ts=_TS,
    )
    return FeatureWriteSet(
        view="user_credit_risk",
        view_version=1,
        entity_key=entity_key,
        results={"declared_income": result},
        source_refs={"credit_report": "rep_1"},
        request_id="freq_1",
        job_id="job_1",
    )


def test_offline_write_event_round_trip():
    event = FeatureOfflineWriteRequested(
        request_id="freq_1",
        job_id="job_1",
        correlation_id="corr_1",
        occurred_at=_TS,
        write_set=_write_set(),
    )
    restored = FeatureOfflineWriteRequested.from_dict(event.to_dict())
    assert restored.event_type == "feature_write.offline.requested"
    assert restored.write_set.view == "user_credit_risk"
    assert restored.write_set.results["declared_income"].value == 4_200_000
    assert FeatureOfflineWriteRequested.from_json(event.to_json()) == restored


def test_offline_write_event_contains_values_and_no_payload():
    event = FeatureOfflineWriteRequested(
        request_id="freq_1",
        job_id="job_1",
        correlation_id="corr_1",
        occurred_at=_TS,
        write_set=_write_set(),
    )
    flat = json.dumps(json.loads(event.to_json()))
    assert "4200000" in flat  # the computed value is carried
    assert "payload" not in flat  # but no raw payload


def _dlq_event(payload_bytes: bytes) -> DeadLetterEvent:
    return DeadLetterEvent(
        source_topic="fp.feature-compute.online",
        failure_stage="online_worker",
        failure_status="deserialization_failed",
        error="boom",
        occurred_at=_TS,
        source_payload_b64=base64.b64encode(payload_bytes).decode("ascii"),
    )


def test_dead_letter_event_round_trip():
    event = _dlq_event(b'{"request_id": "freq_1"}')
    restored = DeadLetterEvent.from_dict(event.to_dict())
    assert restored == event
    assert DeadLetterEvent.from_json(event.to_json()) == event
    assert restored.event_type == "event.dlq"


def test_dead_letter_event_captures_arbitrary_invalid_bytes():
    raw = b"\xff\xfe not json at all"
    event = _dlq_event(raw)
    # the original bytes survive a full round-trip via base64
    restored = DeadLetterEvent.from_json(event.to_json())
    assert base64.b64decode(restored.source_payload_b64) == raw


def test_dead_letter_event_best_effort_ids_default_none():
    event = _dlq_event(b"not-json")
    assert event.original_request_id is None
    assert event.original_job_id is None
    assert event.original_correlation_id is None


def test_dead_letter_event_has_no_raw_payload_field():
    data = _dlq_event(b"{}").to_dict()
    assert "payload" not in data
    assert "source_payload_b64" in data


def test_dead_letter_event_round_trip_with_attempts():
    event = DeadLetterEvent(
        source_topic="fp.feature-compute.online",
        failure_stage="online_worker",
        failure_status="compute_failed",
        error="boom",
        occurred_at=_TS,
        source_payload_b64=base64.b64encode(b"{}").decode("ascii"),
        attempt_count=5,
        max_attempts=5,
    )
    restored = DeadLetterEvent.from_dict(event.to_dict())
    assert restored == event
    assert restored.attempt_count == 5
    assert restored.max_attempts == 5


def test_structural_dlq_event_has_no_attempts():
    # Structural-poison DLQ events leave attempt fields None.
    event = _dlq_event(b"{}")
    assert event.attempt_count is None
    assert event.max_attempts is None
    assert DeadLetterEvent.from_dict(event.to_dict()).attempt_count is None


def test_idempotency_key_is_deterministic():
    entity = EntityRef("application", {"user_id": "u1", "application_id": "a1"})
    k1 = build_idempotency_key(entity, ["g1"], [], [_descriptor()])
    k2 = build_idempotency_key(entity, ["g1"], [], [_descriptor()])
    assert k1 == k2
    assert k1.startswith("application:")
