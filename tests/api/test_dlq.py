"""Tests for the shared DLQ helper."""

import base64

from fintech_feature_platform.api.dlq import (
    ATTEMPT_HEADER,
    build_dead_letter_event,
    get_attempt,
    is_poison,
    next_attempt,
    route_to_dlq,
    with_attempt_header,
)
from fintech_feature_platform.fs_core.events.consumer import InMemoryMessage
from fintech_feature_platform.fs_core.events.publisher import InMemoryEventPublisher
from fintech_feature_platform.fs_core.events.topics import DLQ


def test_is_poison_only_structural_statuses():
    assert is_poison("deserialization_failed") is True
    assert is_poison("invalid_event") is True
    for status in (
        "compute_failed",
        "online_write_failed",
        "publish_failed",
        "append_failed",
        "unexpected_error",
        "consume_error",
        "idle",
        "ok",
    ):
        assert is_poison(status) is False


def test_build_dead_letter_event_captures_bytes_and_ids():
    raw = b'{"request_id": "freq_1", "job_id": "job_1", "correlation_id": "corr_1"}'
    event = build_dead_letter_event(
        source_topic="fp.feature-compute.online",
        failure_stage="online_worker",
        failure_status="deserialization_failed",
        error="boom",
        message=InMemoryMessage(raw),
    )
    assert base64.b64decode(event.source_payload_b64) == raw
    assert event.original_request_id == "freq_1"
    assert event.original_job_id == "job_1"
    assert event.original_correlation_id == "corr_1"


def test_build_dead_letter_event_fallback_ids_and_event_type():
    #: batch/score events carry their ids under different keys.
    raw = (
        b'{"event_type": "batch_chunk.processed", "batch_job_id": "batch-1",'
        b' "score_write_id": "sw1"}'
    )
    event = build_dead_letter_event(
        source_topic="fp.batch.events",
        failure_stage="metadata_writer",
        failure_status="structural_conflict",
        error="BatchMetadataConflictError: chunk",
        message=InMemoryMessage(raw),
    )
    assert event.original_job_id == "batch-1"  # batch_job_id fallback
    assert event.original_request_id == "sw1"  # score_write_id fallback
    assert event.original_event_type == "batch_chunk.processed"


def test_build_dead_letter_event_prefers_primary_ids():
    raw = b'{"request_id": "freq_1", "job_id": "job_1", "batch_job_id": "batch-9"}'
    event = build_dead_letter_event(
        source_topic="t",
        failure_stage="metadata_writer",
        failure_status="structural_conflict",
        error=None,
        message=InMemoryMessage(raw),
    )
    assert event.original_request_id == "freq_1"
    assert event.original_job_id == "job_1"  # primary key wins over fallback


def test_build_dead_letter_event_unparseable_bytes_ids_none():
    event = build_dead_letter_event(
        source_topic="fp.feature-compute.online",
        failure_stage="online_worker",
        failure_status="deserialization_failed",
        error="boom",
        message=InMemoryMessage(b"\x00not-json"),
    )
    assert base64.b64decode(event.source_payload_b64) == b"\x00not-json"
    assert event.original_request_id is None


def test_route_to_dlq_publishes_and_returns_true():
    publisher = InMemoryEventPublisher()
    ok = route_to_dlq(
        publisher=publisher,
        source_topic="fp.feature-compute.online",
        failure_stage="online_worker",
        failure_status="deserialization_failed",
        error="boom",
        message=InMemoryMessage(b"not-json"),
    )
    assert ok is True
    assert len(publisher.published) == 1
    assert publisher.published[0].topic == DLQ
    assert publisher.published[0].event.failure_status == "deserialization_failed"


def test_get_attempt_missing_is_zero():
    assert get_attempt({}) == 0


def test_get_attempt_reads_bytes():
    assert get_attempt({ATTEMPT_HEADER: b"2"}) == 2


def test_get_attempt_invalid_is_zero():
    assert get_attempt({ATTEMPT_HEADER: b"not-a-number"}) == 0


def test_next_attempt_increments():
    assert next_attempt({}) == 1
    assert next_attempt({ATTEMPT_HEADER: b"3"}) == 4


def test_with_attempt_header_preserves_and_sets():
    out = with_attempt_header({"other": b"v"}, 2)
    assert out == {"other": b"v", ATTEMPT_HEADER: b"2"}


def test_route_to_dlq_returns_false_on_publish_failure():
    class _FailingPublisher:
        def publish(self, topic, key, event):
            raise RuntimeError("dlq down")

    ok = route_to_dlq(
        publisher=_FailingPublisher(),
        source_topic="fp.feature-compute.online",
        failure_stage="online_worker",
        failure_status="deserialization_failed",
        error="boom",
        message=InMemoryMessage(b"not-json"),
    )
    assert ok is False
