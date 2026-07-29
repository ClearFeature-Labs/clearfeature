"""Tests for the model score writer handler + runner."""

import dataclasses
from datetime import UTC, datetime

from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.model_score_writer import handle_model_score_write
from fintech_feature_platform.api.model_score_writer_runner import process_next
from fintech_feature_platform.fs_core.events.consumer import (
    InMemoryEventConsumer,
    InMemoryMessage,
)
from fintech_feature_platform.fs_core.events.models import (
    EntityRef,
    ModelScoreWriteRequested,
    ScoreItem,
)
from fintech_feature_platform.fs_core.events.topics import DLQ, FEATURE_WRITE_OFFLINE
from fintech_feature_platform.fs_core.models import EntityKey

_TS = datetime(2026, 1, 1, 12, tzinfo=UTC)
_ENTITY = {"user_id": "1", "application_id": "A1"}


def _entity_key() -> EntityKey:
    return EntityKey.from_mapping(_ENTITY, key_order=["user_id", "application_id"])


def _event(*, write_online=True, write_offline=True, value=0.037, data_ts=_TS, sid="sw1"):
    return ModelScoreWriteRequested(
        score_write_id=sid,
        correlation_id="corr_1",
        occurred_at=_TS,
        entity=EntityRef("application", dict(_ENTITY)),
        view="user_credit_risk",
        view_version=1,
        scores=[
            ScoreItem(
                feature="pd_score",
                value=value,
                data_ts=data_ts,
                calc_ts=data_ts,
                model_name="pd_model",
                model_version="v4",
                source_request_id="freq_abc",
            )
        ],
        idempotency_key=sid,
        write_online=write_online,
        write_offline=write_offline,
    )


# --- handler ----------------------------------------------------------------

def test_handler_writes_online_and_publishes_offline_event():
    backend = build_memory_backend()
    result = handle_model_score_write(backend, _event())
    assert result.status == "ok"
    got = backend.online.get("user_credit_risk", 1, _entity_key(), "pd_score", 1)
    assert got is not None
    assert got.value == 0.037
    offline = [r for r in backend.events.published if r.topic == FEATURE_WRITE_OFFLINE]
    assert len(offline) == 1
    assert offline[0].event.write_set.run_id == "sw1"  # stable for offline dedup


def test_handler_online_only():
    backend = build_memory_backend()
    result = handle_model_score_write(backend, _event(write_offline=False))
    assert result.status == "ok"
    assert backend.online.get("user_credit_risk", 1, _entity_key(), "pd_score", 1) is not None
    assert [r for r in backend.events.published if r.topic == FEATURE_WRITE_OFFLINE] == []


def test_handler_offline_only():
    backend = build_memory_backend()
    result = handle_model_score_write(backend, _event(write_online=False))
    assert result.status == "ok"
    assert backend.online.get("user_credit_risk", 1, _entity_key(), "pd_score", 1) is None
    assert len([r for r in backend.events.published if r.topic == FEATURE_WRITE_OFFLINE]) == 1


def test_handler_invalid_feature_is_invalid_event():
    backend = build_memory_backend()
    event = dataclasses.replace(
        _event(),
        scores=[dataclasses.replace(_event().scores[0], feature="declared_income")],
    )
    result = handle_model_score_write(backend, event)
    assert result.status == "invalid_event"


def test_handler_replay_online_cas_is_idempotent():
    backend = build_memory_backend()
    handle_model_score_write(backend, _event())
    result2 = handle_model_score_write(backend, _event())  # same data_ts
    assert result2.status == "ok"
    # D9: same tuple, no fingerprints -> idempotent replay is a noop (pre-D9 skip)
    assert result2.online_written["pd_score:v1"] == "noop"


# --- runner -----------------------------------------------------------------

def test_runner_commits_after_ok():
    backend = build_memory_backend()
    consumer = InMemoryEventConsumer([InMemoryMessage(_event().to_json())])
    result = process_next(consumer, backend)
    assert result.status == "ok"
    assert result.committed is True
    assert len(consumer.committed) == 1


def test_runner_no_commit_on_online_write_failure():
    class _FailingOnline:
        def write_many(self, view, view_version, results):
            raise RuntimeError("valkey down")

        def get(self, *a, **k):
            return None

    backend = dataclasses.replace(build_memory_backend(), online=_FailingOnline())
    consumer = InMemoryEventConsumer([InMemoryMessage(_event().to_json())])
    result = process_next(consumer, backend)
    assert result.status == "online_write_failed"
    assert result.committed is False
    assert consumer.committed == []


def test_runner_no_commit_on_offline_publish_failure():
    class _FailingPublisher:
        def publish(self, topic, key, event):
            raise RuntimeError("kafka down")

    backend = dataclasses.replace(build_memory_backend(), events=_FailingPublisher())
    consumer = InMemoryEventConsumer([InMemoryMessage(_event().to_json())])
    result = process_next(consumer, backend)
    assert result.status == "publish_failed"
    assert result.committed is False
    assert consumer.committed == []


def test_runner_dead_letters_structural_poison():
    backend = build_memory_backend()
    consumer = InMemoryEventConsumer([InMemoryMessage(b"not-json")])
    result = process_next(consumer, backend)
    assert result.status == "dead_lettered"
    assert result.committed is True
    dlq = [r for r in backend.events.published if r.topic == DLQ]
    assert len(dlq) == 1
    assert dlq[0].event.failure_stage == "model_score_writer"


def test_runner_dead_letters_invalid_event():
    backend = build_memory_backend()
    event = dataclasses.replace(
        _event(),
        scores=[dataclasses.replace(_event().scores[0], feature="declared_income")],
    )
    consumer = InMemoryEventConsumer([InMemoryMessage(event.to_json())])
    result = process_next(consumer, backend)
    assert result.status == "dead_lettered"
    assert result.committed is True
