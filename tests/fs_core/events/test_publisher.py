"""Tests for the event publisher seam."""

from datetime import UTC, datetime

import pytest

from fintech_feature_platform.api.backend import build_event_publisher
from fintech_feature_platform.api.settings import Settings
from fintech_feature_platform.fs_core.events.models import (
    EntityRef,
    FeatureComputeRequested,
)
from fintech_feature_platform.fs_core.events.publisher import (
    InMemoryEventPublisher,
    connect_kafka_producer,
)
from fintech_feature_platform.fs_core.events.topics import FEATURE_COMPUTE_ONLINE

_TS = datetime(2026, 6, 27, 10, tzinfo=UTC)


def _event() -> FeatureComputeRequested:
    return FeatureComputeRequested(
        request_id="freq_1",
        job_id="job_1",
        priority="online",
        deadline_ms=1000,
        entity=EntityRef("application", {"user_id": "u1"}),
        view="user_credit_risk",
        view_version=1,
        reports=[],
        write_policy="online_first",
        idempotency_key="idem_1",
        correlation_id="corr_1",
        occurred_at=_TS,
        requested_feature_groups=["pd_model_input_v1"],
    )


def test_in_memory_publisher_records_topic_key_event():
    pub = InMemoryEventPublisher()
    event = _event()

    result = pub.publish(FEATURE_COMPUTE_ONLINE, "user_id=u1", event)

    assert result.delivered is True
    assert result.topic == FEATURE_COMPUTE_ONLINE
    assert result.event_type == "feature_compute.requested"
    assert result.idempotency_key == "idem_1"
    assert len(pub.published) == 1
    record = pub.published[0]
    assert record.topic == FEATURE_COMPUTE_ONLINE
    assert record.key == "user_id=u1"
    assert record.event is event


def test_connect_kafka_producer_raises_clear_error_without_extra():
    # confluent-kafka is not installed under the dev extra; selecting Kafka must fail
    # with a clear, actionable message and never silently degrade.
    with pytest.raises(RuntimeError, match="uv sync --extra kafka"):
        connect_kafka_producer("localhost:19092", "client")


def test_build_event_publisher_kafka_requires_extra():
    settings = Settings(events="kafka")
    with pytest.raises(RuntimeError, match="confluent-kafka"):
        build_event_publisher(settings)


def test_build_event_publisher_memory_is_in_memory():
    assert isinstance(build_event_publisher(Settings()), InMemoryEventPublisher)


# --- headers  ---------------------------------------------------

def test_in_memory_publisher_records_normalized_headers():
    pub = InMemoryEventPublisher()
    pub.publish(FEATURE_COMPUTE_ONLINE, "k", _event(), headers={"x-fsp-attempt": "3"})
    assert pub.published[0].headers == {"x-fsp-attempt": b"3"}


def test_in_memory_publisher_without_headers_defaults_empty():
    pub = InMemoryEventPublisher()
    pub.publish(FEATURE_COMPUTE_ONLINE, "k", _event())
    assert pub.published[0].headers == {}


def test_kafka_publisher_forwards_normalized_headers(monkeypatch):
    from fintech_feature_platform.fs_core.events import publisher as pub_mod

    produced: dict = {}

    class _FakeMsg:
        def partition(self):
            return 0

        def offset(self):
            return 1

    class _FakeProducer:
        def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
            produced["headers"] = headers
            on_delivery(None, _FakeMsg())

        def flush(self, timeout):
            return 0

    monkeypatch.setattr(
        pub_mod, "connect_kafka_producer", lambda bootstrap, client_id: _FakeProducer()
    )
    publisher = pub_mod.KafkaEventPublisher("localhost:19092", "client")
    publisher.publish(FEATURE_COMPUTE_ONLINE, "k", _event(), headers={"x-fsp-attempt": "3"})

    assert produced["headers"] == [("x-fsp-attempt", b"3")]
