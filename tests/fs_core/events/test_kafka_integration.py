"""Optional live Kafka publish/consume round trip.

Skipped by default. Requires the ``kafka`` extra and a running Redpanda broker:

    uv sync --extra kafka
    docker compose up -d --wait redpanda
    FSP_EVENTS=kafka FSP_KAFKA_INTEGRATION=1 \
        uv run pytest tests/fs_core/events/test_kafka_integration.py
    docker compose down
"""

import os
import uuid
from datetime import UTC, datetime

import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("FSP_KAFKA_INTEGRATION") != "1",
    reason="set FSP_KAFKA_INTEGRATION=1 (and run Redpanda) to enable",
)


def test_kafka_publish_and_consume_round_trip():
    pytest.importorskip("confluent_kafka")
    from confluent_kafka import Consumer

    from fintech_feature_platform.fs_core.events.models import (
        EntityRef,
        FeatureComputeRequested,
    )
    from fintech_feature_platform.fs_core.events.publisher import KafkaEventPublisher

    bootstrap = os.getenv("FSP_KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    topic = f"fp.test.{uuid.uuid4().hex}"
    event = FeatureComputeRequested(
        request_id="freq_int",
        job_id="job_int",
        priority="online",
        deadline_ms=1000,
        entity=EntityRef("application", {"user_id": "u1"}),
        view="user_credit_risk",
        view_version=1,
        reports=[],
        write_policy="online_first",
        idempotency_key="idem_int",
        correlation_id="corr_int",
        occurred_at=datetime(2026, 6, 27, 10, tzinfo=UTC),
        requested_feature_groups=["g1"],
    )

    publisher = KafkaEventPublisher(bootstrap, "itest")
    result = publisher.publish(topic, event.entity.encoded(), event)
    assert result.delivered is True

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"itest-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    try:
        msg = None
        for _ in range(50):
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                break
        assert msg is not None and not msg.error()
        received = FeatureComputeRequested.from_json(msg.value())
        assert received == event
    finally:
        consumer.close()
