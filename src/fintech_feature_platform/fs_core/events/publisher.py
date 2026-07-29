"""Event publisher seam.

``EventPublisher`` is the small write-only contract the API uses to emit events.
``InMemoryEventPublisher`` records published events for default (Docker-free) tests.
``KafkaEventPublisher`` publishes to a Kafka-compatible broker using a lazily-imported
``confluent-kafka`` client; selecting it without the optional ``kafka`` extra raises a
clear error. No consumer/offset-commit logic lives here (deferred to a later task).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from fintech_feature_platform.fs_core.events.consumer import normalize_headers


@dataclass(frozen=True)
class PublishResult:
    topic: str
    key: str
    event_type: str
    idempotency_key: str
    delivered: bool
    partition: int | None = None
    offset: int | None = None
    error: str | None = None


@dataclass(frozen=True)
class PublishedRecord:
    """One recorded publish, exposed by ``InMemoryEventPublisher`` for tests."""

    topic: str
    key: str
    event: Any
    headers: dict[str, bytes] = field(default_factory=dict)


class EventPublisher(Protocol):
    def publish(
        self,
        topic: str,
        key: str,
        event: Any,
        *,
        headers: Mapping[str, bytes | str] | None = None,
    ) -> PublishResult: ...


def _event_fields(event: Any) -> tuple[str, str]:
    """Best-effort (event_type, idempotency_key) for the PublishResult."""
    event_type = getattr(event, "event_type", "unknown")
    idempotency_key = getattr(event, "idempotency_key", "")
    return event_type, idempotency_key


def _event_to_json_bytes(event: Any) -> bytes:
    if hasattr(event, "to_json"):
        return event.to_json()
    if hasattr(event, "to_dict"):
        return json.dumps(event.to_dict(), sort_keys=True).encode("utf-8")
    raise TypeError(f"event {type(event)!r} is not serializable (needs to_json/to_dict)")


class InMemoryEventPublisher:
    """Records events in memory. Default publisher; needs no broker."""

    def __init__(self) -> None:
        self.published: list[PublishedRecord] = []

    def publish(
        self,
        topic: str,
        key: str,
        event: Any,
        *,
        headers: Mapping[str, bytes | str] | None = None,
    ) -> PublishResult:
        self.published.append(
            PublishedRecord(
                topic=topic, key=key, event=event, headers=normalize_headers(headers)
            )
        )
        event_type, idempotency_key = _event_fields(event)
        return PublishResult(
            topic=topic,
            key=key,
            event_type=event_type,
            idempotency_key=idempotency_key,
            delivered=True,
        )


def connect_kafka_producer(bootstrap_servers: str, client_id: str) -> Any:
    """Create a confluent-kafka Producer, importing the optional client lazily.

    Raises a clear RuntimeError if the ``kafka`` extra is not installed.
    """
    try:
        from confluent_kafka import Producer
    except ImportError as exc:  # pragma: no cover - exercised via missing-extra test
        raise RuntimeError(
            "Kafka event publishing requires the 'confluent-kafka' package; "
            "install with: uv sync --extra kafka"
        ) from exc

    return Producer({"bootstrap.servers": bootstrap_servers, "client.id": client_id})


class KafkaEventPublisher:
    """Publishes events to a Kafka-compatible broker (confluent-kafka, lazy import).

    Constructing this eagerly creates the producer, so selecting Kafka without the
    optional extra fails fast and clearly at backend-build time. ``publish`` flushes
    and surfaces delivery failures as exceptions, so the caller never reports
    ``accepted`` after a failed publish.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        client_id: str,
        flush_timeout_s: float = 10.0,
    ) -> None:
        self._producer = connect_kafka_producer(bootstrap_servers, client_id)
        self._flush_timeout_s = flush_timeout_s

    def ping(self, timeout_s: float = 2.0) -> None:
        """Bounded broker-connectivity check (readiness,).

        A cluster-metadata request over the EXISTING producer (librdkafka handles are
        thread-safe): no topic writes, no consumer-group effects. Raises on failure.
        """
        self._producer.list_topics(timeout=timeout_s)

    def publish(
        self,
        topic: str,
        key: str,
        event: Any,
        *,
        headers: Mapping[str, bytes | str] | None = None,
    ) -> PublishResult:
        event_type, idempotency_key = _event_fields(event)
        value = _event_to_json_bytes(event)
        kafka_headers = list(normalize_headers(headers).items()) or None
        delivery: dict[str, Any] = {}

        def _on_delivery(err: Any, msg: Any) -> None:
            delivery["err"] = err
            delivery["msg"] = msg

        self._producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=value,
            headers=kafka_headers,
            on_delivery=_on_delivery,
        )
        remaining = self._producer.flush(self._flush_timeout_s)
        if remaining:
            raise RuntimeError(
                f"Kafka publish to {topic!r} did not flush within "
                f"{self._flush_timeout_s}s ({remaining} message(s) pending)"
            )
        err = delivery.get("err")
        if err is not None:
            raise RuntimeError(f"Kafka delivery failed for topic {topic!r}: {err}")

        msg = delivery.get("msg")
        return PublishResult(
            topic=topic,
            key=key,
            event_type=event_type,
            idempotency_key=idempotency_key,
            delivered=True,
            partition=msg.partition() if msg is not None else None,
            offset=msg.offset() if msg is not None else None,
        )
