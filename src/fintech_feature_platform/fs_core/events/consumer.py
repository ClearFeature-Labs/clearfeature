"""Event consumer seam (mirror of the publisher seam).

``EventConsumer`` is the narrow read contract the online worker runner polls.
``InMemoryEventConsumer`` replays canned messages and records commits for default
(Docker-free) tests. ``connect_kafka_consumer`` builds a confluent-kafka consumer with
**auto-commit disabled** (the runner commits explicitly, only after a successful
handler) using a lazily-imported client.

This module stays independent of ``api`` (no ``Settings`` import) — it takes simple
parameters, exactly like ``connect_kafka_producer``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol


def normalize_headers(
    headers: Mapping[str, bytes | str] | None,
) -> dict[str, bytes]:
    """Normalize header values to ``bytes`` (str -> UTF-8, None -> empty)."""
    if not headers:
        return {}
    normalized: dict[str, bytes] = {}
    for key, value in headers.items():
        if isinstance(value, bytes):
            normalized[key] = value
        elif value is None:
            normalized[key] = b""
        else:
            normalized[key] = str(value).encode("utf-8")
    return normalized


class ConsumedMessage(Protocol):
    def value(self) -> bytes: ...

    def error(self) -> object | None: ...

    def headers(self) -> dict[str, bytes]: ...


class EventConsumer(Protocol):
    def poll(self, timeout_s: float) -> ConsumedMessage | None: ...

    def commit(self, message: ConsumedMessage) -> None: ...


class InMemoryMessage:
    """A simple ConsumedMessage for tests (``error``/``headers`` default to empty)."""

    def __init__(
        self,
        value: bytes,
        error: object | None = None,
        headers: Mapping[str, bytes | str] | None = None,
    ) -> None:
        self._value = value
        self._error = error
        self._headers = normalize_headers(headers)

    def value(self) -> bytes:
        return self._value

    def error(self) -> object | None:
        return self._error

    def headers(self) -> dict[str, bytes]:
        return dict(self._headers)


class InMemoryEventConsumer:
    """Replays a fixed list of messages; records committed messages for assertions."""

    def __init__(self, messages: list[ConsumedMessage] | None = None) -> None:
        self._messages: list[ConsumedMessage] = list(messages or [])
        self._index = 0
        self.committed: list[ConsumedMessage] = []

    def poll(self, timeout_s: float) -> ConsumedMessage | None:
        if self._index >= len(self._messages):
            return None
        message = self._messages[self._index]
        self._index += 1
        return message

    def commit(self, message: ConsumedMessage) -> None:
        self.committed.append(message)


def build_consumer_config(
    bootstrap_servers: str,
    group_id: str,
    auto_offset_reset: str,
    client_id: str,
) -> dict[str, Any]:
    """Pure consumer config builder (testable without confluent-kafka).

    ``enable.auto.commit`` is always ``False``: the runner commits explicitly and only
    after the handler returns ``ok``.
    """
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "client.id": client_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": False,
    }


class _KafkaConsumedMessage:
    """Adapts a confluent ``Message`` to ``ConsumedMessage``.

    confluent exposes ``headers()`` as a ``list[(str, bytes)]`` (or ``None``); this maps
    it to ``dict[str, bytes]``. The raw message is retained so the consumer can commit it.
    """

    def __init__(self, raw: Any) -> None:
        self._raw = raw

    def value(self) -> bytes:
        return self._raw.value()

    def error(self) -> object | None:
        return self._raw.error()

    def headers(self) -> dict[str, bytes]:
        raw_headers = self._raw.headers()
        if not raw_headers:
            return {}
        return normalize_headers(dict(raw_headers))


class _KafkaEventConsumer:
    """Thin adapter so a confluent ``Consumer`` satisfies ``EventConsumer``.

    ``poll`` wraps the confluent ``Message`` in ``_KafkaConsumedMessage`` (for header
    normalization); ``commit`` unwraps it for a synchronous per-message commit.
    """

    def __init__(self, consumer: Any) -> None:
        self._consumer = consumer

    def poll(self, timeout_s: float) -> ConsumedMessage | None:
        raw = self._consumer.poll(timeout_s)
        return _KafkaConsumedMessage(raw) if raw is not None else None

    def commit(self, message: ConsumedMessage) -> None:
        raw = message._raw if isinstance(message, _KafkaConsumedMessage) else message
        self._consumer.commit(message=raw, asynchronous=False)

    def close(self) -> None:
        self._consumer.close()


def connect_kafka_consumer(
    bootstrap_servers: str,
    group_id: str,
    topic: str | Sequence[str],
    auto_offset_reset: str = "earliest",
    client_id: str = "fintech-feature-platform",
) -> _KafkaEventConsumer:
    """Create a confluent-kafka consumer subscribed to ``topic`` (lazy import).

    ``topic`` may be a single topic (str) or a list of topics (the Metadata Writer
    subscribes to several). Auto-commit is disabled. Raises a clear RuntimeError if the
    ``kafka`` extra is not installed.
    """
    try:
        from confluent_kafka import Consumer
    except ImportError as exc:  # pragma: no cover - exercised via missing-extra test
        raise RuntimeError(
            "Kafka event consuming requires the 'confluent-kafka' package; "
            "install with: uv sync --extra kafka"
        ) from exc

    topics = [topic] if isinstance(topic, str) else list(topic)
    consumer = Consumer(
        build_consumer_config(bootstrap_servers, group_id, auto_offset_reset, client_id)
    )
    consumer.subscribe(topics)
    return _KafkaEventConsumer(consumer)
