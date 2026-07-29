"""Tests for the event consumer seam header plumbing."""

import pytest

from fintech_feature_platform.fs_core.events.consumer import (
    InMemoryEventConsumer,
    InMemoryMessage,
    connect_kafka_consumer,
    normalize_headers,
)


def test_in_memory_message_headers_default_empty():
    assert InMemoryMessage(b"x").headers() == {}


def test_in_memory_message_headers_normalized_to_bytes():
    msg = InMemoryMessage(b"x", headers={"x-fsp-attempt": "2", "raw": b"v"})
    assert msg.headers() == {"x-fsp-attempt": b"2", "raw": b"v"}


def test_in_memory_message_headers_returns_copy():
    msg = InMemoryMessage(b"x", headers={"k": b"v"})
    grabbed = msg.headers()
    grabbed["k"] = b"mutated"
    assert msg.headers() == {"k": b"v"}


def test_in_memory_consumer_preserves_headers_through_poll():
    consumer = InMemoryEventConsumer(
        [InMemoryMessage(b"x", headers={"x-fsp-attempt": "1"})]
    )
    message = consumer.poll(0.0)
    assert message is not None
    assert message.headers() == {"x-fsp-attempt": b"1"}


def test_normalize_headers_handles_none_str_bytes_and_none_value():
    assert normalize_headers(None) == {}
    assert normalize_headers({"a": "1", "b": b"2", "c": None}) == {
        "a": b"1",
        "b": b"2",
        "c": b"",
    }


def test_connect_kafka_consumer_accepts_str_or_list_without_extra():
    # confluent-kafka absent -> both single-topic and multi-topic signatures raise the
    # same clear error (i.e. the list form is accepted, not a TypeError).
    with pytest.raises(RuntimeError, match="uv sync --extra kafka"):
        connect_kafka_consumer("localhost:19092", "g", "fp.feature-compute.online")
    with pytest.raises(RuntimeError, match="uv sync --extra kafka"):
        connect_kafka_consumer(
            "localhost:19092", "g", ["fp.feature-compute.online", "fp.dlq"]
        )
