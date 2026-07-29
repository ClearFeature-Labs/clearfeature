"""Shared dead-letter-queue helper for the Kafka-first worker runners.

Only **structural-poison** statuses are dead-lettered in this task — messages that can
never be processed regardless of code/config: a message whose bytes don't deserialize
(``deserialization_failed``) or that parses but is structurally invalid
(``invalid_event``). Ambiguous/transient failures are NOT dead-lettered here; they stay
uncommitted and replay until the attempt-limit task.

``route_to_dlq`` publishes the original message (base64) to the DLQ topic and returns
whether the publish was acknowledged. It never commits — the runner commits the source
message only after a successful DLQ publish (non-lossy).
"""

from __future__ import annotations

import base64
import json
import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from fintech_feature_platform.fs_core.events.consumer import ConsumedMessage
from fintech_feature_platform.fs_core.events.models import DeadLetterEvent
from fintech_feature_platform.fs_core.events.publisher import EventPublisher
from fintech_feature_platform.fs_core.events.topics import DLQ
from fintech_feature_platform.fs_core.observability.logs import get_logger, log_event

_logger = get_logger("worker")

# Statuses that are permanently unprocessable -> safe to dead-letter without a retry count.
_POISON_STATUSES = frozenset({"deserialization_failed", "invalid_event"})

# Durable per-message retry-attempt counter; lives in the message header.
ATTEMPT_HEADER = "x-fsp-attempt"


def is_poison(status: str) -> bool:
    """True only for structural-poison statuses (deserialization_failed, invalid_event)."""
    return status in _POISON_STATUSES


def get_attempt(headers: Mapping[str, bytes]) -> int:
    """Current attempt count from ``x-fsp-attempt`` (missing/invalid -> 0)."""
    raw = headers.get(ATTEMPT_HEADER)
    if raw is None:
        return 0
    try:
        return int(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
    except (ValueError, AttributeError):
        return 0


def next_attempt(headers: Mapping[str, bytes]) -> int:
    """The attempt number for the *next* delivery (current + 1)."""
    return get_attempt(headers) + 1


def with_attempt_header(
    headers: Mapping[str, bytes], attempt: int
) -> dict[str, bytes]:
    """Copy headers with ``x-fsp-attempt`` set to ``attempt`` (bytes)."""
    new = dict(headers)
    new[ATTEMPT_HEADER] = str(attempt).encode("utf-8")
    return new


def republish_with_attempt(
    *,
    publisher: EventPublisher,
    topic: str,
    key: str,
    event: Any,
    headers: Mapping[str, bytes],
    attempt: int,
) -> bool:
    """Republish a parsed event to ``topic`` with an incremented attempt header.

    Returns True iff the publish was acknowledged. Does not commit — the runner commits
    the original message only when this returns True.
    """
    try:
        publisher.publish(
            topic, key, event, headers=with_attempt_header(headers, attempt)
        )
    except Exception:  # noqa: BLE001 - broker unavailable: do not commit, let it replay
        return False
    # Structured retry event : technical correlation ids only.
    log_event(
        _logger, logging.WARNING, "message_retry_republished",
        operation="retry", topic=topic, attempt=attempt,
        request_id=getattr(event, "request_id", None),
        correlation_id=getattr(event, "correlation_id", None),
    )
    return True


def build_dead_letter_event(
    *,
    source_topic: str,
    failure_stage: str,
    failure_status: str,
    error: str | None,
    message: ConsumedMessage,
    attempt_count: int | None = None,
    max_attempts: int | None = None,
) -> DeadLetterEvent:
    """Build a DLQ event capturing the original bytes; best-effort ids if parseable.

    ``attempt_count``/``max_attempts`` are set for attempt-limit DLQ  and
    left ``None`` for structural-poison DLQ.
    """
    raw = message.value()
    payload_b64 = base64.b64encode(raw).decode("ascii")

    request_id = job_id = correlation_id = event_type = None
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            # Best-effort id fallbacks cover score writes and batch chunk events.
            request_id = parsed.get("request_id") or parsed.get("score_write_id")
            job_id = parsed.get("job_id") or parsed.get("batch_job_id")
            correlation_id = parsed.get("correlation_id")
            event_type = parsed.get("event_type")
    except Exception:  # noqa: BLE001 - unparseable poison: ids stay None
        pass

    return DeadLetterEvent(
        source_topic=source_topic,
        failure_stage=failure_stage,
        failure_status=failure_status,
        error=error,
        occurred_at=datetime.now(tz=UTC),
        source_payload_b64=payload_b64,
        original_request_id=request_id,
        original_job_id=job_id,
        original_correlation_id=correlation_id,
        attempt_count=attempt_count,
        max_attempts=max_attempts,
        original_event_type=event_type,
    )


def route_to_dlq(
    *,
    publisher: EventPublisher,
    source_topic: str,
    failure_stage: str,
    failure_status: str,
    error: str | None,
    message: ConsumedMessage,
    attempt_count: int | None = None,
    max_attempts: int | None = None,
) -> bool:
    """Publish the poison message to the DLQ topic; return True iff the publish succeeded.

    Does not commit — the runner commits the source message only when this returns True.
    """
    event = build_dead_letter_event(
        source_topic=source_topic,
        failure_stage=failure_stage,
        failure_status=failure_status,
        error=error,
        message=message,
        attempt_count=attempt_count,
        max_attempts=max_attempts,
    )
    try:
        publisher.publish(DLQ, source_topic, event)
    except Exception:  # noqa: BLE001 - DLQ unavailable: do not commit, let it replay
        return False
    # Structured DLQ event : bounded fields + the best-effort
    # technical ids already extracted above; never the payload bytes.
    log_event(
        _logger, logging.ERROR, "message_dead_lettered",
        operation="dead_letter", source_topic=source_topic,
        failure_stage=failure_stage, failure_status=failure_status,
        request_id=event.original_request_id, job_id=event.original_job_id,
        correlation_id=event.original_correlation_id,
        attempt_count=attempt_count,
    )
    return True
