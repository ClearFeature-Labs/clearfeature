"""Kafka consumer runner for the Offline Writer (safe offset commit).

Consumes ``fp.feature-write.offline`` and appends offline history via the pure
``handle_feature_offline_write`` handler. Commits the offline-write message **only**
after the append succeeds (``OfflineWriteResult.status == "ok"``); on any failure the
message stays uncommitted and replays (the append is idempotent, so replay is safe).

Minimal and unit-testable: one-shot ``process_next`` + a bounded ``run``. No DLQ, no
retry, no status store, no daemon supervisor.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter as _perf_counter

from fintech_feature_platform.api.backend import AppBackend, build_backend
from fintech_feature_platform.api.dlq import (
    is_poison,
    next_attempt,
    republish_with_attempt,
    route_to_dlq,
)
from fintech_feature_platform.api.offline_writer import (
    OfflineWriteResult,
    handle_feature_offline_write,
)
from fintech_feature_platform.api.runner_daemon import (
    FOREVER_ROUND,
    close_consumer,
    install_shutdown_signals,
    log_round,
    maybe_start_metrics_server,
    record_worker_item,
    shutdown_requested,
)
from fintech_feature_platform.api.settings import load_settings
from fintech_feature_platform.fs_core.events.consumer import ConsumedMessage, EventConsumer
from fintech_feature_platform.fs_core.events.models import FeatureOfflineWriteRequested
from fintech_feature_platform.fs_core.events.topics import FEATURE_WRITE_OFFLINE
from fintech_feature_platform.fs_core.observability.logs import configure_logging

_DEFAULT_POLL_TIMEOUT_S = 1.0
_DEFAULT_MAX_ATTEMPTS = 5
_FAILURE_STAGE = "offline_writer"


@dataclass(frozen=True)
class ProcessResult:
    # status: idle | ok | consume_error | deserialization_failed | unexpected_error
    # | a non-ok OfflineWriteResult status (invalid_event / append_failed)
    status: str
    offline_result: OfflineWriteResult | None = None
    committed: bool = False
    error: str | None = None


def _safe_update(backend: AppBackend, request_id: str, **changes: object) -> None:
    """Best-effort request-status update; never raises, never affects commit."""
    try:
        backend.status.update(request_id, **changes)
    except Exception:  # noqa: BLE001 - status is observability only
        return


def _status_after_retry_or_dlq(
    backend: AppBackend,
    request_id: str,
    result: ProcessResult,
    failure_status: str,
    error: str | None,
) -> None:
    # Offline failures only move the offline sub-status; never flip top-level to failed.
    now = datetime.now(tz=UTC)
    if result.status == "retry_republished":
        _safe_update(
            backend, request_id, offline_write_status="retrying",
            error=f"retrying: {failure_status}", updated_at=now,
        )
    elif result.status == "dead_lettered":
        _safe_update(
            backend, request_id, offline_write_status="failed_dlq",
            error=error or failure_status, updated_at=now,
        )


def _dead_letter(
    consumer: EventConsumer,
    backend: AppBackend,
    message: ConsumedMessage,
    failure_status: str,
    error: str | None,
    offline_result: OfflineWriteResult | None = None,
) -> ProcessResult:
    """Publish a poison message to the DLQ; commit only if the DLQ publish succeeded."""
    published = route_to_dlq(
        publisher=backend.events,
        source_topic=FEATURE_WRITE_OFFLINE,
        failure_stage=_FAILURE_STAGE,
        failure_status=failure_status,
        error=error,
        message=message,
    )
    if published:
        consumer.commit(message)
        return ProcessResult(
            status="dead_lettered", offline_result=offline_result, committed=True, error=error
        )
    return ProcessResult(
        status="dlq_publish_failed", offline_result=offline_result, committed=False, error=error
    )


def _retry_or_dlq(
    consumer: EventConsumer,
    backend: AppBackend,
    message: ConsumedMessage,
    event: FeatureOfflineWriteRequested,
    failure_status: str,
    error: str | None,
    max_attempts: int,
) -> ProcessResult:
    """Republish-to-source with an incremented attempt header; DLQ at max_attempts.

    Commit the original only after the retry republish (or DLQ) is acknowledged.
    """
    failed_attempt = next_attempt(message.headers())
    if failed_attempt < max_attempts:
        published = republish_with_attempt(
            publisher=backend.events,
            topic=FEATURE_WRITE_OFFLINE,
            key=event.write_set.entity_key.encode(),
            event=event,
            headers=message.headers(),
            attempt=failed_attempt,
        )
        if published:
            consumer.commit(message)
            return ProcessResult(status="retry_republished", committed=True, error=error)
        return ProcessResult(status="retry_publish_failed", committed=False, error=error)

    published = route_to_dlq(
        publisher=backend.events,
        source_topic=FEATURE_WRITE_OFFLINE,
        failure_stage=_FAILURE_STAGE,
        failure_status=failure_status,
        error=error,
        message=message,
        attempt_count=failed_attempt,
        max_attempts=max_attempts,
    )
    if published:
        consumer.commit(message)
        return ProcessResult(status="dead_lettered", committed=True, error=error)
    return ProcessResult(status="dlq_publish_failed", committed=False, error=error)


def process_next(
    consumer: EventConsumer,
    backend: AppBackend,
    *,
    poll_timeout_s: float = _DEFAULT_POLL_TIMEOUT_S,
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
) -> ProcessResult:
    """Poll and process at most one offline-write message; commit only on success."""
    message = consumer.poll(poll_timeout_s)
    if message is None:
        return ProcessResult(status="idle")
    if message.error() is not None:
        return ProcessResult(status="consume_error", error=str(message.error()))

    try:
        event = FeatureOfflineWriteRequested.from_json(message.value())
    except Exception as exc:  # noqa: BLE001 - structural poison: route to DLQ
        return _dead_letter(consumer, backend, message, "deserialization_failed", str(exc))

    try:
        result = handle_feature_offline_write(backend, event)
    except Exception as exc:  # noqa: BLE001 - event-deterministic: attempt-limit then DLQ
        pr = _retry_or_dlq(
            consumer, backend, message, event, "unexpected_error", str(exc), max_attempts
        )
        _status_after_retry_or_dlq(backend, event.request_id, pr, "unexpected_error", str(exc))
        return pr

    if result.status == "ok":
        consumer.commit(message)
        _safe_update(
            backend, event.request_id, status="completed",
            offline_write_status="written", updated_at=datetime.now(tz=UTC),
        )
        return ProcessResult(status="ok", offline_result=result, committed=True)

    # Structural poison (invalid_event) -> DLQ; transient (append_failed) -> replay.
    if is_poison(result.status):
        pr = _dead_letter(
            consumer, backend, message, result.status, result.error, offline_result=result
        )
        if pr.status == "dead_lettered":
            # Do NOT flip a completed request to failed; only mark the offline sub-status.
            _safe_update(
                backend, event.request_id, offline_write_status="failed_dlq",
                error=result.error, updated_at=datetime.now(tz=UTC),
            )
        return pr

    return ProcessResult(status=result.status, offline_result=result, error=result.error)


def run(
    consumer: EventConsumer,
    backend: AppBackend,
    *,
    max_messages: int | None = None,
    poll_timeout_s: float = _DEFAULT_POLL_TIMEOUT_S,
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
) -> list[ProcessResult]:
    """Process up to ``max_messages`` iterations (bounded helper, not a daemon)."""
    results: list[ProcessResult] = []
    while max_messages is None or len(results) < max_messages:
        if shutdown_requested():
            break  # graceful stop : finish nothing new; close after the loop
        _item_started = _perf_counter()
        result = process_next(
            consumer, backend, poll_timeout_s=poll_timeout_s, max_attempts=max_attempts
        )
        record_worker_item(
            backend, "offline-writer", result.status, _perf_counter() - _item_started
        )
        results.append(result)
        if max_messages is None and result.status == "idle":
            break
    return results


def _main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Offline writer: consume fp.feature-write.offline into offline history."
    )
    parser.add_argument("--once", action="store_true", help="process a single message")
    parser.add_argument(
        "--max-messages", type=int, default=None, help="process N iterations then stop"
    )
    parser.add_argument(
        "--forever", action="store_true",
        help="poll forever in bounded rounds (daemon mode for compose/systemd)",
    )
    args = parser.parse_args(argv)

    configure_logging("offline-writer")
    settings = load_settings()
    backend = build_backend(settings)
    maybe_start_metrics_server("offline-writer", backend, settings)
    from fintech_feature_platform.fs_core.events.consumer import connect_kafka_consumer

    consumer = connect_kafka_consumer(
        settings.kafka_bootstrap_servers,
        settings.kafka_offline_writer_group,
        FEATURE_WRITE_OFFLINE,
        settings.kafka_auto_offset_reset,
        settings.kafka_client_id,
    )
    if args.forever:
        install_shutdown_signals("offline-writer")
        while not shutdown_requested():  # daemon mode : same consumer, bounded rounds
            results = run(
                consumer,
                backend,
                max_messages=FOREVER_ROUND,
                poll_timeout_s=settings.kafka_poll_timeout_ms / 1000,
                max_attempts=settings.kafka_max_attempts,
            )
            log_round(results, "offline-writer")
        close_consumer(consumer, "offline-writer")
        return
    max_messages = 1 if args.once else args.max_messages
    results = run(
        consumer,
        backend,
        max_messages=max_messages,
        poll_timeout_s=settings.kafka_poll_timeout_ms / 1000,
        max_attempts=settings.kafka_max_attempts,
    )
    committed = sum(1 for r in results if r.committed)
    print(f"processed={len(results)} committed={committed}")


if __name__ == "__main__":  # pragma: no cover - manual entrypoint
    _main()
