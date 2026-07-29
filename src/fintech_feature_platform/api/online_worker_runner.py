"""Kafka consumer runner for the online feature worker (safe offset commit).

This connects an ``EventConsumer`` to the existing pure handler
(``handle_feature_compute_requested``) and enforces the the platform design rule rule:

    Commit the source offset ONLY when WorkerResult.status == "ok"
    (i.e. online write succeeded AND the completion publish was acknowledged).

It is intentionally minimal: a one-shot ``process_next`` plus a bounded ``run`` loop.
There is **no DLQ, no retry counter, no status store, and no daemon supervisor**.
Poison messages stay uncommitted and may replay until the DLQ/retry task exists.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter
from time import perf_counter as _perf_counter

from fintech_feature_platform.api.backend import AppBackend, build_backend
from fintech_feature_platform.api.dlq import (
    next_attempt,
    republish_with_attempt,
    route_to_dlq,
)
from fintech_feature_platform.api.online_worker import (
    WorkerResult,
    handle_feature_compute_requested,
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
from fintech_feature_platform.fs_core.events.models import FeatureComputeRequested
from fintech_feature_platform.fs_core.events.topics import FEATURE_COMPUTE_ONLINE
from fintech_feature_platform.fs_core.observability.logs import configure_logging
from fintech_feature_platform.fs_core.observability.metrics import recorder_of
from fintech_feature_platform.fs_core.stores.request_result import RequestResult
from fintech_feature_platform.fs_core.write_guard import aggregate_online_status

_FAILURE_STAGE = "online_worker"

_DEFAULT_POLL_TIMEOUT_S = 1.0
_DEFAULT_MAX_ATTEMPTS = 5

# Event-deterministic failures that are bounded by attempt count (vs infra failures,
# which replay forever).
_RETRY_LIMITED = frozenset({"compute_failed", "unexpected_error"})


@dataclass(frozen=True)
class ProcessResult:
    # status: idle | ok | deadline_expired | consume_error | deserialization_failed
    # | unexpected_error | result_store_failed (request-result write failed: no commit)
    # | a non-ok WorkerResult status (compute_failed / online_write_failed / publish_failed)
    status: str
    worker_result: WorkerResult | None = None
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
    now = datetime.now(tz=UTC)
    if result.status == "retry_republished":
        _safe_update(
            backend, request_id, status="running",
            error=f"retrying: {failure_status}", updated_at=now,
        )
    elif result.status == "dead_lettered":
        _safe_update(
            backend, request_id, status="failed",
            error=error or failure_status, finished_at=now, updated_at=now,
        )


def _dead_letter(
    consumer: EventConsumer,
    backend: AppBackend,
    message: ConsumedMessage,
    failure_status: str,
    error: str | None,
) -> ProcessResult:
    """Publish a poison message to the DLQ; commit only if the DLQ publish succeeded."""
    published = route_to_dlq(
        publisher=backend.events,
        source_topic=FEATURE_COMPUTE_ONLINE,
        failure_stage=_FAILURE_STAGE,
        failure_status=failure_status,
        error=error,
        message=message,
    )
    if published:
        consumer.commit(message)
        return ProcessResult(status="dead_lettered", committed=True, error=error)
    return ProcessResult(status="dlq_publish_failed", committed=False, error=error)


def _retry_or_dlq(
    consumer: EventConsumer,
    backend: AppBackend,
    message: ConsumedMessage,
    event: FeatureComputeRequested,
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
            topic=FEATURE_COMPUTE_ONLINE,
            key=event.entity.encoded(),
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
        source_topic=FEATURE_COMPUTE_ONLINE,
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
    """Poll and process at most one message; commit only on a fully successful handler."""
    message = consumer.poll(poll_timeout_s)
    if message is None:
        return ProcessResult(status="idle")
    if message.error() is not None:
        return ProcessResult(status="consume_error", error=str(message.error()))

    try:
        event = FeatureComputeRequested.from_json(message.value())
    except Exception as exc:  # noqa: BLE001 - structural poison: route to DLQ
        return _dead_letter(
            consumer, backend, message, "deserialization_failed", str(exc)
        )

    _safe_update(
        backend, event.request_id, status="running",
        started_at=datetime.now(tz=UTC), updated_at=datetime.now(tz=UTC),
    )

    started = datetime.now(tz=UTC)
    try:
        result = handle_feature_compute_requested(backend, event)
    except Exception as exc:  # noqa: BLE001 - event-deterministic: attempt-limit then DLQ
        recorder_of(backend).incr("online_request_errors_total", {"outcome": "unexpected"})
        pr = _retry_or_dlq(
            consumer, backend, message, event, "unexpected_error", str(exc), max_attempts
        )
        _status_after_retry_or_dlq(backend, event.request_id, pr, "unexpected_error", str(exc))
        return pr

    metrics = recorder_of(backend)
    metrics.incr("online_requests_total", {"outcome": result.status})
    elapsed_s = max(0.0, (datetime.now(tz=UTC) - started).total_seconds())
    metrics.observe("online_request_latency_ms", elapsed_s * 1000)
    # LEVEL 1 whole-operation duration (; same wall-clock boundary
    # as the legacy ms metric, in seconds with operation-scale buckets).
    metrics.observe("fsp_online_operation_duration_seconds", elapsed_s)
    if result.status not in ("ok", "deadline_expired"):
        metrics.incr("online_request_errors_total", {"outcome": result.status})

    if result.status == "ok":
        # Request-scoped result FIRST: status=completed must imply the result is
        # readable (hybrid serves it; it never re-reads Valkey /latest). A failed
        # result write is retriable infrastructure: no offset commit, replay —
        # D9 noop/dedup make the replay idempotent.
        try:
            result_write_started = perf_counter()
            backend.results.put(
                RequestResult.from_write_set(
                    event.request_id, result.write_set, result.online_written
                )
            )
            metrics.observe(
                "fsp_pipeline_stage_duration_seconds",
                perf_counter() - result_write_started,
                {"execution_mode": "online", "stage": "result_write"},
            )
        except Exception as exc:  # noqa: BLE001 - infra failure: do not commit
            return ProcessResult(status="result_store_failed", error=str(exc))
        consumer.commit(message)
        now = datetime.now(tz=UTC)
        _safe_update(
            backend, event.request_id, status="completed",
            online_write_status=aggregate_online_status(result.online_written.values()),
            offline_write_status="pending",
            metadata_write_status="pending", finished_at=now, updated_at=now, error=None,
        )
        return ProcessResult(status="ok", worker_result=result, committed=True)

    if result.status == "deadline_expired":
        # Terminal deadline outcome: the completion event was already published in the
        # handler (no Valkey write, no offline event, no request-scoped result). Commit
        # so it does not replay; record it observably as completed + deadline_expired.
        consumer.commit(message)
        now = datetime.now(tz=UTC)
        _safe_update(
            backend, event.request_id, status="completed",
            online_write_status="deadline_expired",
            offline_write_status=None,
            metadata_write_status="pending", finished_at=now, updated_at=now, error=None,
        )
        return ProcessResult(
            status="deadline_expired", worker_result=result, committed=True
        )

    # Event-deterministic failures are attempt-limited; infra failures replay forever.
    if result.status in _RETRY_LIMITED:
        pr = _retry_or_dlq(
            consumer, backend, message, event, result.status, result.error, max_attempts
        )
        _status_after_retry_or_dlq(backend, event.request_id, pr, result.status, result.error)
        return pr

    # Infra/transient (online_write_failed/publish_failed): no commit -> replay.
    return ProcessResult(
        status=result.status, worker_result=result, error=result.error
    )


def run(
    consumer: EventConsumer,
    backend: AppBackend,
    *,
    max_messages: int | None = None,
    poll_timeout_s: float = _DEFAULT_POLL_TIMEOUT_S,
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
) -> list[ProcessResult]:
    """Process up to ``max_messages`` iterations.

    Bounded helper (not a daemon): with ``max_messages`` it runs exactly that many
    iterations; with ``None`` it drains until a poll returns nothing (``idle``).
    """
    results: list[ProcessResult] = []
    while max_messages is None or len(results) < max_messages:
        if shutdown_requested():
            break  # graceful stop : finish nothing new; close after the loop
        _item_started = _perf_counter()
        result = process_next(
            consumer, backend, poll_timeout_s=poll_timeout_s, max_attempts=max_attempts
        )
        record_worker_item(
            backend, "online-worker", result.status, _perf_counter() - _item_started
        )
        results.append(result)
        if max_messages is None and result.status == "idle":
            break
    return results


def _main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Online feature worker: consume fp.feature-compute.online."
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

    configure_logging("online-worker")
    settings = load_settings()
    backend = build_backend(settings)
    maybe_start_metrics_server("online-worker", backend, settings)
    # Imported lazily so the module imports without the kafka extra installed.
    from fintech_feature_platform.fs_core.events.consumer import connect_kafka_consumer

    consumer = connect_kafka_consumer(
        settings.kafka_bootstrap_servers,
        settings.kafka_consumer_group,
        FEATURE_COMPUTE_ONLINE,
        settings.kafka_auto_offset_reset,
        settings.kafka_client_id,
    )
    if args.forever:
        install_shutdown_signals("online-worker")
        # Daemon mode : same consumer across rounds (no group-rebalance
        # churn), bounded per-round results so memory stays flat. Stop via SIGTERM.
        while not shutdown_requested():
            results = run(
                consumer,
                backend,
                max_messages=FOREVER_ROUND,
                poll_timeout_s=settings.kafka_poll_timeout_ms / 1000,
                max_attempts=settings.kafka_max_attempts,
            )
            log_round(results, "online-worker")
        close_consumer(consumer, "online-worker")
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
