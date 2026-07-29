"""Kafka consumer runner for the batch worker (safe offset commit).

Consumes ``fp.feature-compute.batch`` and processes each ``BatchChunkRequested`` via the
pure ``handle_batch_chunk``. Commits the chunk offset **only** after the chunk is processed
(``ok``); an infra failure leaves it uncommitted and replays (offline dedup + online CAS
make replay safe). Structural-poison / invalid events go to the DLQ (reuse ``api/dlq.py``).

V1 has **no attempt-limit retry** — the API validates before publish, so deterministic
failures are rare; when seen they are dead-lettered, not looped.
"""

from __future__ import annotations

import argparse
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from time import perf_counter as _perf_counter

from fintech_feature_platform.api.backend import AppBackend, build_backend
from fintech_feature_platform.api.batch_controls import (
    NoopBatchRuntimeControls,
    build_batch_runtime_controls,
)
from fintech_feature_platform.api.batch_worker import (
    BatchWorkerResult,
    handle_batch_chunk,
)
from fintech_feature_platform.api.dlq import route_to_dlq
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
from fintech_feature_platform.fs_core.events.models import BatchChunkRequested
from fintech_feature_platform.fs_core.events.topics import FEATURE_COMPUTE_BATCH
from fintech_feature_platform.fs_core.observability.logs import (
    configure_logging,
    get_logger,
    log_event,
)
from fintech_feature_platform.fs_core.observability.metrics import recorder_of

_logger = get_logger("worker")

_DEFAULT_POLL_TIMEOUT_S = 1.0
_FAILURE_STAGE = "batch_worker"
# Terminal non-commit control statuses: the message stays uncommitted (replay-safe), so
# the daemon must back off before re-polling instead of hot-looping on the same message.
_BACKOFF_STATUSES = frozenset({"paused", "rate_limited"})


@dataclass(frozen=True)
class ProcessResult:
    # status: idle | ok | consume_error | dead_lettered | dlq_publish_failed
    # | infra_failed | paused | rate_limited
    status: str
    worker_result: BatchWorkerResult | None = None
    committed: bool = False
    error: str | None = None
    reason: str | None = None  # pause/rate-limit reason (observability)


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
        source_topic=FEATURE_COMPUTE_BATCH,
        failure_stage=_FAILURE_STAGE,
        failure_status=failure_status,
        error=error,
        message=message,
    )
    if published:
        consumer.commit(message)
        return ProcessResult(status="dead_lettered", committed=True, error=error)
    return ProcessResult(status="dlq_publish_failed", committed=False, error=error)


def process_next(
    consumer: EventConsumer,
    backend: AppBackend,
    *,
    controls=None,
    poll_timeout_s: float = _DEFAULT_POLL_TIMEOUT_S,
) -> ProcessResult:
    """Poll and process at most one chunk; commit only after the chunk is processed.

    Before any expensive work, consult ``controls`` (a ``BatchRuntimeControls``; default
    Noop): if paused (e.g. online consumer lag too high) or rate-limited, return without
    computing/committing — the chunk stays uncommitted and replays later (online SLO wins
    over batch throughput). The online worker is never gated by this.
    """
    if controls is None:
        controls = NoopBatchRuntimeControls()
    message = consumer.poll(poll_timeout_s)
    if message is None:
        return ProcessResult(status="idle")
    if message.error() is not None:
        return ProcessResult(status="consume_error", error=str(message.error()))

    # Isolation gate BEFORE consuming payload work: pause, then per-chunk rate limit.
    # Both leave the offset uncommitted (replay-safe): no compute, no writes, no publish.
    decision = controls.should_pause()
    if decision.paused:
        recorder_of(backend).incr("batch_pause_events_total")
        return ProcessResult(status="paused", committed=False, reason=decision.reason)
    if controls.rate_limiter.try_acquire(1) < 1:
        recorder_of(backend).incr("batch_rate_limited_events_total")
        return ProcessResult(
            status="rate_limited", committed=False, reason="batch chunk rate limit",
        )

    try:
        event = BatchChunkRequested.from_json(message.value())
    except Exception as exc:  # noqa: BLE001 - structural poison: route to DLQ
        return _dead_letter(consumer, backend, message, "deserialization_failed", str(exc))

    try:
        _chunk_started = _perf_counter()
        result = handle_batch_chunk(backend, event, controls)
    except Exception as exc:  # noqa: BLE001 - infra failure: no commit, replay
        return ProcessResult(status="infra_failed", committed=False, error=str(exc))

    if result.status == "ok":
        consumer.commit(message)
        # Structured batch summary : one event per chunk (bounded),
        # with the chunk's own technical correlation ids — never per-item logs.
        log_event(
            _logger, logging.INFO, "batch_chunk_completed",
            worker_role="batch-worker", operation="batch_chunk", result="success",
            job_id=event.batch_job_id, chunk_id=event.chunk_id,
            items=len(event.items),
            duration_ms=round((_perf_counter() - _chunk_started) * 1000, 3),
        )
        return ProcessResult(status="ok", worker_result=result, committed=True)

    if result.status == "publish_failed":
        # Processed-event publish failed after computing -> do not commit, replay chunk.
        return ProcessResult(
            status="publish_failed", worker_result=result, committed=False,
            error=result.error,
        )

    # invalid_event (deterministic bad view/features) -> DLQ, do not loop.
    return _dead_letter(consumer, backend, message, "invalid_event", result.error)


def run(
    consumer: EventConsumer,
    backend: AppBackend,
    *,
    controls=None,
    max_messages: int | None = None,
    poll_timeout_s: float = _DEFAULT_POLL_TIMEOUT_S,
    pause_backoff_s: float = 0.0,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> list[ProcessResult]:
    """Process up to ``max_messages`` iterations (bounded helper, not a daemon).

    ``process_next`` stays pure (never sleeps). Backoff lives here: when a chunk is
    ``paused``/``rate_limited`` the offset is intentionally NOT committed, so the same
    message would be re-polled immediately — the loop sleeps ``pause_backoff_s`` first so
    the daemon does not hot-loop. ``sleep_fn`` is injectable for deterministic tests.
    """
    results: list[ProcessResult] = []
    while max_messages is None or len(results) < max_messages:
        if shutdown_requested():
            break  # graceful stop : finish nothing new; close after the loop
        _item_started = _perf_counter()
        result = process_next(
            consumer, backend, controls=controls, poll_timeout_s=poll_timeout_s
        )
        record_worker_item(
            backend, "batch-worker", result.status, _perf_counter() - _item_started
        )
        results.append(result)
        if result.status in _BACKOFF_STATUSES and pause_backoff_s > 0:
            sleep_fn(pause_backoff_s)  # back off before re-polling the same message
        if max_messages is None and result.status == "idle":
            break
    return results


def _main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Batch worker: consume fp.feature-compute.batch and compute chunks."
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

    configure_logging("batch-worker")
    settings = load_settings()
    backend = build_backend(settings)
    maybe_start_metrics_server("batch-worker", backend, settings)
    from fintech_feature_platform.fs_core.events.consumer import connect_kafka_consumer

    consumer = connect_kafka_consumer(
        settings.kafka_bootstrap_servers,
        settings.kafka_batch_worker_group,
        FEATURE_COMPUTE_BATCH,
        settings.kafka_auto_offset_reset,
        settings.kafka_client_id,
    )
    controls = build_batch_runtime_controls(settings)
    if args.forever:
        install_shutdown_signals("batch-worker")
        while not shutdown_requested():  # daemon mode : same consumer, bounded rounds
            results = run(
                consumer,
                backend,
                controls=controls,
                max_messages=FOREVER_ROUND,
                poll_timeout_s=settings.kafka_poll_timeout_ms / 1000,
                pause_backoff_s=settings.batch_pause_sleep_ms / 1000,
            )
            log_round(results, "batch-worker")
        close_consumer(consumer, "batch-worker")
        return
    max_messages = 1 if args.once else args.max_messages
    results = run(
        consumer,
        backend,
        controls=controls,
        max_messages=max_messages,
        poll_timeout_s=settings.kafka_poll_timeout_ms / 1000,
        pause_backoff_s=settings.batch_pause_sleep_ms / 1000,
    )
    committed = sum(1 for r in results if r.committed)
    print(f"processed={len(results)} committed={committed}")


if __name__ == "__main__":  # pragma: no cover - manual entrypoint
    _main()
