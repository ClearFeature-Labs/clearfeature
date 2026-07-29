"""Kafka consumer runner for the reactive propagation worker.

Consumes ``fp.feature-updates``. Each message is deserialized and **observed** into an
in-process ``DebounceStore`` (``process_next``); the source message is held (not yet
committed) until a ``flush`` runs the coalesced recompute **wave** and the wave's offline
appends + downstream publishes succeed. Only then are the held offsets committed.

Commit discipline (mirrors the offline writer's replay-safety):
- structural-poison ``FeatureUpdated`` -> DLQ + commit after ack (never replay-loops);
- observe is pure (no store/publish), so it cannot fail transiently;
- a wave infra failure (offline store / publisher unavailable) leaves the held offsets
  uncommitted -> replay. Replay re-observes (debounce coalesces) and re-runs the wave;
  offline dedup makes the recompute idempotent.

In-memory debounce is a deliberate scope choice; durable debounce is a documented
later-hardening deferral. A crash between commit-less observe and flush simply replays.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass, field
from time import perf_counter as _perf_counter

from fintech_feature_platform.api.backend import AppBackend, build_backend
from fintech_feature_platform.api.dlq import route_to_dlq
from fintech_feature_platform.api.propagation_worker import (
    RecomputeWave,
    execute_wave,
    handle_feature_updated,
)
from fintech_feature_platform.api.runner_daemon import (
    PROPAGATION_FOREVER_ROUND,
    close_consumer,
    install_shutdown_signals,
    maybe_start_metrics_server,
    record_worker_item,
    shutdown_requested,
)
from fintech_feature_platform.api.settings import load_settings
from fintech_feature_platform.fs_core.events.consumer import ConsumedMessage, EventConsumer
from fintech_feature_platform.fs_core.events.models import FeatureUpdated
from fintech_feature_platform.fs_core.events.topics import FEATURE_UPDATES
from fintech_feature_platform.fs_core.observability.logs import (
    configure_logging,
    get_logger,
    log_event,
)
from fintech_feature_platform.fs_core.observability.metrics import recorder_of
from fintech_feature_platform.fs_core.propagation import DebounceStore

_logger = get_logger("worker")

_DEFAULT_POLL_TIMEOUT_S = 1.0
_FAILURE_STAGE = "propagation_worker"


@dataclass(frozen=True)
class ProcessResult:
    # status: idle | observed | consume_error | dead_lettered | dlq_publish_failed
    status: str
    candidates: int = 0
    committed: bool = False
    error: str | None = None


@dataclass(frozen=True)
class FlushResult:
    # status: ok | wave_failed
    status: str
    wave: RecomputeWave | None = None
    committed: int = 0
    error: str | None = None


@dataclass
class PendingBatch:
    """Source messages observed but not yet committed (committed together on flush)."""

    messages: list[ConsumedMessage] = field(default_factory=list)


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
        source_topic=FEATURE_UPDATES,
        failure_stage=_FAILURE_STAGE,
        failure_status=failure_status,
        error=error,
        message=message,
    )
    if published:
        recorder_of(backend).incr(
            "dlq_events_total", {"stage": _FAILURE_STAGE, "status": failure_status}
        )
        consumer.commit(message)
        return ProcessResult(status="dead_lettered", committed=True, error=error)
    return ProcessResult(status="dlq_publish_failed", committed=False, error=error)


def process_next(
    consumer: EventConsumer,
    backend: AppBackend,
    debounce: DebounceStore,
    pending: PendingBatch,
    *,
    poll_timeout_s: float = _DEFAULT_POLL_TIMEOUT_S,
) -> ProcessResult:
    """Poll one update, observe it into the debounce store, and hold the offset for flush."""
    message = consumer.poll(poll_timeout_s)
    if message is None:
        return ProcessResult(status="idle")
    if message.error() is not None:
        return ProcessResult(status="consume_error", error=str(message.error()))

    try:
        event = FeatureUpdated.from_json(message.value())
    except Exception as exc:  # noqa: BLE001 - structural poison: route to DLQ
        return _dead_letter(consumer, backend, message, "deserialization_failed", str(exc))

    result = handle_feature_updated(backend, debounce, event)
    if result.status != "ok":  # structurally invalid (e.g. unknown source) -> DLQ
        return _dead_letter(consumer, backend, message, "invalid_event", result.error)

    pending.messages.append(message)
    return ProcessResult(status="observed", candidates=result.candidates, committed=False)


def flush(
    consumer: EventConsumer,
    backend: AppBackend,
    debounce: DebounceStore,
    pending: PendingBatch,
    *,
    model_runner=None,
) -> FlushResult:
    """Run the coalesced recompute wave; commit held offsets only on durable success.

    ``model_runner`` (optional) enables F3 dependents to be recomputed in the wave; without
    one, F3 units are counted skipped (a clear defer). The daemon leaves it ``None`` in beta.
    """
    try:
        wave = execute_wave(backend, debounce, model_runner=model_runner)
    except Exception as exc:  # noqa: BLE001 - wave infra failure: no commit, replay
        return FlushResult(status="wave_failed", committed=False, error=str(exc))

    committed = 0
    for message in pending.messages:
        consumer.commit(message)
        committed += 1
    pending.messages.clear()
    return FlushResult(status="ok", wave=wave, committed=committed)


def run(
    consumer: EventConsumer,
    backend: AppBackend,
    debounce: DebounceStore | None = None,
    *,
    max_messages: int | None = None,
    poll_timeout_s: float = _DEFAULT_POLL_TIMEOUT_S,
    model_runner=None,
) -> tuple[list[ProcessResult], FlushResult]:
    """Observe up to ``max_messages`` iterations, then flush one wave (bounded helper)."""
    debounce = debounce if debounce is not None else DebounceStore()
    pending = PendingBatch()
    results: list[ProcessResult] = []
    while max_messages is None or len(results) < max_messages:
        if shutdown_requested():
            break  # graceful stop : finish nothing new; close after the loop
        _item_started = _perf_counter()
        result = process_next(
            consumer, backend, debounce, pending, poll_timeout_s=poll_timeout_s
        )
        record_worker_item(
            backend, "propagation-worker", result.status, _perf_counter() - _item_started
        )
        results.append(result)
        if max_messages is None and result.status == "idle":
            break
    flush_result = flush(consumer, backend, debounce, pending, model_runner=model_runner)
    return results, flush_result


def _main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Reactive propagation worker: consume fp.feature-updates into waves."
    )
    parser.add_argument("--once", action="store_true", help="process a single message")
    parser.add_argument(
        "--max-messages", type=int, default=None, help="process N iterations then flush"
    )
    parser.add_argument(
        "--forever", action="store_true",
        help="observe/flush forever in bounded rounds (daemon mode for compose/systemd)",
    )
    args = parser.parse_args(argv)

    configure_logging("propagation-worker")
    settings = load_settings()
    backend = build_backend(settings)
    maybe_start_metrics_server("propagation-worker", backend, settings)
    from fintech_feature_platform.fs_core.events.consumer import connect_kafka_consumer

    consumer = connect_kafka_consumer(
        settings.kafka_bootstrap_servers,
        settings.kafka_propagation_worker_group,
        FEATURE_UPDATES,
        settings.kafka_auto_offset_reset,
        settings.kafka_client_id,
    )
    if args.forever:
        install_shutdown_signals("propagation-worker")
        # Daemon mode : each round observes then flushes ONE wave, so the
        # round length is the worst-case debounce window (~30s at the 1s poll timeout).
        # A wave_failed flush leaves offsets uncommitted -> next round replays (safe).
        while not shutdown_requested():
            results, flush_result = run(
                consumer,
                backend,
                max_messages=PROPAGATION_FOREVER_ROUND,
                poll_timeout_s=settings.kafka_poll_timeout_ms / 1000,
            )
            wave = flush_result.wave
            if flush_result.committed or (wave is not None and wave.planned):
                # Structured wave summary.
                log_event(
                    _logger, logging.INFO, "propagation_wave_completed",
                    worker_role="propagation-worker", operation="recompute_wave",
                    result=flush_result.status,
                    observed=sum(1 for r in results if r.status != "idle"),
                    committed=flush_result.committed,
                    computed=wave.computed if wave is not None else 0,
                )
        close_consumer(consumer, "propagation-worker")
        return
    max_messages = 1 if args.once else args.max_messages
    results, flush_result = run(
        consumer,
        backend,
        max_messages=max_messages,
        poll_timeout_s=settings.kafka_poll_timeout_ms / 1000,
    )
    wave = flush_result.wave
    computed = wave.computed if wave is not None else 0
    print(
        f"observed={len(results)} committed={flush_result.committed} "
        f"wave_status={flush_result.status} computed={computed}"
    )


if __name__ == "__main__":  # pragma: no cover - manual entrypoint
    _main()
