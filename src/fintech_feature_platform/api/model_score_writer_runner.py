"""Kafka consumer runner for the model score writer (safe offset commit).

Consumes ``fp.model-score.write`` and materializes scores via the pure
``handle_model_score_write`` handler. Commits **only** after the required side effects
(online write and/or the offline-write event publish) are acknowledged; on an infra
failure the message stays uncommitted and replays (online CAS + offline dedup make replay
safe). Structural-poison / invalid events go to the DLQ (reuse ``api/dlq.py``).

V1 deliberately has **no attempt-limit retry** — the API validates before publish, so
deterministic-invalid events are rare; when seen they are dead-lettered, not looped.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from time import perf_counter as _perf_counter

from fintech_feature_platform.api.backend import AppBackend, build_backend
from fintech_feature_platform.api.dlq import route_to_dlq
from fintech_feature_platform.api.model_score_writer import (
    WriterResult,
    handle_model_score_write,
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
from fintech_feature_platform.fs_core.events.models import ModelScoreWriteRequested
from fintech_feature_platform.fs_core.events.topics import MODEL_SCORE_WRITE
from fintech_feature_platform.fs_core.observability.logs import configure_logging

_DEFAULT_POLL_TIMEOUT_S = 1.0
_FAILURE_STAGE = "model_score_writer"


@dataclass(frozen=True)
class ProcessResult:
    # status: idle | ok | consume_error | dead_lettered | dlq_publish_failed
    #         | online_write_failed | publish_failed
    status: str
    writer_result: WriterResult | None = None
    committed: bool = False
    error: str | None = None


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
        source_topic=MODEL_SCORE_WRITE,
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
    poll_timeout_s: float = _DEFAULT_POLL_TIMEOUT_S,
) -> ProcessResult:
    """Poll and process at most one score-write message; commit only on success."""
    message = consumer.poll(poll_timeout_s)
    if message is None:
        return ProcessResult(status="idle")
    if message.error() is not None:
        return ProcessResult(status="consume_error", error=str(message.error()))

    try:
        event = ModelScoreWriteRequested.from_json(message.value())
    except Exception as exc:  # noqa: BLE001 - structural poison: route to DLQ
        return _dead_letter(consumer, backend, message, "deserialization_failed", str(exc))

    try:
        result = handle_model_score_write(backend, event)
    except Exception as exc:  # noqa: BLE001 - unexpected: no commit, replay
        return ProcessResult(status="unexpected_error", committed=False, error=str(exc))

    if result.status == "ok":
        consumer.commit(message)
        return ProcessResult(status="ok", writer_result=result, committed=True)

    if result.status == "invalid_event":
        # Deterministic-invalid (registry drift / bad event) -> DLQ, do not loop.
        return _dead_letter(consumer, backend, message, "invalid_event", result.error)

    # Infra failures (online_write_failed / publish_failed) -> no commit, replay.
    return ProcessResult(status=result.status, writer_result=result, error=result.error)


def run(
    consumer: EventConsumer,
    backend: AppBackend,
    *,
    max_messages: int | None = None,
    poll_timeout_s: float = _DEFAULT_POLL_TIMEOUT_S,
) -> list[ProcessResult]:
    """Process up to ``max_messages`` iterations (bounded helper, not a daemon)."""
    results: list[ProcessResult] = []
    while max_messages is None or len(results) < max_messages:
        if shutdown_requested():
            break  # graceful stop : finish nothing new; close after the loop
        _item_started = _perf_counter()
        result = process_next(consumer, backend, poll_timeout_s=poll_timeout_s)
        record_worker_item(
            backend, "model-score-writer", result.status, _perf_counter() - _item_started
        )
        results.append(result)
        if max_messages is None and result.status == "idle":
            break
    return results


def _main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Model score writer: consume fp.model-score.write and materialize."
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

    configure_logging("model-score-writer")
    settings = load_settings()
    backend = build_backend(settings)
    maybe_start_metrics_server("model-score-writer", backend, settings)
    from fintech_feature_platform.fs_core.events.consumer import connect_kafka_consumer

    consumer = connect_kafka_consumer(
        settings.kafka_bootstrap_servers,
        settings.kafka_model_score_writer_group,
        MODEL_SCORE_WRITE,
        settings.kafka_auto_offset_reset,
        settings.kafka_client_id,
    )
    if args.forever:
        install_shutdown_signals("model-score-writer")
        while not shutdown_requested():  # daemon mode : same consumer, bounded rounds
            results = run(
                consumer,
                backend,
                max_messages=FOREVER_ROUND,
                poll_timeout_s=settings.kafka_poll_timeout_ms / 1000,
            )
            log_round(results, "model-score-writer")
        close_consumer(consumer, "model-score-writer")
        return
    max_messages = 1 if args.once else args.max_messages
    results = run(
        consumer,
        backend,
        max_messages=max_messages,
        poll_timeout_s=settings.kafka_poll_timeout_ms / 1000,
    )
    committed = sum(1 for r in results if r.committed)
    print(f"processed={len(results)} committed={committed}")


if __name__ == "__main__":  # pragma: no cover - manual entrypoint
    _main()
