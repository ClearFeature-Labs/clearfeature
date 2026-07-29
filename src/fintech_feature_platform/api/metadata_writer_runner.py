"""Kafka consumer runner for the Metadata Writer (separate consumer group).

Consumes the request/status topics and projects each message into the metadata store via
the pure ``project_event``. It is **off the online critical path** (its own consumer
group/offsets) and commits an offset **only after** the metadata write succeeds — or
after the message is durably dead-lettered :

    ok / idempotent duplicate      -> commit
    structural_conflict            -> DLQ, commit only after the publish is acked
    deserialization_failed poison  -> DLQ, commit only after the publish is acked
    unknown_event_type             -> DLQ, commit only after the publish is acked
    transient store failure        -> no DLQ, no commit (replay; projection idempotent)
    DLQ publish failure            -> no commit (non-lossy; retried on replay)

The writer classifies; this runner owns the DLQ publish and the commit decision. No
attempt-limit here: structural results are deterministic, transient failures replay.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from time import perf_counter as _perf_counter

from fintech_feature_platform.api.backend import AppBackend, build_backend
from fintech_feature_platform.api.dlq import route_to_dlq
from fintech_feature_platform.api.metadata_writer import project_event
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
from fintech_feature_platform.fs_core.events.consumer import (
    ConsumedMessage,
    EventConsumer,
)
from fintech_feature_platform.fs_core.events.models import (
    EVENT_TYPE_BATCH_CHUNK_PROCESSED,
    EVENT_TYPE_BATCH_CHUNK_REQUESTED,
    EVENT_TYPE_DLQ,
    EVENT_TYPE_FEATURE_COMPUTE_COMPLETED,
    EVENT_TYPE_FEATURE_COMPUTE_REQUESTED,
    EVENT_TYPE_FEATURE_OFFLINE_WRITE_REQUESTED,
    EVENT_TYPE_MODEL_SCORE_WRITE,
)
from fintech_feature_platform.fs_core.events.publisher import EventPublisher
from fintech_feature_platform.fs_core.events.topics import (
    BATCH_JOB_EVENTS,
    DLQ,
    FEATURE_COMPUTE_BATCH,
    FEATURE_COMPUTE_COMPLETED,
    FEATURE_COMPUTE_ONLINE,
    FEATURE_WRITE_OFFLINE,
    MODEL_SCORE_WRITE,
)
from fintech_feature_platform.fs_core.observability.logs import configure_logging
from fintech_feature_platform.fs_core.raw.meta_repository import RawReportMetaRepository
from fintech_feature_platform.fs_core.stores.batch_metadata import BatchMetadataStore
from fintech_feature_platform.fs_core.stores.metadata import MetadataStore

_DEFAULT_POLL_TIMEOUT_S = 1.0

_FAILURE_STAGE = "metadata_writer"

# Writer statuses that are structural (can never succeed on replay) -> DLQ + commit.
_STRUCTURAL_STATUSES = frozenset(
    {"structural_conflict", "deserialization_failed", "unknown_event_type"}
)

# Topics the Metadata Writer projects into Postgres.
METADATA_TOPICS = (
    FEATURE_COMPUTE_ONLINE,
    FEATURE_COMPUTE_COMPLETED,
    FEATURE_WRITE_OFFLINE,
    MODEL_SCORE_WRITE,
    FEATURE_COMPUTE_BATCH,
    BATCH_JOB_EVENTS,
    DLQ,
)

# The Metadata Writer subscribes to several topics and ConsumedMessage does not expose
# the source topic, so DLQ attribution maps the parsed event_type to the topic it flows
# on; unparseable poison stays "unknown".
_TOPIC_BY_EVENT_TYPE = {
    EVENT_TYPE_FEATURE_COMPUTE_REQUESTED: FEATURE_COMPUTE_ONLINE,
    EVENT_TYPE_FEATURE_COMPUTE_COMPLETED: FEATURE_COMPUTE_COMPLETED,
    EVENT_TYPE_FEATURE_OFFLINE_WRITE_REQUESTED: FEATURE_WRITE_OFFLINE,
    EVENT_TYPE_MODEL_SCORE_WRITE: MODEL_SCORE_WRITE,
    EVENT_TYPE_BATCH_CHUNK_REQUESTED: FEATURE_COMPUTE_BATCH,
    EVENT_TYPE_BATCH_CHUNK_PROCESSED: BATCH_JOB_EVENTS,
    EVENT_TYPE_DLQ: DLQ,
}


@dataclass(frozen=True)
class ProcessResult:
    # status: idle | consume_error | ok | dead_lettered | dlq_publish_failed | store_failed
    status: str
    event_type: str | None = None
    committed: bool = False
    error: str | None = None


def _dead_letter(
    consumer: EventConsumer,
    publisher: EventPublisher,
    message: ConsumedMessage,
    failure_status: str,
    event_type: str | None,
    error: str | None,
) -> ProcessResult:
    """Publish a structural poison/conflict to the DLQ; commit only after the ack."""
    published = route_to_dlq(
        publisher=publisher,
        source_topic=_TOPIC_BY_EVENT_TYPE.get(event_type, "unknown"),
        failure_stage=_FAILURE_STAGE,
        failure_status=failure_status,
        error=error,
        message=message,
    )
    if published:
        consumer.commit(message)
        return ProcessResult(
            status="dead_lettered", event_type=event_type, committed=True, error=error
        )
    return ProcessResult(
        status="dlq_publish_failed", event_type=event_type, error=error
    )


def _mark_metadata_written(request_status_store, request_id: str) -> None:
    """Flip ``metadata_write_status`` to its truthful terminal state.

    Called only AFTER the projection durably committed. Best-effort (status is
    observability, never the record of truth), idempotent, and monotonic: it only
    ever sets ``written`` — a replayed event can never regress it to ``pending``.
    """
    try:
        current = request_status_store.get(request_id)
        if current is None or current.metadata_write_status == "written":
            return
        request_status_store.put(
            replace(
                current,
                metadata_write_status="written",
                updated_at=datetime.now(tz=UTC),
            )
        )
    except Exception:  # noqa: BLE001 - never fail projection/commit for status
        pass


def process_next(
    consumer: EventConsumer,
    store: MetadataStore,
    raw_meta: RawReportMetaRepository,
    batch_meta: BatchMetadataStore,
    publisher: EventPublisher,
    *,
    poll_timeout_s: float = _DEFAULT_POLL_TIMEOUT_S,
    request_status_store=None,
) -> ProcessResult:
    """Poll and project at most one message; commit only after write or DLQ ack."""
    message = consumer.poll(poll_timeout_s)
    if message is None:
        return ProcessResult(status="idle")
    if message.error() is not None:
        return ProcessResult(status="consume_error", error=str(message.error()))

    result = project_event(store, raw_meta, batch_meta, message.value())

    if result.status == "store_failed":
        # Transient: do NOT commit -> the message replays (projection is idempotent).
        return ProcessResult(
            status="store_failed", event_type=result.event_type, error=result.error
        )

    if result.status in _STRUCTURAL_STATUSES:
        # Can never succeed on replay: dead-letter (non-lossy), then commit.
        return _dead_letter(
            consumer, publisher, message, result.status, result.event_type, result.error
        )

    # The request's terminal projection is durably committed at this point — make
    # the operational status truthful.
    if (
        request_status_store is not None
        and result.request_id
        and result.event_type == EVENT_TYPE_FEATURE_COMPUTE_COMPLETED
    ):
        _mark_metadata_written(request_status_store, result.request_id)

    consumer.commit(message)
    return ProcessResult(
        status="ok", event_type=result.event_type, committed=True, error=result.error
    )


def run(
    consumer: EventConsumer,
    store: MetadataStore,
    raw_meta: RawReportMetaRepository,
    batch_meta: BatchMetadataStore,
    publisher: EventPublisher,
    *,
    max_messages: int | None = None,
    poll_timeout_s: float = _DEFAULT_POLL_TIMEOUT_S,
    request_status_store=None,
    backend=None,
) -> list[ProcessResult]:
    """Process up to ``max_messages`` iterations (bounded helper, not a daemon).

    ``backend`` (optional) only supplies the metrics recorder for the shared per-item
    instrumentation; the projection itself uses the explicit store parameters.
    """
    results: list[ProcessResult] = []
    while max_messages is None or len(results) < max_messages:
        if shutdown_requested():
            break  # graceful stop : finish nothing new; close after the loop
        _item_started = _perf_counter()
        result = process_next(
            consumer, store, raw_meta, batch_meta, publisher,
            poll_timeout_s=poll_timeout_s,
            request_status_store=request_status_store,
        )
        record_worker_item(
            backend, "metadata-writer", result.status, _perf_counter() - _item_started
        )
        results.append(result)
        if max_messages is None and result.status == "idle":
            break
    return results


def _main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Metadata writer: project request/status events into Postgres."
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

    configure_logging("metadata-writer")
    settings = load_settings()
    backend: AppBackend = build_backend(settings)
    maybe_start_metrics_server("metadata-writer", backend, settings)
    from fintech_feature_platform.fs_core.events.consumer import connect_kafka_consumer

    consumer = connect_kafka_consumer(
        settings.kafka_bootstrap_servers,
        settings.kafka_metadata_writer_group,
        list(METADATA_TOPICS),
        settings.kafka_auto_offset_reset,
        settings.kafka_client_id,
    )
    if args.forever:
        install_shutdown_signals("metadata-writer")
        while not shutdown_requested():  # daemon mode : same consumer, bounded rounds
            results = run(
                consumer,
                backend.metadata,
                backend.metas,
                backend.batch_meta,
                backend.events,
                max_messages=FOREVER_ROUND,
                poll_timeout_s=settings.kafka_poll_timeout_ms / 1000,
                request_status_store=backend.status,
                backend=backend,
            )
            log_round(results, "metadata-writer")
        close_consumer(consumer, "metadata-writer")
        return
    max_messages = 1 if args.once else args.max_messages
    results = run(
        consumer,
        backend.metadata,
        backend.metas,
        backend.batch_meta,
        backend.events,
        max_messages=max_messages,
        poll_timeout_s=settings.kafka_poll_timeout_ms / 1000,
        request_status_store=backend.status,
        backend=backend,
    )
    committed = sum(1 for r in results if r.committed)
    print(f"processed={len(results)} committed={committed}")


if __name__ == "__main__":  # pragma: no cover - manual entrypoint
    _main()
