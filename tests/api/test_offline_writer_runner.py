"""Tests for the offline writer runner (commit only after a successful append)."""

import dataclasses
from datetime import UTC, datetime

from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.offline_writer_runner import process_next, run
from fintech_feature_platform.api.online_worker import handle_feature_compute_requested
from fintech_feature_platform.fs_core.events.consumer import (
    InMemoryEventConsumer,
    InMemoryMessage,
)
from fintech_feature_platform.fs_core.events.models import (
    EntityRef,
    FeatureComputeRequested,
    ReportDescriptor,
)
from fintech_feature_platform.fs_core.events.topics import DLQ, FEATURE_WRITE_OFFLINE
from fintech_feature_platform.fs_core.models import EntityKey
from fintech_feature_platform.fs_core.stores.request_status import RequestStatus

_TS = datetime(2026, 6, 27, 10, tzinfo=UTC)
_ENTITY = {"user_id": "1", "application_id": "A1"}
_OBJECT_KEY = "mem://rep_credit_owr"


def _entity_key() -> EntityKey:
    return EntityKey.from_mapping(_ENTITY, key_order=["user_id", "application_id"])


def _compute_event() -> FeatureComputeRequested:
    descriptor = ReportDescriptor(
        report_ref="rep_credit_owr",
        source_name="credit_report",
        report_type="credit_report",
        schema_version="v1",
        report_ts=_TS,
        object_key=_OBJECT_KEY,
        content_hash="sha256:x",
        size_bytes=10,
        compression="none",
        format="json",
    )
    return FeatureComputeRequested(
        request_id="freq_owr",
        job_id="job_owr",
        priority="online",
        deadline_ms=1000,
        entity=EntityRef("application", dict(_ENTITY)),
        view="user_credit_risk",
        view_version=1,
        reports=[descriptor],
        write_policy="online_first",
        idempotency_key="idem_owr",
        correlation_id="corr_owr",
        occurred_at=_TS,
        requested_features=["declared_income"],
    )


def _offline_event_bytes(backend) -> bytes:
    backend.payloads.put(
        _OBJECT_KEY, {"declared_income": 4_200_000, "monthly_obligations": 800_000}
    )
    handle_feature_compute_requested(backend, _compute_event())
    return backend.events.published[0].event.to_json()


def _produce_event_bytes() -> bytes:
    """Produce offline-write event bytes on a throwaway backend (clean events)."""
    return _offline_event_bytes(build_memory_backend())


def test_runner_commits_after_successful_append():
    backend = build_memory_backend()
    consumer = InMemoryEventConsumer([InMemoryMessage(_offline_event_bytes(backend))])

    result = process_next(consumer, backend)

    assert result.status == "ok"
    assert result.committed is True
    assert len(consumer.committed) == 1
    assert len(backend.offline.get(_entity_key(), feature_name="declared_income")) == 1


def test_runner_dead_letters_deserialization_failure_and_commits():
    backend = build_memory_backend()
    consumer = InMemoryEventConsumer([InMemoryMessage(b"not-json")])

    result = process_next(consumer, backend)

    assert result.status == "dead_lettered"
    assert result.committed is True
    assert len(consumer.committed) == 1
    dlq = [r for r in backend.events.published if r.topic == DLQ]
    assert len(dlq) == 1
    assert dlq[0].event.failure_stage == "offline_writer"
    assert dlq[0].event.failure_status == "deserialization_failed"


def test_runner_dead_letters_invalid_event_and_commits(monkeypatch):
    from fintech_feature_platform.api import offline_writer_runner as runner_mod
    from fintech_feature_platform.api.offline_writer import OfflineWriteResult

    backend = build_memory_backend()
    event_bytes = _offline_event_bytes(backend)
    monkeypatch.setattr(
        runner_mod,
        "handle_feature_offline_write",
        lambda backend, event: OfflineWriteResult(
            status="invalid_event", view="", view_version=0, entity_key="", error="bad"
        ),
    )
    consumer = InMemoryEventConsumer([InMemoryMessage(event_bytes)])

    result = process_next(consumer, backend)

    assert result.status == "dead_lettered"
    assert result.committed is True
    dlq = [r for r in backend.events.published if r.topic == DLQ]
    assert len(dlq) == 1
    assert dlq[0].event.failure_status == "invalid_event"


def test_runner_invalid_event_dlq_publish_failure_does_not_commit(monkeypatch):
    from fintech_feature_platform.api import offline_writer_runner as runner_mod
    from fintech_feature_platform.api.offline_writer import OfflineWriteResult

    class _FailingPublisher:
        def publish(self, topic, key, event):
            raise RuntimeError("dlq down")

    event_bytes = _produce_event_bytes()
    backend = dataclasses.replace(build_memory_backend(), events=_FailingPublisher())
    monkeypatch.setattr(
        runner_mod,
        "handle_feature_offline_write",
        lambda backend, event: OfflineWriteResult(
            status="invalid_event", view="", view_version=0, entity_key="", error="bad"
        ),
    )
    consumer = InMemoryEventConsumer([InMemoryMessage(event_bytes)])

    result = process_next(consumer, backend)

    assert result.status == "dlq_publish_failed"
    assert result.committed is False
    assert consumer.committed == []


def test_unexpected_error_republishes_with_attempt_and_commits(monkeypatch):
    from fintech_feature_platform.api import offline_writer_runner as runner_mod

    def _boom(backend, event):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(runner_mod, "handle_feature_offline_write", _boom)
    backend = build_memory_backend()
    consumer = InMemoryEventConsumer([InMemoryMessage(_produce_event_bytes())])

    result = process_next(consumer, backend, max_attempts=5)

    assert result.status == "retry_republished"
    assert result.committed is True
    republished = [r for r in backend.events.published if r.topic == FEATURE_WRITE_OFFLINE]
    assert len(republished) == 1
    assert republished[0].headers == {"x-fsp-attempt": b"1"}


def test_unexpected_error_at_max_dead_letters(monkeypatch):
    from fintech_feature_platform.api import offline_writer_runner as runner_mod

    def _boom(backend, event):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(runner_mod, "handle_feature_offline_write", _boom)
    backend = build_memory_backend()
    consumer = InMemoryEventConsumer(
        [InMemoryMessage(_produce_event_bytes(), headers={"x-fsp-attempt": "4"})]
    )

    result = process_next(consumer, backend, max_attempts=5)

    assert result.status == "dead_lettered"
    assert result.committed is True
    dlq = [r for r in backend.events.published if r.topic == DLQ]
    assert len(dlq) == 1
    assert dlq[0].event.attempt_count == 5
    assert dlq[0].event.max_attempts == 5


# --- request status updates  -------------------------------------

def _seed(backend, request_id="freq_owr", status="completed"):
    backend.status.put(
        RequestStatus(
            request_id=request_id,
            job_id="job_owr",
            status=status,
            entity_type="application",
            entity_key=dict(_ENTITY),
            view="user_credit_risk",
            view_version=1,
            created_at=_TS,
            updated_at=_TS,
        )
    )


def test_status_offline_written_on_ok_keeps_completed():
    backend = build_memory_backend()
    event_bytes = _offline_event_bytes(backend)
    _seed(backend, status="completed")
    consumer = InMemoryEventConsumer([InMemoryMessage(event_bytes)])

    process_next(consumer, backend)

    st = backend.status.get("freq_owr")
    assert st.status == "completed"
    assert st.offline_write_status == "written"


def test_status_offline_retrying_on_retry(monkeypatch):
    from fintech_feature_platform.api import offline_writer_runner as runner_mod

    def _boom(backend, event):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(runner_mod, "handle_feature_offline_write", _boom)
    backend = build_memory_backend()
    _seed(backend, status="completed")
    consumer = InMemoryEventConsumer([InMemoryMessage(_produce_event_bytes())])

    process_next(consumer, backend, max_attempts=5)

    st = backend.status.get("freq_owr")
    assert st.offline_write_status == "retrying"
    assert st.status == "completed"  # top-level unchanged


def test_status_offline_failed_dlq_does_not_flip_completed(monkeypatch):
    from fintech_feature_platform.api import offline_writer_runner as runner_mod

    def _boom(backend, event):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(runner_mod, "handle_feature_offline_write", _boom)
    backend = build_memory_backend()
    _seed(backend, status="completed")
    consumer = InMemoryEventConsumer(
        [InMemoryMessage(_produce_event_bytes(), headers={"x-fsp-attempt": "4"})]
    )

    process_next(consumer, backend, max_attempts=5)

    st = backend.status.get("freq_owr")
    assert st.offline_write_status == "failed_dlq"
    assert st.status == "completed"  # NOT flipped to failed


def test_offline_failing_status_store_does_not_block_commit():
    class _FailingStatus:
        def put(self, status):
            raise RuntimeError("down")

        def get(self, request_id):
            raise RuntimeError("down")

        def update(self, request_id, **changes):
            raise RuntimeError("down")

    backend = build_memory_backend()
    event_bytes = _offline_event_bytes(backend)
    backend = dataclasses.replace(backend, status=_FailingStatus())
    consumer = InMemoryEventConsumer([InMemoryMessage(event_bytes)])

    result = process_next(consumer, backend)
    assert result.status == "ok"
    assert result.committed is True


def test_runner_does_not_commit_or_dlq_on_append_failed():
    class _FailingOffline:
        def get(self, *args, **kwargs):
            return []

        def append_many(self, view, view_version, results):
            raise RuntimeError("postgres down")

    backend = build_memory_backend()
    event_bytes = _offline_event_bytes(backend)
    backend = dataclasses.replace(backend, offline=_FailingOffline())
    consumer = InMemoryEventConsumer([InMemoryMessage(event_bytes)])

    result = process_next(consumer, backend)

    assert result.status == "append_failed"
    assert result.committed is False
    assert consumer.committed == []
    # transient failure is NOT dead-lettered
    assert [r for r in backend.events.published if r.topic == DLQ] == []


def test_runner_idle_when_no_message():
    backend = build_memory_backend()
    consumer = InMemoryEventConsumer([])
    result = process_next(consumer, backend)
    assert result.status == "idle"
    assert result.committed is False


def test_run_processes_exactly_max_messages():
    backend = build_memory_backend()
    event_bytes = _offline_event_bytes(backend)
    consumer = InMemoryEventConsumer(
        [InMemoryMessage(event_bytes), InMemoryMessage(event_bytes)]
    )

    results = run(consumer, backend, max_messages=2)

    assert len(results) == 2
    # first appends; second is an idempotent duplicate -> still ok -> committed
    assert all(r.status == "ok" for r in results)
    assert len(consumer.committed) == 2
    assert len(backend.offline.get(_entity_key(), feature_name="declared_income")) == 1
