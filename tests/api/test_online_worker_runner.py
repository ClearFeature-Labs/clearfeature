"""Tests for the Kafka consumer runner and safe offset commit (no Kafka required)."""

import dataclasses
from datetime import UTC, datetime, timedelta

import pytest

from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.online_worker_runner import process_next, run
from fintech_feature_platform.fs_core.events.consumer import (
    InMemoryEventConsumer,
    InMemoryMessage,
    build_consumer_config,
    connect_kafka_consumer,
)
from fintech_feature_platform.fs_core.events.models import (
    EntityRef,
    FeatureComputeRequested,
    ReportDescriptor,
)
from fintech_feature_platform.fs_core.events.topics import DLQ, FEATURE_COMPUTE_ONLINE
from fintech_feature_platform.fs_core.models import EntityKey
from fintech_feature_platform.fs_core.stores.request_status import RequestStatus

_TS = datetime(2026, 6, 27, 10, tzinfo=UTC)
_ENTITY = {"user_id": "1", "application_id": "A1"}
_OBJECT_KEY = "mem://rep_credit_runner"


def _entity_key() -> EntityKey:
    return EntityKey.from_mapping(_ENTITY, key_order=["user_id", "application_id"])


def _backend_with_payload():
    backend = build_memory_backend()
    backend.payloads.put(
        _OBJECT_KEY,
        {"declared_income": 4_200_000, "monthly_obligations": 800_000},
    )
    return backend


def _event(view: str = "user_credit_risk") -> FeatureComputeRequested:
    descriptor = ReportDescriptor(
        report_ref="rep_credit_runner",
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
        request_id="freq_r",
        job_id="job_r",
        priority="online",
        deadline_ms=1000,
        entity=EntityRef("application", dict(_ENTITY)),
        view=view,
        view_version=1,
        reports=[descriptor],
        write_policy="online_first",
        idempotency_key="idem_r",
        correlation_id="corr_r",
        occurred_at=_TS,
        requested_features=["declared_income"],
    )


def _consumer(*messages) -> InMemoryEventConsumer:
    return InMemoryEventConsumer(list(messages))


# --- happy path: commit only on ok ------------------------------------------

def test_process_next_ok_calls_handler_and_commits():
    backend = _backend_with_payload()
    consumer = _consumer(InMemoryMessage(_event().to_json()))

    result = process_next(consumer, backend)

    assert result.status == "ok"
    assert result.committed is True
    assert consumer.committed  # exactly the processed message
    assert len(consumer.committed) == 1
    # handler ran: online value present
    assert result.worker_result is not None
    assert result.worker_result.online_written["declared_income:v1"] == "written"


def _seed_accepted_status(backend, request_id: str) -> None:
    # The API submit normally creates the "accepted" entry; tests seed it directly.
    backend.status.put(
        RequestStatus(
            request_id=request_id,
            job_id="job_r",
            status="accepted",
            entity_type="application",
            entity_key=dict(_ENTITY),
            view="user_credit_risk",
            view_version=1,
            created_at=_TS,
            updated_at=_TS,
        )
    )


def test_process_next_ok_stores_request_result_before_completed_status():
    backend = _backend_with_payload()
    _seed_accepted_status(backend, "freq_r")
    consumer = _consumer(InMemoryMessage(_event().to_json()))

    result = process_next(consumer, backend)

    assert result.status == "ok"
    stored = backend.results.get("freq_r")
    assert stored is not None
    item = stored.features["declared_income"]
    assert item["value"] == 4_200_000
    assert item["online_write_status"] == "written"
    # values never leak into the status store
    status = backend.status.get("freq_r")
    assert status is not None and status.status == "completed"
    assert status.online_write_status == "written"


def test_process_next_all_skipped_stale_is_completed_skipped_stale():
    backend = _backend_with_payload()
    # First request writes the fresher value...
    assert process_next(
        _consumer(InMemoryMessage(_event().to_json())), backend
    ).status == "ok"
    # ...a replayed-older event: same content, older report_ts -> D9 skipped_stale.
    older = dataclasses.replace(
        _event(),
        request_id="freq_old",
        reports=[dataclasses.replace(_event().reports[0], report_ts=_TS.replace(hour=8))],
    )
    _seed_accepted_status(backend, "freq_old")
    result = process_next(_consumer(InMemoryMessage(older.to_json())), backend)

    assert result.status == "ok"  # still completed: computed, just not fresher
    assert result.worker_result.online_written["declared_income:v1"] == "skipped_stale"
    status = backend.status.get("freq_old")
    assert status.status == "completed"
    assert status.online_write_status == "skipped_stale"
    # request-scoped value is still stored for the hybrid caller
    assert backend.results.get("freq_old").features["declared_income"]["value"] == 4_200_000


def test_process_next_result_store_failure_does_not_commit():
    class _FailingResults:
        def put(self, result):
            raise RuntimeError("valkey down")

        def get(self, request_id):
            return None

    backend = dataclasses.replace(_backend_with_payload(), results=_FailingResults())
    consumer = _consumer(InMemoryMessage(_event().to_json()))

    result = process_next(consumer, backend)

    # Retriable infra failure: no offset commit -> replay; never marked completed.
    assert result.status == "result_store_failed"
    assert result.committed is False
    assert consumer.committed == []
    status = backend.status.get("freq_r")
    assert status is None or status.status != "completed"


# --- deadline_expired  -------------------------------------------

def _expired_event(request_id="freq_r"):
    now = datetime.now(tz=UTC)
    return dataclasses.replace(
        _event(),
        request_id=request_id,
        event_ts=now - timedelta(seconds=10),
        expires_at=now - timedelta(seconds=5),
    )


def test_process_next_deadline_expired_commits_and_records_status():
    backend = _backend_with_payload()
    _seed_accepted_status(backend, "freq_r")
    consumer = _consumer(InMemoryMessage(_expired_event().to_json()))

    result = process_next(consumer, backend)

    # Terminal deadline outcome: committed (no replay), observable, no result stored.
    assert result.status == "deadline_expired"
    assert result.committed is True
    assert len(consumer.committed) == 1
    status = backend.status.get("freq_r")
    assert status.status == "completed"
    assert status.online_write_status == "deadline_expired"
    assert status.offline_write_status is None
    # No online value written and no request-scoped result exists.
    assert backend.online.get(
        "user_credit_risk", 1, _entity_key(), "declared_income", 1
    ) is None
    assert backend.results.get("freq_r") is None


def test_process_next_expired_completion_publish_failure_replays():
    class _FailingPublisher:
        def publish(self, topic, key, event, *, headers=None):
            raise RuntimeError("kafka down")

    backend = dataclasses.replace(_backend_with_payload(), events=_FailingPublisher())
    consumer = _consumer(InMemoryMessage(_expired_event().to_json()))

    result = process_next(consumer, backend)

    # Completion publish failed -> infra/transient -> no commit -> replay.
    assert result.committed is False
    assert consumer.committed == []


# --- failure paths: never commit --------------------------------------------

def test_compute_failed_republishes_with_incremented_attempt_and_commits():
    backend = _backend_with_payload()
    consumer = _consumer(InMemoryMessage(_event(view="does_not_exist").to_json()))

    result = process_next(consumer, backend, max_attempts=5)

    assert result.status == "retry_republished"
    assert result.committed is True
    assert len(consumer.committed) == 1
    republished = [r for r in backend.events.published if r.topic == FEATURE_COMPUTE_ONLINE]
    assert len(republished) == 1
    assert republished[0].headers == {"x-fsp-attempt": b"1"}
    assert [r for r in backend.events.published if r.topic == DLQ] == []


def test_compute_failed_at_max_attempts_dead_letters():
    backend = _backend_with_payload()
    consumer = _consumer(
        InMemoryMessage(
            _event(view="does_not_exist").to_json(), headers={"x-fsp-attempt": "4"}
        )
    )

    result = process_next(consumer, backend, max_attempts=5)

    assert result.status == "dead_lettered"
    assert result.committed is True
    dlq = [r for r in backend.events.published if r.topic == DLQ]
    assert len(dlq) == 1
    assert dlq[0].event.attempt_count == 5
    assert dlq[0].event.max_attempts == 5
    assert [r for r in backend.events.published if r.topic == FEATURE_COMPUTE_ONLINE] == []


def test_compute_failed_retry_publish_failure_does_not_commit():
    class _FailingPublisher:
        def publish(self, topic, key, event, *, headers=None):
            raise RuntimeError("broker down")

    backend = dataclasses.replace(_backend_with_payload(), events=_FailingPublisher())
    consumer = _consumer(InMemoryMessage(_event(view="does_not_exist").to_json()))

    result = process_next(consumer, backend, max_attempts=5)

    assert result.status == "retry_publish_failed"
    assert result.committed is False
    assert consumer.committed == []


def test_compute_failed_dlq_publish_failure_at_max_does_not_commit():
    class _FailingPublisher:
        def publish(self, topic, key, event, *, headers=None):
            raise RuntimeError("dlq down")

    backend = dataclasses.replace(_backend_with_payload(), events=_FailingPublisher())
    consumer = _consumer(
        InMemoryMessage(
            _event(view="does_not_exist").to_json(), headers={"x-fsp-attempt": "4"}
        )
    )

    result = process_next(consumer, backend, max_attempts=5)

    assert result.status == "dlq_publish_failed"
    assert result.committed is False
    assert consumer.committed == []


def test_unexpected_error_is_attempt_limited(monkeypatch):
    from fintech_feature_platform.api import online_worker_runner as runner_mod

    def _boom(backend, event):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(runner_mod, "handle_feature_compute_requested", _boom)
    backend = _backend_with_payload()
    consumer = _consumer(InMemoryMessage(_event().to_json()))

    result = process_next(consumer, backend, max_attempts=5)

    assert result.status == "retry_republished"
    assert result.committed is True
    assert [r for r in backend.events.published if r.topic == FEATURE_COMPUTE_ONLINE]


def test_process_next_online_write_failed_does_not_commit():
    class _FailingOnline:
        def write_many(self, view, view_version, results):
            raise RuntimeError("valkey down")

    backend = dataclasses.replace(_backend_with_payload(), online=_FailingOnline())
    consumer = _consumer(InMemoryMessage(_event().to_json()))

    result = process_next(consumer, backend)

    assert result.status == "online_write_failed"
    assert result.committed is False
    assert consumer.committed == []


def test_process_next_publish_failed_does_not_commit():
    class _FailingPublisher:
        def publish(self, topic, key, event):
            raise RuntimeError("broker down")

    backend = dataclasses.replace(_backend_with_payload(), events=_FailingPublisher())
    consumer = _consumer(InMemoryMessage(_event().to_json()))

    result = process_next(consumer, backend)

    assert result.status == "publish_failed"
    assert result.committed is False
    assert consumer.committed == []


def test_process_next_deserialization_failed_is_dead_lettered_and_commits():
    backend = _backend_with_payload()
    consumer = _consumer(InMemoryMessage(b"not-json"))

    result = process_next(consumer, backend)

    assert result.status == "dead_lettered"
    assert result.committed is True
    assert len(consumer.committed) == 1
    dlq = [r for r in backend.events.published if r.topic == DLQ]
    assert len(dlq) == 1
    assert dlq[0].event.failure_stage == "online_worker"
    assert dlq[0].event.failure_status == "deserialization_failed"


def test_process_next_dlq_publish_failure_does_not_commit():
    class _FailingPublisher:
        def publish(self, topic, key, event):
            raise RuntimeError("dlq down")

    backend = dataclasses.replace(_backend_with_payload(), events=_FailingPublisher())
    consumer = _consumer(InMemoryMessage(b"not-json"))

    result = process_next(consumer, backend)

    assert result.status == "dlq_publish_failed"
    assert result.committed is False
    assert consumer.committed == []


def test_process_next_consume_error_does_not_commit():
    backend = _backend_with_payload()
    consumer = _consumer(InMemoryMessage(b"", error="partition error"))

    result = process_next(consumer, backend)

    assert result.status == "consume_error"
    assert result.committed is False
    assert consumer.committed == []


def test_process_next_idle_when_no_message():
    backend = _backend_with_payload()
    consumer = _consumer()  # empty

    result = process_next(consumer, backend)

    assert result.status == "idle"
    assert result.committed is False
    assert consumer.committed == []


# --- run() bounded ----------------------------------------------------------

def test_run_processes_exactly_max_messages():
    backend = _backend_with_payload()
    consumer = _consumer(
        InMemoryMessage(_event().to_json()),
        InMemoryMessage(_event().to_json()),
    )

    results = run(consumer, backend, max_messages=2)

    assert len(results) == 2
    assert all(r.status == "ok" for r in results)
    assert len(consumer.committed) == 2


# --- Kafka consumer config / lazy import ------------------------------------

# --- request status updates  -------------------------------------

def _seed_accepted(backend, request_id="freq_r"):
    backend.status.put(
        RequestStatus(
            request_id=request_id,
            job_id="job_r",
            status="accepted",
            entity_type="application",
            entity_key=dict(_ENTITY),
            view="user_credit_risk",
            view_version=1,
            created_at=_TS,
            updated_at=_TS,
        )
    )


def test_status_set_running_before_handler(monkeypatch):
    from fintech_feature_platform.api import online_worker_runner as runner_mod

    backend = _backend_with_payload()
    _seed_accepted(backend)
    seen = {}
    real = runner_mod.handle_feature_compute_requested

    def _capture(backend, event):
        seen["status"] = backend.status.get(event.request_id).status
        return real(backend, event)

    monkeypatch.setattr(runner_mod, "handle_feature_compute_requested", _capture)
    process_next(_consumer(InMemoryMessage(_event().to_json())), backend)
    assert seen["status"] == "running"


def test_status_completed_on_ok():
    backend = _backend_with_payload()
    _seed_accepted(backend)
    process_next(_consumer(InMemoryMessage(_event().to_json())), backend)
    st = backend.status.get("freq_r")
    assert st.status == "completed"
    assert st.online_write_status == "written"
    assert st.offline_write_status == "pending"


def test_status_running_on_retry():
    backend = _backend_with_payload()
    _seed_accepted(backend)
    consumer = _consumer(InMemoryMessage(_event(view="does_not_exist").to_json()))
    process_next(consumer, backend, max_attempts=5)
    st = backend.status.get("freq_r")
    assert st.status == "running"
    assert "retrying" in (st.error or "")


def test_status_failed_on_dead_letter():
    backend = _backend_with_payload()
    _seed_accepted(backend)
    consumer = _consumer(
        InMemoryMessage(_event(view="does_not_exist").to_json(), headers={"x-fsp-attempt": "4"})
    )
    process_next(consumer, backend, max_attempts=5)
    assert backend.status.get("freq_r").status == "failed"


def test_failing_status_store_does_not_block_commit():
    class _FailingStatus:
        def put(self, status):
            raise RuntimeError("down")

        def get(self, request_id):
            raise RuntimeError("down")

        def update(self, request_id, **changes):
            raise RuntimeError("down")

    backend = dataclasses.replace(_backend_with_payload(), status=_FailingStatus())
    consumer = _consumer(InMemoryMessage(_event().to_json()))
    result = process_next(consumer, backend)
    assert result.status == "ok"
    assert result.committed is True
    assert len(consumer.committed) == 1


def test_consumer_config_disables_auto_commit():
    config = build_consumer_config(
        "localhost:19092", "fsp-online-worker", "earliest", "client-x"
    )
    assert config["enable.auto.commit"] is False
    assert config["group.id"] == "fsp-online-worker"
    assert config["auto.offset.reset"] == "earliest"


def test_connect_kafka_consumer_requires_extra():
    # confluent-kafka is not installed under the dev extra.
    with pytest.raises(RuntimeError, match="uv sync --extra kafka"):
        connect_kafka_consumer(
            "localhost:19092", "g", "fp.feature-compute.online"
        )
