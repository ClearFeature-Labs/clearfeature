"""Tests for the batch worker handler + runner."""

import dataclasses
from datetime import UTC, datetime

from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.batch_worker import handle_batch_chunk
from fintech_feature_platform.api.batch_worker_runner import process_next
from fintech_feature_platform.fs_core.events.consumer import (
    InMemoryEventConsumer,
    InMemoryMessage,
)
from fintech_feature_platform.fs_core.events.models import BatchChunkRequested, BatchItem
from fintech_feature_platform.fs_core.events.topics import BATCH_JOB_EVENTS, DLQ
from fintech_feature_platform.fs_core.models import EntityKey
from fintech_feature_platform.fs_core.stores.batch_status import (
    BatchChunkStatus,
    BatchJobStatus,
)

_TS = datetime(2026, 1, 1, tzinfo=UTC)


def _entity_key(user_id="u1") -> EntityKey:
    return EntityKey.from_mapping(
        {"user_id": user_id, "application_id": "A1"},
        key_order=["user_id", "application_id"],
    )


def _item(user_id="u1", payload=None) -> BatchItem:
    return BatchItem(
        entity_type="application",
        entity_key={"user_id": user_id, "application_id": "A1"},
        inline_sources={
            "credit_report": {
                "report_type": "credit_report",
                "report_ts": "2026-01-01T00:00:00Z",
                "payload": payload
                or {"declared_income": 100_000, "monthly_obligations": 700_000},
            }
        },
    )


def _event(items=None, *, features=("declared_income",), groups=(), write_online=False,
           view="user_credit_risk", job_id="job1", chunk_index=0) -> BatchChunkRequested:
    return BatchChunkRequested(
        batch_job_id=job_id,
        chunk_id=f"{job_id}:{chunk_index}",
        chunk_index=chunk_index,
        chunk_count=1,
        correlation_id=job_id,
        occurred_at=_TS,
        view=view,
        view_version=1,
        items=list(items if items is not None else [_item()]),
        requested_features=list(features),
        requested_feature_groups=list(groups),
        write_online=write_online,
    )


def _seed_job(backend, job_id="job1", chunk_count=1):
    backend.batch_status.put(
        BatchJobStatus(
            job_id=job_id, status="accepted", view="user_credit_risk", view_version=1,
            created_at=_TS, updated_at=_TS, total_items=chunk_count, chunk_count=chunk_count,
            write_online=False,
            chunks={
                f"{job_id}:{i}": BatchChunkStatus(
                    chunk_id=f"{job_id}:{i}", chunk_index=i, status="accepted",
                    item_count=1, updated_at=_TS,
                )
                for i in range(chunk_count)
            },
        )
    )


# --- handler ----------------------------------------------------------------

def test_worker_publishes_processed_event():
    backend = build_memory_backend()
    result = handle_batch_chunk(backend, _event(items=[_item("u1"), _item("u2")]))
    assert result.status == "ok"
    processed = [r for r in backend.events.published if r.topic == BATCH_JOB_EVENTS]
    assert len(processed) == 1
    ev = processed[0].event
    assert ev.event_type == "batch.chunk.processed"
    assert ev.chunk_id == "job1:0"
    assert ev.status == "completed"
    assert ev.ok_items == 2
    assert ev.failed_items == 0


def test_worker_publish_failure_returns_publish_failed():
    class _FailingPublisher:
        def publish(self, topic, key, event):
            raise RuntimeError("kafka down")

    backend = dataclasses.replace(build_memory_backend(), events=_FailingPublisher())
    result = handle_batch_chunk(backend, _event())
    assert result.status == "publish_failed"


def test_runner_no_commit_on_processed_publish_failure():
    class _FailingPublisher:
        def publish(self, topic, key, event):
            raise RuntimeError("kafka down")

    backend = dataclasses.replace(build_memory_backend(), events=_FailingPublisher())
    consumer = InMemoryEventConsumer([InMemoryMessage(_event().to_json())])
    result = process_next(consumer, backend)
    assert result.status == "publish_failed"
    assert result.committed is False
    assert consumer.committed == []


def test_worker_computes_and_writes_offline():
    backend = build_memory_backend()
    result = handle_batch_chunk(backend, _event(items=[_item("u1"), _item("u2")]))
    assert result.status == "ok"
    assert result.ok_items == 2
    assert result.failed_items == 0
    assert len(backend.offline.get(_entity_key("u1"), feature_name="declared_income")) == 1
    assert len(backend.offline.get(_entity_key("u2"), feature_name="declared_income")) == 1


def test_worker_uses_feature_planner_groups():
    backend = build_memory_backend()
    # affordability_input_v1 = [declared_income, monthly_obligations]
    result = handle_batch_chunk(
        backend, _event(features=(), groups=("affordability_input_v1",))
    )
    assert result.status == "ok"
    ek = _entity_key("u1")
    assert len(backend.offline.get(ek, feature_name="declared_income")) == 1
    assert len(backend.offline.get(ek, feature_name="monthly_obligations")) == 1


def test_worker_writes_online_only_when_flag_set():
    backend = build_memory_backend()
    handle_batch_chunk(backend, _event(write_online=False))
    assert backend.online.get("user_credit_risk", 1, _entity_key(), "declared_income", 1) is None

    backend2 = build_memory_backend()
    handle_batch_chunk(backend2, _event(write_online=True))
    assert (
        backend2.online.get("user_credit_risk", 1, _entity_key(), "declared_income", 1)
        is not None
    )


def test_worker_per_item_error_does_not_fail_chunk():
    backend = build_memory_backend()
    bad = _item("u2", payload={"monthly_obligations": 1})  # missing declared_income
    result = handle_batch_chunk(backend, _event(items=[_item("u1"), bad]))
    assert result.status == "ok"
    assert result.ok_items == 1
    assert result.failed_items == 1
    assert result.first_errors  # bounded error captured


def test_worker_updates_chunk_status_completed_with_errors():
    backend = build_memory_backend()
    _seed_job(backend)
    bad = _item("u2", payload={"monthly_obligations": 1})
    handle_batch_chunk(backend, _event(items=[_item("u1"), bad]))
    job = backend.batch_status.get("job1")
    assert job.status == "completed_with_errors"
    assert job.failed_items() == 1


# --- runner -----------------------------------------------------------------

def test_runner_commits_after_ok():
    backend = build_memory_backend()
    consumer = InMemoryEventConsumer([InMemoryMessage(_event().to_json())])
    result = process_next(consumer, backend)
    assert result.status == "ok"
    assert result.committed is True
    assert len(consumer.committed) == 1


def test_runner_no_commit_on_infra_failure():
    # persist_and_compute calls backend.payloads.put directly; a store outage there is a
    # non-deterministic infra failure -> handler propagates -> runner does not commit.
    class _FailingPayloads:
        def put(self, storage_uri, payload):
            raise RuntimeError("minio down")

        def get_payload(self, storage_uri):
            raise RuntimeError("minio down")

    backend = dataclasses.replace(build_memory_backend(), payloads=_FailingPayloads())
    consumer = InMemoryEventConsumer([InMemoryMessage(_event().to_json())])
    result = process_next(consumer, backend)
    assert result.status == "infra_failed"
    assert result.committed is False
    assert consumer.committed == []


def test_runner_structural_poison_to_dlq():
    backend = build_memory_backend()
    consumer = InMemoryEventConsumer([InMemoryMessage(b"not-json")])
    result = process_next(consumer, backend)
    assert result.status == "dead_lettered"
    assert result.committed is True
    dlq = [r for r in backend.events.published if r.topic == DLQ]
    assert len(dlq) == 1
    assert dlq[0].event.failure_stage == "batch_worker"


def test_runner_invalid_event_to_dlq():
    backend = build_memory_backend()
    consumer = InMemoryEventConsumer([InMemoryMessage(_event(view="nope").to_json())])
    result = process_next(consumer, backend)
    assert result.status == "dead_lettered"
    assert result.committed is True


def test_replay_same_chunk_is_idempotent():
    backend = build_memory_backend()
    _seed_job(backend)
    handle_batch_chunk(backend, _event())
    handle_batch_chunk(backend, _event())  # replay
    # offline dedup: no duplicate rows
    assert len(backend.offline.get(_entity_key(), feature_name="declared_income")) == 1
    # status not double-counted
    assert backend.batch_status.get("job1").completed_chunks() == 1
