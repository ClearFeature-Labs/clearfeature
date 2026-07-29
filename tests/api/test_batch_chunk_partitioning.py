"""Batch chunk Kafka partitioning + concurrent job accounting.

Docker-free. Covers the canonical chunk key (chunk-specific, stable, deterministic),
the job_id keying of job-status events, and the accounting invariants under duplicate
delivery and concurrent completion (threads against the shared status-store seam).
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from fintech_feature_platform.api.app import create_app
from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.batch_worker import handle_batch_chunk
from fintech_feature_platform.fs_core.events.models import (
    BatchChunkRequested,
    batch_chunk_partition_key,
)
from fintech_feature_platform.fs_core.events.topics import (
    BATCH_JOB_EVENTS,
    FEATURE_COMPUTE_BATCH,
)
from fintech_feature_platform.fs_core.stores.batch_status import (
    BatchChunkStatus,
    BatchJobStatus,
    InMemoryBatchJobStatusStore,
    merge_chunk,
)

_TS = datetime(2026, 1, 1, 12, tzinfo=UTC)


def _chunk_event(job_id: str, index: int) -> BatchChunkRequested:
    return BatchChunkRequested(
        batch_job_id=job_id,
        chunk_id=f"{job_id}:{index}",
        chunk_index=index,
        chunk_count=4,
        correlation_id=job_id,
        occurred_at=_TS,
        view="user_credit_risk",
        view_version=1,
        items=[],
    )


def _job(job_id: str, chunk_count: int) -> BatchJobStatus:
    chunks = {
        f"{job_id}:{i}": BatchChunkStatus(
            chunk_id=f"{job_id}:{i}", chunk_index=i, status="accepted",
            item_count=1, updated_at=_TS,
        )
        for i in range(chunk_count)
    }
    return BatchJobStatus(
        job_id=job_id, status="accepted", view="user_credit_risk", view_version=1,
        created_at=_TS, updated_at=_TS, total_items=chunk_count,
        chunk_count=chunk_count, write_online=False, chunks=chunks,
    )


def _done(job_id: str, index: int, status: str = "completed") -> BatchChunkStatus:
    return BatchChunkStatus(
        chunk_id=f"{job_id}:{index}", chunk_index=index, status=status,
        item_count=1, ok_items=1 if status != "failed" else 0,
        failed_items=0 if status != "failed" else 1, updated_at=_TS, finished_at=_TS,
    )


# --- canonical chunk key ------------------------------------------------------


def test_chunk_key_is_chunk_specific_and_stable():
    event = _chunk_event("job-a", 3)
    # Deterministic across independent calls (retry of the same chunk = same key,
    # same partition); different chunks of one job get different keys.
    assert batch_chunk_partition_key(event) == "job-a:3"
    assert batch_chunk_partition_key(event) == batch_chunk_partition_key(
        _chunk_event("job-a", 3)
    )
    assert batch_chunk_partition_key(_chunk_event("job-a", 0)) != batch_chunk_partition_key(
        _chunk_event("job-a", 1)
    )
    # Stable string identity — no process-randomized hash() anywhere in the key.
    assert batch_chunk_partition_key(event) == event.chunk_id


def test_batch_submission_publishes_chunk_keys_and_jobid_status_events():
    """End-to-end through the API + worker: chunk events keyed per chunk, job-status
    events keyed per job."""
    backend = build_memory_backend()
    client = TestClient(create_app(backend))
    items = [
        {
            "entity_type": "application",
            "entity_key": {"user_id": f"u{i}", "application_id": "A1"},
            "inline_sources": {
                "credit_report": {
                    "report_type": "credit_report",
                    "report_ts": "2026-01-01T10:00:00Z",
                    "payload": {"declared_income": 100, "monthly_obligations": 10},
                }
            },
        }
        for i in range(4)
    ]
    response = client.post(
        "/v1/batch/jobs",
        json={
            "view": "user_credit_risk", "view_version": 1,
            "scope": {"type": "inline", "items": items},
            "requested_features": ["declared_income"],
            "chunk_size": 2, "idempotency_key": "job-key-test",
        },
    )
    assert response.status_code == 202, response.text
    chunk_records = [
        r for r in backend.events.published if r.topic == FEATURE_COMPUTE_BATCH
    ]
    assert [r.key for r in chunk_records] == ["job-key-test:0", "job-key-test:1"]
    for record in chunk_records:
        assert record.key == record.event.chunk_id  # canonical helper, not job_id
    # Process one chunk with the real worker handler: the accounting event on
    # fp.batch.events must remain keyed by job_id (job-level locality).
    handle_batch_chunk(backend, chunk_records[0].event)
    status_records = [
        r for r in backend.events.published if r.topic == BATCH_JOB_EVENTS
    ]
    assert status_records and all(r.key == "job-key-test" for r in status_records)
    # Event schema unchanged: same payload fields as before the key change.
    round_trip = BatchChunkRequested.from_json(chunk_records[0].event.to_json())
    assert round_trip == chunk_records[0].event


# --- concurrent job accounting ------------------------------------------------


def test_duplicate_terminal_delivery_is_noop_and_never_exceeds_totals():
    store = InMemoryBatchJobStatusStore()
    store.put(_job("dup", 2))
    store.set_chunk("dup", _done("dup", 0))
    again = store.set_chunk("dup", _done("dup", 0))  # duplicate delivery
    assert again.completed_chunks() == 1
    assert again.completed_chunks() + again.failed_chunks() <= again.chunk_count
    assert again.status == "running"  # chunk 1 still pending — no premature COMPLETED


def test_terminal_chunk_never_regresses_to_running():
    # Redelivery artifact: a late "running" update after the chunk already finished
    # (other-worker reprocessing window) must not un-finish the chunk or the job.
    job = merge_chunk(_job("reg", 1), _done("reg", 0))
    assert job.status == "completed"
    late_running = BatchChunkStatus(
        chunk_id="reg:0", chunk_index=0, status="running", item_count=1, updated_at=_TS
    )
    assert merge_chunk(job, late_running) is job  # exact no-op
    # A duplicate terminal for the same chunk stays terminal and idempotent.
    assert merge_chunk(job, _done("reg", 0)).status == "completed"


def test_failed_chunk_yields_correct_terminal_status():
    store = InMemoryBatchJobStatusStore()
    store.put(_job("fail", 2))
    store.set_chunk("fail", _done("fail", 0))
    job = store.set_chunk("fail", _done("fail", 1, status="failed"))
    assert job.status == "completed_with_errors"  # never COMPLETED and FAILED at once
    assert job.completed_chunks() == 1 and job.failed_chunks() == 1
    all_failed = InMemoryBatchJobStatusStore()
    all_failed.put(_job("fail2", 1))
    assert all_failed.set_chunk("fail2", _done("fail2", 0, status="failed")).status == "failed"


def test_concurrent_chunk_completion_loses_no_updates():
    """N threads complete distinct chunks of ONE job simultaneously (the exact
    situation chunk-keyed events create). Every unique terminal state must survive."""
    chunk_count = 32
    store = InMemoryBatchJobStatusStore()
    store.put(_job("conc", chunk_count))
    barrier = threading.Barrier(8)

    def _complete(indexes: list[int]) -> None:
        barrier.wait()
        for i in indexes:
            store.set_chunk("conc", _done("conc", i))
            store.set_chunk("conc", _done("conc", i))  # duplicate delivery per chunk

    threads = [
        threading.Thread(target=_complete, args=([w + 8 * k for k in range(4)],))
        for w in range(8)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    job = store.get("conc")
    assert job.completed_chunks() == chunk_count  # no lost updates, no double counts
    assert job.failed_chunks() == 0
    assert job.status == "completed"  # only after ALL unique chunks are terminal
    assert job.completed_chunks() <= job.chunk_count
