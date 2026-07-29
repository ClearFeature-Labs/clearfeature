"""Tests for the durable batch metadata store (records + in-memory + Postgres mappers)."""

from datetime import UTC, datetime

import pytest

from fintech_feature_platform.fs_core.stores.batch_metadata import (
    BatchChunkRecord,
    BatchJobRecord,
    BatchMetadataConflictError,
    InMemoryBatchMetadataStore,
    PostgresBatchMetadataStore,
    _chunk_to_params,
    _job_to_params,
    derive_job_status,
)

_TS = datetime(2026, 1, 1, 12, tzinfo=UTC)


def _job(job_id="job1", chunk_count=2, total_items=2) -> BatchJobRecord:
    return BatchJobRecord(
        job_id=job_id, status="accepted", created_at=_TS, updated_at=_TS,
        view="user_credit_risk", view_version=1, requested_features=["declared_income"],
        total_items=total_items, chunk_count=chunk_count, write_online=False,
    )


def _chunk(job_id, index, status, *, ok=1, failed=0, item_count=1) -> BatchChunkRecord:
    return BatchChunkRecord(
        chunk_id=f"{job_id}:{index}", job_id=job_id, chunk_index=index,
        chunk_count=2, status=status, item_count=item_count, ok_items=ok,
        failed_items=failed, created_at=_TS, updated_at=_TS,
    )


def test_records_round_trip():
    job = _job()
    assert BatchJobRecord.from_dict(job.to_dict()) == job
    chunk = _chunk("job1", 0, "completed")
    assert BatchChunkRecord.from_dict(chunk.to_dict()) == chunk


def test_derive_job_status():
    assert derive_job_status([], 2) == "accepted"
    accepted = [_chunk("j", 0, "requested"), _chunk("j", 1, "requested")]
    assert derive_job_status(accepted, 2) == "accepted"
    partial = [_chunk("j", 0, "completed")]
    assert derive_job_status(partial, 2) == "running"
    done = [_chunk("j", 0, "completed"), _chunk("j", 1, "completed")]
    assert derive_job_status(done, 2) == "completed"
    with_err = [
        _chunk("j", 0, "completed"),
        _chunk("j", 1, "completed_with_errors", ok=0, failed=1),
    ]
    assert derive_job_status(with_err, 2) == "completed_with_errors"


# --- in-memory store --------------------------------------------------------

def test_requested_then_processed_derives_completed():
    store = InMemoryBatchMetadataStore()
    store.upsert_job(_job())
    store.upsert_chunk_requested(_chunk("job1", 0, "requested"))
    store.upsert_chunk_requested(_chunk("job1", 1, "requested"))
    assert store.get_job("job1").status == "accepted"
    store.upsert_chunk_processed(_chunk("job1", 0, "completed"))
    store.upsert_chunk_processed(_chunk("job1", 1, "completed"))
    job = store.get_job("job1")
    assert job.status == "completed"
    assert job.finished_at is not None
    assert job.view == "user_credit_risk"  # snapshot preserved


def test_processed_with_errors_derives_completed_with_errors():
    store = InMemoryBatchMetadataStore()
    store.upsert_job(_job())
    store.upsert_chunk_processed(_chunk("job1", 0, "completed"))
    store.upsert_chunk_processed(
        _chunk("job1", 1, "completed_with_errors", ok=0, failed=1)
    )
    assert store.get_job("job1").status == "completed_with_errors"


def test_requested_replay_after_processed_does_not_downgrade():
    store = InMemoryBatchMetadataStore()
    store.upsert_job(_job(chunk_count=1))
    store.upsert_chunk_processed(_chunk("job1", 0, "completed"))
    store.upsert_chunk_requested(_chunk("job1", 0, "requested"))  # replay
    chunk = store.list_chunks("job1")[0]
    assert chunk.status == "completed"  # not downgraded


def test_processed_before_requested_works():
    store = InMemoryBatchMetadataStore()
    store.upsert_chunk_processed(_chunk("job1", 0, "completed"))  # no job/requested yet
    assert store.get_job("job1") is not None  # partial job created
    store.upsert_job(_job(chunk_count=1))  # snapshot fills later
    job = store.get_job("job1")
    assert job.view == "user_credit_risk"
    assert job.status == "completed"


def test_identical_processed_replay_is_no_op():
    store = InMemoryBatchMetadataStore()
    store.upsert_job(_job(chunk_count=1))
    store.upsert_chunk_processed(_chunk("job1", 0, "completed"))
    store.upsert_chunk_processed(_chunk("job1", 0, "completed"))  # identical replay
    assert store.get_job("job1").status == "completed"


def test_conflicting_processed_raises():
    store = InMemoryBatchMetadataStore()
    store.upsert_job(_job(chunk_count=1))
    store.upsert_chunk_processed(_chunk("job1", 0, "completed", ok=1, failed=0))
    with pytest.raises(BatchMetadataConflictError):
        store.upsert_chunk_processed(
            _chunk("job1", 0, "completed_with_errors", ok=0, failed=1)
        )


# --- Postgres mappers -------------------------------------------------------

def test_job_and_chunk_to_params():
    jp = _job_to_params(_job())
    assert jp["job_id"] == "job1"
    assert jp["feature_view"] == "user_credit_risk"  # SQL column avoids reserved word
    cp = _chunk_to_params(_chunk("job1", 0, "completed"))
    assert cp["chunk_id"] == "job1:0"
    assert cp["ok_items"] == 1


class _FakeCursor:
    def __init__(self, rows=()):
        self._rows = list(rows)
        self.executed: list = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class _FakeConnection:
    def __init__(self, rows=()):
        self.cursor_obj = _FakeCursor(rows)
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


def test_postgres_get_job_missing_is_none():
    assert PostgresBatchMetadataStore(_FakeConnection(rows=())).get_job("job1") is None


def test_postgres_upsert_chunk_processed_writes_and_commits():
    conn = _FakeConnection(rows=())  # no existing chunk/job -> fresh insert
    PostgresBatchMetadataStore(conn).upsert_chunk_processed(_chunk("job1", 0, "completed"))
    assert conn.commits >= 1
    # at least the chunk upsert + a job upsert were issued
    assert any("batch_chunks" in sql for sql, _ in conn.cursor_obj.executed)
    assert any("batch_jobs" in sql for sql, _ in conn.cursor_obj.executed)
