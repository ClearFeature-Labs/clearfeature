"""Tests for the batch job status store (model + in-memory + Valkey fake)."""

from datetime import UTC, datetime

from fintech_feature_platform.fs_core.stores.batch_status import (
    BatchChunkStatus,
    BatchJobStatus,
    InMemoryBatchJobStatusStore,
    ValkeyBatchJobStatusStore,
    batch_job_key,
)

_TS = datetime(2026, 1, 1, 12, tzinfo=UTC)


def _job(job_id="job1", chunk_count=2) -> BatchJobStatus:
    chunks = {
        f"{job_id}:{i}": BatchChunkStatus(
            chunk_id=f"{job_id}:{i}", chunk_index=i, status="accepted",
            item_count=1, updated_at=_TS,
        )
        for i in range(chunk_count)
    }
    return BatchJobStatus(
        job_id=job_id, status="accepted", view="user_credit_risk", view_version=1,
        created_at=_TS, updated_at=_TS, total_items=chunk_count, chunk_count=chunk_count,
        write_online=False, requested_features=["declared_income"], chunks=chunks,
    )


def _chunk(job_id, index, status, ok=1, failed=0) -> BatchChunkStatus:
    return BatchChunkStatus(
        chunk_id=f"{job_id}:{index}", chunk_index=index, status=status,
        item_count=ok + failed, ok_items=ok, failed_items=failed, updated_at=_TS,
        finished_at=_TS,
    )


def test_job_status_round_trip():
    job = _job()
    assert BatchJobStatus.from_dict(job.to_dict()) == job
    assert BatchJobStatus.from_json(job.to_json()) == job


def test_derived_counts_are_zero_when_accepted():
    job = _job()
    assert job.completed_chunks() == 0
    assert job.failed_chunks() == 0
    assert job.failed_items() == 0


# --- in-memory: per-chunk state drives derived job status -------------------

def test_set_chunk_running_then_completed_derives_job_status():
    store = InMemoryBatchJobStatusStore()
    store.put(_job())
    store.set_chunk("job1", _chunk("job1", 0, "completed"))
    mid = store.set_chunk("job1", _chunk("job1", 1, "completed"))
    assert mid.status == "completed"
    assert mid.completed_chunks() == 2
    assert mid.finished_at is not None


def test_set_chunk_with_errors_derives_completed_with_errors():
    store = InMemoryBatchJobStatusStore()
    store.put(_job())
    store.set_chunk("job1", _chunk("job1", 0, "completed"))
    job = store.set_chunk("job1", _chunk("job1", 1, "completed_with_errors", ok=0, failed=1))
    assert job.status == "completed_with_errors"
    assert job.failed_items() == 1


def test_replay_same_chunk_does_not_double_count():
    store = InMemoryBatchJobStatusStore()
    store.put(_job())
    store.set_chunk("job1", _chunk("job1", 0, "completed"))
    store.set_chunk("job1", _chunk("job1", 0, "completed"))  # replay same chunk
    job = store.get("job1")
    assert job.completed_chunks() == 1  # not 2
    assert job.status == "running"  # chunk 1 still accepted


def test_set_chunk_missing_job_is_none():
    store = InMemoryBatchJobStatusStore()
    assert store.set_chunk("ghost", _chunk("ghost", 0, "completed")) is None


# --- Valkey (fake client) ---------------------------------------------------

class _FakePipeline:
    """WATCH/MULTI pipeline shape used by ``ValkeyBatchJobStatusStore.set_chunk``:
    immediate ``get`` while watching, then ``multi()`` + queued ``set``."""

    def __init__(self, client):
        self._client = client
        self._queued: list = []
        self._immediate = True

    def get(self, name):
        assert self._immediate, "get after multi() is not immediate"
        return self._client.store.get(name)

    def multi(self):
        self._immediate = False

    def set(self, name, value, ex=None):
        assert not self._immediate, "set must be queued after multi()"
        self._queued.append((name, value, ex))

    def _execute(self):
        for name, value, ex in self._queued:
            self._client.set(name, value, ex=ex)


class _FakeRedis:
    def __init__(self):
        self.store: dict = {}
        self.ttls: dict = {}
        self.transactions = 0

    def set(self, name, value, ex=None):
        self.store[name] = value
        self.ttls[name] = ex

    def get(self, name):
        return self.store.get(name)

    def transaction(self, func, *keys):
        # Single-writer fake: runs the callback once and applies the queued writes
        # (redis-py additionally retries on WATCH conflicts).
        self.transactions += 1
        pipe = _FakePipeline(self)
        func(pipe)
        pipe._execute()


def test_valkey_store_key_ttl_and_set_chunk():
    client = _FakeRedis()
    store = ValkeyBatchJobStatusStore(client, ttl_s=99)
    store.put(_job())
    assert batch_job_key("job1") in client.store
    assert client.ttls[batch_job_key("job1")] == 99
    store.set_chunk("job1", _chunk("job1", 0, "completed"))
    got = store.get("job1")
    assert got.completed_chunks() == 1
    assert client.transactions == 1  # chunk merge runs inside WATCH/MULTI
    assert client.ttls[batch_job_key("job1")] == 99  # TTL preserved by the txn write
    assert store.get("missing") is None
    assert store.set_chunk("missing", _chunk("missing", 0, "completed")) is None
