"""Tests for the request status store (model + in-memory + Valkey)."""

from datetime import UTC, datetime

import pytest

from fintech_feature_platform.fs_core.stores.request_status import (
    InMemoryRequestStatusStore,
    RequestStatus,
    ValkeyRequestStatusStore,
    status_key,
)

_TS = datetime(2026, 6, 27, 10, tzinfo=UTC)


def _status(request_id: str = "freq_1", status: str = "accepted") -> RequestStatus:
    return RequestStatus(
        request_id=request_id,
        job_id="job_1",
        status=status,
        entity_type="application",
        entity_key={"user_id": "1", "application_id": "A1"},
        view="user_credit_risk",
        view_version=1,
        requested_features=["declared_income"],
        created_at=_TS,
        updated_at=_TS,
        metadata_write_status="pending",
    )


def test_request_status_round_trip():
    s = _status()
    assert RequestStatus.from_dict(s.to_dict()) == s
    assert RequestStatus.from_json(s.to_json()) == s


def test_request_status_rejects_naive_datetime():
    with pytest.raises(ValueError, match="timezone-aware"):
        RequestStatus(
            request_id="x",
            job_id="j",
            status="accepted",
            entity_type="application",
            entity_key={"user_id": "1"},
            view="v",
            view_version=1,
            created_at=datetime(2026, 6, 27, 10),  # naive
            updated_at=_TS,
        )


# --- in-memory store --------------------------------------------------------

def test_in_memory_put_get():
    store = InMemoryRequestStatusStore()
    store.put(_status())
    assert store.get("freq_1").status == "accepted"


def test_in_memory_get_missing_is_none():
    assert InMemoryRequestStatusStore().get("nope") is None


def test_in_memory_update_merges_and_refreshes():
    store = InMemoryRequestStatusStore()
    store.put(_status())
    merged = store.update("freq_1", status="running", started_at=_TS)
    assert merged.status == "running"
    assert merged.started_at == _TS
    assert merged.created_at == _TS  # preserved
    assert merged.updated_at >= _TS  # refreshed


def test_in_memory_update_missing_without_create_fields_is_none():
    store = InMemoryRequestStatusStore()
    assert store.update("ghost", status="running") is None


def test_in_memory_update_missing_with_create_fields_upserts():
    store = InMemoryRequestStatusStore()
    created = store.update(
        "freq_2",
        job_id="job_2",
        status="completed",
        entity_type="application",
        entity_key={"user_id": "9"},
        view="user_credit_risk",
        view_version=1,
    )
    assert created is not None
    assert store.get("freq_2").status == "completed"


# --- Valkey store (fake client) ---------------------------------------------

class _FakeRedis:
    def __init__(self):
        self.store: dict = {}
        self.ttls: dict = {}

    def set(self, name, value, ex=None):
        self.store[name] = value
        self.ttls[name] = ex

    def get(self, name):
        return self.store.get(name)


def test_valkey_status_store_uses_key_and_ttl():
    client = _FakeRedis()
    store = ValkeyRequestStatusStore(client, ttl_s=99)
    store.put(_status())
    assert status_key("freq_1") == "fs:request-status:freq_1"
    assert "fs:request-status:freq_1" in client.store
    assert client.ttls["fs:request-status:freq_1"] == 99


def test_valkey_status_store_get_round_trip_and_missing():
    client = _FakeRedis()
    store = ValkeyRequestStatusStore(client, ttl_s=99)
    store.put(_status())
    assert store.get("freq_1").status == "accepted"
    assert store.get("missing") is None


def test_valkey_status_store_update_merges():
    client = _FakeRedis()
    store = ValkeyRequestStatusStore(client, ttl_s=99)
    store.put(_status())
    merged = store.update("freq_1", offline_write_status="written")
    assert merged.offline_write_status == "written"
    assert store.get("freq_1").offline_write_status == "written"
