"""Tests for the request metadata store (models + in-memory + Postgres mappers)."""

import json
import sys
from datetime import UTC, datetime

from fintech_feature_platform.fs_core.stores.metadata import (
    InMemoryMetadataStore,
    PostgresMetadataStore,
    RequestEvent,
    RequestMetadata,
    _request_to_params,
    _row_to_request,
    event_hash,
)

_TS = datetime(2026, 6, 27, 10, tzinfo=UTC)


def _meta(request_id="freq_1", **overrides) -> RequestMetadata:
    base = dict(
        request_id=request_id,
        updated_at=_TS,
        job_id="job_1",
        status="accepted",
        entity_type="application",
        entity_key={"user_id": "1", "application_id": "A1"},
        view="user_credit_risk",
        view_version=1,
        requested_features=["declared_income"],
        created_at=_TS,
        metadata_write_status=None,
    )
    base.pop("metadata_write_status", None)
    base.update(overrides)
    return RequestMetadata(**base)


def _event(event_hash_value="h1") -> RequestEvent:
    return RequestEvent(
        event_hash=event_hash_value,
        event_type="feature_compute.requested",
        occurred_at=_TS,
        created_at=_TS,
        request_id="freq_1",
        summary={"view": "user_credit_risk"},
    )


# --- models -----------------------------------------------------------------

def test_request_metadata_round_trip():
    m = _meta()
    assert RequestMetadata.from_dict(m.to_dict()) == m
    assert RequestMetadata.from_json(m.to_json()) == m


def test_request_event_round_trip():
    e = _event()
    assert RequestEvent.from_dict(e.to_dict()) == e
    assert RequestEvent.from_json(e.to_json()) == e


def test_event_hash_is_sha256_of_raw_bytes():
    import hashlib

    assert event_hash(b"abc") == hashlib.sha256(b"abc").hexdigest()


# --- in-memory store --------------------------------------------------------

def test_in_memory_upsert_and_get():
    store = InMemoryMetadataStore()
    store.upsert_request(_meta())
    assert store.get_request("freq_1").status == "accepted"


def test_in_memory_get_missing_is_none():
    assert InMemoryMetadataStore().get_request("nope") is None


def test_in_memory_append_event_dedups_by_hash():
    store = InMemoryMetadataStore()
    assert store.append_event(_event("h1")) is True
    assert store.append_event(_event("h1")) is False


def test_in_memory_upsert_does_not_regress_terminal_status():
    store = InMemoryMetadataStore()
    store.upsert_request(_meta(status="completed", online_write_status="written"))
    # a replayed "accepted" snapshot must not downgrade a completed request
    store.upsert_request(_meta(status="accepted"))
    got = store.get_request("freq_1")
    assert got.status == "completed"
    assert got.online_write_status == "written"


def test_in_memory_upsert_merges_offline_status_without_losing_features():
    store = InMemoryMetadataStore()
    store.upsert_request(_meta(status="completed"))
    store.upsert_request(
        RequestMetadata(request_id="freq_1", updated_at=_TS, offline_write_status="written")
    )
    got = store.get_request("freq_1")
    assert got.offline_write_status == "written"
    assert got.status == "completed"
    assert got.requested_features == ["declared_income"]  # preserved


# --- Postgres mappers + fake connection -------------------------------------

def test_module_imports_without_psycopg():
    assert "psycopg" not in sys.modules


def test_request_to_params_and_row_round_trip():
    params = _request_to_params(_meta())
    assert params["request_id"] == "freq_1"
    assert json.loads(params["entity_key_json"]) == {
        "user_id": "1",
        "application_id": "A1",
    }
    assert json.loads(params["requested_features_json"]) == ["declared_income"]
    # simulate psycopg returning JSONB pre-parsed
    row = {
        "request_id": params["request_id"],
        "job_id": params["job_id"],
        "status": params["status"],
        "entity_type": params["entity_type"],
        "entity_key_json": json.loads(params["entity_key_json"]),
        "feature_view": params["feature_view"],
        "view_version": params["view_version"],
        "requested_features_json": json.loads(params["requested_features_json"]),
        "requested_feature_groups_json": json.loads(
            params["requested_feature_groups_json"]
        ),
        "online_write_status": params["online_write_status"],
        "offline_write_status": params["offline_write_status"],
        "created_at": params["created_at"],
        "updated_at": params["updated_at"],
        "finished_at": params["finished_at"],
        "error": params["error"],
    }
    restored = _row_to_request(row)
    assert restored.request_id == "freq_1"
    assert restored.entity_key == {"user_id": "1", "application_id": "A1"}
    assert restored.requested_features == ["declared_income"]


class _FakeCursor:
    def __init__(self, rows=(), rowcount=1):
        self._rows = list(rows)
        self.rowcount = rowcount
        self.executed: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConnection:
    def __init__(self, rows=(), rowcount=1):
        self.cursor_obj = _FakeCursor(rows, rowcount)
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


def test_postgres_append_event_returns_true_on_insert():
    conn = _FakeConnection(rowcount=1)
    assert PostgresMetadataStore(conn).append_event(_event("h1")) is True
    assert conn.commits == 1


def test_postgres_append_event_returns_false_on_conflict():
    conn = _FakeConnection(rowcount=0)
    assert PostgresMetadataStore(conn).append_event(_event("h1")) is False


def test_postgres_get_request_missing_is_none():
    conn = _FakeConnection(rows=())
    assert PostgresMetadataStore(conn).get_request("freq_1") is None


def test_postgres_upsert_request_selects_then_upserts_and_commits():
    conn = _FakeConnection(rows=())  # no existing row -> fresh insert
    PostgresMetadataStore(conn).upsert_request(_meta())
    # one SELECT (get_request) + one UPSERT
    assert len(conn.cursor_obj.executed) == 2
    assert conn.commits == 1
