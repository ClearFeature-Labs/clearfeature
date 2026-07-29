"""Tests for the source-dataset manifest/item store."""

import json
from datetime import UTC, datetime

import pytest

from fintech_feature_platform.fs_core.stores.source_dataset import (
    ITEM_WRITTEN,
    STATUS_COMPLETED,
    InMemorySourceDatasetStore,
    PostgresSourceDatasetStore,
    SourceDatasetItem,
    SourceDatasetManifest,
)

_NOW = datetime(2026, 7, 8, 10, tzinfo=UTC)
_EVENT = datetime(2026, 7, 1, 10, tzinfo=UTC)


def _manifest(manifest_id="sdm_1", **overrides):
    kwargs = dict(
        manifest_id=manifest_id,
        dataset_id="ds_1",
        source_kind="object_storage_jsonl",
        entity_type="customer",
        source_name="bureau",
        report_type="credit_report",
        copy_mode="copy",
        created_at=_NOW,
        status=STATUS_COMPLETED,
        item_count_read=2,
        item_count_written=2,
        watermark_min_event_ts=_EVENT,
        watermark_max_event_ts=_EVENT,
        content_hash="sha256:abc",
    )
    kwargs.update(overrides)
    return SourceDatasetManifest(**kwargs)


def _item(item_index=0, manifest_id="sdm_1"):
    return SourceDatasetItem(
        manifest_id=manifest_id, item_index=item_index, status=ITEM_WRITTEN,
        source_name="bureau", report_type="credit_report",
        entity_key={"customer_id": "c1"}, report_ref="rep_1", event_ts=_EVENT,
        content_hash="sha256:x",
    )


# --- model validation ---------------------------------------------------------

def test_manifest_rejects_unknown_copy_mode():
    with pytest.raises(ValueError, match="copy_mode"):
        _manifest(copy_mode="teleport")


def test_manifest_rejects_naive_watermark():
    with pytest.raises(ValueError, match="watermark"):
        _manifest(watermark_min_event_ts=datetime(2026, 7, 1, 10))  # noqa: DTZ001


def test_item_rejects_naive_event_ts():
    with pytest.raises(ValueError, match="event_ts"):
        SourceDatasetItem(
            manifest_id="m", item_index=0, status=ITEM_WRITTEN,
            source_name="s", report_type="t",
            event_ts=datetime(2026, 7, 1, 10),  # noqa: DTZ001
        )


def test_manifest_round_trip():
    m = _manifest()
    assert SourceDatasetManifest.from_dict(m.to_dict()) == m


def test_item_round_trip():
    item = _item()
    assert SourceDatasetItem.from_dict(item.to_dict()) == item


# --- in-memory store ----------------------------------------------------------

def test_in_memory_store_manifest_and_items():
    store = InMemorySourceDatasetStore()
    store.upsert_manifest(_manifest())
    store.add_items([_item(0), _item(1)])
    assert store.get_manifest("sdm_1").item_count_written == 2
    assert [i.item_index for i in store.list_items("sdm_1")] == [0, 1]
    assert store.get_manifest("missing") is None
    assert store.list_items("missing") == []


# --- Postgres store (SQL shape via fake connection) ---------------------------

class _FakeCursor:
    def __init__(self, rows=()):
        self._rows = list(rows)
        self.executed = []
        self.executemany_calls = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, seq):
        self.executemany_calls.append((sql, list(seq)))

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


def test_postgres_upsert_manifest_is_parametrized_and_commits():
    conn = _FakeConnection()
    PostgresSourceDatasetStore(conn).upsert_manifest(_manifest())
    sql, params = conn.cursor_obj.executed[0]
    assert "INSERT INTO source_dataset_manifests" in sql
    assert "ON CONFLICT (manifest_id) DO UPDATE" in sql
    assert params["manifest_id"] == "sdm_1"
    assert params["created_at"] == _NOW  # datetime object, not iso string
    assert conn.commits == 1


def test_postgres_add_items_serializes_entity_key_and_commits_once():
    conn = _FakeConnection()
    PostgresSourceDatasetStore(conn).add_items([_item(0), _item(1)])
    sql, param_list = conn.cursor_obj.executemany_calls[0]
    assert "INSERT INTO source_dataset_items" in sql
    assert "ON CONFLICT (manifest_id, item_index) DO NOTHING" in sql
    assert len(param_list) == 2
    assert json.loads(param_list[0]["entity_key"]) == {"customer_id": "c1"}
    assert conn.commits == 1


def test_postgres_add_items_empty_is_noop():
    conn = _FakeConnection()
    PostgresSourceDatasetStore(conn).add_items([])
    assert conn.cursor_obj.executemany_calls == []
    assert conn.commits == 0


def test_postgres_get_manifest_reconstructs():
    from fintech_feature_platform.fs_core.stores.source_dataset import _MANIFEST_COLUMNS

    row = {c: getattr(_manifest(), c) for c in _MANIFEST_COLUMNS}
    conn = _FakeConnection(rows=[row])
    got = PostgresSourceDatasetStore(conn).get_manifest("sdm_1")
    assert got == _manifest()
