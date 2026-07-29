"""Tests for JSONL raw-report ingestion into landing form (a)."""

import json
from datetime import UTC, datetime

import pytest

from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.jsonl_ingestion import (
    JsonlReportRow,
    run_jsonl_ingestion,
)
from fintech_feature_platform.fs_core.models import EntityKey
from fintech_feature_platform.fs_core.stores.source_dataset import (
    ITEM_DUPLICATE,
    ITEM_REJECTED,
    ITEM_WRITTEN,
    SOURCE_KIND_OBJECT_STORAGE_JSONL,
)


def _row(customer_id="c1", *, event_ts="2026-07-01T10:00:00Z", income=1000, **extra):
    row = {
        "entity_key": {"customer_id": customer_id},
        "event_ts": event_ts,
        "payload": {"income": income},
    }
    row.update(extra)
    return json.dumps(row)


def _ingest(backend, lines, **kw):
    return run_jsonl_ingestion(
        backend=backend,
        lines=lines,
        entity_type="customer",
        source_name="bureau",
        report_type="credit_report",
        **kw,
    )


# --- row parser ---------------------------------------------------------------

def test_valid_row_parses():
    row = JsonlReportRow.model_validate(json.loads(_row()))
    assert row.entity_key == {"customer_id": "c1"}
    assert row.event_ts == datetime(2026, 7, 1, 10, tzinfo=UTC)


def test_missing_entity_key_rejected():
    with pytest.raises(ValueError):
        JsonlReportRow.model_validate({"event_ts": "2026-07-01T10:00:00Z", "payload": {}})


def test_empty_entity_key_rejected():
    with pytest.raises(ValueError):
        JsonlReportRow.model_validate(
            {"entity_key": {}, "event_ts": "2026-07-01T10:00:00Z", "payload": {}}
        )


def test_missing_event_ts_rejected():
    with pytest.raises(ValueError):
        JsonlReportRow.model_validate({"entity_key": {"c": "1"}, "payload": {}})


def test_naive_event_ts_rejected():
    with pytest.raises(ValueError):
        JsonlReportRow.model_validate(
            {"entity_key": {"c": "1"}, "event_ts": "2026-07-01T10:00:00", "payload": {}}
        )


# --- ingestion lands payload + meta + manifest --------------------------------

def test_ingestion_writes_payload_and_raw_meta():
    backend = build_memory_backend()
    manifest = _ingest(backend, [_row()])
    assert manifest.item_count_written == 1
    item = backend.source_datasets.list_items(manifest.manifest_id)[0]
    assert item.status == ITEM_WRITTEN
    # raw_reports_meta written and its payload landed in the payload store.
    meta = backend.metas.get_meta(item.report_ref)
    assert meta.report_type == "credit_report"
    assert meta.content_hash == item.content_hash
    assert backend.payloads.get_payload(meta.storage_uri) == {"income": 1000}


def test_manifest_and_items_recorded_with_report_refs():
    backend = build_memory_backend()
    manifest = _ingest(backend, [_row("c1"), _row("c2", income=2000)])
    assert manifest.source_kind == SOURCE_KIND_OBJECT_STORAGE_JSONL
    assert manifest.item_count_read == 2
    assert manifest.item_count_written == 2
    items = backend.source_datasets.list_items(manifest.manifest_id)
    assert [i.item_index for i in items] == [0, 1]
    assert all(i.report_ref for i in items)  # refs recorded for a later batch job


def test_watermarks_track_min_and_max_event_ts():
    backend = build_memory_backend()
    manifest = _ingest(
        backend,
        [
            _row("c1", event_ts="2026-07-01T00:00:00Z"),
            _row("c2", event_ts="2026-07-05T00:00:00Z"),
            _row("c3", event_ts="2026-07-03T00:00:00Z"),
        ],
    )
    assert manifest.watermark_min_event_ts == datetime(2026, 7, 1, tzinfo=UTC)
    assert manifest.watermark_max_event_ts == datetime(2026, 7, 5, tzinfo=UTC)


# --- dedup / idempotency ------------------------------------------------------

def test_identical_rerun_is_idempotent_no_duplicate_rows():
    backend = build_memory_backend()
    first = _ingest(backend, [_row("c1"), _row("c2")])
    meta_count = len(backend.metas._meta)  # noqa: SLF001 - test inspects the fake store
    payload_count = len(backend.payloads._payloads)  # noqa: SLF001

    second = _ingest(backend, [_row("c1"), _row("c2")])

    assert second.item_count_written == 0
    assert second.item_count_duplicate == 2
    # No new raw_reports_meta or payload rows on the identical rerun.
    assert len(backend.metas._meta) == meta_count  # noqa: SLF001
    assert len(backend.payloads._payloads) == payload_count  # noqa: SLF001
    # Content fingerprint is stable across identical reruns.
    assert second.content_hash == first.content_hash


def test_same_report_ref_different_content_is_rejected_as_conflict():
    backend = build_memory_backend()
    _ingest(backend, [_row("c1", income=1000, report_ref="fixed-1")])
    manifest = _ingest(backend, [_row("c1", income=9999, report_ref="fixed-1")])
    assert manifest.item_count_rejected == 1
    assert manifest.item_count_written == 0
    item = backend.source_datasets.list_items(manifest.manifest_id)[0]
    assert item.status == ITEM_REJECTED
    assert "conflict" in item.error
    # The stored payload was NOT overwritten with the conflicting content.
    meta = backend.metas.get_meta("fixed-1")
    assert backend.payloads.get_payload(meta.storage_uri) == {"income": 1000}


def test_same_report_ref_same_content_is_a_duplicate():
    backend = build_memory_backend()
    _ingest(backend, [_row("c1", income=1000, report_ref="fixed-1")])
    manifest = _ingest(backend, [_row("c1", income=1000, report_ref="fixed-1")])
    assert manifest.item_count_duplicate == 1
    assert manifest.item_count_rejected == 0


# --- DQ summary: bad rows rejected, run not aborted ---------------------------

def test_bad_rows_rejected_without_aborting_run():
    backend = build_memory_backend()
    lines = [
        _row("c1"),                                  # ok
        "not-json{",                                 # malformed JSON
        json.dumps({"event_ts": "2026-07-01T10:00:00Z", "payload": {}}),  # no entity_key
        _row("c2", event_ts="2026-07-01T10:00:00"),  # naive event_ts
        "",                                          # blank -> not read
        _row("c3"),                                  # ok
    ]
    manifest = _ingest(backend, lines)
    assert manifest.item_count_read == 5  # blank line skipped
    assert manifest.item_count_written == 2
    assert manifest.item_count_rejected == 3
    rejected = [
        i for i in backend.source_datasets.list_items(manifest.manifest_id)
        if i.status == ITEM_REJECTED
    ]
    assert len(rejected) == 3
    assert all(i.error for i in rejected)  # every rejection carries a reason


def test_dq_counts_add_up_to_read():
    backend = build_memory_backend()
    _ingest(backend, [_row("c1"), _row("c1")])  # second is a content duplicate
    manifest = backend.source_datasets.get_manifest(
        _ingest(backend, [_row("c9")]).manifest_id
    )
    total = (
        manifest.item_count_written
        + manifest.item_count_duplicate
        + manifest.item_count_rejected
    )
    assert total == manifest.item_count_read


# --- row-level source/report_type overrides + copy_mode -----------------------

def test_row_overrides_source_and_report_type():
    backend = build_memory_backend()
    line = _row("c1", source_name="row_src", report_type="row_type")
    manifest = _ingest(backend, [line])
    item = backend.source_datasets.list_items(manifest.manifest_id)[0]
    assert item.source_name == "row_src"
    assert item.report_type == "row_type"
    assert backend.metas.get_meta(item.report_ref).report_type == "row_type"


def test_register_in_place_is_rejected():
    backend = build_memory_backend()
    with pytest.raises(ValueError, match="register_in_place"):
        _ingest(backend, [_row()], copy_mode="register_in_place")


def test_content_duplicate_within_same_run_counted():
    backend = build_memory_backend()
    manifest = _ingest(backend, [_row("c1"), _row("c1")])  # identical rows
    assert manifest.item_count_written == 1
    assert manifest.item_count_duplicate == 1
    items = backend.source_datasets.list_items(manifest.manifest_id)
    assert [i.status for i in items] == [ITEM_WRITTEN, ITEM_DUPLICATE]


def test_entity_key_reconstructs():
    backend = build_memory_backend()
    manifest = _ingest(backend, [_row("c1")])
    item = backend.source_datasets.list_items(manifest.manifest_id)[0]
    assert EntityKey.from_mapping(item.entity_key).encode() == "customer_id=c1"
