"""Tests for dataset-scoped (manifest) batch jobs  (E2 ingestion->compute)."""

import json

from fastapi.testclient import TestClient

from fintech_feature_platform.api.app import create_app
from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.batch_worker import handle_batch_chunk
from fintech_feature_platform.api.jsonl_ingestion import run_jsonl_ingestion
from fintech_feature_platform.fs_core.events.models import BatchChunkRequested
from fintech_feature_platform.fs_core.events.topics import FEATURE_COMPUTE_BATCH
from fintech_feature_platform.fs_core.models import EntityKey

_VIEW = "user_credit_risk"
_KEY_ORDER = ["user_id", "application_id"]


def _client(backend):
    return TestClient(create_app(backend))


def _row(user_id="1", *, income=100_000):
    return json.dumps(
        {
            "entity_key": {"user_id": user_id, "application_id": "A1"},
            "event_ts": "2026-07-01T00:00:00Z",
            "payload": {"declared_income": income, "monthly_obligations": 700_000},
        }
    )


def _ingest(backend, rows):
    # source_name/report_type match the registry's `credit_report` source.
    return run_jsonl_ingestion(
        backend=backend,
        lines=rows,
        entity_type="application",
        source_name="credit_report",
        report_type="credit_report",
    )


def _batch_body(manifest_id, **overrides):
    body = {
        "view": _VIEW,
        "view_version": 1,
        "requested_features": ["declared_income"],
        "scope": {"type": "source_dataset_manifest", "manifest_id": manifest_id},
        "idempotency_key": "batch-m-1",
        "write_online": False,
    }
    body.update(overrides)
    return body


def _key(user_id="1"):
    return EntityKey.from_mapping(
        {"user_id": user_id, "application_id": "A1"}, key_order=_KEY_ORDER
    )


def _published_chunks(backend):
    return [
        r.event for r in backend.events.published
        if r.topic == FEATURE_COMPUTE_BATCH
        and isinstance(r.event, BatchChunkRequested)
    ]


# --- API: manifest scope ------------------------------------------------------

def test_manifest_scope_accepts_and_publishes_ref_only_chunks():
    backend = build_memory_backend()
    manifest = _ingest(backend, [_row("1"), _row("2")])
    resp = _client(backend).post("/v1/batch/jobs", json=_batch_body(manifest.manifest_id))
    assert resp.status_code == 202
    body = resp.json()
    assert body["manifest_id"] == manifest.manifest_id
    assert body["total_items"] == 2

    chunks = _published_chunks(backend)
    assert chunks and all(c.manifest_id == manifest.manifest_id for c in chunks)
    item = chunks[0].items[0]
    assert item.source_refs  # refs recorded
    assert list(item.source_refs) == ["credit_report"]
    assert item.inline_sources == {}


def test_manifest_chunk_event_carries_no_payload_or_object_key():
    backend = build_memory_backend()
    # Alphabetic sentinel value can't collide with the manifest_id UUID hex in the event.
    manifest = _ingest(backend, [_row("1", income="SENTINEL_INCOME")])
    _client(backend).post("/v1/batch/jobs", json=_batch_body(manifest.manifest_id))
    # Inspect the serialized event bytes: refs only, no payload/object_key/storage_uri.
    raw = _published_chunks(backend)[0].to_json().decode()
    assert "SENTINEL_INCOME" not in raw
    assert "payload" not in raw
    assert "payload_json" not in raw
    assert "object_key" not in raw
    assert "storage_uri" not in raw
    assert "manifest_id" in raw
    assert "source_refs" in raw


def test_unknown_manifest_returns_404():
    backend = build_memory_backend()
    resp = _client(backend).post("/v1/batch/jobs", json=_batch_body("sdm_nope"))
    assert resp.status_code == 404


def test_feature_rows_manifest_rejected_for_compute():
    # A DWH feature-row import produces a landing_form=feature_rows manifest.
    from fintech_feature_platform.api.dwh_ingestion import (
        DwhFeatureConfig,
        run_dwh_feature_import,
    )
    from fintech_feature_platform.fs_core.dwh.reader import InMemoryDwhReader

    backend = build_memory_backend()
    reader = InMemoryDwhReader({"q": [{
        "entity_key": {"user_id": "1", "application_id": "A1"},
        "feature_name": "declared_income", "feature_version": 1, "value": 1000,
        "data_ts": "2026-07-01T00:00:00Z", "calc_ts": "2026-07-01T00:00:00Z",
    }]})
    manifest = run_dwh_feature_import(
        backend=backend, reader=reader,
        config=DwhFeatureConfig(entity_type="application", view=_VIEW, view_version=1,
                                query_name="q"),
    )
    resp = _client(backend).post("/v1/batch/jobs", json=_batch_body(manifest.manifest_id))
    assert resp.status_code == 400
    assert "feature" in resp.json()["detail"]


def test_empty_eligible_items_returns_400():
    backend = build_memory_backend()
    manifest = _ingest(backend, [])  # no rows -> no eligible items
    resp = _client(backend).post("/v1/batch/jobs", json=_batch_body(manifest.manifest_id))
    assert resp.status_code == 400


def test_write_online_true_with_manifest_scope_accepted_as_guarded():
    #: write_online=true is now allowed as a guarded Mode-2 refresh.
    backend = build_memory_backend()
    manifest = _ingest(backend, [_row("1")])
    resp = _client(backend).post(
        "/v1/batch/jobs", json=_batch_body(manifest.manifest_id, write_online=True)
    )
    assert resp.status_code == 202
    event = _published_chunks(backend)[0]
    assert event.write_online is True  # worker will run guarded refresh


def test_write_online_true_non_guarded_mode_rejected():
    backend = build_memory_backend()
    manifest = _ingest(backend, [_row("1")])
    resp = _client(backend).post(
        "/v1/batch/jobs",
        json=_batch_body(
            manifest.manifest_id, write_online=True, online_refresh_mode="unguarded"
        ),
    )
    assert resp.status_code == 400
    assert "guarded" in resp.json()["detail"]


def test_duplicate_items_excluded_by_default():
    backend = build_memory_backend()
    # Same row twice in one ingestion -> one written, one duplicate item.
    manifest = _ingest(backend, [_row("1"), _row("1")])
    assert manifest.item_count_written == 1
    assert manifest.item_count_duplicate == 1
    resp = _client(backend).post("/v1/batch/jobs", json=_batch_body(manifest.manifest_id))
    assert resp.json()["total_items"] == 1  # duplicate excluded


def test_operational_status_includes_manifest_id_and_total_items():
    backend = build_memory_backend()
    manifest = _ingest(backend, [_row("1")])
    client = _client(backend)
    job_id = client.post(
        "/v1/batch/jobs", json=_batch_body(manifest.manifest_id)
    ).json()["job_id"]
    status = client.get(f"/v1/batch/jobs/{job_id}").json()
    assert status["manifest_id"] == manifest.manifest_id
    assert status["total_items"] == 1


# --- worker: compute from refs ------------------------------------------------

def test_worker_computes_manifest_item_from_report_ref():
    backend = build_memory_backend()
    manifest = _ingest(backend, [_row("1", income=123456)])
    _client(backend).post("/v1/batch/jobs", json=_batch_body(manifest.manifest_id))
    event = _published_chunks(backend)[0]
    payload_keys_before = set(backend.payloads._payloads)  # noqa: SLF001

    result = handle_batch_chunk(backend, event)

    assert result.status == "ok"
    assert result.ok_items == 1 and result.failed_items == 0
    records = backend.offline.get(_key("1"), feature_name="declared_income")
    assert len(records) == 1 and records[0].result.value == 123456
    # The worker did NOT re-store any raw payload (it resolved the existing one).
    assert set(backend.payloads._payloads) == payload_keys_before  # noqa: SLF001


def test_worker_replay_does_not_duplicate_offline():
    backend = build_memory_backend()
    manifest = _ingest(backend, [_row("1")])
    _client(backend).post("/v1/batch/jobs", json=_batch_body(manifest.manifest_id))
    event = _published_chunks(backend)[0]
    handle_batch_chunk(backend, event)
    handle_batch_chunk(backend, event)  # replay
    records = backend.offline.get(_key("1"), feature_name="declared_income")
    assert len(records) == 1  # offline dedup


def test_worker_missing_report_ref_is_per_item_error_not_infra():
    backend = build_memory_backend()
    manifest = _ingest(backend, [_row("1")])
    _client(backend).post("/v1/batch/jobs", json=_batch_body(manifest.manifest_id))
    event = _published_chunks(backend)[0]
    # Corrupt the ref so raw_reports_meta lookup fails.
    broken = event.items[0]
    broken_item = type(broken)(
        entity_type=broken.entity_type, entity_key=broken.entity_key,
        source_refs={"credit_report": "rep_does_not_exist"},
    )
    broken_event = type(event).from_dict({**event.to_dict(),
                                          "items": [broken_item.to_dict()]})
    result = handle_batch_chunk(backend, broken_event)
    # per-item deterministic failure, chunk still completes (no infra exception/replay).
    assert result.status == "ok"
    assert result.failed_items == 1 and result.ok_items == 0
    assert result.first_errors


def test_worker_ref_chunk_appends_offline_once_for_whole_chunk():
    #: a multi-item ref chunk computes all items, then ONE bulk append_many.
    backend = build_memory_backend()
    manifest = _ingest(backend, [_row("1"), _row("2"), _row("3")])
    _client(backend).post("/v1/batch/jobs", json=_batch_body(manifest.manifest_id))
    event = _published_chunks(backend)[0]
    assert len(event.items) == 3  # single chunk (default chunk_size=100)

    calls: list[int] = []
    orig = backend.offline.append_many

    def _spy(view, view_version, results):
        results = list(results)
        calls.append(len(results))
        return orig(view, view_version, results)

    backend.offline.append_many = _spy

    result = handle_batch_chunk(backend, event)
    assert result.status == "ok" and result.ok_items == 3
    assert calls == [3]  # exactly one bulk append covering all 3 items


def test_worker_bulk_append_failure_blocks_processed_and_commit():
    from fintech_feature_platform.api import batch_worker_runner
    from fintech_feature_platform.fs_core.events.consumer import (
        InMemoryEventConsumer,
        InMemoryMessage,
    )
    from fintech_feature_platform.fs_core.events.topics import BATCH_JOB_EVENTS

    backend = build_memory_backend()
    manifest = _ingest(backend, [_row("1")])
    _client(backend).post("/v1/batch/jobs", json=_batch_body(manifest.manifest_id))
    event = _published_chunks(backend)[0]
    published_before = len(backend.events.published)

    def _boom(*args, **kwargs):
        raise RuntimeError("offline store down")

    backend.offline.append_many = _boom
    consumer = InMemoryEventConsumer([InMemoryMessage(event.to_json())])

    result = batch_worker_runner.process_next(consumer, backend)

    # Infra failure -> no commit -> replay; no BatchChunkProcessed emitted.
    assert result.status == "infra_failed"
    assert result.committed is False
    assert consumer.committed == []
    processed = [
        r for r in backend.events.published[published_before:]
        if r.topic == BATCH_JOB_EVENTS
    ]
    assert processed == []


def test_worker_ref_chunk_mixes_good_and_bad_items():
    backend = build_memory_backend()
    manifest = _ingest(backend, [_row("1", income=555)])
    _client(backend).post("/v1/batch/jobs", json=_batch_body(manifest.manifest_id))
    good = _published_chunks(backend)[0].items[0]
    bad = type(good)(
        entity_type=good.entity_type,
        entity_key={"user_id": "9", "application_id": "A1"},
        source_refs={"credit_report": "rep_missing"},
    )
    event = _published_chunks(backend)[0]
    mixed = type(event).from_dict(
        {**event.to_dict(), "items": [good.to_dict(), bad.to_dict()]}
    )
    result = handle_batch_chunk(backend, mixed)
    assert result.ok_items == 1 and result.failed_items == 1
    # The good item was still bulk-written.
    assert backend.offline.get(_key("1"), feature_name="declared_income")[0].result.value == 555


def test_manifest_scope_accepts_more_than_inline_cap(monkeypatch):
    monkeypatch.setenv("FSP_BATCH_MAX_ITEMS", "2")  # tiny inline cap
    backend = build_memory_backend()
    manifest = _ingest(backend, [_row("1"), _row("2"), _row("3")])  # 3 > inline cap
    resp = _client(backend).post("/v1/batch/jobs", json=_batch_body(manifest.manifest_id))
    assert resp.status_code == 202  # manifest cap is far larger, not the inline cap
    assert resp.json()["total_items"] == 3


def test_inline_scope_still_capped(monkeypatch):
    monkeypatch.setenv("FSP_BATCH_MAX_ITEMS", "2")
    backend = build_memory_backend()
    item = {
        "entity_type": "application",
        "entity_key": {"user_id": "1", "application_id": "A1"},
        "inline_sources": {
            "credit_report": {
                "report_type": "credit_report",
                "report_ts": "2026-01-01T00:00:00Z",
                "payload": {"declared_income": 1, "monthly_obligations": 2},
            }
        },
    }
    body = {
        "view": _VIEW, "view_version": 1, "requested_features": ["declared_income"],
        "scope": {"type": "inline", "items": [item, item, item]},
        "idempotency_key": "inline-cap", "write_online": False,
    }
    resp = _client(backend).post("/v1/batch/jobs", json=body)
    assert resp.status_code == 400  # 3 > inline cap of 2


def test_worker_never_imports_dwh_reader():
    import fintech_feature_platform.api.batch_worker as bw

    with open(bw.__file__, encoding="utf-8") as handle:
        text = handle.read()
    # The batch worker must never import or call a DWH reader (I5: SQL is ingestion only).
    assert "fs_core.dwh" not in text
    assert "DwhReader" not in text
    assert "run_dwh" not in text
