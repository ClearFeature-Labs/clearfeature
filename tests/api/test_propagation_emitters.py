"""Emission of FeatureUpdated after durable offline writes/imports.

Covers the five emitters wired through the single shared ``emit_feature_updates`` seam:
Offline Writer, Batch worker (inline + ref), table import, DWH feature import.
"""

import json
from datetime import UTC, datetime

import pytest

from fintech_feature_platform.api.backend import AppBackend
from fintech_feature_platform.api.batch_worker import handle_batch_chunk
from fintech_feature_platform.api.dwh_ingestion import DwhFeatureConfig, run_dwh_feature_import
from fintech_feature_platform.api.offline_writer import handle_feature_offline_write
from fintech_feature_platform.api.storage_uri import build_memory_storage_uri
from fintech_feature_platform.api.table_import import (
    TableFeatureColumn,
    TableFeatureImportConfig,
    run_table_feature_import,
)
from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.dwh.reader import InMemoryDwhReader
from fintech_feature_platform.fs_core.events.models import (
    BatchChunkRequested,
    BatchItem,
    FeatureOfflineWriteRequested,
)
from fintech_feature_platform.fs_core.events.publisher import InMemoryEventPublisher
from fintech_feature_platform.fs_core.feature_store import FeatureStore
from fintech_feature_platform.fs_core.hashing import value_hash
from fintech_feature_platform.fs_core.models import (
    EntityKey,
    FeatureRef,
    FeatureResult,
    FeatureWriteSet,
)
from fintech_feature_platform.fs_core.observability.metrics import InMemoryMetricsRecorder
from fintech_feature_platform.fs_core.raw.meta_repository import InMemoryMetaRepository
from fintech_feature_platform.fs_core.raw.payload_store import InMemoryPayloadStore
from fintech_feature_platform.fs_core.raw.report_resolver import ReportResolver
from fintech_feature_platform.fs_core.registry.loader import build_registry
from fintech_feature_platform.fs_core.stores.batch_metadata import InMemoryBatchMetadataStore
from fintech_feature_platform.fs_core.stores.batch_status import InMemoryBatchJobStatusStore
from fintech_feature_platform.fs_core.stores.metadata import InMemoryMetadataStore
from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore
from fintech_feature_platform.fs_core.stores.online import InMemoryOnlineStore
from fintech_feature_platform.fs_core.stores.request_result import InMemoryRequestResultStore
from fintech_feature_platform.fs_core.stores.request_status import InMemoryRequestStatusStore
from fintech_feature_platform.fs_core.stores.source_dataset import InMemorySourceDatasetStore

_TS = datetime(2026, 1, 10, tzinfo=UTC)


def _registry():
    # a: leaf (source), reactive input of c. Importers/writers touching a should announce it.
    data = {
        "registry_version": "test-v1",
        "entities": {"e": {"key_fields": ["id"]}},
        "sources": {
            "src": {"type": "raw_report", "report_type": "r", "ts_field": "report_ts"},
        },
        "feature_views": {
            "v": {"entity": "e", "key_fields": ["id"], "view_version": 1,
                  "owner": "o", "status": "active", "features": {
                      "a": {"kind": "udf", "feature_version": 1, "udf": "udf.a",
                            "dtype": "int", "status": "active", "inputs": ["src"]},
                      "c": {"kind": "udf", "feature_version": 1, "udf": "udf.c",
                            "dtype": "int", "status": "active", "deps": [
                                {"feature": "a", "version": 1, "propagation": "reactive"}]},
                  }}
        },
    }
    return build_registry(data)


def _full_backend(publisher=None):
    registry = _registry()
    udfs = UdfRegistry({
        "udf.a": lambda s, d: s["src"]["v"],
        "udf.c": lambda s, d: d["a"],
    })
    payloads = InMemoryPayloadStore()
    metas = InMemoryMetaRepository()
    resolver = ReportResolver(payloads, metas)
    online = InMemoryOnlineStore()
    offline = InMemoryOfflineStore()
    store = FeatureStore(registry, udfs, resolver, offline, online)

    def make_feature_store(request_resolver):
        return FeatureStore(registry, udfs, request_resolver, offline, online)

    return AppBackend(
        registry=registry, store=store, payloads=payloads, metas=metas,
        online=online, offline=offline,
        make_storage_uri=lambda rt, ref, ts: build_memory_storage_uri(ref),
        raw_format="json", raw_compression="none",
        serialize_payload=lambda p: json.dumps(p, sort_keys=True, default=str).encode(),
        make_feature_store=make_feature_store,
        events=publisher or InMemoryEventPublisher(),
        status=InMemoryRequestStatusStore(), results=InMemoryRequestResultStore(),
        metadata=InMemoryMetadataStore(), batch_status=InMemoryBatchJobStatusStore(),
        batch_meta=InMemoryBatchMetadataStore(),
        source_datasets=InMemorySourceDatasetStore(),
        metrics=InMemoryMetricsRecorder(),
    )


class _FailingPublisher:
    def publish(self, *args, **kwargs):
        raise RuntimeError("broker unavailable")


def _key(id_="1"):
    return EntityKey.from_mapping({"id": id_}, key_order=["id"])


def _updates(publisher):
    return [
        r.event for r in publisher.published
        if getattr(r.event, "event_type", "") == "feature_updated"
    ]


def _offline_write_event(feature_name="a", value=5):
    result = FeatureResult(
        ref=FeatureRef(feature_name, 1), entity_key=_key(), value=value,
        data_ts=_TS, calc_ts=_TS, max_input_data_ts=_TS,
        input_fingerprint="fp_a", value_hash=value_hash(value),
    )
    write_set = FeatureWriteSet(
        view="v", view_version=1, entity_key=_key(),
        results={feature_name: result}, source_refs={}, run_id="run_1", job_id="job_1",
    )
    return FeatureOfflineWriteRequested(
        request_id="req_1", job_id="job_1", correlation_id="corr_1",
        occurred_at=_TS, write_set=write_set,
    )


# --- Offline Writer ----------------------------------------------------------

def test_offline_writer_emits_feature_updated_after_append():
    backend = _full_backend()
    result = handle_feature_offline_write(backend, _offline_write_event())
    assert result.status == "ok"
    assert result.propagation_updates_emitted == 1
    updates = _updates(backend.events)
    assert [u.feature_name for u in updates] == ["a"]
    assert updates[0].source == "offline_writer"
    assert updates[0].run_id == "run_1"


def test_offline_writer_no_emit_for_feature_without_reactive_dependents():
    backend = _full_backend()
    # c has no reactive dependents -> writing c announces nothing (topic stays bounded).
    result = handle_feature_offline_write(backend, _offline_write_event(feature_name="c"))
    assert result.status == "ok"
    assert result.propagation_updates_emitted == 0
    assert _updates(backend.events) == []


def test_offline_writer_publish_failure_is_replay_safe():
    backend = _full_backend(publisher=_FailingPublisher())
    result = handle_feature_offline_write(backend, _offline_write_event())
    # Publish failed -> status signals no-commit; the offline row is already durable so a
    # replay re-appends nothing and re-emits.
    assert result.status == "propagation_publish_failed"
    assert backend.offline.get(_key(), feature_name="a", feature_version=1)


# --- Batch worker ------------------------------------------------------------

def test_batch_worker_inline_chunk_emits_after_append():
    backend = _full_backend()
    event = BatchChunkRequested(
        batch_job_id="bj_1", chunk_id="ck_1", chunk_index=0, chunk_count=1,
        correlation_id="corr_1", occurred_at=_TS, view="v", view_version=1,
        items=[BatchItem(
            entity_type="e", entity_key={"id": "1"},
            inline_sources={"src": {"report_type": "r", "report_ts": _TS.isoformat(),
                                    "payload": {"v": 5}}},
        )],
        requested_features=["a"], write_online=False,
    )
    result = handle_batch_chunk(backend, event)
    assert result.status == "ok"
    updates = _updates(backend.events)
    assert [u.feature_name for u in updates] == ["a"]
    assert updates[0].source == "batch_worker"
    assert updates[0].job_id == "bj_1"


# --- table import ------------------------------------------------------------

def test_table_import_emits_and_records_enqueue_status():
    backend = _full_backend()
    config = TableFeatureImportConfig(
        view="v", view_version=1, entity_columns=["id"], data_ts_column="data_ts",
        features={"a": TableFeatureColumn(column="a_col", dtype="int")},
    )
    rows = [{"id": "1", "a_col": 5, "data_ts": _TS.isoformat()}]
    summary = run_table_feature_import(backend=backend, config=config, rows=rows)
    assert summary.propagation_enqueue_status == "enqueued"
    assert summary.propagation_updates_emitted == 1
    assert [u.feature_name for u in _updates(backend.events)] == ["a"]
    assert _updates(backend.events)[0].source == "table_import"


def test_table_import_publish_failure_records_failed_status_visibly():
    backend = _full_backend(publisher=_FailingPublisher())
    config = TableFeatureImportConfig(
        view="v", view_version=1, entity_columns=["id"], data_ts_column="data_ts",
        features={"a": TableFeatureColumn(column="a_col", dtype="int")},
    )
    rows = [{"id": "1", "a_col": 5, "data_ts": _TS.isoformat()}]
    summary = run_table_feature_import(backend=backend, config=config, rows=rows)
    # Not silently lost: the failure is visible in run accounting.
    assert summary.propagation_enqueue_status == "failed"
    assert any("propagation_enqueue_failed" in w for w in summary.warnings)
    assert backend.offline.get(_key(), feature_name="a", feature_version=1)


# --- DWH feature import ------------------------------------------------------

def _dwh_config():
    return DwhFeatureConfig(entity_type="e", view="v", view_version=1, query_name="q")


def _dwh_rows():
    return [{
        "entity_key": {"id": "1"}, "feature_name": "a", "feature_version": 1,
        "value": 5, "data_ts": _TS.isoformat(), "calc_ts": _TS.isoformat(),
    }]


def test_dwh_feature_import_emits_feature_updated():
    backend = _full_backend()
    reader = InMemoryDwhReader({"q": _dwh_rows()})
    run_dwh_feature_import(backend=backend, reader=reader, config=_dwh_config())
    updates = _updates(backend.events)
    assert [u.feature_name for u in updates] == ["a"]
    assert updates[0].source == "dwh_import"


def test_dwh_feature_import_publish_failure_fails_run_clearly():
    backend = _full_backend(publisher=_FailingPublisher())
    reader = InMemoryDwhReader({"q": _dwh_rows()})
    with pytest.raises(ValueError, match="feature_updates publish failed"):
        run_dwh_feature_import(backend=backend, reader=reader, config=_dwh_config())
    # The offline rows are already durable (an idempotent rerun re-emits + completes).
    assert backend.offline.get(_key(), feature_name="a", feature_version=1)
