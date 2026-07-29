"""Golden-metric instrumentation across the worker paths."""

from datetime import UTC, datetime
from types import SimpleNamespace

from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.batch_worker import handle_batch_chunk
from fintech_feature_platform.api.offline_writer import handle_feature_offline_write
from fintech_feature_platform.api.online_worker_runner import process_next
from fintech_feature_platform.api.propagation_worker import (
    execute_wave,
    handle_feature_updated,
)
from fintech_feature_platform.api.propagation_worker_runner import (
    PendingBatch,
)
from fintech_feature_platform.api.propagation_worker_runner import (
    process_next as prop_process_next,
)
from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.events.consumer import (
    InMemoryEventConsumer,
    InMemoryMessage,
)
from fintech_feature_platform.fs_core.events.models import (
    BatchChunkRequested,
    BatchItem,
    EntityRef,
    FeatureComputeRequested,
    FeatureOfflineWriteRequested,
    FeatureUpdated,
    ReportDescriptor,
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
from fintech_feature_platform.fs_core.propagation import DebounceStore
from fintech_feature_platform.fs_core.raw.meta_repository import InMemoryMetaRepository
from fintech_feature_platform.fs_core.raw.payload_store import InMemoryPayloadStore
from fintech_feature_platform.fs_core.raw.report_resolver import ReportResolver
from fintech_feature_platform.fs_core.registry.loader import build_registry
from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore
from fintech_feature_platform.fs_core.stores.online import InMemoryOnlineStore

_TS = datetime(2026, 1, 10, tzinfo=UTC)
_ENTITY = {"user_id": "u1", "application_id": "a1"}


def _counters(backend):
    return backend.metrics.snapshot()["counters"]


def _demo_key():
    return EntityKey.from_mapping(_ENTITY, key_order=["user_id", "application_id"])


# --- offline writer ----------------------------------------------------------

def test_offline_writer_records_append_rows():
    backend = build_memory_backend()
    result = FeatureResult(
        ref=FeatureRef("declared_income", 1), entity_key=_demo_key(), value=5000,
        data_ts=_TS, calc_ts=_TS, value_hash=value_hash(5000),
    )
    ws = FeatureWriteSet(view="user_credit_risk", view_version=1, entity_key=_demo_key(),
                         results={"declared_income": result}, source_refs={})
    event = FeatureOfflineWriteRequested(
        request_id="r", job_id="j", correlation_id="c", occurred_at=_TS, write_set=ws
    )
    handle_feature_offline_write(backend, event)
    assert _counters(backend)["offline_append_rows_total"] == 1


# --- batch worker ------------------------------------------------------------

def test_batch_worker_records_chunk_and_item_metrics():
    backend = build_memory_backend()
    event = BatchChunkRequested(
        batch_job_id="bj", chunk_id="ck", chunk_index=0, chunk_count=1,
        correlation_id="c", occurred_at=_TS, view="user_credit_risk", view_version=1,
        items=[BatchItem(
            entity_type="application", entity_key=dict(_ENTITY),
            inline_sources={"credit_report": {
                "report_type": "credit_report", "report_ts": _TS.isoformat(),
                "payload": {"declared_income": 5000, "monthly_obligations": 800},
            }},
        )],
        requested_features=["declared_income"], write_online=False,
    )
    result = handle_batch_chunk(backend, event)
    assert result.status == "ok"
    counters = _counters(backend)
    assert counters["batch_chunks_total{status=completed}"] == 1
    assert counters["batch_items_total{outcome=ok}"] == 1
    assert counters["batch_rows_written_total"] >= 1


# --- online worker (via runner) ----------------------------------------------

def _online_event():
    backend = build_memory_backend()
    backend.payloads.put("mem://rep", {"declared_income": 5000, "monthly_obligations": 800})
    descriptor = ReportDescriptor(
        report_ref="rep", source_name="credit_report", report_type="credit_report",
        schema_version="v1", report_ts=_TS, object_key="mem://rep",
        content_hash="sha256:x", size_bytes=10, compression="none", format="json",
    )
    event = FeatureComputeRequested(
        request_id="freq", job_id="job", priority="online", deadline_ms=1000,
        entity=EntityRef("application", dict(_ENTITY)), view="user_credit_risk",
        view_version=1, reports=[descriptor], write_policy="online_first",
        idempotency_key="idem", correlation_id="corr", occurred_at=_TS,
        requested_features=["declared_income"],
    )
    return backend, event


def test_online_worker_records_request_metrics():
    backend, event = _online_event()
    consumer = InMemoryEventConsumer([InMemoryMessage(event.to_json())])
    process_next(consumer, backend)
    counters = _counters(backend)
    assert counters["online_requests_total{outcome=ok}"] == 1
    hist = backend.metrics.snapshot()["histograms"]
    assert "online_request_latency_ms" in hist


# --- propagation worker + wave -----------------------------------------------

def _reactive_backend():
    registry = build_registry({
        "registry_version": "t",
        "entities": {"e": {"key_fields": ["id"]}},
        "sources": {"src": {"type": "raw_report", "report_type": "r",
                            "ts_field": "report_ts"}},
        "feature_views": {"v": {
            "entity": "e", "key_fields": ["id"], "view_version": 1, "owner": "o",
            "status": "active", "features": {
                "a": {"kind": "udf", "feature_version": 1, "udf": "udf.a",
                      "dtype": "int", "status": "active", "inputs": ["src"]},
                "b": {"kind": "udf", "feature_version": 1, "udf": "udf.b",
                      "dtype": "int", "status": "active", "inputs": ["src"]},
                "c": {"kind": "udf", "feature_version": 1, "udf": "udf.c",
                      "dtype": "int", "status": "active", "deps": [
                          {"feature": "a", "version": 1, "propagation": "reactive"},
                          {"feature": "b", "version": 1, "propagation": "reactive"}]},
            }}},
    })
    udfs = UdfRegistry({
        "udf.a": lambda s, d: s["src"]["v"], "udf.b": lambda s, d: s["src"]["v"],
        "udf.c": lambda s, d: d["a"] + d["b"],
    })
    offline = InMemoryOfflineStore()
    online = InMemoryOnlineStore()
    store = FeatureStore(registry, udfs, ReportResolver(InMemoryPayloadStore(),
                         InMemoryMetaRepository()), offline, online)
    return SimpleNamespace(registry=registry, store=store, offline=offline, online=online,
                           events=InMemoryEventPublisher(),
                           metrics=InMemoryMetricsRecorder())


def _seed(offline, name, value, id_="1"):
    offline.append("v", 1, FeatureResult(
        ref=FeatureRef(name, 1), entity_key=EntityKey.from_mapping({"id": id_}, ["id"]),
        value=value, data_ts=_TS, calc_ts=_TS, max_input_data_ts=_TS,
        input_fingerprint=f"fp{name}", value_hash=value_hash(value),
    ))


def _update(feature):
    return FeatureUpdated(
        update_id="u1", entity=EntityRef("e", {"id": "1"}), view="v", view_version=1,
        feature_name=feature, feature_version=1, data_ts=_TS, calc_ts=_TS,
        source="offline_writer", occurred_at=_TS,
    )


def test_propagation_records_update_debounce_and_wave_metrics():
    backend = _reactive_backend()
    _seed(backend.offline, "a", 3)
    _seed(backend.offline, "b", 4)
    debounce = DebounceStore()
    handle_feature_updated(backend, debounce, _update("a"))
    handle_feature_updated(backend, debounce, _update("a"))  # coalesces -> debounced
    execute_wave(backend, debounce, calc_ts=datetime(2026, 1, 12, tzinfo=UTC))

    counters = _counters(backend)
    assert counters["feature_updates_total{source=offline_writer}"] == 2
    assert counters["propagation_debounced_total"] == 1
    assert counters["propagation_waves_total"] == 1
    assert counters["propagation_wave_items_total{outcome=computed}"] == 1
    gauges = backend.metrics.snapshot()["gauges"]
    assert "propagation_pending_waves" in gauges
    hist = backend.metrics.snapshot()["histograms"]
    assert "propagation_lag_seconds" in hist


def test_propagation_runner_records_dlq_metric():
    backend = _reactive_backend()
    consumer = InMemoryEventConsumer([InMemoryMessage(b"not-json")])
    prop_process_next(consumer, backend, DebounceStore(), PendingBatch())
    counters = _counters(backend)
    assert counters["dlq_events_total{stage=propagation_worker,status=deserialization_failed}"] == 1
