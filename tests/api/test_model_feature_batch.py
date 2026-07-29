"""Batch-only F3 model-as-feature compute.

Covers PIT-safe input reads, vector-first single-batch model call, D3/D9 + lineage on the
output, offline write, downstream emission, golden 1-row-vs-N-row consistency, digest/URI
failures, and recompute-wave integration (compute with runner, defer without).
"""

import json
from datetime import UTC, datetime

import pytest

from fintech_feature_platform.api.backend import AppBackend
from fintech_feature_platform.api.model_feature_batch import compute_model_feature_batch
from fintech_feature_platform.api.propagation_worker import (
    execute_wave,
    handle_feature_updated,
)
from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.events.models import EntityRef, FeatureUpdated
from fintech_feature_platform.fs_core.events.publisher import InMemoryEventPublisher
from fintech_feature_platform.fs_core.feature_store import FeatureStore
from fintech_feature_platform.fs_core.hashing import value_hash
from fintech_feature_platform.fs_core.model_runner import FakeModelRunner
from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult
from fintech_feature_platform.fs_core.observability.metrics import InMemoryMetricsRecorder
from fintech_feature_platform.fs_core.propagation import DebounceStore
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

_INCOME_TS = datetime(2026, 1, 10, tzinfo=UTC)
_DEBT_TS = datetime(2026, 1, 5, tzinfo=UTC)
_OBS_TS = datetime(2026, 1, 12, tzinfo=UTC)
_DIGEST = "sha256:pdmodel"


def _registry(uri="mlflow://pd_model/17"):
    data = {
        "registry_version": "test-v1",
        "entities": {"e": {"key_fields": ["id"]}},
        "sources": {
            "src": {"type": "raw_report", "report_type": "r", "ts_field": "report_ts"},
        },
        "feature_views": {
            "v": {"entity": "e", "key_fields": ["id"], "view_version": 1,
                  "owner": "o", "status": "active", "features": {
                      "income": {"kind": "udf", "feature_version": 1, "udf": "udf.income",
                                 "dtype": "float", "status": "active", "inputs": ["src"]},
                      "debt": {"kind": "udf", "feature_version": 1, "udf": "udf.debt",
                               "dtype": "float", "status": "active", "inputs": ["src"]},
                      "pd_score": {"kind": "model", "feature_version": 1, "dtype": "float",
                                   "status": "active", "deps": [
                                       {"feature": "income", "version": 1,
                                        "propagation": "reactive"},
                                       {"feature": "debt", "version": 1,
                                        "propagation": "reactive"}],
                                   "model": {"uri": uri, "digest": _DIGEST,
                                             "output_name": "score"}},
                      # reactive dependent of pd_score -> F3 has downstream to emit.
                      "risk_band": {"kind": "udf", "feature_version": 1, "udf": "udf.band",
                                    "dtype": "float", "status": "active", "deps": [
                                        {"feature": "pd_score", "version": 1,
                                         "propagation": "reactive"}]},
                  }}
        },
    }
    return build_registry(data)


def _backend(publisher=None, uri="mlflow://pd_model/17"):
    registry = _registry(uri)
    udfs = UdfRegistry({
        "udf.income": lambda s, d: s["src"]["income"],
        "udf.debt": lambda s, d: s["src"]["debt"],
        "udf.band": lambda s, d: d["pd_score"],
    })
    payloads = InMemoryPayloadStore()
    metas = InMemoryMetaRepository()
    resolver = ReportResolver(payloads, metas)
    online = InMemoryOnlineStore()
    offline = InMemoryOfflineStore()
    store = FeatureStore(registry, udfs, resolver, offline, online)
    return AppBackend(
        registry=registry, store=store, payloads=payloads, metas=metas,
        online=online, offline=offline,
        make_storage_uri=lambda rt, ref, ts: ref, raw_format="json",
        raw_compression="none", serialize_payload=lambda p: b"{}",
        make_feature_store=lambda r: FeatureStore(registry, udfs, r, offline, online),
        events=publisher or InMemoryEventPublisher(),
        status=InMemoryRequestStatusStore(), results=InMemoryRequestResultStore(),
        metadata=InMemoryMetadataStore(), batch_status=InMemoryBatchJobStatusStore(),
        batch_meta=InMemoryBatchMetadataStore(),
        source_datasets=InMemorySourceDatasetStore(),
        metrics=InMemoryMetricsRecorder(),
    )


def _key(id_="1"):
    return EntityKey.from_mapping({"id": id_}, key_order=["id"])


def _seed(offline, name, value, ts, id_="1"):
    offline.append("v", 1, FeatureResult(
        ref=FeatureRef(name, 1), entity_key=_key(id_), value=value,
        data_ts=ts, calc_ts=ts, max_input_data_ts=ts,
        input_fingerprint=f"fp_{name}_{id_}", value_hash=value_hash(value),
    ))


def _seed_entity(offline, id_, income=100, debt=40):
    _seed(offline, "income", income, _INCOME_TS, id_)
    _seed(offline, "debt", debt, _DEBT_TS, id_)


def _compute(backend, runner, entity_keys, **kw):
    return compute_model_feature_batch(
        backend, runner, view="v", view_version=1, feature_name="pd_score",
        entity_keys=entity_keys, observation_ts=_OBS_TS, **kw,
    )


# --- PIT read + vector-first + offline write ---------------------------------

def test_reads_offline_inputs_pit_safe_and_writes_offline():
    backend = _backend()
    _seed_entity(backend.offline, "1", income=100, debt=40)
    runner = FakeModelRunner(expected_digest=_DIGEST)

    result = _compute(backend, runner, [_key("1")])

    assert result.status == "ok"
    assert result.computed == 1
    records = backend.offline.get(_key("1"), feature_name="pd_score", feature_version=1)
    assert [r.result.value for r in records] == [140.0]  # income + debt


def test_missing_required_input_skips_and_model_not_called_for_row():
    backend = _backend()
    _seed_entity(backend.offline, "1")           # complete
    _seed(backend.offline, "income", 50, _INCOME_TS, "2")  # entity 2 missing debt
    runner = FakeModelRunner(expected_digest=_DIGEST)

    result = _compute(backend, runner, [_key("1"), _key("2")])

    assert result.computed == 1
    assert result.skipped == 1
    assert runner.calls == [1]  # model called once, with only the complete row


def test_model_called_once_with_batch_not_per_item():
    backend = _backend()
    for id_ in ("1", "2", "3"):
        _seed_entity(backend.offline, id_)
    runner = FakeModelRunner(expected_digest=_DIGEST)

    result = _compute(backend, runner, [_key("1"), _key("2"), _key("3")])

    assert result.computed == 3
    assert runner.calls == [3]  # single vector call for the whole batch


# --- D3/D9 + lineage ---------------------------------------------------------

def test_output_has_d3_d9_metadata():
    backend = _backend()
    _seed_entity(backend.offline, "1")
    result = _compute(backend, _backend_runner(), [_key("1")])
    assert result.computed == 1
    row = backend.offline.get(_key("1"), feature_name="pd_score", feature_version=1)[0].result
    assert row.data_ts == _DEBT_TS           # min(income Jan10, debt Jan5)
    assert row.max_input_data_ts == _INCOME_TS  # max
    assert row.input_fingerprint is not None
    assert row.value_hash == value_hash(140.0)


def test_output_records_model_lineage():
    backend = _backend()
    _seed_entity(backend.offline, "1")
    result = _compute(backend, _backend_runner(), [_key("1")])
    row = backend.offline.get(_key("1"), feature_name="pd_score", feature_version=1)[0].result
    assert row.model_uri == "mlflow://pd_model/17"
    assert row.model_digest == _DIGEST
    assert row.model_output_name == "score"
    assert result.model_uri == "mlflow://pd_model/17"
    assert result.model_digest == _DIGEST
    assert set(result.input_feature_refs) == {"income:v1", "debt:v1"}


def test_status_and_events_are_values_free():
    backend = _backend()
    _seed_entity(backend.offline, "1")
    result = _compute(backend, _backend_runner(), [_key("1")])
    updates = [r.event for r in backend.events.published
               if getattr(r.event, "event_type", "") == "feature_updated"]
    blob = json.dumps({"result_counts": result.counts(),
                       "refs": list(result.input_feature_refs),
                       "model_uri": result.model_uri, "model_digest": result.model_digest})
    blob += "".join(json.dumps(u.to_dict()) for u in updates)
    for forbidden in ("payload", "object_key", "storage_uri", "\"value\"",
                      "artifact", "SQL"):
        assert forbidden not in blob


# --- downstream emission -----------------------------------------------------

def test_emits_feature_updated_when_f3_has_reactive_dependents():
    backend = _backend()
    _seed_entity(backend.offline, "1")
    result = _compute(backend, _backend_runner(), [_key("1")])
    assert result.downstream_emitted == 1
    updates = [r.event for r in backend.events.published
               if getattr(r.event, "event_type", "") == "feature_updated"]
    assert [u.feature_name for u in updates] == ["pd_score"]
    assert updates[0].source == "batch_worker"


# --- golden consistency ------------------------------------------------------

def test_golden_one_row_matches_row_in_multi_row_batch():
    # Same model + same inputs: entity 1's output must be identical whether scored alone
    # or inside a 3-entity batch (protects the vector-first contract).
    single = _backend()
    _seed_entity(single.offline, "1", income=100, debt=40)
    _compute(single, _backend_runner(), [_key("1")])
    single_value = single.offline.get(
        _key("1"), feature_name="pd_score", feature_version=1)[0].result.value

    multi = _backend()
    _seed_entity(multi.offline, "1", income=100, debt=40)
    _seed_entity(multi.offline, "2", income=200, debt=10)
    _seed_entity(multi.offline, "3", income=50, debt=5)
    _compute(multi, _backend_runner(), [_key("1"), _key("2"), _key("3")])
    multi_value = multi.offline.get(
        _key("1"), feature_name="pd_score", feature_version=1)[0].result.value

    assert single_value == multi_value == 140.0


# --- failure modes -----------------------------------------------------------

def test_wrong_model_digest_fails_clearly():
    backend = _backend()
    _seed_entity(backend.offline, "1")
    runner = FakeModelRunner(expected_digest="sha256:different")
    with pytest.raises(ValueError, match="digest mismatch"):
        _compute(backend, runner, [_key("1")])


def test_unsupported_model_uri_fails_clearly():
    backend = _backend(uri="http://pd_model/17")
    _seed_entity(backend.offline, "1")
    with pytest.raises(ValueError, match="unsupported model uri"):
        _compute(backend, _backend_runner(), [_key("1")])


# --- recompute-wave integration ----------------------------------------------

def _update_income(id_="1", update_id="u1"):
    return FeatureUpdated(
        update_id=update_id, entity=EntityRef("e", {"id": id_}),
        view="v", view_version=1, feature_name="income", feature_version=1,
        data_ts=_INCOME_TS, calc_ts=_INCOME_TS, source="offline_writer",
        occurred_at=_INCOME_TS,
    )


def test_wave_computes_f3_when_model_runner_provided():
    backend = _backend()
    _seed_entity(backend.offline, "1")
    debounce = DebounceStore()
    handle_feature_updated(backend, debounce, _update_income())  # plans pd_score (reactive)

    wave = execute_wave(backend, debounce, calc_ts=_OBS_TS,
                        model_runner=_backend_runner())

    assert wave.computed == 1
    assert backend.offline.get(_key("1"), feature_name="pd_score", feature_version=1)


def test_wave_defers_f3_without_model_runner():
    backend = _backend()
    _seed_entity(backend.offline, "1")
    debounce = DebounceStore()
    handle_feature_updated(backend, debounce, _update_income())

    wave = execute_wave(backend, debounce, calc_ts=_OBS_TS)  # no model_runner

    assert wave.computed == 0
    assert wave.skipped == 1  # F3 deferred clearly, not computed without its model
    assert backend.offline.get(_key("1"), feature_name="pd_score", feature_version=1) == []


def _backend_runner():
    return FakeModelRunner(expected_digest=_DIGEST)
