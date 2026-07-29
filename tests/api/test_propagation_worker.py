"""Recompute-wave execution (offline-only child run)."""

import json
from datetime import UTC, datetime
from types import SimpleNamespace

from fintech_feature_platform.api.propagation_worker import (
    execute_wave,
    handle_feature_updated,
)
from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.events.models import EntityRef, FeatureUpdated
from fintech_feature_platform.fs_core.events.publisher import InMemoryEventPublisher
from fintech_feature_platform.fs_core.feature_store import FeatureStore
from fintech_feature_platform.fs_core.hashing import value_hash
from fintech_feature_platform.fs_core.models import (
    EntityKey,
    FeatureRef,
    FeatureResult,
)
from fintech_feature_platform.fs_core.propagation import DebounceStore
from fintech_feature_platform.fs_core.raw.meta_repository import InMemoryMetaRepository
from fintech_feature_platform.fs_core.raw.payload_store import InMemoryPayloadStore
from fintech_feature_platform.fs_core.raw.report_resolver import ReportResolver
from fintech_feature_platform.fs_core.registry.loader import build_registry
from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore
from fintech_feature_platform.fs_core.stores.online import InMemoryOnlineStore

_SEED_TS = datetime(2026, 1, 10, tzinfo=UTC)
_WAVE_TS = datetime(2026, 1, 12, tzinfo=UTC)


def _registry(include_e2=False):
    # a, b: leaf features (source). c = a + b (reactive). d = c * 10 (reactive on c).
    features = {
        "a": {"kind": "udf", "feature_version": 1, "udf": "udf.a",
              "dtype": "int", "status": "active", "inputs": ["src"]},
        "b": {"kind": "udf", "feature_version": 1, "udf": "udf.b",
              "dtype": "int", "status": "active", "inputs": ["src"]},
        "c": {"kind": "udf", "feature_version": 1, "udf": "udf.c",
              "dtype": "int", "status": "active", "deps": [
                  {"feature": "a", "version": 1, "propagation": "reactive"},
                  {"feature": "b", "version": 1, "propagation": "reactive"}]},
        "d": {"kind": "udf", "feature_version": 1, "udf": "udf.d",
              "dtype": "int", "status": "active", "deps": [
                  {"feature": "c", "version": 1, "propagation": "reactive"}]},
    }
    if include_e2:
        # e2 reads a raw source AND a dep -> not recomputable in a wave (deterministic fail).
        features["e2"] = {
            "kind": "udf", "feature_version": 1, "udf": "udf.e2",
            "dtype": "int", "status": "active", "inputs": ["src"], "deps": [
                {"feature": "a", "version": 1, "propagation": "reactive"}]}
    data = {
        "registry_version": "test-v1",
        "entities": {"e": {"key_fields": ["id"]}},
        "sources": {
            "src": {"type": "raw_report", "report_type": "r", "ts_field": "report_ts"},
        },
        "feature_views": {
            "v": {"entity": "e", "key_fields": ["id"], "view_version": 1,
                  "owner": "o", "status": "active", "features": features}
        },
    }
    return build_registry(data)


def _backend(include_e2=False):
    registry = _registry(include_e2)
    udfs = UdfRegistry({
        "udf.a": lambda s, d: s["src"]["v"],
        "udf.b": lambda s, d: s["src"]["v"],
        "udf.c": lambda s, d: d["a"] + d["b"],
        "udf.d": lambda s, d: d["c"] * 10,
        "udf.e2": lambda s, d: s["src"]["v"] + d["a"],
    })
    payloads = InMemoryPayloadStore()
    metas = InMemoryMetaRepository()
    resolver = ReportResolver(payloads, metas)
    online = InMemoryOnlineStore()
    offline = InMemoryOfflineStore()
    store = FeatureStore(registry, udfs, resolver, offline, online)
    return SimpleNamespace(
        registry=registry, store=store, offline=offline, online=online,
        events=InMemoryEventPublisher(),
    )


def _key(id_="1"):
    return EntityKey.from_mapping({"id": id_}, key_order=["id"])


def _seed(offline, name, value, id_="1"):
    result = FeatureResult(
        ref=FeatureRef(name, 1), entity_key=_key(id_), value=value,
        data_ts=_SEED_TS, calc_ts=_SEED_TS, max_input_data_ts=_SEED_TS,
        input_fingerprint=f"fp_{name}_{id_}", value_hash=value_hash(value),
    )
    offline.append("v", 1, result)


def _update(feature_name, *, id_="1", update_id="u1"):
    return FeatureUpdated(
        update_id=update_id, entity=EntityRef("e", {"id": id_}),
        view="v", view_version=1, feature_name=feature_name, feature_version=1,
        data_ts=_SEED_TS, calc_ts=_SEED_TS, source="offline_writer", occurred_at=_SEED_TS,
    )


def _observe(backend, debounce, event):
    handle_feature_updated(backend, debounce, event)


def test_wave_reads_offline_inputs_computes_and_appends():
    backend = _backend()
    _seed(backend.offline, "a", 3)
    _seed(backend.offline, "b", 4)
    debounce = DebounceStore()
    _observe(backend, debounce, _update("a"))

    wave = execute_wave(backend, debounce, calc_ts=_WAVE_TS)

    assert wave.computed == 1
    assert wave.planned == 1
    # c = a + b = 7 durably appended to offline.
    records = backend.offline.get(_key(), feature_name="c", feature_version=1)
    assert [r.result.value for r in records] == [7]
    # D3/D9 preserved: derived data_ts = min(inputs), max = max(inputs).
    assert records[0].result.data_ts == _SEED_TS
    assert records[0].result.max_input_data_ts == _SEED_TS
    assert records[0].result.input_fingerprint is not None


def test_wave_emits_downstream_update_when_dependent_has_reactive_dependents():
    backend = _backend()
    _seed(backend.offline, "a", 3)
    _seed(backend.offline, "b", 4)
    debounce = DebounceStore()
    _observe(backend, debounce, _update("a"))

    wave = execute_wave(backend, debounce, calc_ts=_WAVE_TS)

    # c has reactive dependent d -> one downstream FeatureUpdated for c is published.
    assert wave.downstream_emitted == 1
    updates = [
        r.event for r in backend.events.published
        if getattr(r.event, "event_type", "") == "feature_updated"
    ]
    assert [u.feature_name for u in updates] == ["c"]
    assert updates[0].source == "recompute_wave"


def test_wave_is_offline_only_no_online_write():
    backend = _backend()
    _seed(backend.offline, "a", 3)
    _seed(backend.offline, "b", 4)
    debounce = DebounceStore()
    _observe(backend, debounce, _update("a"))

    execute_wave(backend, debounce, calc_ts=_WAVE_TS)

    # Offline-only by default: no online latest for the recomputed dependent.
    assert backend.online.get("v", 1, _key(), "c", 1) is None


def test_wave_missing_required_input_is_skipped_others_continue():
    backend = _backend()
    # entity 1 has both inputs; entity 2 is missing b -> c skipped for entity 2.
    _seed(backend.offline, "a", 3, id_="1")
    _seed(backend.offline, "b", 4, id_="1")
    _seed(backend.offline, "a", 5, id_="2")
    debounce = DebounceStore()
    _observe(backend, debounce, _update("a", id_="1", update_id="u1"))
    _observe(backend, debounce, _update("a", id_="2", update_id="u2"))

    wave = execute_wave(backend, debounce, calc_ts=_WAVE_TS)

    assert wave.computed == 1  # entity 1
    assert wave.skipped == 1   # entity 2 (missing b)
    assert wave.entity_count == 2
    # entity 1 still produced its result despite entity 2 skipping.
    assert backend.offline.get(_key("1"), feature_name="c", feature_version=1)


def test_wave_raw_source_dependent_is_deterministic_failure_others_continue():
    backend = _backend(include_e2=True)
    _seed(backend.offline, "a", 3)
    debounce = DebounceStore()
    # e2 depends reactively on a but also reads a raw source -> wave cannot compute it.
    _observe(backend, debounce, _update("a"))

    wave = execute_wave(backend, debounce, calc_ts=_WAVE_TS)

    # c is skipped (b missing), e2 fails deterministically (needs a raw source). Neither
    # halts the wave; both are counted.
    assert wave.failed == 1  # e2
    assert wave.skipped == 1  # c (b missing)
    assert wave.status == "completed_with_errors"


def test_wave_accounting_is_counts_only_no_values():
    backend = _backend()
    _seed(backend.offline, "a", 3)
    _seed(backend.offline, "b", 4)
    debounce = DebounceStore()
    _observe(backend, debounce, _update("a"))

    wave = execute_wave(backend, debounce, calc_ts=_WAVE_TS)

    counts = wave.counts()
    assert set(counts) == {
        "planned", "computed", "skipped", "failed", "debounced", "downstream_emitted",
    }
    assert all(isinstance(v, int) for v in counts.values())
    # The wave object exposes refs + counts, never a feature value or payload.
    blob = json.dumps({
        "wave_id": wave.wave_id, "counts": counts,
        "dependent_feature_refs": list(wave.dependent_feature_refs),
        "trigger_update_ids": list(wave.trigger_update_ids),
    })
    for forbidden in ("value", "payload", "object_key", "storage_uri", "sql"):
        assert forbidden not in blob
