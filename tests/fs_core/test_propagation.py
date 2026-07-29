"""Reverse-DAG recompute planner + debounce store  (pure)."""

from datetime import UTC, datetime

from fintech_feature_platform.fs_core.events.models import EntityRef, FeatureUpdated
from fintech_feature_platform.fs_core.propagation import (
    DebounceStore,
    features_with_reactive_dependents,
    plan_recompute_wave,
)
from fintech_feature_platform.fs_core.registry.loader import build_registry

_TS = datetime(2026, 1, 10, tzinfo=UTC)


def _registry(deps_by_feature):
    """Build a view where each derived feature declares deps with a chosen policy.

    ``deps_by_feature``: {feature_name: [(dep_feature, policy), ...]}. Base leaf features
    ``a`` and ``b`` read a source; derived features carry the given dep edges.
    """
    features = {
        "a": {"kind": "udf", "feature_version": 1, "udf": "udf.a",
              "dtype": "int", "status": "active", "inputs": ["src"]},
        "b": {"kind": "udf", "feature_version": 1, "udf": "udf.b",
              "dtype": "int", "status": "active", "inputs": ["src"]},
    }
    for name, deps in deps_by_feature.items():
        features[name] = {
            "kind": "udf", "feature_version": 1, "udf": f"udf.{name}",
            "dtype": "int", "status": "active",
            "deps": [{"feature": d, "version": 1, "propagation": p} for d, p in deps],
        }
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


def _update(feature_name, *, key="1", update_id="u1", data_ts=_TS):
    return FeatureUpdated(
        update_id=update_id,
        entity=EntityRef("e", {"id": key}),
        view="v", view_version=1,
        feature_name=feature_name, feature_version=1,
        data_ts=data_ts, calc_ts=data_ts, source="offline_writer", occurred_at=data_ts,
    )


# --- reverse-DAG planner -----------------------------------------------------

def test_lazy_edge_produces_no_candidate():
    registry = _registry({"c": [("a", "lazy")]})
    assert plan_recompute_wave(registry, _update("a")) == []


def test_none_and_scheduled_edges_produce_no_candidate():
    registry = _registry({"c": [("a", "none")], "d": [("a", "scheduled")]})
    assert plan_recompute_wave(registry, _update("a")) == []


def test_reactive_edge_produces_candidate():
    registry = _registry({"c": [("a", "reactive")]})
    candidates = plan_recompute_wave(registry, _update("a"))
    assert len(candidates) == 1
    c = candidates[0]
    assert c.dependent_feature == "c"
    assert c.changed_input_feature == "a"
    assert c.policy == "reactive"


def test_planner_multiple_dependents_deterministic_order():
    # Both z and c depend reactively on a; result must be sorted by dependent name.
    registry = _registry({"z": [("a", "reactive")], "c": [("a", "reactive")]})
    candidates = plan_recompute_wave(registry, _update("a"))
    assert [c.dependent_feature for c in candidates] == ["c", "z"]


def test_planner_ignores_non_matching_input():
    registry = _registry({"c": [("a", "reactive")]})
    # b has no reactive dependents -> no candidates.
    assert plan_recompute_wave(registry, _update("b")) == []


def test_features_with_reactive_dependents():
    registry = _registry({"c": [("a", "reactive"), ("b", "lazy")]})
    view = registry.feature_views[0]
    assert features_with_reactive_dependents(view) == frozenset({"a"})


def test_planner_unknown_view_returns_empty():
    registry = _registry({"c": [("a", "reactive")]})
    event = _update("a")
    other = FeatureUpdated.from_dict({**event.to_dict(), "view": "nope"})
    assert plan_recompute_wave(registry, other) == []


# --- debounce ----------------------------------------------------------------

def test_debounce_same_entity_and_dependent_coalesces_to_one():
    registry = _registry({"c": [("a", "reactive")]})
    store = DebounceStore()
    now = _TS
    for i in range(3):
        for candidate in plan_recompute_wave(registry, _update("a", update_id=f"u{i}")):
            store.observe(candidate, _update("a", update_id=f"u{i}"), now)
    entries = store.drain()
    assert len(entries) == 1
    assert entries[0].count == 3
    assert entries[0].dependent_feature == "c"
    assert set(entries[0].trigger_update_ids) == {"u0", "u1", "u2"}


def test_debounce_different_entity_separate_units():
    registry = _registry({"c": [("a", "reactive")]})
    store = DebounceStore()
    for key in ("1", "2"):
        for candidate in plan_recompute_wave(registry, _update("a", key=key)):
            store.observe(candidate, _update("a", key=key), _TS)
    assert len(store.drain()) == 2


def test_debounce_different_dependent_separate_units():
    registry = _registry({"c": [("a", "reactive")], "z": [("a", "reactive")]})
    store = DebounceStore()
    for candidate in plan_recompute_wave(registry, _update("a")):
        store.observe(candidate, _update("a"), _TS)
    assert len(store.drain()) == 2


def test_debounce_keeps_latest_watermark():
    registry = _registry({"c": [("a", "reactive")]})
    store = DebounceStore()
    older = datetime(2026, 1, 1, tzinfo=UTC)
    newer = datetime(2026, 2, 1, tzinfo=UTC)
    (cand,) = plan_recompute_wave(registry, _update("a"))
    store.observe(cand, _update("a", update_id="old", data_ts=older), _TS)
    store.observe(cand, _update("a", update_id="new", data_ts=newer), _TS)
    (entry,) = store.drain()
    assert entry.latest_update_id == "new"
    assert entry.latest_data_ts == newer
