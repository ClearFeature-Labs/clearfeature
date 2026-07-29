"""Immutable registry bundles + deterministic digests."""

from datetime import UTC, datetime

import pytest

from fintech_feature_platform.fs_core.models import (
    EntityKey,
    FeatureRef,
    FeatureResult,
    FeatureWriteSet,
)
from fintech_feature_platform.fs_core.registry.bundle import (
    InMemoryBundleStore,
    RegistryBundle,
    build_registry_bundle,
    compute_bundle_digest,
)
from fintech_feature_platform.fs_core.registry.loader import build_registry

_NOW = datetime(2026, 1, 10, tzinfo=UTC)


def _reg(features, *, sources=None, registry_version="reg-v1"):
    sources = sources or {
        "src": {"type": "raw_report", "report_type": "r", "ts_field": "report_ts"},
    }
    data = {
        "registry_version": registry_version,
        "entities": {"e": {"key_fields": ["id"]}},
        "sources": sources,
        "feature_views": {
            "v": {"entity": "e", "key_fields": ["id"], "view_version": 1,
                  "owner": "o", "status": "active", "features": features}
        },
    }
    return build_registry(data)


def _udf(name, *, dtype="float", status="active", deps=None):
    body = {"kind": "udf", "feature_version": 1, "udf": f"udf.{name}",
            "dtype": dtype, "status": status}
    if deps is None:
        body["inputs"] = ["src"]
    else:
        body["deps"] = deps
    return body


def _base_registry(**overrides):
    features = {
        "a": _udf("a"),
        "b": _udf("b"),
        "c": _udf("c", deps=[{"feature": "a", "version": 1},
                             {"feature": "b", "version": 1}]),
    }
    features.update(overrides)
    return _reg(features)


# --- digest determinism ------------------------------------------------------

def test_digest_deterministic_across_ordering():
    # Same logical registry, different declaration order (features + deps) -> same digest.
    ordered = _reg({
        "a": _udf("a"), "b": _udf("b"),
        "c": _udf("c", deps=[{"feature": "a", "version": 1},
                             {"feature": "b", "version": 1}]),
    })
    reordered = _reg({
        "c": _udf("c", deps=[{"feature": "b", "version": 1},
                             {"feature": "a", "version": 1}]),
        "b": _udf("b"), "a": _udf("a"),
    })
    assert compute_bundle_digest(ordered) == compute_bundle_digest(reordered)


def test_digest_changes_on_feature_definition_change():
    base = _base_registry()
    changed = _base_registry(a=_udf("a", dtype="int"))  # dtype differs
    assert compute_bundle_digest(base) != compute_bundle_digest(changed)


def test_digest_changes_on_model_digest_change():
    def _reg_with_model(model_digest):
        return _reg({
            "a": _udf("a"),
            "pd": {"kind": "model", "feature_version": 1, "dtype": "float",
                   "status": "active", "deps": [{"feature": "a", "version": 1}],
                   "model": {"uri": "mlflow://m/1", "digest": model_digest,
                             "output_name": "score"}},
        })
    assert compute_bundle_digest(_reg_with_model("sha256:one")) != compute_bundle_digest(
        _reg_with_model("sha256:two")
    )


def test_digest_changes_on_dependency_change():
    base = _base_registry()
    fewer_deps = _base_registry(
        c=_udf("c", deps=[{"feature": "a", "version": 1}])  # dropped b
    )
    assert compute_bundle_digest(base) != compute_bundle_digest(fewer_deps)


def test_digest_changes_on_lifecycle_change():
    # Change the lifecycle of the top feature (nothing depends on it) from live to shadow.
    base = _base_registry()
    shadowed = _base_registry(
        c=_udf("c", status="shadow", deps=[{"feature": "a", "version": 1},
                                           {"feature": "b", "version": 1}])
    )
    assert compute_bundle_digest(base) != compute_bundle_digest(shadowed)


def test_digest_ignores_active_vs_live_alias():
    # active and live are the same lifecycle -> identical digest.
    active = _base_registry(a=_udf("a", status="active"))
    live = _base_registry(a=_udf("a", status="live"))
    assert compute_bundle_digest(active) == compute_bundle_digest(live)


def test_bundle_digest_ignores_volatile_created_at():
    registry = _base_registry()
    b1 = build_registry_bundle(registry, created_at=_NOW)
    b2 = build_registry_bundle(
        registry, created_at=datetime(2027, 6, 1, tzinfo=UTC)
    )
    assert b1.bundle_digest == b2.bundle_digest


# --- bundle store ------------------------------------------------------------

def test_bundle_store_put_get_list():
    store = InMemoryBundleStore()
    bundle = build_registry_bundle(_base_registry(), created_at=_NOW)
    store.put(bundle)
    assert store.get(bundle.bundle_digest) == bundle
    assert store.list() == [bundle]
    assert store.get("sha256:missing") is None


def test_bundle_store_same_digest_same_content_idempotent():
    store = InMemoryBundleStore()
    registry = _base_registry()
    store.put(build_registry_bundle(registry, created_at=_NOW))
    # Re-put with a different created_at but identical definitions -> idempotent.
    store.put(build_registry_bundle(registry, created_at=datetime(2028, 1, 1, tzinfo=UTC)))
    assert len(store.list()) == 1


def test_bundle_store_same_digest_different_content_fails():
    store = InMemoryBundleStore()
    digest = "sha256:same"
    store.put(RegistryBundle(bundle_id="1", bundle_digest=digest, created_at=_NOW,
                             views=("v:v1",), features=("v:v1:a:v1",)))
    with pytest.raises(ValueError, match="immutable"):
        store.put(RegistryBundle(bundle_id="2", bundle_digest=digest, created_at=_NOW,
                                 views=("v:v1",), features=("v:v1:b:v1",)))


# --- lineage-ready bundle_digest on FeatureResult ----------------------------

def test_feature_result_carries_bundle_digest_round_trip():
    result = FeatureResult(
        ref=FeatureRef("a", 1),
        entity_key=EntityKey.from_mapping({"id": "1"}, key_order=["id"]),
        value=1.0, data_ts=_NOW, calc_ts=_NOW, value_hash="sha256:v",
        bundle_digest="sha256:bundle",
    )
    ws = FeatureWriteSet(view="v", view_version=1, entity_key=result.entity_key,
                         results={"a": result}, source_refs={})
    restored = FeatureWriteSet.from_json(ws.to_json())
    assert restored.results["a"].bundle_digest == "sha256:bundle"


def test_feature_result_bundle_digest_defaults_none_backward_compatible():
    # A legacy serialized write set (no bundle_digest) still parses.
    result = FeatureResult(
        ref=FeatureRef("a", 1),
        entity_key=EntityKey.from_mapping({"id": "1"}, key_order=["id"]),
        value=1.0, data_ts=_NOW, calc_ts=_NOW, value_hash="sha256:v",
    )
    ws = FeatureWriteSet(view="v", view_version=1, entity_key=result.entity_key,
                         results={"a": result}, source_refs={})
    payload = ws.to_dict()
    del payload["results"]["a"]["bundle_digest"]  # simulate an old event
    restored = FeatureWriteSet.from_dict(payload)
    assert restored.results["a"].bundle_digest is None
