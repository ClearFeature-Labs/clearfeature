"""Artifact-bound bundle identity.

registry_definition_digest (definitions only) and final_bundle_digest (definitions +
feature_artifact) are distinct. Legacy unbound bundles are byte-identical to before.
"""

from datetime import UTC, datetime

from fintech_feature_platform.fs_core.registry.bundle import (
    RegistryBundle,
    build_registry_bundle,
    compute_bundle_digest,
    compute_final_bundle_digest,
)
from fintech_feature_platform.fs_core.registry.loader import build_registry

_NOW = datetime(2026, 1, 10, tzinfo=UTC)


def _reg():
    return build_registry(
        {
            "registry_version": "reg-v1",
            "entities": {"e": {"key_fields": ["id"]}},
            "sources": {"src": {"type": "raw_report", "report_type": "r", "ts_field": "report_ts"}},
            "feature_views": {
                "v": {
                    "entity": "e", "key_fields": ["id"], "view_version": 1, "owner": "o",
                    "status": "active",
                    "features": {
                        "f": {"kind": "udf", "feature_version": 1, "udf": "udf.f",
                              "dtype": "float", "status": "live", "inputs": ["src"]},
                    },
                }
            },
        }
    )


_ARTIFACT = {"name": "customer_features", "version": "0.1.0",
             "provider": "customer_features.features:build_udfs", "sha256": "sha256:" + "a" * 64}


def test_legacy_unbound_bundle_digest_unchanged():
    reg = _reg()
    bundle = build_registry_bundle(reg, created_at=_NOW)
    assert bundle.bundle_digest == compute_bundle_digest(reg)
    assert bundle.feature_artifact is None
    # Legacy serialization must not gain new keys.
    d = bundle.to_dict()
    assert "feature_artifact" not in d
    assert "registry_definition_digest" not in d
    assert RegistryBundle.from_dict(d) == bundle


def test_artifact_bound_final_digest_is_distinct():
    reg = _reg()
    definition = compute_bundle_digest(reg)
    bundle = build_registry_bundle(reg, created_at=_NOW, feature_artifact=_ARTIFACT)
    assert bundle.registry_definition_digest == definition
    assert bundle.bundle_digest == compute_final_bundle_digest(definition, _ARTIFACT)
    assert bundle.bundle_digest != definition  # final != definition
    assert bundle.feature_artifact == _ARTIFACT
    d = bundle.to_dict()
    assert d["feature_artifact"] == _ARTIFACT
    assert d["registry_definition_digest"] == definition
    assert RegistryBundle.from_dict(d) == bundle


def test_final_digest_changes_with_sha_but_definition_does_not():
    reg = _reg()
    definition = compute_bundle_digest(reg)
    other = {**_ARTIFACT, "sha256": "sha256:" + "b" * 64}
    b1 = build_registry_bundle(reg, created_at=_NOW, feature_artifact=_ARTIFACT)
    b2 = build_registry_bundle(reg, created_at=_NOW, feature_artifact=other)
    assert b1.registry_definition_digest == b2.registry_definition_digest == definition
    assert b1.bundle_digest != b2.bundle_digest  # code changed -> identity changed
