"""Promotion identity for artifact-bound bundles  (§12).

The same registry definitions with two different wheels are two separately promotable
releases (same registry_definition_digest, different final_bundle_digest), and rollback
restores the exact previous final bundle and its feature artifact.
"""

from datetime import UTC, datetime

from fintech_feature_platform.fs_core.registry.bundle import (
    FileBundleStore,
    build_registry_bundle,
    compute_bundle_digest,
)
from fintech_feature_platform.fs_core.registry.loader import build_registry
from fintech_feature_platform.fs_core.registry.promotion import (
    FilePointerStore,
    promote,
    rollback,
)

_NOW = datetime(2026, 1, 10, tzinfo=UTC)


def _registry():
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


def _artifact(sha_char):
    sha = "sha256:" + sha_char * 64
    return {
        "name": "customer-features", "version": "0.1.0",
        "provider": "customer_features.features:build_udfs", "sha256": sha,
        "filename": "customer_features-0.1.0-py3-none-any.whl",
        "storage_key": (
            f"customer-features/0.1.0/{sha_char * 64}/customer_features-0.1.0-py3-none-any.whl"
        ),
    }


def test_same_registry_two_wheels_are_separate_releases(tmp_path):
    reg = _registry()
    definition = compute_bundle_digest(reg)
    a = build_registry_bundle(reg, created_at=_NOW, feature_artifact=_artifact("a"))
    b = build_registry_bundle(reg, created_at=_NOW, feature_artifact=_artifact("b"))

    # Same definitions, different executable artifact -> different promotable identity.
    assert a.registry_definition_digest == b.registry_definition_digest == definition
    assert a.bundle_digest != b.bundle_digest

    store = FileBundleStore(str(tmp_path / "bundles"))
    store.put(a)
    store.put(b)
    assert store.get(a.bundle_digest).feature_artifact["sha256"].endswith("a" * 8)
    assert store.get(b.bundle_digest).feature_artifact["sha256"].endswith("b" * 8)


def test_rollback_restores_exact_final_bundle_and_artifact(tmp_path):
    reg = _registry()
    a = build_registry_bundle(reg, created_at=_NOW, feature_artifact=_artifact("a"))
    b = build_registry_bundle(reg, created_at=_NOW, feature_artifact=_artifact("b"))
    bundles = FileBundleStore(str(tmp_path / "bundles"))
    bundles.put(a)
    bundles.put(b)
    pointers = FilePointerStore(str(tmp_path / "pointers"))

    # Promote A then B on the shadow pointer (identity uses final_bundle_digest).
    promote(bundle_exists=True, pointer_store=pointers, bundle_digest=a.bundle_digest,
            env="prod", stage="shadow", actor="ds", reason="A", now=_NOW)
    promote(bundle_exists=True, pointer_store=pointers, bundle_digest=b.bundle_digest,
            env="prod", stage="shadow", actor="ds", reason="B", now=_NOW)
    assert pointers.get_pointer("prod", "shadow").bundle_digest == b.bundle_digest

    # Rollback restores A's exact final bundle digest and its feature artifact.
    rollback(pointer_store=pointers, env="prod", actor="ops", reason="revert",
             now=_NOW, stage="shadow", to_previous=True)
    restored_digest = pointers.get_pointer("prod", "shadow").bundle_digest
    assert restored_digest == a.bundle_digest
    assert bundles.get(restored_digest).feature_artifact == a.feature_artifact
