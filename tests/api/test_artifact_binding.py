"""Runtime artifact-binding gate + binding policy.

Exercises the shared seam (`build_registry_and_udfs` / `build_backend`) that every
feature-computation role passes through: the binding policy matrix, the production-like
startup guard, fail-closed verification of a bound promoted bundle, and positive online +
batch computation through the verified backend.
"""

import importlib
import json
import sys
from datetime import UTC, datetime

import pytest

from fintech_feature_platform.api.artifact_binding import enforce_artifact_binding
from fintech_feature_platform.api.backend import build_backend, build_registry_and_udfs
from fintech_feature_platform.api.settings import load_settings
from fintech_feature_platform.cli.artifact import (
    artifact_storage_key,
    build_wheel,
    install_wheel,
    sha256_file,
)
from fintech_feature_platform.cli.scaffold import write_scaffold
from fintech_feature_platform.fs_core.models import EntityKey, SourceStamp
from fintech_feature_platform.fs_core.registry.artifact import canonicalize_name
from fintech_feature_platform.fs_core.registry.bundle import (
    FileBundleStore,
    build_registry_bundle,
)
from fintech_feature_platform.fs_core.registry.loader import load_registry_file
from fintech_feature_platform.fs_core.registry.promotion import (
    EnvironmentPointer,
    FilePointerStore,
)
from fintech_feature_platform.fs_core.runtime import artifact_verifier as av

_NOW = datetime(2026, 1, 10, tzinfo=UTC)
CORE = importlib.metadata.version("fintech-feature-platform")


def _install_and_promote(tmp_path, monkeypatch, *, requires_core=">=0.1,<0.2", bind=True):
    """Build+install a wheel, promote a bound bundle, write runtime evidence, set env."""
    root = tmp_path / "proj"
    write_scaffold(root, "customer-features", "customer_features")
    built = build_wheel(root, tmp_path / "dist")
    sha = sha256_file(built)
    runtime_wheel = tmp_path / "runtime" / built.name
    runtime_wheel.parent.mkdir(parents=True)
    runtime_wheel.write_bytes(built.read_bytes())
    target = tmp_path / "site"
    install_wheel(runtime_wheel, target)
    monkeypatch.syspath_prepend(str(target))
    for mod in [m for m in sys.modules if m.startswith("customer_features")]:
        del sys.modules[mod]
    importlib.invalidate_caches()

    reg_path = target / "customer_features" / "registry" / "features_v1.yaml"
    registry = load_registry_file(str(reg_path))
    name, version = "customer-features", "0.1.0"
    provider, filename = "customer_features.features:build_udfs", built.name
    fa = None
    if bind:
        fa = {
            "name": name, "version": version, "provider": provider, "sha256": sha,
            "filename": filename,
            "storage_key": artifact_storage_key(canonicalize_name(name), version, sha, filename),
        }
        if requires_core:
            fa["requires_core"] = requires_core
    bundle = build_registry_bundle(registry, created_at=_NOW, feature_artifact=fa)

    bundle_store = tmp_path / "bundles"
    pointer_store = tmp_path / "pointers"
    FileBundleStore(str(bundle_store)).put(bundle)
    FilePointerStore(str(pointer_store)).set_pointer(
        EnvironmentPointer(env="prod", stage="live", bundle_digest=bundle.bundle_digest,
                           updated_at=_NOW)
    )
    manifest = {
        "format_version": 1, "name": name, "version": version, "provider": provider,
        "filename": filename, "sha256": sha, "wheel_path": str(runtime_wheel),
    }
    manifest_path = tmp_path / "feature-artifact.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    monkeypatch.setenv("FSP_REGISTRY_PATH", str(reg_path))
    monkeypatch.setenv("FSP_UDF_PROVIDER", provider)
    monkeypatch.setenv("FSP_BUNDLE_STORE", str(bundle_store))
    monkeypatch.setenv("FSP_POINTER_STORE", str(pointer_store))
    monkeypatch.setenv("FSP_BUNDLE_ENV", "prod")
    monkeypatch.setenv("FSP_BUNDLE_STAGE", "live")
    monkeypatch.setenv("FSP_FEATURE_ARTIFACT_MANIFEST", str(manifest_path))
    monkeypatch.setenv("FSP_ARTIFACT_BINDING", "required")
    return {"bundle": bundle, "registry": registry, "manifest_path": manifest_path,
            "runtime_wheel": runtime_wheel, "target": target, "provider": provider}


# --- policy matrix ----------------------------------------------------------

def test_unbound_legacy_development_allowed(monkeypatch):
    # Default env: no bundle configured, legacy-compatible, development -> passthrough.
    monkeypatch.delenv("FSP_BUNDLE_STORE", raising=False)
    monkeypatch.setenv("FSP_ARTIFACT_BINDING", "legacy-compatible")
    registry, udfs = build_registry_and_udfs(load_settings())
    assert registry is not None  # served the built-in demo, no binding


def test_unbound_required_rejected(monkeypatch):
    monkeypatch.delenv("FSP_BUNDLE_STORE", raising=False)
    monkeypatch.setenv("FSP_ARTIFACT_BINDING", "required")
    with pytest.raises(av.ArtifactVerificationError) as e:
        build_registry_and_udfs(load_settings())
    assert e.value.category == av.CAT_REQUIRED


def test_production_like_legacy_startup_rejected(monkeypatch):
    monkeypatch.setenv("FSP_ENVIRONMENT", "production")
    monkeypatch.setenv("FSP_ARTIFACT_BINDING", "legacy-compatible")
    with pytest.raises(av.ArtifactVerificationError) as e:
        build_registry_and_udfs(load_settings())
    assert e.value.category == av.CAT_REQUIRED


def test_production_like_required_bound_accepted(tmp_path, monkeypatch):
    _install_and_promote(tmp_path, monkeypatch)
    monkeypatch.setenv("FSP_ENVIRONMENT", "production")
    registry, udfs = build_registry_and_udfs(load_settings())
    assert registry is not None


# --- bound bundle is always verified ---------------------------------------

def test_bound_bundle_verified_ok(tmp_path, monkeypatch):
    _install_and_promote(tmp_path, monkeypatch)
    registry, udfs = build_registry_and_udfs(load_settings())
    assert registry is not None


def test_bound_mismatch_rejected_even_in_legacy_compatible(tmp_path, monkeypatch):
    ctx = _install_and_promote(tmp_path, monkeypatch)
    monkeypatch.setenv("FSP_ARTIFACT_BINDING", "legacy-compatible")  # still dev
    # Tamper the runtime wheel -> actual SHA differs -> must reject even in compatibility.
    ctx["runtime_wheel"].write_bytes(ctx["runtime_wheel"].read_bytes() + b"tamper")
    with pytest.raises(av.ArtifactVerificationError) as e:
        build_registry_and_udfs(load_settings())
    assert e.value.category == av.CAT_SHA_MISMATCH


def test_served_provider_mismatch_rejected(tmp_path, monkeypatch):
    _install_and_promote(tmp_path, monkeypatch)
    monkeypatch.setenv("FSP_UDF_PROVIDER", "customer_features.features:something_else")
    with pytest.raises(av.ArtifactVerificationError) as e:
        build_registry_and_udfs(load_settings())
    assert e.value.category == av.CAT_PROVIDER_MISMATCH


def test_served_registry_mismatch_rejected(tmp_path, monkeypatch):
    ctx = _install_and_promote(tmp_path, monkeypatch)
    # Point the served registry at a different (demo) registry -> definition digest mismatch.
    other = tmp_path / "other.yaml"
    other.write_text(
        (ctx["target"] / "customer_features" / "registry" / "features_v1.yaml")
        .read_text(encoding="utf-8")
        .replace("event_count_7d", "event_count_14d"),
        encoding="utf-8",
    )
    monkeypatch.setenv("FSP_REGISTRY_PATH", str(other))
    with pytest.raises(av.ArtifactVerificationError) as e:
        build_registry_and_udfs(load_settings())
    assert e.value.category == av.CAT_REGISTRY_MISMATCH


def test_manifest_sha_not_sole_proof(tmp_path, monkeypatch):
    # The manifest (and bundle) still claim the correct SHA, but the actual installed wheel
    # bytes are wrong -> reject. A JSON field is never accepted as the only proof.
    ctx = _install_and_promote(tmp_path, monkeypatch)
    ctx["runtime_wheel"].write_bytes(b"not a wheel")
    with pytest.raises(av.ArtifactVerificationError) as e:
        build_registry_and_udfs(load_settings())
    assert e.value.category == av.CAT_SHA_MISMATCH


def test_core_version_from_importlib_not_hardcoded():
    from fintech_feature_platform.api.artifact_binding import installed_core_version

    assert installed_core_version() == CORE  # sourced from importlib.metadata


# --- positive online + batch compute through the shared verifier ------------

def test_positive_online_and_batch_compute_through_verified_backend(tmp_path, monkeypatch):
    _install_and_promote(tmp_path, monkeypatch)
    # Building the backend runs the shared verifier; reaching a built backend proves it passed.
    backend = build_backend(load_settings())
    key = EntityKey.from_mapping({"customer_id": "C1"}, key_order=["customer_id"])
    stamps = {"event": SourceStamp(report_ts=_NOW, content_hash="sha256:event")}

    # Online path (the online worker's exact call): F1 windowed count.
    online = backend.store.compute_write_set(
        view="activity", view_version=1, entity_key=key,
        requested_features=["event_count_7d"], source_refs={"event": "ref1"},
        source_stamps=stamps, calc_ts=_NOW,
        source_loader=lambda name: {"count_7d": 42},
    )
    assert online.results["event_count_7d"].value == 42

    # Batch path (same store method): dependent F2 computed from F1.
    batch = backend.store.compute_write_set(
        view="activity", view_version=1, entity_key=key,
        requested_features=["activity_score"], source_refs={"event": "ref1"},
        source_stamps=stamps, calc_ts=_NOW,
        source_loader=lambda name: {"count_7d": 42},
    )
    assert batch.results["activity_score"].value == 1.0


def test_enforce_returns_none_for_unbound_legacy(monkeypatch):
    monkeypatch.delenv("FSP_BUNDLE_STORE", raising=False)
    monkeypatch.setenv("FSP_ARTIFACT_BINDING", "legacy-compatible")
    settings = load_settings()
    registry, _ = build_registry_and_udfs(settings)
    assert enforce_artifact_binding(registry, settings) is None
