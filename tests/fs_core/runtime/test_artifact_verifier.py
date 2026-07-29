"""Runtime feature-artifact verifier.

Builds a real wheel, installs it into an isolated target on sys.path, and drives the pure
verifier through its positive path and every fail-closed negative case. Package name +
version alone are proven insufficient (installed-code RECORD verification).
"""

import importlib
import importlib.metadata
import json
import sys

import pytest

from fintech_feature_platform.cli.artifact import (
    artifact_storage_key,
    build_wheel,
    install_wheel,
    sha256_file,
)
from fintech_feature_platform.cli.scaffold import write_scaffold
from fintech_feature_platform.fs_core.registry.artifact import canonicalize_name
from fintech_feature_platform.fs_core.runtime import artifact_verifier as av

CORE = importlib.metadata.version("fintech-feature-platform")


def _setup(tmp_path, monkeypatch, *, requires_core=">=0.1,<0.2"):
    root = tmp_path / "proj"
    write_scaffold(root, "customer-features", "customer_features")
    built = build_wheel(root, tmp_path / "dist")
    sha = sha256_file(built)
    # A runtime copy of the wheel (as an image would carry it) + an isolated install target.
    runtime_wheel = tmp_path / "runtime" / built.name
    runtime_wheel.parent.mkdir(parents=True)
    runtime_wheel.write_bytes(built.read_bytes())
    target = tmp_path / "site"
    install_wheel(runtime_wheel, target)
    monkeypatch.syspath_prepend(str(target))
    for mod in [m for m in sys.modules if m.startswith("customer_features")]:
        del sys.modules[mod]
    importlib.invalidate_caches()

    name, version = "customer-features", "0.1.0"
    provider, filename = "customer_features.features:build_udfs", built.name
    fa = {
        "name": name, "version": version, "provider": provider, "sha256": sha,
        "filename": filename,
        "storage_key": artifact_storage_key(canonicalize_name(name), version, sha, filename),
    }
    if requires_core:
        fa["requires_core"] = requires_core
    manifest = {
        "format_version": 1, "name": name, "version": version, "provider": provider,
        "filename": filename, "sha256": sha, "wheel_path": str(runtime_wheel),
    }
    manifest_path = tmp_path / "feature-artifact.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return {
        "fa": fa, "manifest_path": manifest_path, "wheel": runtime_wheel, "target": target,
        "sha": sha, "name": name, "version": version, "provider": provider, "filename": filename,
    }


def _verify(s, *, fa=None, core=CORE):
    ev = av.load_runtime_evidence(s["manifest_path"])
    return av.verify_feature_artifact(
        feature_artifact=fa or s["fa"], evidence=ev, installed_core_version=core
    )


# --- positive ---------------------------------------------------------------

def test_verify_ok(tmp_path, monkeypatch):
    s = _setup(tmp_path, monkeypatch)
    res = _verify(s)
    assert res["verified"] is True
    assert res["expected_name"] == "customer-features"


# --- manifest loading -------------------------------------------------------

def test_manifest_missing(tmp_path):
    with pytest.raises(av.ArtifactVerificationError) as e:
        av.load_runtime_evidence(tmp_path / "nope.json")
    assert e.value.category == av.CAT_MANIFEST_MISSING


def test_manifest_invalid_json(tmp_path):
    p = tmp_path / "m.json"
    p.write_text("{not json", encoding="utf-8")
    with pytest.raises(av.ArtifactVerificationError) as e:
        av.load_runtime_evidence(p)
    assert e.value.category == av.CAT_METADATA_INVALID


def test_manifest_missing_fields(tmp_path):
    p = tmp_path / "m.json"
    p.write_text(json.dumps({"name": "x"}), encoding="utf-8")
    with pytest.raises(av.ArtifactVerificationError) as e:
        av.load_runtime_evidence(p)
    assert e.value.category == av.CAT_METADATA_INVALID


# --- negative verification cases -------------------------------------------

def test_wheel_missing(tmp_path, monkeypatch):
    s = _setup(tmp_path, monkeypatch)
    s["wheel"].unlink()
    with pytest.raises(av.ArtifactVerificationError) as e:
        _verify(s)
    assert e.value.category == av.CAT_WHEEL_MISSING


def test_wheel_bytes_modified_only_manifest_reports_correct_sha(tmp_path, monkeypatch):
    # Manifest + bundle still claim the correct SHA, but the actual wheel bytes were changed.
    s = _setup(tmp_path, monkeypatch)
    s["wheel"].write_bytes(s["wheel"].read_bytes() + b"tamper")
    with pytest.raises(av.ArtifactVerificationError) as e:
        _verify(s)
    assert e.value.category == av.CAT_SHA_MISMATCH


def test_bundle_sha_mismatch(tmp_path, monkeypatch):
    s = _setup(tmp_path, monkeypatch)
    fa = {**s["fa"], "sha256": "sha256:" + "0" * 64}
    fa["storage_key"] = artifact_storage_key(
        "customer-features", "0.1.0", fa["sha256"], s["filename"]
    )
    with pytest.raises(av.ArtifactVerificationError) as e:
        _verify(s, fa=fa)
    assert e.value.category == av.CAT_SHA_MISMATCH


def test_filename_mismatch(tmp_path, monkeypatch):
    s = _setup(tmp_path, monkeypatch)
    fa = {**s["fa"], "filename": "customer_features-0.1.0-py3-none-other.whl"}
    fa["storage_key"] = artifact_storage_key("customer-features", "0.1.0", s["sha"], fa["filename"])
    with pytest.raises(av.ArtifactVerificationError) as e:
        _verify(s, fa=fa)
    assert e.value.category == av.CAT_DISTRIBUTION_MISMATCH


def test_package_name_mismatch(tmp_path, monkeypatch):
    s = _setup(tmp_path, monkeypatch)
    fa = {**s["fa"], "name": "other-package"}
    fa["storage_key"] = artifact_storage_key("other-package", "0.1.0", s["sha"], s["filename"])
    with pytest.raises(av.ArtifactVerificationError) as e:
        _verify(s, fa=fa)
    assert e.value.category == av.CAT_DISTRIBUTION_MISMATCH


def test_package_version_mismatch(tmp_path, monkeypatch):
    s = _setup(tmp_path, monkeypatch)
    fa = {**s["fa"], "version": "9.9.9"}
    fa["storage_key"] = artifact_storage_key("customer-features", "9.9.9", s["sha"], s["filename"])
    with pytest.raises(av.ArtifactVerificationError) as e:
        _verify(s, fa=fa)
    assert e.value.category == av.CAT_DISTRIBUTION_MISMATCH


def test_provider_mismatch_manifest_vs_bundle(tmp_path, monkeypatch):
    s = _setup(tmp_path, monkeypatch)
    fa = {**s["fa"], "provider": "customer_features.features:something_else"}
    with pytest.raises(av.ArtifactVerificationError) as e:
        _verify(s, fa=fa)
    assert e.value.category == av.CAT_PROVIDER_MISMATCH


def test_provider_imports_from_another_distribution(tmp_path, monkeypatch):
    # Provider agrees between manifest and bundle, imports & is callable, but belongs to a
    # different distribution (stdlib json) — must fail closed.
    s = _setup(tmp_path, monkeypatch)
    manifest = json.loads(s["manifest_path"].read_text())
    manifest["provider"] = "json:dumps"
    s["manifest_path"].write_text(json.dumps(manifest), encoding="utf-8")
    fa = {**s["fa"], "provider": "json:dumps"}
    with pytest.raises(av.ArtifactVerificationError) as e:
        _verify(s, fa=fa)
    assert e.value.category == av.CAT_PROVIDER_MISMATCH


def test_installed_files_do_not_match_wheel_record(tmp_path, monkeypatch):
    s = _setup(tmp_path, monkeypatch)
    installed = s["target"] / "customer_features" / "features.py"
    installed.write_text(installed.read_text(encoding="utf-8") + "\n# tampered\n", encoding="utf-8")
    with pytest.raises(av.ArtifactVerificationError) as e:
        _verify(s)
    assert e.value.category == av.CAT_INSTALLATION_MISMATCH


def test_metadata_invalid_storage_key(tmp_path, monkeypatch):
    s = _setup(tmp_path, monkeypatch)
    fa = {**s["fa"], "storage_key": "wrong/key"}
    with pytest.raises(av.ArtifactVerificationError) as e:
        _verify(s, fa=fa)
    assert e.value.category == av.CAT_METADATA_INVALID


def test_requires_core_incompatible(tmp_path, monkeypatch):
    s = _setup(tmp_path, monkeypatch, requires_core=">=0.2,<0.3")
    with pytest.raises(av.ArtifactVerificationError) as e:
        _verify(s)
    assert e.value.category == av.CAT_CORE_INCOMPATIBLE


def test_requires_core_compatible(tmp_path, monkeypatch):
    s = _setup(tmp_path, monkeypatch, requires_core=">=0.1,<0.2")
    assert _verify(s)["verified"] is True


def test_bound_bundle_without_requires_core_still_verified(tmp_path, monkeypatch):
    s = _setup(tmp_path, monkeypatch, requires_core=None)
    assert "requires_core" not in s["fa"]
    assert _verify(s)["verified"] is True


@pytest.mark.parametrize(
    "installed,spec,expected",
    [
        # compatible range / incompatible range
        ("0.1.0", ">=0.1,<0.2", True),
        ("0.1.9", ">=0.1,<0.2", True),
        ("0.2.0", ">=0.1,<0.2", False),
        ("0.0.9", ">=0.1,<0.2", False),
        # exact match
        ("0.1.0", "==0.1.0", True),
        ("0.1.1", "==0.1.0", False),
        # boundary versions (normalized 0.1 == 0.1.0; strict < at the upper bound)
        ("0.1", ">=0.1", True),
        ("0.1.0", ">=0.1", True),
        ("0.2.0", "<0.2", False),
        ("0.1.4", ">=0.1.4,<0.2", True),
        ("1.0.0", ">0.9,<=1.0", True),
    ],
)
def test_core_version_satisfies(installed, spec, expected):
    from fintech_feature_platform.fs_core.runtime.artifact_verifier import core_version_satisfies

    assert core_version_satisfies(installed, spec) is expected


@pytest.mark.parametrize(
    "spec",
    [
        "garbage",            # malformed
        ">=0.1.x",            # malformed version component
        "!=0.2.0",            # unsupported operator
        "~=0.1.0",            # unsupported operator (compatible-release)
        "===0.1.0",           # unsupported operator
        "==1.*",              # unsupported wildcard
        ">=1.0.0a1",          # unsupported prerelease
        ">=1.0.0.dev1",       # unsupported dev release
        ">=0.1,",             # empty trailing clause (stray comma)
        "",                   # empty specifier
    ],
)
def test_core_version_unsupported_or_malformed_raises(spec):
    from fintech_feature_platform.fs_core.runtime.artifact_verifier import core_version_satisfies

    with pytest.raises(ValueError):
        core_version_satisfies("0.1.0", spec)


def test_unsupported_requires_core_maps_to_metadata_invalid(tmp_path, monkeypatch):
    # A bound bundle whose requires_core uses unsupported (~=) syntax fails closed as
    # feature_artifact_metadata_invalid — never partially interpreted.
    s = _setup(tmp_path, monkeypatch, requires_core="~=0.1.0")
    with pytest.raises(av.ArtifactVerificationError) as e:
        _verify(s)
    assert e.value.category == av.CAT_METADATA_INVALID


def test_error_evidence_is_safe(tmp_path, monkeypatch):
    s = _setup(tmp_path, monkeypatch)
    fa = {**s["fa"], "sha256": "sha256:" + "0" * 64}
    fa["storage_key"] = artifact_storage_key(
        "customer-features", "0.1.0", fa["sha256"], s["filename"]
    )
    with pytest.raises(av.ArtifactVerificationError) as e:
        _verify(s, fa=fa)
    ev = e.value.evidence
    assert ev["category"] == av.CAT_SHA_MISMATCH
    assert set(ev).issubset(
        {"expected_name", "expected_version", "expected_sha256", "expected_provider", "category"}
    )
