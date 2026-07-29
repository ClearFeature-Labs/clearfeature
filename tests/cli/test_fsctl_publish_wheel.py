"""fsctl publish --project: build -> hash -> install -> golden-test -> upload -> verify -> persist.
 (hardened). These tests build a real wheel with ``uv build --offline`` and
INSTALL it into an isolated ``--target`` (source cannot shadow it), so they exercise the
true supply-chain path. Runtime verification is covered by the artifact-binding suites.
"""

import json

import pytest

from fintech_feature_platform.cli.artifact import (
    ArtifactStoreError,
    WheelIdentityError,
    WheelStore,
    build_wheel,
    install_wheel,
    installed_identity,
    sha256_file,
    verify_wheel_identity,
)
from fintech_feature_platform.cli.fsctl import main
from fintech_feature_platform.cli.scaffold import write_scaffold
from fintech_feature_platform.fs_core.registry.bundle import FileBundleStore


def _run(argv, capsys):
    code = main(argv)
    return code, json.loads(capsys.readouterr().out)


def _project(tmp_path, name="customer-features", package="customer_features"):
    root = tmp_path / "proj"
    write_scaffold(root, name, package)
    return root


def _publish(root, tmp_path, capsys, *, wheel=None, bundle_store=None, wheel_store=None):
    argv = ["publish", "--project", str(root),
            "--bundle-store", str(bundle_store or tmp_path / "bundles"),
            "--wheel-store", str(wheel_store or tmp_path / "wheels")]
    if wheel is not None:
        argv += ["--wheel", str(wheel)]
    return _run(argv, capsys)


# --- happy path: three distinct digests, stored bundle, stored wheel ---------

def test_publish_wheel_bound_exposes_three_digests(tmp_path, capsys):
    root = _project(tmp_path)
    bundle_store = tmp_path / "bundles"
    code, out = _publish(root, tmp_path, capsys, bundle_store=bundle_store)
    assert code == 0 and out["ok"] is True
    for key in ("registry_definition_digest", "feature_artifact_sha256", "final_bundle_digest"):
        assert key in out and out[key]
    assert out["final_bundle_digest"] != out["registry_definition_digest"]
    assert out["feature_artifact_sha256"].startswith("sha256:")

    stored = FileBundleStore(str(bundle_store)).get(out["final_bundle_digest"])
    assert stored is not None
    assert stored.feature_artifact["sha256"] == out["feature_artifact_sha256"]
    assert stored.registry_definition_digest == out["registry_definition_digest"]


def test_publish_requires_resolved_wheel_store(tmp_path, capsys, monkeypatch):
    # A project whose manifest has no wheel_store, no flag, no env -> refuse to publish.
    root = _project(tmp_path)
    manifest = root / "feature_project.yaml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace("wheel_store: ./.wheels\n", ""),
        encoding="utf-8",
    )
    monkeypatch.delenv("FSP_WHEEL_STORE", raising=False)
    code, out = _run(
        ["publish", "--project", str(root), "--bundle-store", str(tmp_path / "b")], capsys
    )
    assert code == 1 and out["ok"] is False
    assert any("wheel_store" in e for e in out["errors"])


def test_publish_content_addressed_layout_and_release_record(tmp_path, capsys):
    root = _project(tmp_path)
    wheel_store = tmp_path / "wheels"
    bundle_store = tmp_path / "bundles"
    code, out = _publish(root, tmp_path, capsys, bundle_store=bundle_store, wheel_store=wheel_store)
    assert code == 0 and out["ok"] is True

    stored = FileBundleStore(str(bundle_store)).get(out["final_bundle_digest"])
    artifact = stored.feature_artifact
    # feature_artifact carries filename + a deterministic relative storage_key.
    sha_hex = out["feature_artifact_sha256"].split(":", 1)[-1]
    expected_key = f"customer-features/0.1.0/{sha_hex}/{artifact['filename']}"
    assert artifact["storage_key"] == expected_key
    assert artifact["filename"].endswith(".whl")
    # storage_key must not leak bucket URLs or absolute paths.
    assert "s3://" not in artifact["storage_key"]
    assert str(wheel_store) not in artifact["storage_key"]

    # Content-addressed layout on disk + immutable release record.
    assert (wheel_store / expected_key).is_file()
    assert sha256_file(wheel_store / expected_key) == out["feature_artifact_sha256"]
    release = json.loads((wheel_store / "customer-features/0.1.0/release.json").read_text())
    assert release["sha256"] == out["feature_artifact_sha256"]
    assert release["storage_key"] == expected_key
    assert out["wheel_location"].endswith(expected_key)


def test_feature_artifact_in_bundle_digest(tmp_path, capsys):
    # filename + storage_key are part of feature_artifact and therefore final_bundle_digest.
    from datetime import UTC, datetime

    from fintech_feature_platform.fs_core.registry.bundle import (
        build_registry_bundle,
        compute_bundle_digest,
    )
    from fintech_feature_platform.fs_core.registry.loader import load_registry_file

    root = _project(tmp_path)
    reg = load_registry_file(str(root / "customer_features" / "registry" / "features_v1.yaml"))
    base = {"name": "customer-features", "version": "0.1.0",
            "provider": "customer_features.features:build_udfs", "sha256": "sha256:" + "a" * 64}
    definition = compute_bundle_digest(reg)
    with_key = build_registry_bundle(
        reg, created_at=datetime(2026, 1, 1, tzinfo=UTC),
        feature_artifact={**base, "filename": "customer_features-0.1.0-py3-none-any.whl",
                          "storage_key": "customer-features/0.1.0/" + "a" * 64 + "/x.whl"},
    )
    other_key = build_registry_bundle(
        reg, created_at=datetime(2026, 1, 1, tzinfo=UTC),
        feature_artifact={**base, "filename": "customer_features-0.1.0-py3-none-any.whl",
                          "storage_key": "customer-features/0.1.0/" + "a" * 64 + "/y.whl"},
    )
    assert with_key.registry_definition_digest == definition
    assert with_key.bundle_digest != other_key.bundle_digest  # storage_key affects identity


# --- refinement 2: source mutation after build cannot change the tested/published wheel ---

def test_source_mutation_after_build_does_not_change_published_wheel(tmp_path, capsys):
    root = _project(tmp_path)
    wheel = build_wheel(root, tmp_path / "dist")
    sha_before = sha256_file(wheel)

    features = root / "customer_features" / "features.py"
    features.write_text(
        features.read_text(encoding="utf-8").replace(
            "return round(min(deps[\"event_count_7d\"] / 10.0, 1.0), 6)",
            "return float(deps[\"event_count_7d\"])",
        ),
        encoding="utf-8",
    )
    code, out = _publish(root, tmp_path, capsys, wheel=wheel)
    assert code == 0 and out["ok"] is True
    assert out["feature_artifact_sha256"] == sha_before


# --- publication gates: golden failure, identity mismatch, missing provider --

def test_publish_aborts_when_wheel_golden_fails(tmp_path, capsys):
    root = _project(tmp_path)
    golden = root / "customer_features" / "tests" / "golden.yaml"
    golden.write_text(
        golden.read_text(encoding="utf-8").replace("value: 42", "value: 999"), encoding="utf-8"
    )
    code, out = _publish(root, tmp_path, capsys)
    assert code == 1 and out["ok"] is False
    assert out["errors"]


def test_publish_aborts_on_missing_provider(tmp_path, capsys):
    root = _project(tmp_path)
    manifest = root / "feature_project.yaml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(
            "udf_provider: customer_features.features:build_udfs",
            "udf_provider: customer_features.features:not_a_real_provider",
        ),
        encoding="utf-8",
    )
    code, out = _publish(root, tmp_path, capsys)
    assert code == 1 and out["ok"] is False
    assert any("provider" in e.lower() for e in out["errors"])


def test_verify_wheel_identity_filename_metadata_mismatch(tmp_path):
    # Install a real 0.1.0 wheel, then check identity against a mismatched filename.
    root = _project(tmp_path)
    wheel = build_wheel(root, tmp_path / "dist")
    target = tmp_path / "site"
    install_wheel(wheel, target)
    assert installed_identity(target) == ("customer-features", "0.1.0")
    # Version mismatch in the (fake) filename must fail closed.
    with pytest.raises(WheelIdentityError):
        verify_wheel_identity(
            "customer_features-9.9.9-py3-none-any.whl", target,
            manifest_project="customer-features",
        )
    # Project-name mismatch must fail closed.
    with pytest.raises(WheelIdentityError):
        verify_wheel_identity(wheel, target, manifest_project="something-else")


# --- refinement: wheel-store failure leaves the bundle store unchanged -------

def test_wheel_store_failure_leaves_bundle_store_unchanged(tmp_path, capsys):
    root = _project(tmp_path)
    wheel_store = tmp_path / "wheels"
    # First publish stores wheel v0.1.0.
    code, _ = _publish(root, tmp_path, capsys, wheel_store=wheel_store)
    assert code == 0

    # Rebuild a DIFFERENT wheel with the same name+version (mutate source), publish to a
    # FRESH bundle store but the SAME immutable wheel store -> store must fail closed.
    features = root / "customer_features" / "features.py"
    features.write_text(features.read_text(encoding="utf-8") + "\n# drift\n", encoding="utf-8")
    wheel2 = build_wheel(root, tmp_path / "dist2")
    assert sha256_file(wheel2) != sha256_file(sorted(wheel_store.rglob("*.whl"))[0])

    fresh_bundle_store = tmp_path / "bundles_fresh"
    code, out = _publish(
        root, tmp_path, capsys, wheel=wheel2,
        bundle_store=fresh_bundle_store, wheel_store=wheel_store,
    )
    assert code == 1 and out["ok"] is False
    # No bundle was persisted because the wheel could not be stored.
    assert not fresh_bundle_store.exists() or not any(fresh_bundle_store.rglob("*.json"))


# --- legacy unbound publication remains byte-compatible ----------------------

def test_legacy_unbound_publish_backward_compatible(tmp_path, capsys):
    root = _project(tmp_path)
    registry = str(root / "customer_features" / "registry" / "features_v1.yaml")
    bundle_store = tmp_path / "bundles"
    code, out = _run(
        ["publish", "--registry", registry, "--bundle-store", str(bundle_store)], capsys
    )
    assert code == 0 and out["ok"] is True
    assert "final_bundle_digest" not in out  # legacy output shape unchanged
    stored = FileBundleStore(str(bundle_store)).get(out["bundle_digest"])
    assert stored is not None and stored.feature_artifact is None


# --- WheelStore unit: content-addressed immutability -------------------------

def test_wheel_store_idempotent_same_bytes(tmp_path):
    root = _project(tmp_path)
    wheel = build_wheel(root, tmp_path / "dist")
    sha = sha256_file(wheel)
    store = WheelStore(str(tmp_path / "store"))
    r1 = store.put(wheel, name="customer-features", version="0.1.0", sha256=sha)
    r2 = store.put(wheel, name="customer-features", version="0.1.0", sha256=sha)  # replay
    assert r1 == r2
    assert r1["storage_key"] == f"customer-features/0.1.0/{sha.split(':')[-1]}/{wheel.name}"
    verified = store.read_back_verify(name="customer-features", version="0.1.0", sha256=sha)
    assert verified["sha256"] == sha


def test_wheel_store_same_version_different_bytes_fails_closed(tmp_path):
    root = _project(tmp_path)
    wheel = build_wheel(root, tmp_path / "dist")
    store = WheelStore(str(tmp_path / "store"))
    store.put(wheel, name="customer-features", version="0.1.0", sha256=sha256_file(wheel))
    # Rebuild different bytes (same name+version) -> release record fails closed.
    (root / "customer_features" / "features.py").write_text(
        (root / "customer_features" / "features.py").read_text(encoding="utf-8") + "\n# x\n",
        encoding="utf-8",
    )
    wheel2 = build_wheel(root, tmp_path / "dist2")
    assert wheel2.name == wheel.name and sha256_file(wheel2) != sha256_file(wheel)
    with pytest.raises(ArtifactStoreError):
        store.put(wheel2, name="customer-features", version="0.1.0", sha256=sha256_file(wheel2))


# --- concurrency: release.json creation is atomic / first-writer-wins --------

def test_release_record_atomic_under_concurrent_publishers(tmp_path):
    import concurrent.futures as futures

    root = _project(tmp_path)
    wheel_a = build_wheel(root, tmp_path / "distA")
    sha_a = sha256_file(wheel_a)
    # A different-bytes wheel with the SAME filename (same name+version, different SHA).
    wheel_b_dir = tmp_path / "distB"
    wheel_b_dir.mkdir()
    wheel_b = wheel_b_dir / wheel_a.name
    wheel_b.write_bytes(wheel_a.read_bytes() + b"\x00drift")
    sha_b = sha256_file(wheel_b)
    assert sha_a != sha_b

    store = WheelStore(str(tmp_path / "store"))
    results: list[dict] = []
    errors: list[Exception] = []

    def publish(wheel, sha):
        try:
            results.append(store.put(wheel, name="customer-features", version="0.1.0", sha256=sha))
        except ArtifactStoreError as exc:
            errors.append(exc)

    with futures.ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(publish, wheel_a, sha_a)
        f2 = pool.submit(publish, wheel_b, sha_b)
        f1.result()
        f2.result()

    # Exactly one publisher wins; the other (different SHA) fails closed.
    assert len(results) == 1 and len(errors) == 1
    winner_sha = results[0]["sha256"]
    loser_sha = sha_b if winner_sha == sha_a else sha_a

    # release.json holds the winner and was never overwritten by the second publisher.
    release_path = tmp_path / "store" / "customer-features" / "0.1.0" / "release.json"
    release = json.loads(release_path.read_text())
    assert release["sha256"] == winner_sha
    assert release["sha256"] != loser_sha

    # Replaying the winner's SHA is idempotent success (still no overwrite).
    winner_wheel = wheel_a if winner_sha == sha_a else wheel_b
    replay = store.put(winner_wheel, name="customer-features", version="0.1.0", sha256=winner_sha)
    assert replay["sha256"] == winner_sha
    assert json.loads(release_path.read_text())["sha256"] == winner_sha


def test_concurrent_same_sha_publishers_both_succeed(tmp_path):
    import concurrent.futures as futures

    root = _project(tmp_path)
    wheel = build_wheel(root, tmp_path / "dist")
    sha = sha256_file(wheel)
    store = WheelStore(str(tmp_path / "store"))
    results: list[dict] = []

    def publish():
        results.append(store.put(wheel, name="customer-features", version="0.1.0", sha256=sha))

    with futures.ThreadPoolExecutor(max_workers=4) as pool:
        for fut in [pool.submit(publish) for _ in range(4)]:
            fut.result()

    assert len(results) == 4
    assert {r["sha256"] for r in results} == {sha}  # all idempotent successes, one release


# --- gated live MinIO/S3 artifact-store check --------------------------------

@pytest.mark.skipif(
    __import__("os").getenv("FSP_MINIO_INTEGRATION") != "1",
    reason="set FSP_MINIO_INTEGRATION=1 (and run local MinIO) to enable",
)
def test_live_minio_wheel_store_roundtrip(tmp_path):
    import uuid

    from fintech_feature_platform.fs_core.raw.minio_payload_store import connect_minio

    root = _project(tmp_path)
    wheel = build_wheel(root, tmp_path / "dist")
    sha = sha256_file(wheel)
    version = f"0.1.{uuid.uuid4().hex[:8]}"  # unique version per run (immutable store)
    client = connect_minio("localhost:9000", "minioadmin", "minioadmin", secure=False)
    store = WheelStore("s3://fsp-wheels/test", client=client)

    release = store.put(wheel, name="customer-features", version=version, sha256=sha)
    sha_hex = sha.split(":")[-1]
    assert release["storage_key"] == f"customer-features/{version}/{sha_hex}/{wheel.name}"
    # upload + read-back SHA verification (wheel bytes AND release record)
    verified = store.read_back_verify(name="customer-features", version=version, sha256=sha)
    assert verified["sha256"] == sha
    # idempotent replay
    assert store.put(wheel, name="customer-features", version=version, sha256=sha) == release
    # same name+version, different SHA -> fail closed
    with pytest.raises(ArtifactStoreError):
        store.put(wheel, name="customer-features", version=version, sha256="sha256:" + "0" * 64)


@pytest.mark.skipif(
    __import__("os").getenv("FSP_MINIO_INTEGRATION") != "1",
    reason="set FSP_MINIO_INTEGRATION=1 (and run local MinIO) to enable",
)
def test_live_minio_release_claim_atomic_under_concurrency(tmp_path):
    """Two simultaneous S3 publishers, same name+version, different SHA -> exactly one wins."""
    import concurrent.futures as futures
    import uuid

    from fintech_feature_platform.fs_core.raw.minio_payload_store import connect_minio

    root = _project(tmp_path)
    wheel_a = build_wheel(root, tmp_path / "distA")
    sha_a = sha256_file(wheel_a)
    wheel_b_dir = tmp_path / "distB"
    wheel_b_dir.mkdir()
    wheel_b = wheel_b_dir / wheel_a.name
    wheel_b.write_bytes(wheel_a.read_bytes() + b"\x00drift")
    sha_b = sha256_file(wheel_b)
    assert sha_a != sha_b

    version = f"0.1.{uuid.uuid4().hex[:8]}"
    uri = "s3://fsp-wheels/concurrency"
    results: list[dict] = []
    errors: list[Exception] = []

    def publish(wheel, sha):
        # a separate client per publisher, like independent processes
        store = WheelStore(uri, client=connect_minio("localhost:9000", "minioadmin", "minioadmin"))
        try:
            results.append(store.put(wheel, name="customer-features", version=version, sha256=sha))
        except ArtifactStoreError as exc:
            errors.append(exc)

    with futures.ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(publish, wheel_a, sha_a)
        f2 = pool.submit(publish, wheel_b, sha_b)
        f1.result()
        f2.result()

    # Exactly one succeeds; exactly one receives a version conflict.
    assert len(results) == 1 and len(errors) == 1
    winner_sha = results[0]["sha256"]
    loser_sha = sha_b if winner_sha == sha_a else sha_a
    winner_wheel = wheel_a if winner_sha == sha_a else wheel_b
    loser_wheel = wheel_b if winner_sha == sha_a else wheel_a

    verify_client = connect_minio("localhost:9000", "minioadmin", "minioadmin")
    verify_store = WheelStore(uri, client=verify_client)
    # release.json remains the winner's value.
    verified = verify_store.read_back_verify(
        name="customer-features", version=version, sha256=winner_sha
    )
    assert verified["sha256"] == winner_sha
    # Winner replay is idempotent; loser replay remains rejected.
    assert verify_store.put(
        winner_wheel, name="customer-features", version=version, sha256=winner_sha
    )["sha256"] == winner_sha
    with pytest.raises(ArtifactStoreError):
        verify_store.put(loser_wheel, name="customer-features", version=version, sha256=loser_sha)
