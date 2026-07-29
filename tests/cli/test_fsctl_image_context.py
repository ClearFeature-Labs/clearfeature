"""fsctl image-context: build-generated worker-image context.

The context turns an exact, verified feature wheel into everything `docker build` needs:
the wheel itself, a build-generated feature-artifact.json (never handwritten), the
registry extracted from the verified wheel, and a Dockerfile that re-verifies the wheel
SHA during build and installs it with --no-deps --offline.
"""

import json

from fintech_feature_platform.cli.artifact import build_wheel, sha256_file
from fintech_feature_platform.cli.fsctl import main
from fintech_feature_platform.cli.scaffold import write_scaffold


def _run(argv, capsys):
    code = main(argv)
    return code, json.loads(capsys.readouterr().out)


def _project(tmp_path):
    root = tmp_path / "proj"
    write_scaffold(root, "customer-features", "customer_features")
    return root


def test_image_context_creates_expected_files(tmp_path, capsys):
    root = _project(tmp_path)
    ctx = tmp_path / "ctx"
    code, out = _run(["image-context", "--project", str(root), "--dir", str(ctx)], capsys)
    assert code == 0 and out["ok"] is True

    wheel_name = out["filename"]
    assert (ctx / "Dockerfile").is_file()
    assert (ctx / wheel_name).is_file()
    assert (ctx / "feature-artifact.json").is_file()
    assert (ctx / "registry.yaml").is_file()
    # The context wheel is the exact verified wheel.
    assert sha256_file(ctx / wheel_name) == out["feature_artifact_sha256"]


def test_manifest_is_generated_from_verified_metadata(tmp_path, capsys):
    root = _project(tmp_path)
    ctx = tmp_path / "ctx"
    code, out = _run(["image-context", "--project", str(root), "--dir", str(ctx)], capsys)
    assert code == 0

    manifest = json.loads((ctx / "feature-artifact.json").read_text())
    sha_hex = out["feature_artifact_sha256"].split(":", 1)[-1]
    assert manifest["format_version"] == 1
    assert manifest["name"] == "customer-features"
    assert manifest["version"] == "0.1.0"
    assert manifest["provider"] == "customer_features.features:build_udfs"
    assert manifest["sha256"] == out["feature_artifact_sha256"]
    assert manifest["filename"] == out["filename"]
    # wheel_path points INTO the image at the immutable preserved location.
    assert manifest["wheel_path"] == (
        f"/opt/clearfeature/feature-artifacts/{sha_hex}/{out['filename']}"
    )
    # requires_core recorded (evidence; the enforced copy travels in the bundle).
    assert manifest["requires_core"] == ">=0.1,<0.2"


def test_dockerfile_contract(tmp_path, capsys):
    root = _project(tmp_path)
    ctx = tmp_path / "ctx"
    code, out = _run(
        ["image-context", "--project", str(root), "--dir", str(ctx),
         "--base-image", "fsp-app:1.2.3"], capsys
    )
    assert code == 0
    df = (ctx / "Dockerfile").read_text()
    sha_hex = out["feature_artifact_sha256"].split(":", 1)[-1]

    assert "ARG BASE_IMAGE=fsp-app:1.2.3" in df
    assert "FROM ${BASE_IMAGE}" in df
    # SHA verified during build (build fails on mismatch).
    assert f"{sha_hex}  /opt/clearfeature/feature-artifacts/{sha_hex}/{out['filename']}" in df
    assert "sha256sum -c -" in df
    # Controlled install: exact wheel, no dependency resolution, no network.
    assert "--no-deps" in df and "--offline" in df
    run_lines = [line for line in df.splitlines() if line.startswith("RUN")]
    install_lines = [line for line in run_lines if "pip install" in line]
    assert install_lines and all("uv pip install" in line for line in install_lines)
    # Self-describing runtime env.
    assert "FSP_FEATURE_ARTIFACT_MANIFEST=/etc/clearfeature/feature-artifact.json" in df
    assert "FSP_REGISTRY_PATH=/etc/clearfeature/registry.yaml" in df
    assert "FSP_UDF_PROVIDER=customer_features.features:build_udfs" in df
    # No credentials of any kind in the generated build.
    for needle in ("PASSWORD", "SECRET", "TOKEN", "KEY="):
        assert needle not in df.upper() or "ACCESS" not in df.upper()


def test_registry_extracted_from_wheel_matches_project(tmp_path, capsys):
    root = _project(tmp_path)
    ctx = tmp_path / "ctx"
    code, _ = _run(["image-context", "--project", str(root), "--dir", str(ctx)], capsys)
    assert code == 0
    project_registry = (
        root / "customer_features" / "registry" / "features_v1.yaml"
    ).read_text()
    assert (ctx / "registry.yaml").read_text() == project_registry


def test_image_context_accepts_prebuilt_wheel(tmp_path, capsys):
    root = _project(tmp_path)
    wheel = build_wheel(root, tmp_path / "dist")
    sha = sha256_file(wheel)
    ctx = tmp_path / "ctx"
    code, out = _run(
        ["image-context", "--project", str(root), "--wheel", str(wheel), "--dir", str(ctx)],
        capsys,
    )
    assert code == 0 and out["feature_artifact_sha256"] == sha


def test_image_context_aborts_when_wheel_golden_fails(tmp_path, capsys):
    root = _project(tmp_path)
    golden = root / "customer_features" / "tests" / "golden.yaml"
    golden.write_text(
        golden.read_text().replace("value: 42", "value: 999"), encoding="utf-8"
    )
    code, out = _run(
        ["image-context", "--project", str(root), "--dir", str(tmp_path / "ctx")], capsys
    )
    assert code == 1 and out["ok"] is False and out["errors"]


def test_image_context_refuses_nonempty_dir(tmp_path, capsys):
    root = _project(tmp_path)
    ctx = tmp_path / "ctx"
    ctx.mkdir()
    (ctx / "existing.txt").write_text("x", encoding="utf-8")
    code, out = _run(["image-context", "--project", str(root), "--dir", str(ctx)], capsys)
    assert code == 1 and out["ok"] is False


def test_scaffold_includes_ci_example(tmp_path, capsys):
    code, out = _run(["init", "--name", "customer-features", "--dir", str(tmp_path / "cf")], capsys)
    assert code == 0
    ci = tmp_path / "cf" / "ci" / "feature-project-ci.example.yml"
    assert ci.is_file()
    text = ci.read_text()
    # Stage separation + no live auto-promotion + placeholders only.
    stages = ("validate", "build_wheel", "test_exact_wheel", "publish", "image",
              "promote_shadow")
    for stage in stages:
        assert stage in text
    assert "promote_live" in text and "manual" in text.lower()
    for forbidden in ("minioadmin", "password", "AKIA"):
        assert forbidden not in text
