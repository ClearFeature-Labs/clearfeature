"""fsctl CLI: validate / run-local / test / publish / promote / rollback.

Tests call ``main([...])`` directly (Docker-free, no install) and parse the printed JSON.
"""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from fintech_feature_platform.cli.fsctl import main
from fintech_feature_platform.fs_core.registry.bundle import FileBundleStore, RegistryBundle

_REPO = Path(__file__).resolve().parents[2]
_EXAMPLE_REGISTRY = _REPO / "examples" / "registry" / "minimal_credit_risk.yaml"

_SHADOW_REGISTRY = """
registry_version: t
entities:
  e: {key_fields: [id]}
sources:
  src: {type: raw_report, report_type: r, ts_field: report_ts}
feature_views:
  v:
    entity: e
    key_fields: [id]
    view_version: 1
    owner: o
    status: active
    features:
      f: {kind: udf, feature_version: 1, udf: udf.f, dtype: float, status: shadow, inputs: [src]}
"""

_F3_REGISTRY = """
registry_version: t
entities:
  e: {key_fields: [id]}
sources:
  src: {type: raw_report, report_type: r, ts_field: report_ts}
feature_views:
  v:
    entity: e
    key_fields: [id]
    view_version: 1
    owner: o
    status: active
    features:
      base:
        kind: udf
        feature_version: 1
        udf: udf.base
        dtype: float
        status: live
        inputs: [src]
      pd:
        kind: model
        feature_version: 1
        dtype: float
        status: live
        deps: [{feature: base, version: 1}]
        model: {uri: "mlflow://m/1", digest: "sha256:d", output_name: score}
"""

_INVALID_REGISTRY = """
registry_version: t
entities:
  e: {key_fields: [id]}
sources:
  src: {type: raw_report, report_type: r, ts_field: report_ts}
feature_views:
  v:
    entity: e
    key_fields: [id]
    view_version: 1
    owner: o
    status: active
    features:
      f: {kind: udf, feature_version: 1, udf: udf.f, dtype: float, status: bogus, inputs: [src]}
"""

_GOLDEN_PASS = """
cases:
  - name: dti
    feature: debt_to_income_ratio
    deps: {monthly_obligations: 20, income_from_tax: 100}
    expected: {value: 0.2}
"""

_GOLDEN_FAIL = """
cases:
  - name: dti_wrong
    feature: debt_to_income_ratio
    deps: {monthly_obligations: 20, income_from_tax: 100}
    expected: {value: 999}
"""


def _write(tmp_path, name, text):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return str(path)


def _run(capsys, argv):
    code = main(argv)
    lines = capsys.readouterr().out.strip().splitlines()
    return code, (json.loads(lines[-1]) if lines else {})


# --- validate ----------------------------------------------------------------

def test_validate_ok_prints_bundle_digest(capsys):
    code, data = _run(capsys, ["validate", "--registry", str(_EXAMPLE_REGISTRY)])
    assert code == 0
    assert data["valid"] is True
    assert data["bundle_digest"].startswith("sha256:")
    assert data["views_count"] == 1
    assert data["features_count"] == 5


def test_validate_fails_on_invalid_lifecycle(tmp_path, capsys):
    reg = _write(tmp_path, "bad.yaml", _INVALID_REGISTRY)
    code, data = _run(capsys, ["validate", "--registry", reg])
    assert code == 1
    assert data["valid"] is False
    assert data["errors"]


# --- run-local ---------------------------------------------------------------

def test_run_local_rejects_shadow_without_flag(tmp_path, capsys):
    reg = _write(tmp_path, "shadow.yaml", _SHADOW_REGISTRY)
    code, data = _run(capsys, ["run-local", "--registry", reg, "--feature", "f"])
    assert code == 1
    assert data["ok"] is False
    assert "shadow" in data["errors"][0]


def test_run_local_allows_shadow_with_flag(tmp_path, capsys):
    reg = _write(tmp_path, "shadow.yaml", _SHADOW_REGISTRY)
    code, data = _run(
        capsys, ["run-local", "--registry", reg, "--feature", "f", "--allow-shadow"]
    )
    assert code == 0
    assert data["ok"] is True
    assert data["lifecycle"] == "shadow"


def test_run_local_f3_requires_fake_runner(tmp_path, capsys):
    reg = _write(tmp_path, "f3.yaml", _F3_REGISTRY)
    code, data = _run(capsys, ["run-local", "--registry", reg, "--feature", "pd", "--no-udfs"])
    assert code == 1
    assert "--model-runner fake" in data["errors"][0]


def test_run_local_f3_with_fake_runner(tmp_path, capsys):
    reg = _write(tmp_path, "f3.yaml", _F3_REGISTRY)
    code, data = _run(capsys, [
        "run-local", "--registry", reg, "--feature", "pd",
        "--model-runner", "fake", "--no-udfs",
    ])
    assert code == 0
    assert "value" in data


# --- test --------------------------------------------------------------------

def test_test_passes_real_golden_case(tmp_path, capsys):
    tests = _write(tmp_path, "g.yaml", _GOLDEN_PASS)
    code, data = _run(
        capsys, ["test", "--registry", str(_EXAMPLE_REGISTRY), "--tests", tests]
    )
    assert code == 0
    assert data["ok"] is True
    assert data["passed"] == 1


def test_test_fails_on_wrong_expected_value(tmp_path, capsys):
    tests = _write(tmp_path, "g.yaml", _GOLDEN_FAIL)
    code, data = _run(
        capsys, ["test", "--registry", str(_EXAMPLE_REGISTRY), "--tests", tests]
    )
    assert code == 1
    assert data["ok"] is False
    assert data["failed"] == 1
    assert data["cases"][0]["status"] == "failed"


# --- publish -----------------------------------------------------------------

def test_publish_writes_bundle(tmp_path, capsys):
    store = str(tmp_path / "bundles")
    code, data = _run(
        capsys, ["publish", "--registry", str(_EXAMPLE_REGISTRY), "--bundle-store", store]
    )
    assert code == 0
    assert data["ok"] is True
    assert Path(data["bundle_path"]).exists()


def test_publish_idempotent_same_content(tmp_path, capsys):
    store = str(tmp_path / "bundles")
    _run(capsys, ["publish", "--registry", str(_EXAMPLE_REGISTRY), "--bundle-store", store])
    code, data = _run(
        capsys, ["publish", "--registry", str(_EXAMPLE_REGISTRY), "--bundle-store", store]
    )
    assert code == 0 and data["ok"] is True
    assert len(FileBundleStore(store).list()) == 1


def test_file_bundle_store_same_digest_different_content_fails(tmp_path):
    store = FileBundleStore(tmp_path / "bundles")
    now = datetime(2026, 1, 1, tzinfo=UTC)
    store.put(RegistryBundle(bundle_id="1", bundle_digest="sha256:x", created_at=now,
                             views=("v:v1",), features=("a",)))
    # Same digest + same content (different created_at) -> idempotent.
    store.put(RegistryBundle(bundle_id="1b", bundle_digest="sha256:x",
                             created_at=datetime(2027, 1, 1, tzinfo=UTC),
                             views=("v:v1",), features=("a",)))
    assert len(store.list()) == 1
    with pytest.raises(ValueError, match="immutable"):
        store.put(RegistryBundle(bundle_id="2", bundle_digest="sha256:x", created_at=now,
                                 views=("v:v1",), features=("b",)))


# --- promote / rollback (CLI wiring) -----------------------------------------

def _publish(capsys, tmp_path, registry_text=None):
    reg = str(_EXAMPLE_REGISTRY)
    if registry_text is not None:
        reg = _write(tmp_path, "reg.yaml", registry_text)
    store = str(tmp_path / "bundles")
    code, data = _run(capsys, ["publish", "--registry", reg, "--bundle-store", store])
    assert code == 0
    return store, data["bundle_digest"]


def test_promote_to_shadow_then_live_energy_and_rollback(tmp_path, capsys):
    bundles = str(tmp_path / "bundles")
    pointers = str(tmp_path / "env")
    # Publish two distinct bundles (A = example, B = F3 registry).
    _, digest_a = _publish(capsys, tmp_path)
    code, data = _run(capsys, ["publish", "--registry",
                               _write(tmp_path, "b.yaml", _F3_REGISTRY),
                               "--bundle-store", bundles])
    digest_b = data["bundle_digest"]

    def promote(digest, to, extra=()):
        return _run(capsys, ["promote", "--bundle-store", bundles, "--pointer-store", pointers,
                             "--bundle-digest", digest, "--env", "prod", "--to", to,
                             "--actor", "carol", "--reason", "r", *extra])

    # A -> shadow -> live (energy: 1 approver, 0-day soak).
    code, rec = promote(digest_a, "shadow")
    assert code == 0 and rec["record"]["stage"] == "shadow"
    assert rec["record"]["shadow_started_at"] is not None
    code, rec = promote(digest_a, "live", ["--profile", "energy", "--approved-by", "alice"])
    assert code == 0 and rec["record"]["bundle_digest"] == digest_a

    # B -> shadow -> live, so live has a previous digest (A).
    promote(digest_b, "shadow")
    code, rec = promote(digest_b, "live", ["--profile", "energy", "--approved-by", "bob"])
    assert code == 0 and rec["record"]["previous_digest"] == digest_a

    # rollback to previous -> live points back at A.
    code, rec = _run(capsys, ["rollback", "--pointer-store", pointers, "--env", "prod",
                              "--to-previous", "--actor", "carol", "--reason", "bad B"])
    assert code == 0
    assert rec["record"]["action"] == "rollback"
    assert rec["record"]["bundle_digest"] == digest_a


def test_promote_live_bank_one_approver_fails(tmp_path, capsys):
    bundles, digest = _publish(capsys, tmp_path)
    pointers = str(tmp_path / "env")
    _run(capsys, ["promote", "--bundle-store", bundles, "--pointer-store", pointers,
                  "--bundle-digest", digest, "--env", "prod", "--to", "shadow",
                  "--actor", "g", "--reason", "s"])
    code, data = _run(capsys, [
        "promote", "--bundle-store", bundles, "--pointer-store", pointers,
        "--bundle-digest", digest, "--env", "prod", "--to", "live",
        "--profile", "bank", "--approved-by", "alice", "--actor", "g", "--reason", "p",
        "--allow-shadow-age-override", "--override-reason", "test",
    ])
    assert code == 1
    assert "requires 2 unique approver" in data["errors"][0]


def test_promote_unknown_bundle_fails(tmp_path, capsys):
    bundles = str(tmp_path / "bundles")
    pointers = str(tmp_path / "env")
    FileBundleStore(bundles)  # empty store
    code, data = _run(capsys, ["promote", "--bundle-store", bundles, "--pointer-store",
                               pointers, "--bundle-digest", "sha256:nope", "--env", "prod",
                               "--to", "shadow", "--actor", "g", "--reason", "s"])
    assert code == 1
    assert "unknown bundle" in data["errors"][0]


# --- CI templates ------------------------------------------------------------

def test_github_template_calls_fsctl_and_never_auto_promotes():
    text = (_REPO / "ci" / "github" / "feature-registry.yml").read_text(encoding="utf-8")
    assert "uv run fsctl validate" in text
    assert "uv run fsctl test" in text
    # No executed promote/rollback step (comments explaining the manual step are fine).
    assert "uv run fsctl promote" not in text
    assert "--to live" not in text


def test_gitlab_template_calls_fsctl_and_never_auto_promotes():
    text = (_REPO / "ci" / "gitlab" / "feature-registry.gitlab-ci.yml").read_text(
        encoding="utf-8"
    )
    assert "uv run fsctl validate" in text
    assert "uv run fsctl test" in text
    assert "uv run fsctl promote" not in text
    assert "--to live" not in text
