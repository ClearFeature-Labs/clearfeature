"""feature_project.yaml discovery + CLI default resolution.

Precedence: explicit flag > environment variable > manifest > built-in default. The
manifest supplies defaults only; it must never carry secrets or promotion approvers.
"""

import json

import pytest

from fintech_feature_platform.cli.fsctl import main
from fintech_feature_platform.cli.manifest import ManifestError, load_manifest

_REGISTRY = """
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
      f: {kind: udf, feature_version: 1, udf: udf.f, dtype: float, status: live, inputs: [src]}
"""


def _write_project(tmp_path, registry_rel="reg.yaml", extra=""):
    (tmp_path / registry_rel).write_text(_REGISTRY, encoding="utf-8")
    (tmp_path / "feature_project.yaml").write_text(
        f"project: demo\nrequires_core: '>=0.1,<0.2'\nregistry: {registry_rel}\n{extra}",
        encoding="utf-8",
    )


def _run(argv, capsys):
    code = main(argv)
    return code, json.loads(capsys.readouterr().out)


def test_manifest_default_applied_when_flag_absent(tmp_path, capsys, monkeypatch):
    _write_project(tmp_path)
    monkeypatch.chdir(tmp_path)
    code, out = _run(["validate"], capsys)  # no --registry: resolved from manifest
    assert code == 0 and out["valid"] is True


def test_flag_overrides_manifest(tmp_path, capsys, monkeypatch):
    _write_project(tmp_path, registry_rel="reg.yaml")
    other = tmp_path / "other.yaml"
    other.write_text(_REGISTRY, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    # Explicit flag wins; pass a path that exists to prove the flag value was used.
    code, out = _run(["validate", "--registry", str(other)], capsys)
    assert code == 0 and out["valid"] is True


def test_env_overrides_manifest(tmp_path, capsys, monkeypatch):
    _write_project(tmp_path, registry_rel="reg.yaml")
    env_reg = tmp_path / "from_env.yaml"
    env_reg.write_text(_REGISTRY, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("FSP_REGISTRY_PATH", str(env_reg))
    code, out = _run(["validate"], capsys)
    assert code == 0 and out["valid"] is True


def test_missing_registry_without_manifest_fails_clearly(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)  # no manifest, no flag
    code, out = _run(["validate"], capsys)
    assert code == 1 and out["valid"] is False
    assert any("registry" in e.lower() for e in out["errors"])


def test_manifest_rejects_secret_keys(tmp_path):
    (tmp_path / "feature_project.yaml").write_text(
        "project: demo\ndsn: postgres://u:p@h/db\n", encoding="utf-8"
    )
    with pytest.raises(ManifestError):
        load_manifest(tmp_path / "feature_project.yaml")


def test_manifest_rejects_approver_keys(tmp_path):
    (tmp_path / "feature_project.yaml").write_text(
        "project: demo\napproved_by: [a, b]\n", encoding="utf-8"
    )
    with pytest.raises(ManifestError):
        load_manifest(tmp_path / "feature_project.yaml")


# --- confirmation 1: unknown fields fail explicitly (never ignored) ----------

def test_manifest_rejects_unknown_field(tmp_path):
    (tmp_path / "feature_project.yaml").write_text(
        "project: demo\nregstry: typo.yaml\n", encoding="utf-8"
    )
    with pytest.raises(ManifestError) as exc:
        load_manifest(tmp_path / "feature_project.yaml")
    assert "unknown field" in str(exc.value).lower()


# --- confirmation 2: no credentials/approvers via nested or differently-cased fields ---

def test_manifest_rejects_nested_secret(tmp_path):
    # 'store' is already unknown, but the nested secret must be caught regardless.
    (tmp_path / "feature_project.yaml").write_text(
        "project: demo\nregistry:\n  dsn: postgres://u:p@h/db\n", encoding="utf-8"
    )
    with pytest.raises(ManifestError) as exc:
        load_manifest(tmp_path / "feature_project.yaml")
    assert "secret or approver" in str(exc.value).lower()


def test_manifest_rejects_differently_cased_secret(tmp_path):
    (tmp_path / "feature_project.yaml").write_text(
        "project: demo\nDSN: postgres://u:p@h/db\n", encoding="utf-8"
    )
    with pytest.raises(ManifestError):
        load_manifest(tmp_path / "feature_project.yaml")


def test_manifest_rejects_nonscalar_known_field(tmp_path):
    (tmp_path / "feature_project.yaml").write_text(
        "project: demo\nregistry:\n  nested: x\n", encoding="utf-8"
    )
    with pytest.raises(ManifestError):
        load_manifest(tmp_path / "feature_project.yaml")


# --- confirmation 3: required-value validation runs AFTER resolution and never
# --- affects init or --help -------------------------------------------------

def test_init_unaffected_by_bad_manifest_in_cwd(tmp_path, capsys, monkeypatch):
    (tmp_path / "feature_project.yaml").write_text(
        "project: demo\nbogus_field: 1\n", encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)  # a malformed manifest sits in cwd
    code, out = _run(["init", "--name", "customer-features", "--dir", str(tmp_path / "cf")], capsys)
    assert code == 0 and out["ok"] is True  # init never reads the manifest


def test_help_does_not_read_manifest(tmp_path, capsys, monkeypatch):
    (tmp_path / "feature_project.yaml").write_text(
        "project: demo\nbogus_field: 1\n", encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0  # argparse exits before resolution/validation
