"""fsctl error contract for expected user/configuration failures.

earlier-revision found two paths leaking raw tracebacks (malformed feature_project.yaml YAML and
broken --udfs provider imports). Every EXPECTED user failure must produce the existing
machine-readable contract: one deterministic JSON object with {"ok": false, "errors":
[...]} on stdout, non-zero exit, no traceback. Platform bugs still raise on purpose.
"""

import json

import pytest

from fintech_feature_platform.cli.fsctl import main
from fintech_feature_platform.cli.harness import UdfProviderError, load_udfs
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

SENTINEL = "sentinel-Bearer-T0ken-DO-NOT-LEAK"


def _run_json(argv, capsys):
    """Run main(); the ONLY stdout must be one parseable JSON object (no traceback)."""
    code = main(argv)  # must not raise for expected user errors
    out = capsys.readouterr()
    assert "Traceback" not in out.out and "Traceback" not in out.err
    return code, json.loads(out.out.strip())


# --- A/B: malformed manifest -------------------------------------------------

def test_malformed_manifest_yaml_is_json_error_with_location(tmp_path, monkeypatch, capsys):
    (tmp_path / "feature_project.yaml").write_text("registry: [", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    code, payload = _run_json(["validate"], capsys)
    assert code == 1
    assert payload["ok"] is False
    (error,) = payload["errors"]
    assert "feature_project.yaml is not valid YAML" in error
    assert "line 1" in error  # concise parser location, not a stack


def test_manifest_wrong_top_level_shape_is_json_error(tmp_path, monkeypatch, capsys):
    (tmp_path / "feature_project.yaml").write_text("- a\n- b\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    code, payload = _run_json(["validate"], capsys)
    assert code == 1
    assert "must be a mapping" in payload["errors"][0]


def test_manifest_invalid_field_type_is_json_error(tmp_path, monkeypatch, capsys):
    (tmp_path / "feature_project.yaml").write_text(
        "project: demo\nregistry: {nested: map}\n", encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)
    code, payload = _run_json(["validate"], capsys)
    assert code == 1
    assert "must be a string" in payload["errors"][0]


def test_manifest_has_no_required_fields_missing_fields_are_fine(tmp_path):
    # The manifest is defaults-only by contract: an empty mapping is valid.
    (tmp_path / "feature_project.yaml").write_text("{}\n", encoding="utf-8")
    manifest = load_manifest(tmp_path / "feature_project.yaml")
    assert manifest.registry is None


def test_unreadable_manifest_is_manifest_error(tmp_path):
    with pytest.raises(ManifestError, match="cannot be read"):
        load_manifest(tmp_path / "does_not_exist" / "feature_project.yaml")


# --- C/D/E: broken UDF provider ----------------------------------------------

def _project(tmp_path):
    (tmp_path / "reg.yaml").write_text(_REGISTRY, encoding="utf-8")
    (tmp_path / "entity.json").write_text('{"sources": {"src": {"x": 1}}}', encoding="utf-8")
    return [
        "run-local", "--registry", str(tmp_path / "reg.yaml"),
        "--feature", "f", "--entity-json", str(tmp_path / "entity.json"),
    ]


def test_missing_provider_module_is_json_error(tmp_path, capsys):
    code, payload = _run_json(_project(tmp_path) + ["--udfs", "no.such.module:build"], capsys)
    assert code == 1
    (error,) = payload["errors"]
    assert "provider module 'no.such.module' cannot be imported" in error
    assert "uv pip install -e ." in error  # actionable remediation


def test_missing_provider_callable_is_json_error(tmp_path, capsys):
    code, payload = _run_json(_project(tmp_path) + ["--udfs", "json:not_a_real_attr"], capsys)
    assert code == 1
    assert "has no attribute 'not_a_real_attr'" in payload["errors"][0]


def test_provider_module_raising_at_import_is_json_error(tmp_path, monkeypatch, capsys):
    (tmp_path / "exploding_provider.py").write_text(
        "raise RuntimeError('boom at import time')\n", encoding="utf-8"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    code, payload = _run_json(_project(tmp_path) + ["--udfs", "exploding_provider:build"], capsys)
    assert code == 1
    (error,) = payload["errors"]
    assert "raised while importing" in error
    assert "RuntimeError: boom at import time" in error  # original type preserved


def test_provider_callable_raising_is_json_error(tmp_path, monkeypatch, capsys):
    (tmp_path / "failing_provider.py").write_text(
        "def build():\n    raise KeyError('missing config')\n", encoding="utf-8"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    code, payload = _run_json(_project(tmp_path) + ["--udfs", "failing_provider:build"], capsys)
    assert code == 1
    assert "raised while building the UDF registry" in payload["errors"][0]


def test_bad_udfs_spec_format_is_json_error(tmp_path, capsys):
    code, payload = _run_json(_project(tmp_path) + ["--udfs", "no-colon-here"], capsys)
    assert code == 1
    assert "module.path:callable_or_attr" in payload["errors"][0]


def test_udf_provider_error_type_is_a_value_error():
    # Existing callers that caught ValueError keep working (contract preserved).
    assert issubclass(UdfProviderError, ValueError)
    with pytest.raises(ValueError):
        load_udfs("still-no-colon")


# --- F: shadowing rejection stays fail-closed, with a hint ---------------------

def test_shadowing_rejection_message_has_actionable_hint():
    import inspect

    from fintech_feature_platform.fs_core.runtime import artifact_verifier

    source = inspect.getsource(artifact_verifier)
    assert "does not belong to the verified distribution" in source  # still fail-closed
    assert "shadowing the installed package" in source  # the earlier hint


# --- G: no secret leakage ------------------------------------------------------

def test_manifest_error_output_carries_no_secret_values(tmp_path, monkeypatch, capsys):
    # A manifest that both is rejected for a forbidden key AND contains a secret value:
    # the error must name the KEY, never echo the VALUE.
    (tmp_path / "feature_project.yaml").write_text(
        f"project: demo\napi_key: {SENTINEL}\n", encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)
    code = main(["validate"])
    out = capsys.readouterr().out
    assert code == 1
    assert SENTINEL not in out
    assert "must not contain secret" in out
