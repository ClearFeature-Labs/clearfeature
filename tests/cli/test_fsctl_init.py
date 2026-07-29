"""fsctl init: domain-neutral Feature Project scaffold.

Tests call ``main([...])`` directly (Docker-free, no install) and parse the printed JSON.
The scaffold round-trip proves a fresh external project validates and passes golden tests
through the real ComputeCore with no ``PYTHONPATH`` hack (the package dir is placed on
``sys.path`` in-process, standing in for an editable install).
"""

import json
import sys

from fintech_feature_platform.cli.fsctl import main


def _run(argv, capsys):
    code = main(argv)
    out = json.loads(capsys.readouterr().out)
    return code, out


def test_init_creates_expected_tree(tmp_path, capsys):
    code, out = _run(["init", "--name", "customer-features", "--dir", str(tmp_path / "cf")], capsys)
    assert code == 0
    assert out["ok"] is True
    root = tmp_path / "cf"
    expected = [
        "feature_project.yaml",
        "pyproject.toml",
        "README.md",
        ".gitignore",
        "customer_features/__init__.py",
        "customer_features/features.py",
        "customer_features/registry/features_v1.yaml",
        "customer_features/tests/golden.yaml",
        "customer_features/tests/entity.json",
    ]
    for rel in expected:
        assert (root / rel).is_file(), f"missing {rel}"

    manifest = (root / "feature_project.yaml").read_text(encoding="utf-8")
    assert "requires_core" in manifest
    assert "udf_provider" in manifest

    # pyproject must be wheel-buildable (has a build-system table).
    pyproject = (root / "pyproject.toml").read_text(encoding="utf-8")
    assert "[build-system]" in pyproject

    # .gitignore must exclude environments, build output, wheels/caches, package metadata.
    gitignore = (root / ".gitignore").read_text(encoding="utf-8")
    for pattern in (".venv/", "dist/", ".wheels/", "__pycache__/", "*.egg-info/"):
        assert pattern in gitignore, f".gitignore missing {pattern}"


def test_init_is_domain_neutral(tmp_path, capsys):
    _run(["init", "--name", "customer-features", "--dir", str(tmp_path / "cf")], capsys)
    blob = ""
    for path in (tmp_path / "cf").rglob("*"):
        if path.is_file():
            blob += path.read_text(encoding="utf-8").lower()
    for token in ("credit", "loan", "pd_model", "risk_score", "borrower"):
        assert token not in blob, f"scaffold leaked domain token {token!r}"


def test_init_refuses_nonempty_dir(tmp_path, capsys):
    target = tmp_path / "cf"
    target.mkdir()
    (target / "existing.txt").write_text("x", encoding="utf-8")
    code, out = _run(["init", "--name", "customer-features", "--dir", str(target)], capsys)
    assert code == 1
    assert out["ok"] is False
    assert out["errors"]


def test_scaffold_roundtrip_validate_and_test(tmp_path, capsys, monkeypatch):
    _run(["init", "--name", "customer-features", "--dir", str(tmp_path / "cf")], capsys)
    root = tmp_path / "cf"

    # Stand in for an editable install: make the package importable.
    monkeypatch.syspath_prepend(str(root))
    for mod in [m for m in sys.modules if m.startswith("customer_features")]:
        del sys.modules[mod]

    registry = str(root / "customer_features" / "registry" / "features_v1.yaml")
    tests = str(root / "customer_features" / "tests" / "golden.yaml")
    provider = "customer_features.features:build_udfs"

    code, out = _run(["validate", "--registry", registry], capsys)
    assert code == 0 and out["valid"] is True
    assert out["features_count"] == 2

    code, out = _run(["test", "--registry", registry, "--tests", tests, "--udfs", provider], capsys)
    assert code == 0 and out["ok"] is True
    assert out["passed"] == out["total"] and out["total"] >= 2
