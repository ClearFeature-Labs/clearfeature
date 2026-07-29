"""Startup-mode security checks in ISOLATED interpreters.

Why subprocesses: ``tests/conftest.py`` sets the explicit development bypass at import
time, and ``fintech_feature_platform.api.app`` builds its module-level ``app`` (the
real uvicorn entrypoint) the moment it is imported — so inside the pytest process the
env-driven startup path can never be exercised: the module is cached in disabled mode
for the whole run. Every case below therefore launches a FRESH interpreter with an
explicitly controlled environment and imports the real module, exactly like a
container start. The in-process tests in ``test_security.py`` stay authoritative for
role semantics (they pass explicit ``SecurityConfig`` objects and are immune to the
bypass); these tests pin the production import path.
"""

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

SERVICE_SECRET = "ab12" * 16
OPERATOR_SECRET = "cd34" * 16
VALID_KEYS = (
    f'[{{"key_id": "svc-iso", "role": "service", "secret": "{SERVICE_SECRET}"}},'
    f' {{"key_id": "ops-iso", "role": "operator", "secret": "{OPERATOR_SECRET}"}}]'
)


def run_isolated(script: str, env_overrides: dict[str, str]) -> subprocess.CompletedProcess:
    """Fresh interpreter + fresh imports + ONLY the environment we hand it."""
    env = {"PATH": os.environ["PATH"], "HOME": os.environ.get("HOME", "/tmp")}
    env.update(env_overrides)
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True, timeout=120, cwd=REPO_ROOT, env=env,
    )


IMPORT_APP = "import fintech_feature_platform.api.app\nprint('IMPORTED')\n"


def test_case_a_default_mode_without_keys_fails_closed():
    """FSP_SECURITY_MODE and FSP_API_KEYS unset -> the real import must fail."""
    result = run_isolated(IMPORT_APP, {})
    assert result.returncode != 0
    assert "at least one key" in result.stderr
    assert "IMPORTED" not in result.stdout


def test_case_b_explicit_development_bypass_starts_with_warning():
    result = run_isolated(IMPORT_APP, {
        "FSP_SECURITY_MODE": "disabled", "FSP_ENVIRONMENT": "development",
    })
    assert result.returncode == 0, result.stderr
    assert "IMPORTED" in result.stdout
    assert "security_disabled_mode" in result.stdout  # structured warning event


def test_case_c_bypass_outside_development_fails():
    for environment in ("pilot", "production"):
        result = run_isolated(IMPORT_APP, {
            "FSP_SECURITY_MODE": "disabled", "FSP_ENVIRONMENT": environment,
        })
        assert result.returncode != 0, environment
        assert "refusing to start" in result.stderr


def test_case_d_secure_mode_enforces_roles_on_the_real_app():
    """Valid keys -> the MODULE-LEVEL app (uvicorn's object) enforces the matrix."""
    script = (
        "from fastapi.testclient import TestClient\n"
        "from fintech_feature_platform.api.app import app\n"
        f"service = {{'Authorization': 'Bearer {SERVICE_SECRET}'}}\n"
        f"operator = {{'Authorization': 'Bearer {OPERATOR_SECRET}'}}\n"
        "client = TestClient(app)\n"
        "assert client.get('/health').status_code == 200\n"
        "assert client.get('/v1/observability/metrics').status_code == 401\n"
        "assert client.get('/v1/observability/metrics', headers=service).status_code == 403\n"
        "assert client.get('/v1/observability/metrics', headers=operator).status_code == 200\n"
        "assert client.get('/docs').status_code == 404\n"
        "print('MATRIX-OK')\n"
    )
    result = run_isolated(script, {
        "FSP_SECURITY_MODE": "api_key", "FSP_API_KEYS": VALID_KEYS,
    })
    assert result.returncode == 0, result.stderr
    assert "MATRIX-OK" in result.stdout
    assert "security_disabled_mode" not in result.stdout


def test_case_e_conftest_bypass_requires_its_explicit_import():
    """The suite bypass cannot leak into cases A-D: it exists only in a process that
    explicitly imports tests/conftest.py BEFORE the app module (which is exactly what
    pytest does, and exactly what these isolated interpreters never do)."""
    script = (
        "import importlib.util\n"
        "spec = importlib.util.spec_from_file_location('tc', 'tests/conftest.py')\n"
        "module = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(module)\n"
        + IMPORT_APP
    )
    # Same (empty) security env as case A; the ONLY difference is the conftest import.
    result = run_isolated(script, {})
    assert result.returncode == 0, result.stderr
    assert "security_disabled_mode" in result.stdout  # structured warning event
    # And without that import, the identical environment fails closed (case A).
    control = run_isolated(IMPORT_APP, {})
    assert control.returncode != 0
