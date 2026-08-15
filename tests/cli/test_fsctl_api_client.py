"""Shared HTTP/auth client for the fsctl data-workflow verbs — .

Configuration and failure mapping only; command-specific behavior lives in the
per-verb test files. Contract: bounded one-line JSON, exit 0/1, no traceback,
no secret in any output.
"""

import json

import pytest
from tests.cli._fake_api import TEST_API_KEY, http_error, install, manifest_response

from fintech_feature_platform.cli.fsctl import main


def _run_json(argv, capsys):
    code = main(argv)
    out = capsys.readouterr()
    assert "Traceback" not in out.out and "Traceback" not in out.err
    return code, json.loads(out.out.strip()), out


def _row() -> str:
    return json.dumps(
        {
            "entity_key": {"user_id": "u0", "application_id": "a0", "report_id": "r0"},
            "event_ts": "2026-05-01T09:00:00+00:00",
            "payload": {"monthly_income": 1000.0, "report_ts": "2026-05-01T09:00:00+00:00"},
        }
    )


def _ingest_argv(tmp_path, extra=()):
    path = tmp_path / "rows.jsonl"
    path.write_text(_row() + "\n", encoding="utf-8")
    return [
        "ingest",
        "--entity-type", "application_snapshot",
        "--source-name", "socdem_report",
        "--report-type", "socdem_report",
        "--input", str(path),
        *extra,
    ]


# --- 1: API URL precedence -----------------------------------------------------

def test_api_url_precedence_flag_env_fallback_default(monkeypatch):
    from fintech_feature_platform.cli.data_workflow import resolve_api_url

    monkeypatch.delenv("CLEARFEATURE_API_URL", raising=False)
    monkeypatch.delenv("FSP_API_URL", raising=False)
    assert resolve_api_url(None) == "http://127.0.0.1:8000"

    monkeypatch.setenv("FSP_API_URL", "http://fsp:1111/")
    assert resolve_api_url(None) == "http://fsp:1111"

    monkeypatch.setenv("CLEARFEATURE_API_URL", "http://clearfeature:2222")
    assert resolve_api_url(None) == "http://clearfeature:2222"

    assert resolve_api_url("http://flag:3333/") == "http://flag:3333"


def test_cli_flag_overrides_env_url(tmp_path, monkeypatch, capsys):
    transport = install(monkeypatch, manifest_response())
    monkeypatch.setenv("CLEARFEATURE_API_URL", "http://wrong:9")
    code, payload, _ = _run_json(
        _ingest_argv(tmp_path, ["--api-url", "http://flag:3333"]), capsys
    )
    assert code == 0 and payload["ok"] is True
    assert transport.urls() == ["http://flag:3333/v1/source-datasets/ingest-jsonl"]


# --- 2: missing API key ----------------------------------------------------------

def test_missing_api_key_is_actionable_bounded_error(tmp_path, monkeypatch, capsys):
    transport = install(monkeypatch, key=None)
    code, payload, out = _run_json(_ingest_argv(tmp_path), capsys)
    assert code == 1
    assert payload["ok"] is False
    (error,) = payload["errors"]
    assert "CLEARFEATURE_API_KEY" in error and "FSP_CLIENT_API_KEY" in error
    assert transport.requests == []  # nothing was sent unauthenticated


def test_fallback_env_key_is_accepted(tmp_path, monkeypatch, capsys):
    transport = install(monkeypatch, manifest_response(), key=None)
    monkeypatch.setenv("FSP_CLIENT_API_KEY", TEST_API_KEY)
    code, payload, _ = _run_json(_ingest_argv(tmp_path), capsys)
    assert code == 0 and payload["ok"] is True
    request, _timeout = transport.requests[0]
    assert request.get_header("Authorization") == f"Bearer {TEST_API_KEY}"


# --- 3: Authorization header ------------------------------------------------------

def test_authorization_header_sent_correctly(tmp_path, monkeypatch, capsys):
    transport = install(monkeypatch, manifest_response())
    code, _, _ = _run_json(_ingest_argv(tmp_path), capsys)
    assert code == 0
    request, _timeout = transport.requests[0]
    assert request.get_header("Authorization") == f"Bearer {TEST_API_KEY}"
    assert request.get_header("Content-type") == "application/json"


# --- 4: the key never appears in output/errors ------------------------------------

@pytest.mark.parametrize(
    "scripted",
    [
        http_error(500, "Internal Server Error"),
        http_error(401, {"status": "unauthorized", "error": "unknown API key"}),
    ],
)
def test_key_never_appears_in_error_output(tmp_path, monkeypatch, capsys, scripted):
    install(monkeypatch, scripted)
    code, payload, out = _run_json(_ingest_argv(tmp_path), capsys)
    assert code == 1 and payload["ok"] is False
    assert TEST_API_KEY not in out.out and TEST_API_KEY not in out.err


# --- 5: HTTP status and transport failures map to the error contract ---------------

@pytest.mark.parametrize("status", [401, 403, 404, 422, 500])
def test_http_statuses_map_to_bounded_json_error(tmp_path, monkeypatch, capsys, status):
    install(monkeypatch, http_error(status, f"detail for {status}"))
    code, payload, _ = _run_json(_ingest_argv(tmp_path), capsys)
    assert code == 1
    assert payload["ok"] is False
    (error,) = payload["errors"]
    assert str(status) in error
    assert f"detail for {status}" in error


def test_401_error_names_the_key_env_var(tmp_path, monkeypatch, capsys):
    install(monkeypatch, http_error(401, {"status": "unauthorized", "error": "unknown API key"}))
    code, payload, _ = _run_json(_ingest_argv(tmp_path), capsys)
    assert code == 1
    assert "CLEARFEATURE_API_KEY" in payload["errors"][0]


def test_transport_failure_maps_to_bounded_json_error(tmp_path, monkeypatch, capsys):
    import urllib.error

    install(monkeypatch, urllib.error.URLError(ConnectionRefusedError(61, "refused")))
    code, payload, _ = _run_json(_ingest_argv(tmp_path), capsys)
    assert code == 1
    assert payload["ok"] is False
    assert "cannot reach" in payload["errors"][0]


# --- 6: malformed / non-JSON server response ---------------------------------------

def test_non_json_server_response_is_bounded_error(tmp_path, monkeypatch, capsys):
    from tests.cli._fake_api import FakeResponse

    install(monkeypatch, FakeResponse(b"<html>bad gateway" + b"x" * 5000 + b"</html>"))
    code, payload, out = _run_json(_ingest_argv(tmp_path), capsys)
    assert code == 1
    (error,) = payload["errors"]
    assert "non-JSON" in error
    assert len(error) < 1000  # bounded, the 5KB body is not echoed


# --- 7: timeout -------------------------------------------------------------------

def test_http_timeout_is_bounded_error(tmp_path, monkeypatch, capsys):
    install(monkeypatch, TimeoutError("timed out"))
    code, payload, _ = _run_json(
        _ingest_argv(tmp_path, ["--http-timeout-seconds", "7"]), capsys
    )
    assert code == 1
    assert "timed out" in payload["errors"][0]


def test_http_timeout_flag_reaches_transport(tmp_path, monkeypatch, capsys):
    transport = install(monkeypatch, manifest_response())
    code, _, _ = _run_json(_ingest_argv(tmp_path, ["--http-timeout-seconds", "7.5"]), capsys)
    assert code == 0
    _request, timeout = transport.requests[0]
    assert timeout == 7.5
