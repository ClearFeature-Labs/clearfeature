"""``fsctl ingest`` — thin adapter over POST /v1/source-datasets/ingest-jsonl.

The CLI reads canonical JSONL, validates transport shape only, submits ONE
request, and reports the manifest summary. No report transformation, no
client-side chunking (the server contract has no manifest append).
"""

import json

from tests.cli._fake_api import install, manifest_response

from fintech_feature_platform.cli.fsctl import main

ROW_1 = (
    '{"entity_key": {"user_id": "u0", "application_id": "a0", "report_id": "r0"},'
    ' "event_ts": "2026-05-01T09:00:00+00:00",'
    ' "available_at": "2026-05-01T10:00:00+00:00",'
    ' "payload": {"monthly_income": 2148.95, "report_ts": "2026-05-01T09:00:00+00:00"}}'
)
# Odd spacing on purpose: the CLI must pass the line through verbatim.
ROW_2 = (
    '{"entity_key":  {"user_id": "u1", "application_id": "a1", "report_id": "r1"},'
    ' "event_ts": "2026-05-02T09:00:00+00:00",'
    ' "payload":   {"monthly_income": 900.0}, "report_ref": "rep_custom01"}'
)


def _run_json(argv, capsys):
    code = main(argv)
    out = capsys.readouterr()
    assert "Traceback" not in out.out and "Traceback" not in out.err
    return code, json.loads(out.out.strip())


def _argv(path, extra=()):
    return [
        "ingest",
        "--entity-type", "application_snapshot",
        "--source-name", "socdem_report",
        "--report-type", "socdem_report",
        "--input", str(path),
        *extra,
    ]


def _write_input(tmp_path, text):
    path = tmp_path / "rows.jsonl"
    path.write_text(text, encoding="utf-8")
    return path


# --- 8/9/13: exact canonical request, verbatim lines, trusted available_at -------

def test_request_matches_ingestion_api_exactly(tmp_path, monkeypatch, capsys):
    transport = install(monkeypatch, manifest_response())
    path = _write_input(tmp_path, ROW_1 + "\n" + ROW_2 + "\n")
    code, payload = _run_json(
        _argv(path, ["--dataset-id", "ds_custom", "--created-by", "ops"]), capsys
    )
    assert code == 0 and payload["ok"] is True
    assert transport.urls() == ["http://127.0.0.1:8000/v1/source-datasets/ingest-jsonl"]
    (body,) = transport.bodies()
    assert body == {
        "entity_type": "application_snapshot",
        "source_name": "socdem_report",
        "report_type": "socdem_report",
        "lines": [ROW_1, ROW_2],  # verbatim, deterministic input order
        "dataset_id": "ds_custom",
        "created_by": "ops",
    }


def test_trusted_available_at_is_preserved_verbatim(tmp_path, monkeypatch, capsys):
    transport = install(monkeypatch, manifest_response())
    path = _write_input(tmp_path, ROW_1 + "\n")
    code, _ = _run_json(_argv(path), capsys)
    assert code == 0
    (body,) = transport.bodies()
    assert body["lines"] == [ROW_1]
    assert '"available_at": "2026-05-01T10:00:00+00:00"' in body["lines"][0]
    # Optional request fields are omitted when not passed, never invented.
    assert "dataset_id" not in body and "created_by" not in body


def test_blank_lines_are_skipped_and_order_preserved(tmp_path, monkeypatch, capsys):
    transport = install(monkeypatch, manifest_response())
    path = _write_input(tmp_path, ROW_1 + "\n\n   \n" + ROW_2 + "\n")
    code, _ = _run_json(_argv(path), capsys)
    assert code == 0
    (body,) = transport.bodies()
    assert body["lines"] == [ROW_1, ROW_2]


# --- 10: malformed local JSONL fails before any HTTP submission -------------------

def test_malformed_jsonl_fails_before_http(tmp_path, monkeypatch, capsys):
    transport = install(monkeypatch, manifest_response())
    path = _write_input(tmp_path, ROW_1 + "\nnot-json{{{\n")
    code, payload = _run_json(_argv(path), capsys)
    assert code == 1
    assert payload["ok"] is False
    (error,) = payload["errors"]
    assert "line 2" in error
    assert transport.requests == []  # nothing submitted


def test_non_object_jsonl_line_fails_before_http(tmp_path, monkeypatch, capsys):
    transport = install(monkeypatch, manifest_response())
    path = _write_input(tmp_path, ROW_1 + "\n[1, 2, 3]\n")
    code, payload = _run_json(_argv(path), capsys)
    assert code == 1
    assert "line 2" in payload["errors"][0]
    assert "JSON object" in payload["errors"][0]
    assert transport.requests == []


def test_missing_input_file_is_bounded_error(tmp_path, monkeypatch, capsys):
    transport = install(monkeypatch)
    code, payload = _run_json(_argv(tmp_path / "absent.jsonl"), capsys)
    assert code == 1
    assert payload["ok"] is False
    assert transport.requests == []


def test_empty_input_file_is_bounded_error(tmp_path, monkeypatch, capsys):
    transport = install(monkeypatch)
    path = _write_input(tmp_path, "\n\n")
    code, payload = _run_json(_argv(path), capsys)
    assert code == 1
    assert "no rows" in payload["errors"][0]
    assert transport.requests == []


# --- 11: rejected-row counts are surfaced honestly --------------------------------

def test_rejected_counts_surface_in_summary(tmp_path, monkeypatch, capsys):
    install(
        monkeypatch,
        manifest_response(item_count_read=2, item_count_written=1, item_count_rejected=1),
    )
    path = _write_input(tmp_path, ROW_1 + "\n" + ROW_2 + "\n")
    code, payload = _run_json(_argv(path), capsys)
    assert code == 0  # the API accepted the run and created the manifest
    assert payload["item_count_read"] == 2
    assert payload["item_count_written"] == 1
    assert payload["item_count_rejected"] == 1


# --- 12: stable summary output and full response saved with --output ---------------

def test_manifest_summary_is_stable_json(tmp_path, monkeypatch, capsys):
    response = manifest_response()
    install(monkeypatch, response)
    path = _write_input(tmp_path, ROW_1 + "\n")
    code, payload = _run_json(_argv(path), capsys)
    assert code == 0
    for key in (
        "ok", "manifest_id", "dataset_id", "source_name", "entity_type", "report_type",
        "item_count_read", "item_count_written", "item_count_duplicate",
        "item_count_rejected", "watermark_min_event_ts", "watermark_max_event_ts",
    ):
        assert key in payload, key
    assert payload["manifest_id"] == response["manifest_id"]
    assert payload["watermark_min_event_ts"] == response["watermark_min_event_ts"]


def test_output_file_receives_full_response(tmp_path, monkeypatch, capsys):
    response = manifest_response()
    install(monkeypatch, response)
    path = _write_input(tmp_path, ROW_1 + "\n")
    out_file = tmp_path / "demo_output" / "manifest.json"
    code, payload = _run_json(_argv(path, ["--output", str(out_file)]), capsys)
    assert code == 0
    assert json.loads(out_file.read_text(encoding="utf-8")) == response
    assert payload["output"] == str(out_file)
