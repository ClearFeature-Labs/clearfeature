"""``fsctl build-training-dataset`` — adapter over POST /v1/training-datasets/build.

The CLI sends observations in the exact public API shape and persists the public
response atomically. No local PIT joins, no offline-store reads.
"""

import json

from tests.cli._fake_api import http_error, install, training_response

from fintech_feature_platform.cli.fsctl import main

OBS_1 = {
    "entity": {"user_id": "u0000", "application_id": "a0000", "report_id": "r0000"},
    "observation_ts": "2026-05-01T11:00:00+00:00",
}
OBS_2 = {
    "entity": {"user_id": "u0001", "application_id": "a0001", "report_id": "r0001"},
    "observation_ts": "2026-05-02T11:00:00+00:00",
    "context": {"label": 1},
}


def _run_json(argv, capsys):
    code = main(argv)
    out = capsys.readouterr()
    assert "Traceback" not in out.out and "Traceback" not in out.err
    return code, json.loads(out.out.strip()), out


def _write_observations(tmp_path, rows=(OBS_1, OBS_2)):
    path = tmp_path / "observations.jsonl"
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    return path


def _argv(obs_path, out_path, extra=()):
    return [
        "build-training-dataset",
        "--view", "credit_model_inputs",
        "--view-version", "1",
        "--feature", "monthly_income",
        "--feature", "active_monthly_payment",
        "--observations", str(obs_path),
        "--output", str(out_path),
        *extra,
    ]


# --- 25: observation rows match the API model --------------------------------------

def test_request_matches_training_api_model(tmp_path, monkeypatch, capsys):
    transport = install(monkeypatch, training_response())
    obs, out_file = _write_observations(tmp_path), tmp_path / "out.json"
    code, _, _ = _run_json(_argv(obs, out_file), capsys)
    assert code == 0
    assert transport.urls() == ["http://127.0.0.1:8000/v1/training-datasets/build"]
    (body,) = transport.bodies()
    assert body == {
        "view": "credit_model_inputs",
        "view_version": 1,
        "features": ["monthly_income", "active_monthly_payment"],  # user order, deduped
        "observations": [OBS_1, OBS_2],
        "missing_policy": "keep_null",
        "safety_gap_seconds": 0,
    }


def test_malformed_observations_fail_before_http(tmp_path, monkeypatch, capsys):
    transport = install(monkeypatch)
    path = tmp_path / "observations.jsonl"
    path.write_text(json.dumps(OBS_1) + "\n{broken\n", encoding="utf-8")
    code, payload, _ = _run_json(_argv(path, tmp_path / "out.json"), capsys)
    assert code == 1
    assert "line 2" in payload["errors"][0]
    assert transport.requests == []


def test_observation_missing_required_field_fails_before_http(tmp_path, monkeypatch, capsys):
    transport = install(monkeypatch)
    path = tmp_path / "observations.jsonl"
    path.write_text('{"entity": {"user_id": "u0"}}\n', encoding="utf-8")
    code, payload, _ = _run_json(_argv(path, tmp_path / "out.json"), capsys)
    assert code == 1
    assert "observation_ts" in payload["errors"][0]
    assert transport.requests == []


# --- 26: safety gap and missing policy forwarded exactly ---------------------------

def test_safety_gap_and_missing_policy_forwarded(tmp_path, monkeypatch, capsys):
    transport = install(monkeypatch, training_response(safety_gap_seconds=3600))
    obs, out_file = _write_observations(tmp_path), tmp_path / "out.json"
    code, _, _ = _run_json(
        _argv(obs, out_file, ["--safety-gap-seconds", "3600", "--missing-policy", "error"]),
        capsys,
    )
    assert code == 0
    (body,) = transport.bodies()
    assert body["safety_gap_seconds"] == 3600
    assert body["missing_policy"] == "error"


# --- 27: response written atomically to the requested output path ------------------

def test_response_written_to_output_path(tmp_path, monkeypatch, capsys):
    response = training_response()
    install(monkeypatch, response)
    obs = _write_observations(tmp_path)
    out_file = tmp_path / "demo_output" / "training_dataset.json"
    code, payload, _ = _run_json(_argv(obs, out_file), capsys)
    assert code == 0
    assert json.loads(out_file.read_text(encoding="utf-8")) == response
    assert payload["output"] == str(out_file)
    leftovers = [p for p in out_file.parent.iterdir() if p != out_file]
    assert leftovers == []  # no temp files left behind


# --- 28: failed request leaves no misleading output file ---------------------------

def test_failed_request_leaves_no_output_file(tmp_path, monkeypatch, capsys):
    install(monkeypatch, http_error(400, "unknown feature 'nope'"))
    obs = _write_observations(tmp_path)
    out_file = tmp_path / "out.json"
    code, payload, _ = _run_json(_argv(obs, out_file), capsys)
    assert code == 1
    assert payload["ok"] is False
    assert not out_file.exists()


# --- 29: summary counts are correct and the dataset is not dumped to stdout --------

def test_summary_counts_and_no_dataset_on_stdout(tmp_path, monkeypatch, capsys):
    response = training_response(missing_values=7)
    install(monkeypatch, response)
    obs, out_file = _write_observations(tmp_path), tmp_path / "out.json"
    code, payload, out = _run_json(_argv(obs, out_file), capsys)
    assert code == 0
    assert payload["ok"] is True
    assert payload["rows"] == response["summary"]["rows"]
    assert payload["features"] == response["summary"]["features"]
    assert payload["missing_values"] == 7
    assert payload["safety_gap_seconds"] == 0
    assert payload["view"] == "credit_model_inputs"
    assert payload["view_version"] == 1
    # The dataset itself must not be printed: a known cell value stays off stdout.
    assert "2148.95" not in out.out


# --- every filesystem failure in the output path is a bounded error ------------

def test_unwritable_output_parent_is_bounded_error(tmp_path, monkeypatch, capsys):
    # Failure BEFORE temp-file creation: the output "parent directory" is a file,
    # so mkdir raises. Must map to the JSON error contract — no traceback.
    install(monkeypatch, training_response())
    obs = _write_observations(tmp_path)
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory", encoding="utf-8")
    code, payload, _ = _run_json(_argv(obs, blocker / "out.json"), capsys)
    assert code == 1
    assert payload["ok"] is False
    assert "cannot write output file" in payload["errors"][0]


def test_failed_rename_cleans_temp_and_is_bounded(tmp_path, monkeypatch, capsys):
    # Failure AFTER temp-file creation: the final rename explodes. The temp file is
    # removed, no output file appears, and the error is bounded JSON.
    import os as os_module

    real_replace = os_module.replace
    install(monkeypatch, training_response())
    obs = _write_observations(tmp_path)
    out_file = tmp_path / "outdir" / "out.json"

    def failing_replace(src, dst, *args, **kwargs):
        if str(dst) == str(out_file):
            raise PermissionError(13, "simulated rename failure")
        return real_replace(src, dst, *args, **kwargs)

    monkeypatch.setattr(
        "fintech_feature_platform.cli.data_workflow.os.replace", failing_replace
    )
    code, payload, _ = _run_json(_argv(obs, out_file), capsys)
    assert code == 1
    assert payload["ok"] is False
    assert "cannot write output file" in payload["errors"][0]
    assert not out_file.exists()
    assert list((tmp_path / "outdir").iterdir()) == []  # temp file cleaned up


# --- 30: exactly one HTTP call, no local PIT ---------------------------------------

def test_single_http_call_only(tmp_path, monkeypatch, capsys):
    transport = install(monkeypatch, training_response())
    obs, out_file = _write_observations(tmp_path), tmp_path / "out.json"
    code, _, _ = _run_json(_argv(obs, out_file), capsys)
    assert code == 0
    assert len(transport.requests) == 1
