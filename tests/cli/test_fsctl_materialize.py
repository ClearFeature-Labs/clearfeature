"""``fsctl materialize`` — thin adapter over POST /v1/batch/jobs + status polling.

Pinned decisions under test:
- scope is exactly {"type": "source_dataset_manifests", "manifest_ids": [...]} on the
  existing endpoint — no boolean partial-materialization policy exists:
  every DAG-required source is mandatory per entity, missing ones fail as
  deterministic per-item errors;
- the derived idempotency key is stable across manifest/feature input order and
  excludes API URL, key, time, and local paths;
- exit codes: 0 = completed (or submit-only accepted), 2 = completed_with_errors,
  1 = failed / publish_failed / timeout / validation / auth / transport.
"""

import json

from tests.cli._fake_api import accept_response, install, job_status

from fintech_feature_platform.cli.fsctl import main

VIEW_ARGS = ["--view", "credit_model_inputs", "--view-version", "1"]
TWO_MANIFESTS = ["--manifest-id", "sdm_b", "--manifest-id", "sdm_a"]


def _run_json(argv, capsys):
    code = main(argv)
    out = capsys.readouterr()
    assert "Traceback" not in out.out and "Traceback" not in out.err
    return code, json.loads(out.out.strip())


def _argv(extra=()):
    return ["materialize", *VIEW_ARGS, *TWO_MANIFESTS, "--feature-group", "model_inputs", *extra]


def _wait_argv(extra=()):
    return _argv(["--wait", "--poll-interval-seconds", "0", *extra])


def _submitted_key(transport) -> str:
    return transport.bodies()[0]["idempotency_key"]


# --- 14: the request builds the multi-manifest scope exactly ----------------------------------

def test_request_builds_source_dataset_manifests_scope_exactly(monkeypatch, capsys):
    transport = install(monkeypatch, accept_response("job_x"))
    code, payload = _run_json(_argv(), capsys)
    assert code == 0
    assert transport.urls() == ["http://127.0.0.1:8000/v1/batch/jobs"]
    (body,) = transport.bodies()
    key = body.pop("idempotency_key")
    assert key.startswith("job_") and len(key) == len("job_") + 32
    assert body == {
        "view": "credit_model_inputs",
        "view_version": 1,
        "requested_features": [],
        "requested_feature_groups": ["model_inputs"],
        "write_online": False,
        "chunk_size": 100,
        "scope": {
            "type": "source_dataset_manifests",
            "manifest_ids": ["sdm_a", "sdm_b"],  # sorted unique
        },
    }


# --- 15/16: idempotency-key derivation is order-independent and deterministic ------

def test_manifest_order_does_not_change_idempotency_key(monkeypatch, capsys):
    t1 = install(monkeypatch, accept_response("job_x"))
    _run_json(_argv(), capsys)
    key_1 = _submitted_key(t1)

    t2 = install(monkeypatch, accept_response("job_x"))
    argv = ["materialize", *VIEW_ARGS, "--manifest-id", "sdm_a", "--manifest-id", "sdm_b",
            "--feature-group", "model_inputs"]
    _run_json(argv, capsys)
    assert _submitted_key(t2) == key_1
    assert t2.bodies()[0]["scope"]["manifest_ids"] == ["sdm_a", "sdm_b"]


def test_feature_and_group_order_is_normalized_deterministically(monkeypatch, capsys):
    t1 = install(monkeypatch, accept_response("job_x"))
    _run_json(_argv(["--feature", "b_feat", "--feature", "a_feat"]), capsys)
    t2 = install(monkeypatch, accept_response("job_x"))
    _run_json(_argv(["--feature", "a_feat", "--feature", "b_feat", "--feature", "a_feat"]), capsys)

    assert t1.bodies()[0]["requested_features"] == ["a_feat", "b_feat"]
    assert t2.bodies()[0]["requested_features"] == ["a_feat", "b_feat"]
    assert _submitted_key(t1) == _submitted_key(t2)


def test_key_changes_when_the_request_changes(monkeypatch, capsys):
    t1 = install(monkeypatch, accept_response("job_x"))
    _run_json(_argv(), capsys)
    t2 = install(monkeypatch, accept_response("job_x"))
    _run_json(_argv(["--chunk-size", "50"]), capsys)
    assert _submitted_key(t1) != _submitted_key(t2)


def test_key_is_independent_of_api_url(monkeypatch, capsys):
    t1 = install(monkeypatch, accept_response("job_x"))
    _run_json(_argv(), capsys)
    t2 = install(monkeypatch, accept_response("job_x"))
    _run_json(_argv(["--api-url", "http://elsewhere:9999"]), capsys)
    assert _submitted_key(t1) == _submitted_key(t2)


def test_explicit_idempotency_key_is_used_verbatim(monkeypatch, capsys):
    transport = install(monkeypatch, accept_response("my-key"))
    code, payload = _run_json(_argv(["--idempotency-key", "my-key"]), capsys)
    assert code == 0
    assert _submitted_key(transport) == "my-key"


# --- no boolean partial-materialization policy -----------

def test_scope_carries_no_partial_materialization_boolean(monkeypatch, capsys):
    # All DAG-required sources are mandatory per entity; there is no client-side
    # switch. A future partial-materialization mode needs an explicit contract.
    transport = install(monkeypatch, accept_response("job_x"))
    _run_json(_argv(), capsys)
    scope = transport.bodies()[0]["scope"]
    assert sorted(scope) == ["manifest_ids", "type"]


def test_require_all_sources_flag_is_rejected(monkeypatch, capsys):
    import pytest

    install(monkeypatch)
    with pytest.raises(SystemExit):  # argparse usage error: the flag no longer exists
        _run_json(_argv(["--require-all-sources"]), capsys)


# --- validation before HTTP -------------------------------------------------------

def test_no_feature_selector_fails_before_http(monkeypatch, capsys):
    transport = install(monkeypatch)
    code, payload = _run_json(["materialize", *VIEW_ARGS, *TWO_MANIFESTS], capsys)
    assert code == 1
    assert payload["ok"] is False
    assert "--feature" in payload["errors"][0]
    assert transport.requests == []


# --- 19: submit-only returns the accepted job -------------------------------------

def test_submit_only_returns_accepted_job(monkeypatch, capsys):
    install(monkeypatch, accept_response("job_x"))
    code, payload = _run_json(_argv(), capsys)
    assert code == 0
    assert payload["ok"] is True
    assert payload["job_id"] == "job_x"
    assert payload["status"] == "accepted"
    assert payload["manifest_ids"] == ["sdm_a", "sdm_b"]
    assert payload["total_items"] == 1000
    assert payload["chunk_count"] == 10
    assert payload["status_url"] == "/v1/batch/jobs/job_x"


# --- 20: --wait polls to completed ------------------------------------------------

def test_wait_polls_to_completed(monkeypatch, capsys):
    transport = install(
        monkeypatch,
        accept_response("job_x"),
        job_status("job_x", status="running"),
        job_status("job_x", status="completed"),
    )
    code, payload = _run_json(_wait_argv(), capsys)
    assert code == 0
    assert payload["ok"] is True
    assert payload["status"] == "completed"
    assert payload["completed_items"] == 1000
    assert payload["failed_items"] == 0
    assert transport.urls()[1:] == [
        "http://127.0.0.1:8000/v1/batch/jobs/job_x",
        "http://127.0.0.1:8000/v1/batch/jobs/job_x",
    ]


# --- 21: completed_with_errors is honest and exits 2 ------------------------------

def _cwe_status():
    chunks = {
        "job_x:0": {
            "chunk_id": "job_x:0", "chunk_index": 0, "status": "completed_with_errors",
            "item_count": 3, "ok_items": 2, "failed_items": 1,
            "first_errors": ["no report_ref bound for source 'socdem_report'"],
            "updated_at": "2026-08-05T10:00:00+00:00",
            "finished_at": "2026-08-05T10:00:01+00:00",
        }
    }
    return job_status(
        "job_x", status="completed_with_errors", chunks=chunks,
        total_items=3, failed_items=1, completed_chunks=0, failed_chunks=0,
    )


def test_completed_with_errors_exits_2_with_bounded_first_errors(monkeypatch, capsys):
    install(monkeypatch, accept_response("job_x"), _cwe_status())
    code, payload = _run_json(_wait_argv(), capsys)
    assert code == 2  # pinned: 0 completed / 2 completed_with_errors / 1 everything else
    assert payload["ok"] is False
    assert payload["status"] == "completed_with_errors"
    assert payload["completed_items"] == 2
    assert payload["failed_items"] == 1
    assert payload["first_errors"] == ["no report_ref bound for source 'socdem_report'"]
    assert payload["errors"]  # machine contract: ok=false always carries errors


# --- 22: failed and timeout -------------------------------------------------------

def test_failed_job_exits_1(monkeypatch, capsys):
    install(monkeypatch, accept_response("job_x"), job_status("job_x", status="failed"))
    code, payload = _run_json(_wait_argv(), capsys)
    assert code == 1
    assert payload["ok"] is False
    assert payload["status"] == "failed"


def test_publish_failed_job_exits_1(monkeypatch, capsys):
    install(
        monkeypatch,
        accept_response("job_x"),
        job_status("job_x", status="publish_failed",
                   error_summary={"published_chunks": 3, "failed_chunk_index": 3,
                                  "error": "kafka unavailable"}),
    )
    code, payload = _run_json(_wait_argv(), capsys)
    assert code == 1
    assert payload["status"] == "publish_failed"


def test_wait_timeout_is_bounded_and_names_the_job(monkeypatch, capsys):
    install(
        monkeypatch,
        accept_response("job_x"),
        job_status("job_x", status="running"),
    )
    code, payload = _run_json(_wait_argv(["--timeout-seconds", "0"]), capsys)
    assert code == 1
    assert payload["ok"] is False
    (error,) = payload["errors"]
    assert "job_x" in error
    assert "/v1/batch/jobs/job_x" in error


# --- 23: idempotent rerun returns the same job id ---------------------------------

def test_idempotent_rerun_submits_same_key_and_returns_same_job(monkeypatch, capsys):
    t1 = install(monkeypatch, accept_response("job_derived"))
    _, p1 = _run_json(_argv(), capsys)
    t2 = install(monkeypatch, accept_response("job_derived"))
    _, p2 = _run_json(_argv(), capsys)
    assert _submitted_key(t1) == _submitted_key(t2)
    assert p1["job_id"] == p2["job_id"] == "job_derived"


# --- 24: no payload/report-ref/object-key fields in CLI output --------------------

def test_output_carries_no_payloads_or_refs(monkeypatch, capsys):
    install(monkeypatch, accept_response("job_x"), _cwe_status())
    code, payload = _run_json(_wait_argv(), capsys)
    assert code == 2
    text = json.dumps(payload)
    assert "chunks" not in payload  # raw per-chunk dump is not exposed
    assert '"payload"' not in text
    assert "report_ref\":" not in text.replace("no report_ref bound", "")
    assert "source_refs" not in text
