import json

import pytest

from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.backfill import run_raw_json_backfill
from fintech_feature_platform.fs_core.models import EntityKey

_VIEW = "user_credit_risk"


def _row(user_id="1", *, report_ts="2026-01-10T09:00:00Z", income=1200, both=False):
    inline = {
        "credit_report": {
            "report_type": "credit_report",
            "report_ts": report_ts,
            "payload": {"declared_income": income, "monthly_obligations": 300},
        }
    }
    if both:
        inline["tax_report"] = {
            "report_type": "tax_report",
            "report_ts": report_ts,
            "payload": {"income": 2_000_000},
        }
    return json.dumps(
        {
            "entity": {"user_id": user_id, "application_id": "A1"},
            "inline_sources": inline,
            "context": {"request_id": f"req_{user_id}", "source_file": "f.jsonl"},
        }
    )


def _run(lines, *, features=None, fail_fast=False, write_mode="offline_only"):
    backend = build_memory_backend()
    summary = run_raw_json_backfill(
        backend=backend,
        view=_VIEW,
        view_version=1,
        requested_features=features or ["declared_income"],
        lines=lines,
        write_mode=write_mode,
        fail_fast=fail_fast,
    )
    return backend, summary


def _key(user_id="1"):
    return EntityKey.from_mapping(
        {"user_id": user_id, "application_id": "A1"},
        key_order=["user_id", "application_id"],
    )


def test_single_row_writes_offline_not_online():
    backend, summary = _run([_row()])
    assert summary.rows_total == 1
    assert summary.rows_succeeded == 1
    assert summary.rows_failed == 0
    assert summary.features_written == 1
    assert summary.results[0].report_refs["credit_report"].startswith("rep_")
    # raw payload + metadata persisted
    report_ref = summary.results[0].report_refs["credit_report"]
    meta = backend.metas.get_meta(report_ref)
    assert backend.payloads.get_payload(meta.storage_uri)["declared_income"] == 1200
    # offline history written
    assert len(backend.offline.get(_key(), feature_name="declared_income")) == 1
    # online latest NOT written
    assert backend.online.get(_VIEW, 1, _key(), "declared_income", 1) is None


def test_multiple_rows():
    backend, summary = _run([_row("1"), _row("2"), _row("3")])
    assert (summary.rows_total, summary.rows_succeeded, summary.rows_failed) == (3, 3, 0)
    assert summary.features_written == 3
    assert backend.online.get(_VIEW, 1, _key("2"), "declared_income", 1) is None


def test_multiple_inline_sources_in_one_row():
    backend, summary = _run(
        [_row(both=True)], features=["declared_income", "debt_to_income_ratio"]
    )
    assert summary.rows_succeeded == 1
    assert set(summary.results[0].report_refs) == {"credit_report", "tax_report"}
    assert summary.features_written == 2


def test_blank_lines_skipped():
    backend, summary = _run(["", _row(), "   "])
    assert summary.rows_total == 1
    assert summary.rows_succeeded == 1


def test_fail_fast_false_collects_errors_and_continues():
    backend, summary = _run([_row("1"), "{not json", _row("2")], fail_fast=False)
    assert summary.rows_total == 3
    assert summary.rows_succeeded == 2
    assert summary.rows_failed == 1
    assert summary.errors[0]["row_index"] == 1
    assert summary.results[1].status == "error"


def test_fail_fast_true_stops_on_first_error():
    # malformed JSON -> json.JSONDecodeError (a ValueError subclass), re-raised fast.
    with pytest.raises(ValueError):
        _run([_row("1"), "{not json", _row("2")], fail_fast=True)


def test_malformed_json_row_handled():
    backend, summary = _run(["{bad json"], fail_fast=False)
    assert summary.rows_failed == 1
    assert summary.rows_succeeded == 0


def test_naive_report_ts_rejected():
    backend, summary = _run([_row(report_ts="2026-01-10T09:00:00")], fail_fast=False)
    assert summary.rows_failed == 1
    assert "timezone" in summary.errors[0]["error"].lower()


def test_missing_entity_key_rejected():
    line = json.dumps(
        {
            "entity": {"user_id": "1"},  # missing application_id
            "inline_sources": {
                "credit_report": {
                    "report_type": "credit_report",
                    "report_ts": "2026-01-10T09:00:00Z",
                    "payload": {"declared_income": 1200, "monthly_obligations": 300},
                }
            },
        }
    )
    backend, summary = _run([line], fail_fast=False)
    assert summary.rows_failed == 1


def test_unknown_view_raises():
    backend = build_memory_backend()
    with pytest.raises(ValueError):
        run_raw_json_backfill(
            backend=backend,
            view="nope",
            view_version=1,
            requested_features=["declared_income"],
            lines=[_row()],
        )


def test_default_write_mode_is_offline_only():
    backend, summary = _run([_row()])  # default write_mode
    assert summary.online_written == 0
    assert backend.online.get(_VIEW, 1, _key(), "declared_income", 1) is None


def test_write_mode_offline_only_skips_online():
    backend, summary = _run([_row()], write_mode="offline_only")
    assert summary.online_written == 0
    assert len(backend.offline.get(_key(), feature_name="declared_income")) == 1
    assert backend.online.get(_VIEW, 1, _key(), "declared_income", 1) is None


def test_write_mode_offline_and_online_writes_online():
    backend, summary = _run([_row(income=4242)], write_mode="offline_and_online")
    assert summary.online_written >= 1
    got = backend.online.get(_VIEW, 1, _key(), "declared_income", 1)
    assert got is not None and got.value == 4242


def test_unknown_write_mode_raises():
    with pytest.raises(ValueError):
        _run([_row()], write_mode="bogus")


def _rerun(backend, lines, **kw):
    return run_raw_json_backfill(
        backend=backend, view=_VIEW, view_version=1,
        requested_features=["declared_income"], lines=lines, **kw,
    )


def test_identical_rerun_skips_duplicates():
    backend, first = _run([_row()])
    assert first.features_written == 1 and first.duplicates_skipped == 0
    second = _rerun(backend, [_row()])
    assert second.features_written == 0 and second.duplicates_skipped == 1
    assert len(backend.offline.get(_key(), feature_name="declared_income")) == 1


def test_offline_and_online_rerun_does_not_rewrite_online():
    backend, first = _run([_row(income=4242)], write_mode="offline_and_online")
    assert first.online_written == 1
    second = _rerun(backend, [_row(income=4242)], write_mode="offline_and_online")
    assert second.duplicates_skipped == 1 and second.online_written == 0


def test_run_accounting_fields_present():
    backend, summary = _run([_row()])
    assert summary.run_id
    assert summary.started_at is not None and summary.finished_at is not None
    assert summary.duration_ms >= 0


def test_blank_lines_counted_in_rows_read():
    backend, summary = _run(["", _row(), "   "])
    assert summary.rows_read == 3
    assert summary.rows_skipped_blank == 2
    assert summary.rows_total == 1


def test_expected_rows_mismatch_warns():
    summary = _rerun(build_memory_backend(), [_row()], expected_rows=5)
    assert "expected_rows_mismatch" in summary.warnings
    assert summary.expected_rows == 5
