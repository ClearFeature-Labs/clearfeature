"""CLI smoke for the import/backfill scripts: `--summary-output` writes a JSON summary
with run accounting + input accounting fields. Memory backend, Docker-free."""

import importlib.util
import json
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]


def _load(script_name: str):
    path = _REPO / "scripts" / script_name
    spec = importlib.util.spec_from_file_location(script_name.replace(".py", ""), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_table_import_cli_writes_summary_json(tmp_path):
    config = tmp_path / "cfg.json"
    config.write_text(json.dumps({
        "view": "user_credit_risk", "view_version": 1,
        "entity_columns": ["user_id", "application_id"],
        "data_ts_column": "as_of_ts",
        "features": {"declared_income": {"column": "declared_income", "dtype": "int"}},
    }))
    rows = tmp_path / "rows.jsonl"
    rows.write_text(json.dumps({
        "user_id": "1", "application_id": "A1",
        "as_of_ts": "2026-01-10T09:00:00Z", "declared_income": 1000,
    }) + "\n")
    out = tmp_path / "summary.json"

    module = _load("import_table_features.py")
    rc = module.main([
        "--config", str(config), "--input", str(rows), "--format", "jsonl",
        "--expected-rows", "1", "--source-name", "dwh",
        "--summary-output", str(out),
    ])
    assert rc == 0
    summary = json.loads(out.read_text())
    assert summary["run_id"]
    assert summary["expected_rows"] == 1
    assert summary["source_checksum"]
    assert summary["duplicates_skipped"] == 0
    assert summary["features_written"] == 1


def test_backfill_cli_writes_summary_json(tmp_path):
    raw = tmp_path / "raw.jsonl"
    raw.write_text(json.dumps({
        "entity": {"user_id": "1", "application_id": "A1"},
        "inline_sources": {"credit_report": {
            "report_type": "credit_report", "report_ts": "2026-01-05T09:00:00Z",
            "payload": {"declared_income": 1200, "monthly_obligations": 300},
        }},
    }) + "\n")
    out = tmp_path / "summary.json"

    module = _load("backfill_raw_reports.py")
    rc = module.main([
        "--view", "user_credit_risk", "--view-version", "1",
        "--features", "declared_income", "--input", str(raw),
        "--expected-rows", "1", "--summary-output", str(out),
    ])
    assert rc == 0
    summary = json.loads(out.read_text())
    assert summary["run_id"]
    assert summary["expected_rows"] == 1
    assert summary["source_checksum"]
    assert "duplicates_skipped" in summary
    assert summary["rows_read"] == 1
