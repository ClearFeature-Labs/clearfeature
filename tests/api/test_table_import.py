import importlib.util
from datetime import UTC, datetime

import pytest

from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.table_import import (
    TableFeatureImportConfig,
    _convert,
    read_rows,
    run_table_feature_import,
)
from fintech_feature_platform.fs_core.models import EntityKey
from fintech_feature_platform.fs_core.training import (
    TrainingObservation,
    build_training_dataset,
)

_VIEW = "user_credit_risk"
_KEY_ORDER = ["user_id", "application_id"]


def _key(user_id="1"):
    return EntityKey.from_mapping(
        {"user_id": user_id, "application_id": "A1"}, key_order=_KEY_ORDER
    )


def _config(
    *,
    data_ts_column="as_of_ts",
    default_data_ts=None,
    calc_ts_column=None,
    default_calc_ts=None,
    features=None,
):
    return TableFeatureImportConfig.model_validate(
        {
            "view": _VIEW,
            "view_version": 1,
            "entity_columns": ["user_id", "application_id"],
            "data_ts_column": data_ts_column,
            "default_data_ts": default_data_ts,
            "calc_ts_column": calc_ts_column,
            "default_calc_ts": default_calc_ts,
            "features": features
            or {"declared_income": {"column": "declared_income", "dtype": "int"}},
        }
    )


def _row(user_id="1", *, as_of="2026-01-10T09:00:00Z", income=1000, extra=None):
    row = {"user_id": user_id, "application_id": "A1", "declared_income": income}
    if as_of is not None:
        row["as_of_ts"] = as_of
    if extra:
        row.update(extra)
    return row


def _run(rows, *, config=None, write_mode="offline_only", fail_fast=False, **kw):
    backend = build_memory_backend()
    summary = run_table_feature_import(
        backend=backend,
        config=config or _config(),
        rows=rows,
        write_mode=write_mode,
        fail_fast=fail_fast,
        **kw,
    )
    return backend, summary


def _import(backend, rows, *, config=None, write_mode="offline_only", **kw):
    return run_table_feature_import(
        backend=backend, config=config or _config(), rows=rows,
        write_mode=write_mode, **kw,
    )


def _offline_count(backend, user_id="1"):
    return len(
        backend.offline.get(_key(user_id), feature_name="declared_income", feature_version=1)
    )


# --- dtype conversion --------------------------------------------------------

def test_convert_all_dtypes():
    assert _convert("hi", "string") == "hi"
    assert _convert("35", "int") == 35
    assert _convert("0.12", "float") == pytest.approx(0.12)
    assert _convert("true", "bool") is True
    assert _convert("0", "bool") is False
    assert _convert(True, "bool") is True
    assert _convert('{"a": 1}', "json") == {"a": 1}
    assert _convert({"a": 1}, "json") == {"a": 1}


# --- import behaviour --------------------------------------------------------

def test_jsonl_import_writes_offline_records():
    backend, summary = _run([_row(income=1000)])
    assert summary.rows_succeeded == 1
    assert summary.features_written == 1
    assert _offline_count(backend) == 1
    records = backend.offline.get(_key(), feature_name="declared_income")
    assert records[0].result.value == 1000


def test_csv_style_string_rows_convert_and_write():
    # csv.DictReader produces string values; dtype conversion handles them.
    backend, summary = _run([_row(income="2500")])
    assert summary.rows_succeeded == 1
    records = backend.offline.get(_key(), feature_name="declared_income")
    assert records[0].result.value == 2500


def test_imported_features_usable_by_pit_builder():
    # Backtesting import: declare the true availability (default_calc_ts) so the
    # historical rows are PIT-available at the past observation.
    backend, _ = _run(
        [_row(income=4000)],
        config=_config(default_calc_ts="2026-01-10T09:00:00Z"),
    )
    view = next(v for v in backend.registry.feature_views if v.name == _VIEW)
    dataset = build_training_dataset(
        offline=backend.offline,
        view=view,
        view_version=1,
        feature_names=["declared_income"],
        observations=[
            TrainingObservation(
                entity={"user_id": "1", "application_id": "A1"},
                observation_ts=datetime(2026, 1, 15, tzinfo=UTC),
            )
        ],
    )
    assert dataset.rows[0].features["declared_income"] == 4000
    # The imported rows retain both PIT clocks.
    record = backend.offline.get(_key(), feature_name="declared_income")[0]
    assert record.result.data_ts is not None
    assert record.result.calc_ts == datetime(2026, 1, 10, 9, tzinfo=UTC)


def test_import_default_calc_ts_is_now_and_gates_past_observations():
    # Without a declared calc_ts, imports default to now(): strict availability then
    # excludes them from a past observation (no lookahead: we didn't know it yet).
    backend, _ = _run([_row(income=4000)])  # default calc_ts = now
    view = next(v for v in backend.registry.feature_views if v.name == _VIEW)
    dataset = build_training_dataset(
        offline=backend.offline,
        view=view,
        view_version=1,
        feature_names=["declared_income"],
        observations=[
            TrainingObservation(
                entity={"user_id": "1", "application_id": "A1"},
                observation_ts=datetime(2026, 1, 15, tzinfo=UTC),  # far in the past
            )
        ],
    )
    assert dataset.rows[0].features["declared_income"] is None  # calc_ts > observation


def test_offline_only_does_not_update_online():
    backend, summary = _run([_row()], write_mode="offline_only")
    assert summary.online_written == 0
    assert backend.online.get(_VIEW, 1, _key(), "declared_income", 1) is None


def test_offline_and_online_updates_online():
    backend, summary = _run([_row(income=1234)], write_mode="offline_and_online")
    assert summary.online_written == 1
    got = backend.online.get(_VIEW, 1, _key(), "declared_income", 1)
    assert got is not None and got.value == 1234


def test_online_cas_respects_data_ts():
    backend = build_memory_backend()
    # Newer value first, then an older-dated import: CAS must keep the newer one.
    run_table_feature_import(
        backend=backend, config=_config(),
        rows=[_row(income=999, as_of="2026-02-01T00:00:00Z")],
        write_mode="offline_and_online",
    )
    run_table_feature_import(
        backend=backend, config=_config(),
        rows=[_row(income=111, as_of="2026-01-01T00:00:00Z")],
        write_mode="offline_and_online",
    )
    got = backend.online.get(_VIEW, 1, _key(), "declared_income", 1)
    assert got.value == 999  # older import did not overwrite


def test_unknown_feature_in_config_raises():
    config = _config(features={"nope": {"column": "nope", "dtype": "int"}})
    with pytest.raises(ValueError):
        _run([_row()], config=config)


def test_entity_columns_must_match_key_fields():
    config = _config()
    config = TableFeatureImportConfig.model_validate(
        {**config.model_dump(), "entity_columns": ["user_id"]}
    )
    with pytest.raises(ValueError):
        _run([_row()], config=config)


def test_missing_entity_key_row_error():
    row = {"application_id": "A1", "declared_income": 1, "as_of_ts": "2026-01-10T09:00:00Z"}
    backend, summary = _run([row], fail_fast=False)
    assert summary.rows_failed == 1
    assert summary.rows_succeeded == 0


def test_missing_entity_key_fail_fast_raises():
    row = {"application_id": "A1", "declared_income": 1, "as_of_ts": "2026-01-10T09:00:00Z"}
    with pytest.raises(KeyError):
        _run([row], fail_fast=True)


def test_naive_data_ts_row_error():
    backend, summary = _run([_row(as_of="2026-01-10T09:00:00")], fail_fast=False)
    assert summary.rows_failed == 1
    assert "timezone" in summary.errors[0]["error"].lower()


def test_default_data_ts_for_static_features():
    config = _config(data_ts_column=None, default_data_ts="2026-01-01T00:00:00Z")
    backend, summary = _run([_row(as_of=None, income=7)], config=config)
    assert summary.rows_succeeded == 1
    records = backend.offline.get(_key(), feature_name="declared_income")
    assert records[0].result.value == 7
    assert records[0].result.data_ts == datetime(2026, 1, 1, tzinfo=UTC)


def test_no_data_ts_source_raises():
    config = _config(data_ts_column=None, default_data_ts=None)
    with pytest.raises(ValueError):
        _run([_row()], config=config)


def test_missing_feature_value_skipped_and_counted():
    # declared_income column absent -> skipped + counted, row still succeeds (0 written).
    row = {"user_id": "1", "application_id": "A1", "as_of_ts": "2026-01-10T09:00:00Z"}
    backend, summary = _run([row])
    assert summary.missing_values == 1
    assert summary.features_written == 0
    assert _offline_count(backend) == 0


def test_fail_fast_false_collects_and_continues():
    bad = {"application_id": "A1", "declared_income": 1, "as_of_ts": "2026-01-10T09:00:00Z"}
    backend, summary = _run([_row("1"), bad, _row("2")], fail_fast=False)
    assert (summary.rows_total, summary.rows_succeeded, summary.rows_failed) == (3, 2, 1)


def test_naive_default_data_ts_rejected_by_config():
    with pytest.raises(ValueError):
        _config(data_ts_column=None, default_data_ts="2026-01-01T00:00:00")


def test_read_rows_jsonl(tmp_path):
    path = tmp_path / "f.jsonl"
    path.write_text('{"user_id": "1", "declared_income": 5}\n\n', encoding="utf-8")
    rows = read_rows(str(path), "jsonl")
    assert rows == [{"user_id": "1", "declared_income": 5}]


def test_read_rows_csv(tmp_path):
    path = tmp_path / "f.csv"
    path.write_text("user_id,declared_income\n1,5\n", encoding="utf-8")
    rows = read_rows(str(path), "csv")
    assert rows == [{"user_id": "1", "declared_income": "5"}]


def test_parquet_read_requires_pyarrow():
    if importlib.util.find_spec("pyarrow") is not None:
        pytest.skip("pyarrow installed; missing-dependency path not exercised")
    with pytest.raises(RuntimeError, match="pyarrow"):
        read_rows("missing.parquet", "parquet")


# --- idempotency + run/input accounting  -------------------------

def test_identical_rerun_skips_duplicates():
    backend = build_memory_backend()
    first = _import(backend, [_row("1"), _row("2")])
    assert first.features_written == 2 and first.duplicates_skipped == 0
    second = _import(backend, [_row("1"), _row("2")])
    assert second.features_written == 0 and second.duplicates_skipped == 2
    assert _offline_count(backend, "1") == 1 and _offline_count(backend, "2") == 1


def test_partial_rerun_counts():
    backend = build_memory_backend()
    _import(backend, [_row("1")])
    summary = _import(backend, [_row("1"), _row("2")])  # one existing, one new
    assert summary.features_written == 1 and summary.duplicates_skipped == 1


def test_same_data_ts_different_value_appends_correction():
    backend = build_memory_backend()
    _import(backend, [_row("1", income=1000)])
    summary = _import(backend, [_row("1", income=2000)])  # same ts, new value
    assert summary.features_written == 1 and summary.duplicates_skipped == 0
    assert _offline_count(backend, "1") == 2


def test_different_data_ts_appends_new_history():
    backend = build_memory_backend()
    _import(backend, [_row("1", as_of="2026-01-10T09:00:00Z", income=1000)])
    summary = _import(backend, [_row("1", as_of="2026-02-10T09:00:00Z", income=1000)])
    assert summary.features_written == 1 and summary.duplicates_skipped == 0
    assert _offline_count(backend, "1") == 2


def test_pit_returns_corrected_value_for_same_data_ts():
    backend = build_memory_backend()
    config = _config(default_calc_ts="2026-01-10T09:00:00Z")
    _import(backend, [_row("1", income=1000)], config=config)
    _import(backend, [_row("1", income=2000)], config=config)  # correction, same data_ts
    view = next(v for v in backend.registry.feature_views if v.name == _VIEW)
    dataset = build_training_dataset(
        offline=backend.offline, view=view, view_version=1,
        feature_names=["declared_income"],
        observations=[TrainingObservation(
            entity={"user_id": "1", "application_id": "A1"},
            observation_ts=datetime(2026, 1, 15, tzinfo=UTC),
        )],
    )
    assert dataset.rows[0].features["declared_income"] == 2000  # last-append tie-break


def test_offline_and_online_rerun_does_not_rewrite_online():
    backend = build_memory_backend()
    first = _import(backend, [_row("1", income=1000)], write_mode="offline_and_online")
    assert first.online_written == 1
    second = _import(backend, [_row("1", income=1000)], write_mode="offline_and_online")
    assert second.duplicates_skipped == 1 and second.online_written == 0


def test_run_accounting_and_input_accounting_fields():
    backend, summary = _run([_row("1"), _row("2")], expected_rows=2)
    assert summary.run_id
    assert summary.started_at is not None and summary.finished_at is not None
    assert summary.duration_ms >= 0
    assert summary.rows_read == 2 and summary.rows_total == 2
    assert summary.features_candidate == summary.features_written + summary.duplicates_skipped
    assert summary.warnings == []


def test_expected_rows_mismatch_warns():
    backend, summary = _run([_row("1")], expected_rows=100)
    assert "expected_rows_mismatch" in summary.warnings
