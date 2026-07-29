import csv
import importlib.util
import json
from datetime import UTC, datetime

import pytest

from fintech_feature_platform.api.feature_dataset_export import (
    dataset_to_flat_rows,
    dataset_to_jsonl_rows,
    write_dataset,
)
from fintech_feature_platform.fs_core.training import (
    TrainingDataset,
    TrainingDatasetRow,
    TrainingFeatureCell,
)

_TS = datetime(2026, 1, 15, 12, tzinfo=UTC)
_DTS = datetime(2026, 1, 10, 9, tzinfo=UTC)


def _dataset():
    row = TrainingDatasetRow(
        entity={"user_id": "u1", "application_id": "app_1"},
        observation_ts=_TS,
        context={"request_id": "req_1", "observation_id": "obs_1"},
        features={"declared_income": 1200, "debt_to_income_ratio": 0.42},
        feature_metadata={
            "declared_income": TrainingFeatureCell(
                status="ok", value=1200, feature_version=1, data_ts=_DTS
            ),
            "debt_to_income_ratio": TrainingFeatureCell(
                status="missing", value=None, feature_version=None, data_ts=None
            ),
        },
    )
    return TrainingDataset(rows=[row], summary={"rows": 1})


def test_jsonl_rows_preserve_full_shape():
    rows = dataset_to_jsonl_rows(_dataset())
    row = rows[0]
    assert set(row) == {
        "entity", "observation_ts", "context", "features", "feature_metadata"
    }
    assert row["entity"] == {"user_id": "u1", "application_id": "app_1"}
    assert row["features"]["declared_income"] == 1200
    assert row["feature_metadata"]["declared_income"]["status"] == "ok"
    assert row["feature_metadata"]["declared_income"]["data_ts"] == _DTS.isoformat()
    assert row["context"]["request_id"] == "req_1"


def test_jsonl_export_writes_full_rows(tmp_path):
    path = tmp_path / "out.jsonl"
    write_dataset(_dataset(), str(path), "jsonl")
    lines = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    assert len(lines) == 1
    assert lines[0]["features"]["declared_income"] == 1200
    assert "label" not in json.dumps(lines[0])  # no semantic label
    # entity/context keys available for external target join
    assert lines[0]["entity"]["user_id"] == "u1"
    assert lines[0]["context"]["observation_id"] == "obs_1"


def test_flat_rows_have_flattened_feature_and_meta_columns():
    flat = dataset_to_flat_rows(_dataset())[0]
    assert flat["entity.user_id"] == "u1"
    assert flat["entity.application_id"] == "app_1"
    assert flat["observation_ts"] == _TS.isoformat()
    assert flat["context.request_id"] == "req_1"
    assert flat["feature.declared_income"] == 1200
    assert flat["meta.declared_income.status"] == "ok"
    assert flat["meta.declared_income.feature_version"] == 1
    assert flat["meta.declared_income.data_ts"] == _DTS.isoformat()
    assert flat["meta.debt_to_income_ratio.status"] == "missing"
    assert "label" not in " ".join(flat.keys())


def test_csv_export_writes_flattened_columns(tmp_path):
    path = tmp_path / "out.csv"
    write_dataset(_dataset(), str(path), "csv")
    with open(path, newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    row = rows[0]
    assert row["entity.user_id"] == "u1"
    assert row["feature.declared_income"] == "1200"
    assert row["meta.declared_income.status"] == "ok"
    assert row["meta.declared_income.data_ts"] == _DTS.isoformat()
    assert "label" not in ",".join(row.keys())


def test_unknown_format_raises(tmp_path):
    with pytest.raises(ValueError):
        write_dataset(_dataset(), str(tmp_path / "x.txt"), "txt")


def test_parquet_export_requires_pyarrow(tmp_path):
    if importlib.util.find_spec("pyarrow") is not None:
        pytest.skip("pyarrow installed; missing-dependency path not exercised")
    with pytest.raises(RuntimeError, match="pyarrow"):
        write_dataset(_dataset(), str(tmp_path / "out.parquet"), "parquet")


def test_parquet_export_writes_when_pyarrow_installed(tmp_path):
    pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    path = tmp_path / "out.parquet"
    write_dataset(_dataset(), str(path), "parquet")
    table = pq.read_table(str(path)).to_pylist()
    assert table[0]["feature.declared_income"] == 1200
