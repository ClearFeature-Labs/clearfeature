"""ModelRunner seam: vector-first, deterministic, digest-validated."""

import pytest

from fintech_feature_platform.fs_core.model_runner import FakeModelRunner, ModelRef


def _ref(digest="sha256:d"):
    return ModelRef(uri="mlflow://m/1", digest=digest, output_name="score")


def test_predict_returns_one_value_per_row_in_order():
    runner = FakeModelRunner(score_fn=lambda row: row["a"] * 2)
    out = runner.predict(_ref(), [{"a": 1}, {"a": 2}, {"a": 3}])
    assert out == [2, 4, 6]


def test_default_score_sums_numeric_inputs():
    runner = FakeModelRunner()
    assert runner.predict(_ref(), [{"income": 100, "debt": 40}]) == [140.0]


def test_records_batch_call_not_per_item():
    runner = FakeModelRunner()
    runner.predict(_ref(), [{"a": 1}, {"a": 2}])
    runner.predict(_ref(), [{"a": 3}])
    assert runner.calls == [2, 1]  # one entry per predict() call, holding the batch size


def test_digest_mismatch_fails_loudly():
    runner = FakeModelRunner(expected_digest="sha256:expected")
    with pytest.raises(ValueError, match="digest mismatch"):
        runner.predict(_ref(digest="sha256:wrong"), [{"a": 1}])


def test_empty_batch_returns_empty():
    runner = FakeModelRunner()
    assert runner.predict(_ref(), []) == []
    assert runner.calls == [0]
