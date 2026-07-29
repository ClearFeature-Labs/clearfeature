import importlib.util
from pathlib import Path

import pytest

from fintech_feature_platform.fs_core.feature_store import ComputeOutcome

_DEMO_PATH = (
    Path(__file__).resolve().parents[2] / "examples" / "local_credit_risk_demo.py"
)


def _load_demo():
    spec = importlib.util.spec_from_file_location("local_credit_risk_demo", _DEMO_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_run_demo_returns_outcome_and_prints(capsys):
    demo = _load_demo()
    outcome = demo.run_demo()

    assert isinstance(outcome, ComputeOutcome)
    assert set(outcome.results) >= {
        "declared_income",
        "monthly_obligations",
        "income_from_tax",
        "debt_to_income_ratio",
    }
    assert outcome.results["debt_to_income_ratio"].value == pytest.approx(
        700_000 / 3_000_000
    )
    assert outcome.online_written["declared_income:v1"] == "written"

    assert capsys.readouterr().out.strip() != ""
