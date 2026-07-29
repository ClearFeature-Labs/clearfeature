from datetime import UTC, datetime
from pathlib import Path

import pytest

from fintech_feature_platform.fs_core.compute.context import RequestContext
from fintech_feature_platform.fs_core.compute.engine import ComputeCore
from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.models import EntityKey, SourceStamp
from fintech_feature_platform.fs_core.registry.loader import build_registry, load_registry_file

_EXAMPLE = (
    Path(__file__).resolve().parents[3]
    / "examples"
    / "registry"
    / "minimal_credit_risk.yaml"
)
_TS = datetime(2024, 8, 26, 10, tzinfo=UTC)
_LATER = datetime(2024, 8, 26, 12, tzinfo=UTC)

_STAMPS = {
    "credit_report": SourceStamp(report_ts=_TS, content_hash="sha256:credit"),
    "tax_report": SourceStamp(report_ts=_TS, content_hash="sha256:tax"),
}


# --- example UDFs (feature logic lives in the test for this slice) -----------

def _declared_income(sources, deps):
    return sources["credit_report"]["declared_income"]


def _monthly_obligations(sources, deps):
    return sources["credit_report"]["monthly_obligations"]


def _income_from_tax(sources, deps):
    return sources["tax_report"]["income"]


def _safe_ratio(sources, deps):
    income = deps["income_from_tax"]
    return deps["monthly_obligations"] / income if income else None


def _example_udfs() -> dict:
    return {
        "udf.credit.declared_income": _declared_income,
        "udf.credit.monthly_obligations": _monthly_obligations,
        "udf.tax.income": _income_from_tax,
        "udf.common.safe_ratio": _safe_ratio,
    }


# --- helpers -----------------------------------------------------------------

def _payloads() -> dict:
    return {
        "credit_report": {"declared_income": 3_500_000, "monthly_obligations": 700_000},
        "tax_report": {"income": 3_000_000},
    }


def _loader():
    payloads = _payloads()
    return lambda name: payloads[name]


class _CountingLoader:
    def __init__(self, payloads: dict) -> None:
        self._payloads = payloads
        self.calls: dict[str, int] = {}

    def __call__(self, name: str):
        self.calls[name] = self.calls.get(name, 0) + 1
        return self._payloads[name]


def _example_key() -> EntityKey:
    return EntityKey.from_mapping(
        {"user_id": "1", "application_id": "A1", "report_id": "R9"},
        key_order=["user_id", "application_id", "report_id"],
    )


def _udf_feature(udf: str, *, inputs=(), deps=()) -> dict:
    feature = {
        "kind": "udf",
        "feature_version": 1,
        "udf": udf,
        "dtype": "int",
        "status": "active",
    }
    if inputs:
        feature["inputs"] = list(inputs)
    if deps:
        feature["deps"] = list(deps)
    return feature


def _registry_data(features: dict) -> dict:
    return {
        "registry_version": "test-v1",
        "entities": {"application": {"key_fields": ["user_id"]}},
        "sources": {
            "credit_report": {
                "type": "raw_report",
                "report_type": "credit_report",
                "ts_field": "report_ts",
            }
        },
        "feature_views": {
            "v": {
                "entity": "application",
                "key_fields": ["user_id"],
                "view_version": 1,
                "owner": "t",
                "status": "active",
                "features": features,
            }
        },
    }


def _compute(core: ComputeCore, *, view, version, key, requested, context, stamps=None):
    return core.compute(
        view=view,
        view_version=version,
        entity_key=key,
        requested_features=requested,
        context=context,
        source_stamps=stamps if stamps is not None else _STAMPS,
        calc_ts=_TS,
    )


# --- tests -------------------------------------------------------------------

def test_direct_udf_feature_from_example_registry():
    core = ComputeCore(load_registry_file(_EXAMPLE), UdfRegistry(_example_udfs()))
    ctx = RequestContext(_loader())
    key = _example_key()

    results = _compute(
        core,
        view="user_credit_risk",
        version=1,
        key=key,
        requested=["declared_income"],
        context=ctx,
    )

    assert set(results) == {"declared_income"}
    result = results["declared_income"]
    assert result.value == 3_500_000
    assert result.ref.encode() == "declared_income:v1"
    assert result.entity_key is key
    assert result.data_ts == _TS


def test_derived_feature_depends_on_other_features():
    core = ComputeCore(load_registry_file(_EXAMPLE), UdfRegistry(_example_udfs()))
    ctx = RequestContext(_loader())

    results = _compute(
        core,
        view="user_credit_risk",
        version=1,
        key=_example_key(),
        requested=["debt_to_income_ratio"],
        context=ctx,
    )

    result = results["debt_to_income_ratio"]
    assert result.value == pytest.approx(700_000 / 3_000_000)
    assert result.ref.encode() == "debt_to_income_ratio:v1"


def test_only_requested_features_and_dependencies_are_computed():
    computed: list[str] = []

    def _spy_declared_income(sources, deps):
        computed.append("declared_income")
        return sources["credit_report"]["declared_income"]

    udfs = _example_udfs()
    udfs["udf.credit.declared_income"] = _spy_declared_income
    core = ComputeCore(load_registry_file(_EXAMPLE), UdfRegistry(udfs))

    _compute(
        core,
        view="user_credit_risk",
        version=1,
        key=_example_key(),
        requested=["debt_to_income_ratio"],
        context=RequestContext(_loader()),
    )

    # declared_income is not a dependency of debt_to_income_ratio.
    assert computed == []


def test_shared_source_payload_loaded_once():
    loader = _CountingLoader(_payloads())
    core = ComputeCore(load_registry_file(_EXAMPLE), UdfRegistry(_example_udfs()))

    _compute(
        core,
        view="user_credit_risk",
        version=1,
        key=_example_key(),
        requested=["declared_income", "monthly_obligations"],
        context=RequestContext(loader),
    )

    # Both features read credit_report; the loader runs once for it.
    assert loader.calls["credit_report"] == 1


def test_shared_dependency_feature_computed_once():
    data = _registry_data(
        {
            "base": _udf_feature("udf.base", inputs=["credit_report"]),
            "a": _udf_feature("udf.a", deps=["base"]),
            "b": _udf_feature("udf.b", deps=["base"]),
        }
    )
    base_calls: list[str] = []

    def _base(sources, deps):
        base_calls.append("base")
        return sources["credit_report"]["declared_income"]

    udfs = UdfRegistry(
        {
            "udf.base": _base,
            "udf.a": lambda sources, deps: deps["base"],
            "udf.b": lambda sources, deps: deps["base"],
        }
    )
    core = ComputeCore(build_registry(data), udfs)

    _compute(
        core,
        view="v",
        version=1,
        key=EntityKey.from_mapping({"user_id": "1"}),
        requested=["a", "b"],
        context=RequestContext(_loader()),
    )

    assert base_calls == ["base"]


def test_unknown_requested_feature_raises():
    core = ComputeCore(load_registry_file(_EXAMPLE), UdfRegistry(_example_udfs()))
    with pytest.raises(ValueError):
        _compute(
            core,
            view="user_credit_risk",
            version=1,
            key=_example_key(),
            requested=["does_not_exist"],
            context=RequestContext(_loader()),
        )


def test_unknown_udf_id_raises():
    core = ComputeCore(load_registry_file(_EXAMPLE), UdfRegistry({}))
    with pytest.raises(ValueError):
        _compute(
            core,
            view="user_credit_risk",
            version=1,
            key=_example_key(),
            requested=["declared_income"],
            context=RequestContext(_loader()),
        )


# --- per-feature temporal metadata (D3/D9,) -------------------------

def _multi_source_stamps():
    return {
        "credit_report": SourceStamp(report_ts=_TS, content_hash="sha256:credit"),
        "tax_report": SourceStamp(report_ts=_LATER, content_hash="sha256:tax"),
    }


def test_multi_source_features_get_their_own_data_ts():
    core = ComputeCore(load_registry_file(_EXAMPLE), UdfRegistry(_example_udfs()))

    results = _compute(
        core,
        view="user_credit_risk",
        version=1,
        key=_example_key(),
        requested=["declared_income", "income_from_tax"],
        context=RequestContext(_loader()),
        stamps=_multi_source_stamps(),
    )

    # Each feature carries its OWN source's report_ts — never a request-level max.
    assert results["declared_income"].data_ts == _TS
    assert results["declared_income"].max_input_data_ts == _TS
    assert results["income_from_tax"].data_ts == _LATER
    assert results["income_from_tax"].max_input_data_ts == _LATER


def test_derived_feature_data_ts_is_min_and_max_of_inputs():
    core = ComputeCore(load_registry_file(_EXAMPLE), UdfRegistry(_example_udfs()))

    results = _compute(
        core,
        view="user_credit_risk",
        version=1,
        key=_example_key(),
        requested=["debt_to_income_ratio"],
        context=RequestContext(_loader()),
        stamps=_multi_source_stamps(),
    )

    # Inputs: monthly_obligations (credit_report, _TS) + income_from_tax
    # (tax_report, _LATER) -> D3 min / D9 max.
    result = results["debt_to_income_ratio"]
    assert result.data_ts == _TS
    assert result.max_input_data_ts == _LATER
    assert result.value_hash is not None
    assert result.input_fingerprint is not None


def test_fingerprint_is_replay_stable_and_input_sensitive():
    core = ComputeCore(load_registry_file(_EXAMPLE), UdfRegistry(_example_udfs()))

    def run(payloads):
        loader = lambda name: payloads[name]  # noqa: E731 - tiny test loader
        return _compute(
            core,
            view="user_credit_risk",
            version=1,
            key=_example_key(),
            requested=["debt_to_income_ratio"],
            context=RequestContext(loader),
            stamps=_multi_source_stamps(),
        )["debt_to_income_ratio"]

    first = run(_payloads())
    replay = run(_payloads())
    assert replay.input_fingerprint == first.input_fingerprint

    changed_payloads = _payloads()
    changed_payloads["tax_report"]["income"] = 9_000_000
    changed = run(changed_payloads)
    # Same freshness tuple, different input value -> different fingerprint (Case C).
    assert (changed.data_ts, changed.max_input_data_ts) == (
        first.data_ts,
        first.max_input_data_ts,
    )
    assert changed.input_fingerprint != first.input_fingerprint


def test_missing_source_stamp_fails_loudly():
    core = ComputeCore(load_registry_file(_EXAMPLE), UdfRegistry(_example_udfs()))
    with pytest.raises(ValueError, match="source stamp"):
        _compute(
            core,
            view="user_credit_risk",
            version=1,
            key=_example_key(),
            requested=["declared_income"],
            context=RequestContext(_loader()),
            stamps={},
        )


def test_dependency_cycle_rejected_at_registry_build():
    data = _registry_data(
        {
            "a": _udf_feature("udf.a", deps=["b"]),
            "b": _udf_feature("udf.b", deps=["a"]),
        }
    )
    # As of the F2 cycle is rejected at registry validation (build time),
    # before any compute — the earliest, cheapest place to catch it.
    with pytest.raises(ValueError, match="cycle"):
        build_registry(data)
