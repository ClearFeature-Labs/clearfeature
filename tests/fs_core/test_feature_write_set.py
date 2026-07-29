"""Tests for the compute-only FeatureWriteSet seam.

These prove ``compute_write_set`` writes nothing and that ``compute`` behaviour is
unchanged. The fixture mirrors ``test_feature_store.py``.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.feature_store import FeatureStore
from fintech_feature_platform.fs_core.models import (
    EntityKey,
    FeatureResult,
    FeatureWriteSet,
    RawReportMeta,
    SourceStamp,
)
from fintech_feature_platform.fs_core.raw.meta_repository import InMemoryMetaRepository
from fintech_feature_platform.fs_core.raw.payload_store import InMemoryPayloadStore
from fintech_feature_platform.fs_core.raw.report_resolver import ReportResolver
from fintech_feature_platform.fs_core.registry.loader import load_registry_file
from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore
from fintech_feature_platform.fs_core.stores.online import InMemoryOnlineStore
from fintech_feature_platform.fs_core.write_guard import WRITTEN

_EXAMPLE = (
    Path(__file__).resolve().parents[2]
    / "examples"
    / "registry"
    / "minimal_credit_risk.yaml"
)
_TS = datetime(2024, 8, 26, 10, tzinfo=UTC)
_LATER = _TS.replace(hour=12)
_SOURCE_REFS = {"credit_report": "rep_credit", "tax_report": "rep_tax"}


def _declared_income(sources, deps):
    return sources["credit_report"]["declared_income"]


def _monthly_obligations(sources, deps):
    return sources["credit_report"]["monthly_obligations"]


def _income_from_tax(sources, deps):
    return sources["tax_report"]["income"]


def _safe_ratio(sources, deps):
    return deps["monthly_obligations"] / deps["income_from_tax"]


def _udfs() -> UdfRegistry:
    return UdfRegistry(
        {
            "udf.credit.declared_income": _declared_income,
            "udf.credit.monthly_obligations": _monthly_obligations,
            "udf.tax.income": _income_from_tax,
            "udf.common.safe_ratio": _safe_ratio,
        }
    )


def _meta(report_ref: str, report_type: str) -> RawReportMeta:
    return RawReportMeta(
        report_ref=report_ref,
        report_type=report_type,
        entity_type="application",
        entity_key=EntityKey.from_mapping({"user_id": "1"}),
        report_ts=_TS,
        payload_size_bytes=10,
        content_hash="sha256:test",
        storage_uri=f"mem://{report_ref}",
        created_at=_TS,
    )


def _build():
    registry = load_registry_file(_EXAMPLE)
    payloads = InMemoryPayloadStore()
    payloads.put(
        "mem://rep_credit",
        {"declared_income": 3_500_000, "monthly_obligations": 700_000},
    )
    payloads.put("mem://rep_tax", {"income": 3_000_000})
    metas = InMemoryMetaRepository()
    metas.add(_meta("rep_credit", "credit_report"))
    metas.add(_meta("rep_tax", "tax_report"))
    resolver = ReportResolver(payloads, metas)
    offline = InMemoryOfflineStore()
    online = InMemoryOnlineStore()
    store = FeatureStore(registry, _udfs(), resolver, offline, online)
    return store, offline, online


def _key() -> EntityKey:
    return EntityKey.from_mapping(
        {"user_id": "1", "application_id": "A1", "report_id": "R9"},
        key_order=["user_id", "application_id", "report_id"],
    )


def _stamps(report_ts=_TS, tax_report_ts=None):
    return {
        "credit_report": SourceStamp(report_ts=report_ts, content_hash="sha256:credit"),
        "tax_report": SourceStamp(
            report_ts=tax_report_ts if tax_report_ts is not None else report_ts,
            content_hash="sha256:tax",
        ),
    }


def _compute_write_set(store, requested, *, source_stamps=None, **kwargs):
    return store.compute_write_set(
        view="user_credit_risk",
        view_version=1,
        entity_key=_key(),
        requested_features=requested,
        source_refs=_SOURCE_REFS,
        source_stamps=source_stamps if source_stamps is not None else _stamps(),
        calc_ts=_TS,
        **kwargs,
    )


# --- F2 node statuses : in-process observability, not serialized ---

def test_write_set_carries_node_statuses_but_does_not_serialize_them():
    store, _, _ = _build()
    # debt_to_income_ratio depends on monthly_obligations + income_from_tax (legacy deps,
    # no staleness) -> every node OK.
    ws = _compute_write_set(store, ["debt_to_income_ratio"])
    assert ws.node_statuses["debt_to_income_ratio"] == "OK"
    assert ws.node_statuses["monthly_obligations"] == "OK"
    assert ws.node_statuses["income_from_tax"] == "OK"
    # Statuses are in-process only: they never travel on the offline-write event.
    assert "node_statuses" not in ws.to_dict()
    assert FeatureWriteSet.from_dict(ws.to_dict()).node_statuses == {}


# --- serialization  ----------------------------------------------

def test_feature_write_set_json_round_trip_preserves_values():
    store, _, _ = _build()
    ws = _compute_write_set(
        store, ["declared_income"], request_id="freq_1", job_id="job_1"
    )
    restored = FeatureWriteSet.from_dict(ws.to_dict())

    assert restored.view == ws.view
    assert restored.view_version == ws.view_version
    assert restored.entity_key == ws.entity_key
    assert restored.source_refs == ws.source_refs
    assert restored.request_id == "freq_1"
    assert restored.job_id == "job_1"

    original = ws.results["declared_income"]
    got = restored.results["declared_income"]
    assert got.value == original.value
    assert got.ref == original.ref
    assert got.data_ts == original.data_ts
    assert got.calc_ts == original.calc_ts
    assert got.entity_key == original.entity_key
    # D9 metadata survives serialization.
    assert got.max_input_data_ts == original.max_input_data_ts
    assert got.input_fingerprint == original.input_fingerprint
    assert got.value_hash == original.value_hash
    assert original.input_fingerprint is not None

    # bytes round-trip too
    assert (
        FeatureWriteSet.from_json(ws.to_json()).results["declared_income"].value
        == original.value
    )


# --- compute_write_set: computes, writes nothing ----------------------------

def test_compute_write_set_returns_expected_results():
    store, _, _ = _build()
    write_set = _compute_write_set(store, ["declared_income"])
    assert isinstance(write_set, FeatureWriteSet)
    assert write_set.results["declared_income"].value == 3_500_000
    assert isinstance(write_set.results["declared_income"], FeatureResult)


def test_compute_write_set_does_not_append_offline():
    store, offline, _ = _build()
    _compute_write_set(store, ["declared_income"])
    assert offline.get(_key(), feature_name="declared_income") == []


def test_compute_write_set_does_not_write_online():
    store, _, online = _build()
    _compute_write_set(store, ["declared_income"])
    assert online.get("user_credit_risk", 1, _key(), "declared_income", 1) is None


def test_compute_write_set_carries_identifying_context():
    store, _, _ = _build()
    write_set = _compute_write_set(
        store,
        ["declared_income"],
        request_id="freq_1",
        job_id="job_1",
        run_id="run_1",
        correlation_id="corr_1",
        write_policy="online_first",
    )
    assert write_set.view == "user_credit_risk"
    assert write_set.view_version == 1
    assert write_set.entity_key == _key()
    assert write_set.source_refs == _SOURCE_REFS
    assert write_set.request_id == "freq_1"
    assert write_set.job_id == "job_1"
    assert write_set.run_id == "run_1"
    assert write_set.correlation_id == "corr_1"
    assert write_set.write_policy == "online_first"


def test_compute_write_set_optional_ids_default_none():
    store, _, _ = _build()
    write_set = _compute_write_set(store, ["declared_income"])
    assert write_set.request_id is None
    assert write_set.job_id is None
    assert write_set.run_id is None
    assert write_set.correlation_id is None


def test_compute_write_set_data_ts_from_source_stamp():
    store, _, _ = _build()
    write_set = _compute_write_set(
        store, ["declared_income"], source_stamps=_stamps(report_ts=_LATER)
    )
    result = write_set.results["declared_income"]
    # F1: data_ts is the feature's OWN source report_ts; max degenerates to it.
    assert result.data_ts == _LATER
    assert result.max_input_data_ts == _LATER
    assert result.calc_ts == _TS


def test_compute_write_set_dependent_feature_min_max_of_inputs():
    # D3/D9 : derived data_ts = min(inputs), max_input_data_ts = max.
    store, _, _ = _build()
    write_set = _compute_write_set(
        store,
        ["debt_to_income_ratio"],
        source_stamps=_stamps(report_ts=_TS, tax_report_ts=_LATER),
    )
    ratio = write_set.results["debt_to_income_ratio"]
    assert ratio.value == pytest.approx(700_000 / 3_000_000)
    assert ratio.data_ts == _TS
    assert ratio.max_input_data_ts == _LATER
    assert ratio.input_fingerprint is not None
    assert ratio.value_hash is not None


# --- compute(...) still persists exactly as before --------------------------

def _compute(store, requested, *, write_online=True, skip_duplicate_offline=False):
    return store.compute(
        view="user_credit_risk",
        view_version=1,
        entity_key=_key(),
        requested_features=requested,
        source_refs=_SOURCE_REFS,
        source_stamps=_stamps(),
        calc_ts=_TS,
        write_online=write_online,
        skip_duplicate_offline=skip_duplicate_offline,
    )


def test_compute_still_writes_offline_and_online():
    store, offline, online = _build()
    outcome = _compute(store, ["declared_income"])
    assert outcome.online_written["declared_income:v1"] == WRITTEN
    assert len(offline.get(_key(), feature_name="declared_income")) == 1
    got = online.get("user_credit_risk", 1, _key(), "declared_income", 1)
    assert got is not None and got.value == 3_500_000


def test_compute_still_respects_write_online_false():
    store, offline, online = _build()
    outcome = _compute(store, ["declared_income"], write_online=False)
    assert len(offline.get(_key(), feature_name="declared_income")) == 1
    assert online.get("user_credit_risk", 1, _key(), "declared_income", 1) is None
    assert outcome.online_written["declared_income:v1"] == "disabled"


def test_compute_still_respects_skip_duplicate_offline():
    store, offline, _ = _build()
    _compute(store, ["declared_income"], skip_duplicate_offline=True)
    outcome = _compute(store, ["declared_income"], skip_duplicate_offline=True)
    assert outcome.duplicates_skipped == 1
    assert len(offline.get(_key(), feature_name="declared_income")) == 1
