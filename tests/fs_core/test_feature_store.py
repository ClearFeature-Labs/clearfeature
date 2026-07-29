from datetime import UTC, datetime
from pathlib import Path

import pytest

from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.feature_store import FeatureStore
from fintech_feature_platform.fs_core.models import (
    EntityKey,
    RawReportMeta,
    SourceStamp,
)
from fintech_feature_platform.fs_core.raw.meta_repository import InMemoryMetaRepository
from fintech_feature_platform.fs_core.raw.payload_store import InMemoryPayloadStore
from fintech_feature_platform.fs_core.raw.report_resolver import ReportResolver
from fintech_feature_platform.fs_core.registry.loader import load_registry_file
from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore
from fintech_feature_platform.fs_core.stores.online import InMemoryOnlineStore
from fintech_feature_platform.fs_core.write_guard import NOOP, SKIPPED_STALE, WRITTEN

_EXAMPLE = (
    Path(__file__).resolve().parents[2]
    / "examples"
    / "registry"
    / "minimal_credit_risk.yaml"
)
_TS = datetime(2024, 8, 26, 10, tzinfo=UTC)
_LATER = _TS.replace(hour=12)
_SOURCE_REFS = {"credit_report": "rep_credit", "tax_report": "rep_tax"}


# --- example UDFs ------------------------------------------------------------

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


# --- helpers -----------------------------------------------------------------

class _CountingPayloadStore:
    def __init__(self) -> None:
        self._payloads: dict = {}
        self.calls: dict[str, int] = {}

    def put(self, storage_uri: str, payload) -> None:
        self._payloads[storage_uri] = payload

    def get_payload(self, storage_uri: str):
        self.calls[storage_uri] = self.calls.get(storage_uri, 0) + 1
        return self._payloads[storage_uri]


def _populated(store):
    # Payloads are keyed by storage_uri (mem://<report_ref> here).
    store.put(
        "mem://rep_credit",
        {"declared_income": 3_500_000, "monthly_obligations": 700_000},
    )
    store.put("mem://rep_tax", {"income": 3_000_000})
    return store


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


def _build(payloads=None):
    registry = load_registry_file(_EXAMPLE)
    payloads = payloads or _populated(InMemoryPayloadStore())
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


def _stamps(report_ts):
    return {
        "credit_report": SourceStamp(report_ts=report_ts, content_hash="sha256:credit"),
        "tax_report": SourceStamp(report_ts=report_ts, content_hash="sha256:tax"),
    }


def _compute(
    store, requested, *, data_ts=_TS, source_refs=None, write_online=True,
    skip_duplicate_offline=False,
):
    return store.compute(
        view="user_credit_risk",
        view_version=1,
        entity_key=_key(),
        requested_features=requested,
        source_refs=_SOURCE_REFS if source_refs is None else source_refs,
        source_stamps=_stamps(data_ts),
        calc_ts=_TS,
        write_online=write_online,
        skip_duplicate_offline=skip_duplicate_offline,
    )


# --- tests -------------------------------------------------------------------

def test_full_end_to_end_compute_and_writes():
    store, offline, online = _build()

    outcome = _compute(store, ["declared_income"])

    assert outcome.results["declared_income"].value == 3_500_000
    assert outcome.online_written["declared_income:v1"] == WRITTEN
    assert len(offline.get(_key(), feature_name="declared_income")) == 1
    got = online.get("user_credit_risk", 1, _key(), "declared_income", 1)
    assert got is not None and got.value == 3_500_000


def test_compute_write_online_false_writes_offline_only():
    store, offline, online = _build()

    outcome = _compute(store, ["declared_income"], write_online=False)

    # offline history is written
    assert len(offline.get(_key(), feature_name="declared_income")) == 1
    # online latest is NOT written
    assert online.get("user_credit_risk", 1, _key(), "declared_income", 1) is None
    # the outcome clearly reports the online write was disabled
    assert outcome.online_written["declared_income:v1"] == "disabled"


def test_compute_write_online_true_default_unchanged():
    store, offline, online = _build()

    outcome = _compute(store, ["declared_income"])  # default write_online=True

    assert outcome.online_written["declared_income:v1"] == WRITTEN
    assert online.get("user_credit_risk", 1, _key(), "declared_income", 1) is not None
    assert len(offline.get(_key(), feature_name="declared_income")) == 1


def test_compute_skip_duplicate_offline_skips_identical_rerun():
    store, offline, _ = _build()
    _compute(store, ["declared_income"], skip_duplicate_offline=True)
    outcome = _compute(store, ["declared_income"], skip_duplicate_offline=True)
    assert outcome.duplicates_skipped == 1
    # offline did not grow on the idempotent rerun
    assert len(offline.get(_key(), feature_name="declared_income")) == 1


def test_compute_default_does_not_dedup():
    store, offline, _ = _build()
    _compute(store, ["declared_income"])
    _compute(store, ["declared_income"])  # default skip_duplicate_offline=False
    assert len(offline.get(_key(), feature_name="declared_income")) == 2


def test_derived_feature_through_dependencies():
    store, _, _ = _build()
    outcome = _compute(store, ["debt_to_income_ratio"])
    assert outcome.results["debt_to_income_ratio"].value == pytest.approx(
        700_000 / 3_000_000
    )


def test_offline_history_appended_across_calls():
    store, offline, _ = _build()
    _compute(store, ["declared_income"], data_ts=_TS)
    _compute(store, ["declared_income"], data_ts=_LATER)
    assert len(offline.get(_key(), feature_name="declared_income")) == 2


def test_online_cas_writes_when_newer():
    store, _, online = _build()
    _compute(store, ["declared_income"], data_ts=_TS)
    outcome = _compute(store, ["declared_income"], data_ts=_LATER)
    assert outcome.online_written["declared_income:v1"] == WRITTEN
    got = online.get("user_credit_risk", 1, _key(), "declared_income", 1)
    assert got.data_ts == _LATER


def test_online_cas_skips_when_older():
    store, _, online = _build()
    _compute(store, ["declared_income"], data_ts=_LATER)
    outcome = _compute(store, ["declared_income"], data_ts=_TS)
    assert outcome.online_written["declared_income:v1"] == SKIPPED_STALE
    got = online.get("user_credit_risk", 1, _key(), "declared_income", 1)
    assert got.data_ts == _LATER


def test_online_cas_equal_identical_rerun_is_noop():
    store, _, online = _build()
    _compute(store, ["declared_income"], data_ts=_TS)
    # Identical rerun: equal tuple + identical input fingerprint -> noop.
    outcome = _compute(store, ["declared_income"], data_ts=_TS)
    assert outcome.online_written["declared_income:v1"] == NOOP


def test_source_payload_loaded_once():
    payloads = _populated(_CountingPayloadStore())
    store, _, _ = _build(payloads)
    _compute(store, ["declared_income", "monthly_obligations"])
    assert payloads.calls["mem://rep_credit"] == 1


def test_missing_source_ref_propagates():
    store, _, _ = _build()
    with pytest.raises(ValueError):
        _compute(store, ["declared_income"], source_refs={"tax_report": "rep_tax"})
