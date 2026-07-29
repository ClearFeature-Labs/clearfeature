"""Availability contract : four clocks, trusted backfills, safe replays.

All cases run through the REAL core paths (run_jsonl_ingestion, ComputeCore,
OnlineStore.write/D9, partition_new_results, get_pit, build_training_dataset,
lineage builder, the API submit path) — never isolated dataclass checks.
"""

import json
from datetime import UTC, datetime, timedelta

import pytest
from examples.credit_decision_demo.features import build_registry_and_udfs

from fintech_feature_platform.api.backend import build_backend
from fintech_feature_platform.api.jsonl_ingestion import run_jsonl_ingestion
from fintech_feature_platform.api.settings import load_settings
from fintech_feature_platform.fs_core.compute.context import RequestContext
from fintech_feature_platform.fs_core.compute.engine import ComputeCore
from fintech_feature_platform.fs_core.dedup import partition_new_results
from fintech_feature_platform.fs_core.models import (
    EntityKey,
    FeatureResult,
    RawReportMeta,
    SourceStamp,
    trusted_available_at,
)
from fintech_feature_platform.fs_core.observability.lineage import build_feature_lineage
from fintech_feature_platform.fs_core.raw.meta_repository import InMemoryMetaRepository
from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore
from fintech_feature_platform.fs_core.stores.online import InMemoryOnlineStore
from fintech_feature_platform.fs_core.training import (
    TrainingObservation,
    build_training_dataset,
)

NOW = datetime(2026, 7, 19, 12, 0, tzinfo=UTC)
OLD = datetime(2024, 1, 1, tzinfo=UTC)
HIST_AVAILABLE = datetime(2024, 2, 1, tzinfo=UTC)

BUREAU = {"bureau_score": 640, "active_loans": 2, "total_outstanding_amount": 10000,
          "total_monthly_payment": 300, "max_dpd_12m": 0, "last_delinquency_date": None,
          "inquiries_30d": 1, "report_ts": None, "currency_code": "USD"}
TELCO = {"sim_age_days": 900, "active_days_30d": 25, "avg_monthly_topup": 20,
         "telco_score": 0.7}

_REGISTRY, _UDFS = build_registry_and_udfs()
_CORE = ComputeCore(_REGISTRY, _UDFS)
_VIEW, _VV = "credit_decision", 1


def _key(name: str) -> EntityKey:
    return EntityKey.from_mapping(
        {"user_id": name, "application_id": f"a_{name}"},
        key_order=["user_id", "application_id"])


def _compute(feature, entity, stamps, payloads, calc_ts=NOW) -> FeatureResult:
    out = _CORE.compute(view=_VIEW, view_version=_VV, entity_key=entity,
                        requested_features=[feature],
                        context=RequestContext(lambda s: payloads[s]),
                        source_stamps=stamps, calc_ts=calc_ts)
    return out[feature]


def _bureau_result(entity, *, report_ts=OLD, available_at=None,
                   availability_source=None, calc_ts=NOW, tag="c1") -> FeatureResult:
    stamp = SourceStamp(report_ts=report_ts, content_hash=f"sha256:{tag}",
                        available_at=available_at,
                        availability_source=availability_source)
    payload = {**BUREAU, "report_ts": report_ts.isoformat()}
    return _compute("bureau_score", entity, {"credit_bureau_report": stamp},
                    {"credit_bureau_report": payload}, calc_ts)


def _memory_backend():
    return build_backend(load_settings({"FSP_BACKEND": "memory"}))


# --- 1 + 12: ingestion fallbacks and trusted claims -----------------------------------


def test_ingestion_defaults_available_at_to_ingestion_time():
    backend = _memory_backend()
    manifest = run_jsonl_ingestion(
        backend=backend,
        lines=[json.dumps({"entity_key": {"user_id": "u1", "application_id": "a1"},
                           "event_ts": OLD.isoformat(), "payload": {"x": 1}})],
        entity_type="application", source_name="credit_bureau_report",
        report_type="credit_bureau_report", dataset_id="avail_1")
    item = backend.source_datasets.list_items(manifest.manifest_id)[0]
    meta = backend.metas.get_meta(item.report_ref)
    assert meta.availability_source == "ingestion_time"
    assert meta.available_at == meta.created_at  # never event_ts
    assert meta.report_ts == OLD  # business time preserved independently


def test_operator_ingestion_records_trusted_available_at():
    backend = _memory_backend()
    manifest = run_jsonl_ingestion(
        backend=backend,
        lines=[json.dumps({"entity_key": {"user_id": "u2", "application_id": "a2"},
                           "event_ts": OLD.isoformat(),
                           "available_at": HIST_AVAILABLE.isoformat(),
                           "payload": {"x": 1}})],
        entity_type="application", source_name="credit_bureau_report",
        report_type="credit_bureau_report", dataset_id="avail_12")
    item = backend.source_datasets.list_items(manifest.manifest_id)[0]
    meta = backend.metas.get_meta(item.report_ref)
    assert meta.availability_source == "source_provided"
    assert meta.available_at == HIST_AVAILABLE


def test_impossible_available_at_before_report_ts_is_rejected():
    backend = _memory_backend()
    manifest = run_jsonl_ingestion(
        backend=backend,
        lines=[json.dumps({"entity_key": {"user_id": "u3", "application_id": "a3"},
                           "event_ts": OLD.isoformat(),
                           "available_at": (OLD - timedelta(days=1)).isoformat(),
                           "payload": {"x": 1}})],
        entity_type="application", source_name="credit_bureau_report",
        report_type="credit_bureau_report", dataset_id="avail_imp")
    assert manifest.item_count_rejected == 1
    item = backend.source_datasets.list_items(manifest.manifest_id)[0]
    assert "available_at" in (item.error or "")


# --- 2/3/4/10: PIT with the effective availability rule -------------------------------


def test_trusted_historical_backfill_is_pit_eligible_after_available_at():
    offline = InMemoryOfflineStore()
    entity = _key("pit_trusted")
    result = _bureau_result(entity, available_at=HIST_AVAILABLE,
                            availability_source="source_provided")
    assert result.available_at == HIST_AVAILABLE
    offline.append(_VIEW, _VV, result)
    obs = HIST_AVAILABLE + timedelta(days=30)  # long before calc_ts=NOW
    record = offline.get_pit(entity, feature_name="bureau_score", feature_version=1,
                             view=_VIEW, view_version=_VV, observation_ts=obs)
    assert record is not None  # calc_ts no longer blocks a trusted backfill
    before = HIST_AVAILABLE - timedelta(days=1)
    assert offline.get_pit(entity, feature_name="bureau_score", feature_version=1,
                           view=_VIEW, view_version=_VV, observation_ts=before) is None


def test_backfill_without_available_at_stays_blocked_for_history():
    offline = InMemoryOfflineStore()
    entity = _key("pit_legacy")
    result = _bureau_result(entity)  # no availability -> None
    assert result.available_at is None
    offline.append(_VIEW, _VV, result)
    hist = datetime(2024, 6, 1, tzinfo=UTC)
    assert offline.get_pit(entity, feature_name="bureau_score", feature_version=1,
                           view=_VIEW, view_version=_VV, observation_ts=hist) is None
    assert offline.get_pit(entity, feature_name="bureau_score", feature_version=1,
                           view=_VIEW, view_version=_VV, observation_ts=NOW) is not None


def test_training_dataset_uses_effective_availability():
    offline = InMemoryOfflineStore()
    entity = _key("pit_ds")
    offline.append(_VIEW, _VV, _bureau_result(
        entity, available_at=HIST_AVAILABLE, availability_source="source_provided"))
    view_def = next(v for v in _REGISTRY.feature_views if v.name == _VIEW)
    ds = build_training_dataset(
        offline=offline, view=view_def, view_version=_VV,
        feature_names=["bureau_score"],
        observations=[TrainingObservation(
            entity={"user_id": "pit_ds", "application_id": "a_pit_ds"},
            observation_ts=datetime(2024, 6, 1, tzinfo=UTC))])
    assert ds.rows[0].features["bureau_score"] == 640


# --- 5/6: derived availability + unchanged data_ts semantics --------------------------


def test_feature_available_at_is_max_of_inputs_and_data_ts_stays_min():
    entity = _key("multi")
    bureau_stamp = SourceStamp(report_ts=OLD, content_hash="sha256:b",
                               available_at=HIST_AVAILABLE,
                               availability_source="source_provided")
    telco_ts = NOW - timedelta(days=1)
    telco_avail = NOW - timedelta(hours=1)
    telco_stamp = SourceStamp(report_ts=telco_ts, content_hash="sha256:t",
                              available_at=telco_avail,
                              availability_source="source_provided")
    result = _compute("thin_file_flag", entity,
                      {"credit_bureau_report": bureau_stamp, "telco_report": telco_stamp},
                      {"credit_bureau_report": {**BUREAU, "report_ts": OLD.isoformat()},
                       "telco_report": TELCO})
    assert result.data_ts == OLD                       # min unchanged
    assert result.max_input_data_ts == telco_ts        # max unchanged
    assert result.available_at == telco_avail          # max of availability
    assert result.availability_source == "source_provided"


def test_any_unknown_input_availability_degrades_to_conservative_none():
    entity = _key("mixed_none")
    bureau_stamp = SourceStamp(report_ts=OLD, content_hash="sha256:b2",
                               available_at=HIST_AVAILABLE,
                               availability_source="source_provided")
    telco_stamp = SourceStamp(report_ts=NOW, content_hash="sha256:t2")  # no availability
    result = _compute("thin_file_flag", entity,
                      {"credit_bureau_report": bureau_stamp, "telco_report": telco_stamp},
                      {"credit_bureau_report": {**BUREAU, "report_ts": OLD.isoformat()},
                       "telco_report": TELCO})
    assert result.available_at is None  # conservative calc_ts fallback at PIT time


def test_mixed_trust_availability_is_not_part_of_replay_identity():
    entity = _key("mixed_trust")
    bureau_stamp = SourceStamp(report_ts=OLD, content_hash="sha256:b3",
                               available_at=HIST_AVAILABLE,
                               availability_source="source_provided")
    telco_stamp = SourceStamp(report_ts=NOW, content_hash="sha256:t3",
                              available_at=NOW, availability_source="ingestion_time")
    result = _compute("thin_file_flag", entity,
                      {"credit_bureau_report": bureau_stamp, "telco_report": telco_stamp},
                      {"credit_bureau_report": {**BUREAU, "report_ts": OLD.isoformat()},
                       "telco_report": TELCO})
    assert result.available_at is not None
    assert result.availability_source == "ingestion_time"
    assert trusted_available_at(result) is None


# --- 7/8: replay identity -------------------------------------------------------------


def test_corrected_trusted_availability_is_an_auditable_recompute():
    offline = InMemoryOfflineStore()
    online = InMemoryOnlineStore()
    entity = _key("correct")
    first = _bureau_result(entity, available_at=HIST_AVAILABLE,
                           availability_source="source_provided")
    offline.append(_VIEW, _VV, first)
    assert online.write(_VIEW, _VV, first) == "written"
    corrected = _bureau_result(entity, available_at=HIST_AVAILABLE + timedelta(days=5),
                               availability_source="source_provided")
    new, dups = partition_new_results(offline, _VIEW, _VV, [corrected])
    assert (len(new), dups) == (1, 0)  # NOT silently dropped as a duplicate
    assert online.write(_VIEW, _VV, corrected) == "written_recompute"


def test_exact_duplicate_with_identical_availability_stays_noop():
    offline = InMemoryOfflineStore()
    online = InMemoryOnlineStore()
    entity = _key("dup")
    result = _bureau_result(entity, available_at=HIST_AVAILABLE,
                            availability_source="source_provided")
    offline.append(_VIEW, _VV, result)
    online.write(_VIEW, _VV, result)
    replay = _bureau_result(entity, available_at=HIST_AVAILABLE,
                            availability_source="source_provided")
    new, dups = partition_new_results(offline, _VIEW, _VV, [replay])
    assert (len(new), dups) == (0, 1)
    assert online.write(_VIEW, _VV, replay) == "noop"


def test_ingestion_time_availability_never_breaks_rerun_dedup():
    offline = InMemoryOfflineStore()
    entity = _key("rerun")
    first = _bureau_result(entity, available_at=NOW,
                           availability_source="ingestion_time")
    offline.append(_VIEW, _VV, first)
    rerun = _bureau_result(entity, available_at=NOW + timedelta(hours=1),
                           availability_source="ingestion_time")
    new, dups = partition_new_results(offline, _VIEW, _VV, [rerun])
    assert (len(new), dups) == (0, 1)  # incidental clock -> still an exact duplicate


def test_legacy_row_matched_by_availability_carrying_replay_stays_noop():
    offline = InMemoryOfflineStore()
    online = InMemoryOnlineStore()
    entity = _key("legacy")
    legacy = _bureau_result(entity)  # legacy shape: no availability
    offline.append(_VIEW, _VV, legacy)
    online.write(_VIEW, _VV, legacy)
    upgraded = _bureau_result(entity, available_at=HIST_AVAILABLE,
                              availability_source="source_provided")
    new, dups = partition_new_results(offline, _VIEW, _VV, [upgraded])
    assert (len(new), dups) == (0, 1)  # upgrades never rewrite history wholesale
    assert online.write(_VIEW, _VV, upgraded) == "noop"


# --- 9: freshness ordering unchanged --------------------------------------------------


def test_out_of_order_arrival_still_governed_by_d9_freshness():
    online = InMemoryOnlineStore()
    entity = _key("ooo")
    newer = _bureau_result(entity, report_ts=NOW - timedelta(days=1), tag="new",
                           available_at=NOW, availability_source="source_provided")
    older = _bureau_result(entity, report_ts=NOW - timedelta(days=2), tag="old",
                           available_at=NOW + timedelta(hours=1),
                           availability_source="source_provided")
    assert online.write(_VIEW, _VV, newer) == "written"
    # A later availability can never resurrect stale data: freshness tuple rules.
    assert online.write(_VIEW, _VV, older) == "skipped_stale"


# --- 11: online requests cannot backdate ----------------------------------------------


def test_online_request_availability_is_server_stamped_accept_time():
    from fastapi.testclient import TestClient

    from fintech_feature_platform.api.app import create_app
    from fintech_feature_platform.api.security import SecurityConfig

    backend = _memory_backend()
    client = TestClient(create_app(
        backend, security=SecurityConfig(
            mode="disabled", environment="development", keys=())))
    before = datetime.now(tz=UTC)
    response = client.post("/v1/feature-requests", json={
        "entity_type": "application",
        "entity_key": {"user_id": "u", "application_id": "a"},
        "view": "user_credit_risk", "view_version": 1,
        "requested_features": ["declared_income"],
        "reports": [{"source_name": "credit_report", "report_type": "credit_report",
                     "report_ts": OLD.isoformat(),
                     "payload": {"declared_income": 100, "monthly_obligations": 10}}],
    })
    assert response.status_code == 200
    event = backend.events.published[-1].event
    descriptor = event.reports[0]
    # Historical report_ts is preserved, but availability is the ACCEPT time — a
    # service caller has no field to backdate it.
    assert descriptor.report_ts == OLD
    assert descriptor.available_at is not None
    assert before <= descriptor.available_at <= datetime.now(tz=UTC)


# --- 13/14: timezone rules ------------------------------------------------------------


def test_naive_available_at_is_rejected_everywhere():
    with pytest.raises(ValueError, match="available_at"):
        SourceStamp(report_ts=NOW, content_hash="sha256:x",
                    available_at=datetime(2026, 1, 1))
    backend = _memory_backend()
    manifest = run_jsonl_ingestion(
        backend=backend,
        lines=[json.dumps({"entity_key": {"user_id": "n", "application_id": "n"},
                           "event_ts": OLD.isoformat(),
                           "available_at": "2026-01-01T00:00:00",
                           "payload": {"x": 1}})],
        entity_type="application", source_name="credit_bureau_report",
        report_type="credit_bureau_report", dataset_id="avail_naive")
    assert manifest.item_count_rejected == 1


# --- 15: lineage ----------------------------------------------------------------------


def test_lineage_exposes_availability_values_free():
    offline = InMemoryOfflineStore()
    metas = InMemoryMetaRepository()
    entity = _key("lin")
    result = _bureau_result(entity, available_at=HIST_AVAILABLE,
                            availability_source="source_provided")
    offline.append(_VIEW, _VV, result)
    metas.add(RawReportMeta(
        report_ref="rep_lin", report_type="credit_bureau_report",
        entity_type="application", entity_key=entity, report_ts=OLD,
        payload_size_bytes=10, content_hash="sha256:c1", storage_uri="s3://x",
        created_at=NOW, available_at=HIST_AVAILABLE,
        availability_source="source_provided"))
    lineage = build_feature_lineage(
        offline, metas, entity, view=_VIEW, view_version=_VV,
        feature_name="bureau_score", feature_version=1, report_refs=["rep_lin"])
    payload = lineage.to_dict()
    assert payload["available_at"] == HIST_AVAILABLE.isoformat()
    assert payload["availability_effective"] == HIST_AVAILABLE.isoformat()
    assert payload["availability_source"] == "source_provided"
    assert payload["report_refs"][0]["available_at"] == HIST_AVAILABLE.isoformat()
    assert payload["report_refs"][0]["availability_source"] == "source_provided"
    assert "640" not in json.dumps(payload)  # values-free


def test_legacy_row_lineage_reports_calc_ts_as_effective_availability():
    offline = InMemoryOfflineStore()
    metas = InMemoryMetaRepository()
    entity = _key("lin_legacy")
    result = _bureau_result(entity)
    offline.append(_VIEW, _VV, result)
    lineage = build_feature_lineage(
        offline, metas, entity, view=_VIEW, view_version=_VV,
        feature_name="bureau_score", feature_version=1)
    payload = lineage.to_dict()
    assert payload["available_at"] is None
    assert payload["availability_effective"] == result.calc_ts.isoformat()
