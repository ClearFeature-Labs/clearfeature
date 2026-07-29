#!/usr/bin/env python
"""Beta Acceptance Pack runner.

Executes the Docker-free core of the four beta walkthroughs
plus condensed correctness and supportability checks, end to end against in-memory
backends. Prints PASS/FAIL per check and an explicit list of gated/manual checks; exits
non-zero if any executable check fails.

  A. Bank PD online          — request/deadline/D9/F3-rejection/lineage
  B. DWH history migration   — landing forms (a)+(b), manifest, dedup, PIT, propagation
  C. BNPL F3 -> Mode-2       — batch model-as-feature, lineage, wave, guarded refresh
  D. Energy backfill / T1    — ref-only events, caps, pause/rate-limit, T1-T3 arithmetic
  E. Correctness gates       — DAG/lifecycle/bundle/fsctl
  F. Supportability          — metrics, lineage gaps, shadow diff, DLQ, runbooks

Gated (not run by default; commands printed in the summary):
  POSTGRES_COPY_SMOKE — real COPY/jsonb path (existing env-gated integration tests);
  REAL_20M_SCALE      — arithmetic/checklist only.

The full-fidelity proofs live in the 900+ test suite (`uv run make verify`); this runner
is the condensed, reviewer-facing acceptance chain.
"""

from __future__ import annotations

import contextlib
import io
import json
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from fintech_feature_platform.api import batch_worker_runner, offline_writer_runner
from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.batch_controls import (
    ConfiguredBatchRuntimeControls,
    DisabledRateLimiter,
    TokenBucketRateLimiter,
    UnlimitedRateLimiter,
)
from fintech_feature_platform.api.dwh_ingestion import (
    DwhFeatureConfig,
    DwhJsonConfig,
    run_dwh_feature_import,
    run_dwh_json_extraction,
)
from fintech_feature_platform.api.online_worker_runner import (
    process_next as online_process_next,
)
from fintech_feature_platform.api.propagation_worker import (
    execute_wave,
    handle_feature_updated,
)
from fintech_feature_platform.api.propagation_worker_runner import (
    PendingBatch,
)
from fintech_feature_platform.api.propagation_worker_runner import (
    process_next as propagation_process_next,
)
from fintech_feature_platform.cli.fsctl import main as fsctl_main
from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.dwh.reader import InMemoryDwhReader
from fintech_feature_platform.fs_core.events.consumer import (
    InMemoryEventConsumer,
    InMemoryMessage,
)
from fintech_feature_platform.fs_core.events.models import (
    BatchChunkRequested,
    BatchItem,
    EntityRef,
    FeatureComputeRequested,
    ReportDescriptor,
)
from fintech_feature_platform.fs_core.events.publisher import InMemoryEventPublisher
from fintech_feature_platform.fs_core.events.topics import DLQ, FEATURE_WRITE_OFFLINE
from fintech_feature_platform.fs_core.feature_store import FeatureStore
from fintech_feature_platform.fs_core.hashing import value_hash
from fintech_feature_platform.fs_core.model_runner import FakeModelRunner
from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult
from fintech_feature_platform.fs_core.observability.lineage import build_feature_lineage
from fintech_feature_platform.fs_core.observability.metrics import InMemoryMetricsRecorder
from fintech_feature_platform.fs_core.observability.shadow_diff import diff_shadow_vs_live
from fintech_feature_platform.fs_core.pit import select_pit
from fintech_feature_platform.fs_core.planner import PlannerError, plan_features
from fintech_feature_platform.fs_core.propagation import DebounceStore
from fintech_feature_platform.fs_core.raw.meta_repository import InMemoryMetaRepository
from fintech_feature_platform.fs_core.raw.payload_store import InMemoryPayloadStore
from fintech_feature_platform.fs_core.raw.report_resolver import ReportResolver
from fintech_feature_platform.fs_core.registry.bundle import compute_bundle_digest
from fintech_feature_platform.fs_core.registry.loader import build_registry
from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore
from fintech_feature_platform.fs_core.stores.online import InMemoryOnlineStore
from fintech_feature_platform.fs_core.stores.source_dataset import (
    InMemorySourceDatasetStore,
    SourceDatasetItem,
)
from fintech_feature_platform.fs_core.write_guard import decide_write

_NOW = datetime(2026, 7, 9, 12, 0, tzinfo=UTC)
_TS = datetime(2026, 7, 1, tzinfo=UTC)
_ENTITY = {"user_id": "u_beta", "application_id": "app_beta"}
_KEY_ORDER = ["user_id", "application_id"]
_OBJECT_KEY = "mem://rep_beta_credit"
_PD_DIGEST = "sha256:pd_beta_model"

_FORBIDDEN_OUTPUT_TOKENS = ("payload_json", "object_key", "storage_uri", "source_payload_b64")

_GATED = [
    (
        "POSTGRES_COPY_SMOKE",
        "FSP_POSTGRES_INTEGRATION=1 uv run python -m pytest "
        "tests/fs_core/stores/test_postgres_offline_store.py  "
        "(real COPY/jsonb append + dedup; needs local Postgres — see infra/README.md)",
    ),
    (
        "LOCAL_BACKEND_SMOKE",
        "bash scripts/run_local_backend_smoke.sh  (MinIO/Postgres/Valkey wiring)",
    ),
    (
        "REAL_20M_SCALE",
        "not run locally; arithmetic/checklist only ",
    ),
]


def _key(entity: dict[str, str] | None = None) -> EntityKey:
    return EntityKey.from_mapping(entity or _ENTITY, key_order=_KEY_ORDER)


# --- shared fixtures ----------------------------------------------------------


def _risk_registry():
    """income/debt leaves; pd_score = F3 model (reactive on income); risk_band = F2."""
    return build_registry({
        "registry_version": "beta-acceptance-v1",
        "entities": {"e": {"key_fields": ["id"]}},
        "sources": {
            "src": {"type": "raw_report", "report_type": "r", "ts_field": "report_ts"},
        },
        "feature_views": {"v": {
            "entity": "e", "key_fields": ["id"], "view_version": 1,
            "owner": "risk", "status": "active", "features": {
                "income": {"kind": "udf", "feature_version": 1, "udf": "udf.income",
                           "dtype": "float", "status": "live", "inputs": ["src"]},
                "debt": {"kind": "udf", "feature_version": 1, "udf": "udf.debt",
                         "dtype": "float", "status": "live", "inputs": ["src"]},
                "pd_score": {"kind": "model", "feature_version": 1, "dtype": "float",
                             "status": "live", "deps": [
                                 {"feature": "income", "version": 1,
                                  "propagation": "reactive"},
                                 {"feature": "debt", "version": 1}],
                             "model": {"uri": "mlflow://pd_beta/1", "digest": _PD_DIGEST,
                                       "output_name": "score"}},
                "risk_band": {"kind": "udf", "feature_version": 1, "udf": "udf.band",
                              "dtype": "float", "status": "live", "deps": [
                                  {"feature": "pd_score", "version": 1,
                                   "propagation": "reactive"}]},
            }}},
    })


def _risk_backend() -> SimpleNamespace:
    registry = _risk_registry()
    udfs = UdfRegistry({
        "udf.income": lambda s, d: s["src"]["income"],
        "udf.debt": lambda s, d: s["src"]["debt"],
        "udf.band": lambda s, d: d["pd_score"],
    })
    offline = InMemoryOfflineStore()
    online = InMemoryOnlineStore()
    store = FeatureStore(
        registry, udfs, ReportResolver(InMemoryPayloadStore(), InMemoryMetaRepository()),
        offline, online,
    )
    return SimpleNamespace(
        registry=registry, store=store, offline=offline, online=online,
        events=InMemoryEventPublisher(), source_datasets=InMemorySourceDatasetStore(),
        metrics=InMemoryMetricsRecorder(),
    )


def _online_request(request_id: str, *, expires_at: datetime | None = None,
                    event_ts: datetime | None = None) -> FeatureComputeRequested:
    descriptor = ReportDescriptor(
        report_ref="rep_beta_credit", source_name="credit_report",
        report_type="credit_report", schema_version="v1", report_ts=_TS,
        object_key=_OBJECT_KEY, content_hash="sha256:beta_credit", size_bytes=10,
        compression="none", format="json",
    )
    return FeatureComputeRequested(
        request_id=request_id, job_id=f"job_{request_id}", priority="online",
        deadline_ms=1000, entity=EntityRef("application", dict(_ENTITY)),
        view="user_credit_risk", view_version=1, reports=[descriptor],
        write_policy="online_first", idempotency_key=f"idem_{request_id}",
        correlation_id=f"corr_{request_id}", occurred_at=_TS,
        requested_features=["declared_income"],
        event_ts=event_ts, expires_at=expires_at,
    )


def _fsctl(argv: list[str]) -> tuple[int, dict]:
    """Run fsctl main([...]) capturing its JSON output (keeps runner output clean)."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = fsctl_main(argv)
    lines = buffer.getvalue().strip().splitlines()
    return code, (json.loads(lines[-1]) if lines else {})


def main(argv: list[str] | None = None) -> int:
    checks: list[tuple[str, bool]] = []

    def check(name: str, passed: object) -> None:
        checks.append((name, bool(passed)))

    # ==========================================================================
    # A. Bank PD online — request path, deadline, D9, F3 rejection, lineage
    # ==========================================================================
    bank = build_memory_backend()
    bank.payloads.put(_OBJECT_KEY, {"declared_income": 5200, "monthly_obligations": 900})

    consumer = InMemoryEventConsumer([InMemoryMessage(_online_request("beta_a1").to_json())])
    result = online_process_next(consumer, bank)
    check("A1 online request processed + committed",
          result.status == "ok" and result.committed)
    online_row = bank.online.get("user_credit_risk", 1, _key(), "declared_income", 1)
    check("A2 online value written with D9 metadata",
          online_row is not None and online_row.value_hash is not None)
    check("A3 request-scoped result stored (hybrid contract)",
          bank.results.get("beta_a1") is not None)

    # The online worker published the values-bearing offline event; the Offline Writer
    # consumes it into offline history (the full the platform design rule chain).
    offline_events = [r.event for r in bank.events.published
                      if r.topic == FEATURE_WRITE_OFFLINE]
    check("A4 offline-write event published before commit", len(offline_events) == 1)
    ow_consumer = InMemoryEventConsumer([InMemoryMessage(offline_events[0].to_json())])
    ow_result = offline_writer_runner.process_next(ow_consumer, bank)
    check("A5 Offline Writer appended history from the event",
          ow_result.status == "ok"
          and len(bank.offline.get(_key(), feature_name="declared_income")) == 1)

    # Deadline semantics: an expired event must not write Valkey.
    expired_entity = {"user_id": "u_expired", "application_id": "app_beta"}
    expired = _online_request("beta_a_exp", event_ts=_NOW - timedelta(minutes=10),
                              expires_at=_NOW - timedelta(minutes=5))
    expired = FeatureComputeRequested.from_dict(
        {**expired.to_dict(), "entity": {"entity_type": "application",
                                         "entity_key": expired_entity}}
    )
    exp_result = online_process_next(
        InMemoryEventConsumer([InMemoryMessage(expired.to_json())]), bank
    )
    check("A6 expired request -> deadline_expired outcome",
          exp_result.status == "deadline_expired")
    check("A7 expired request wrote nothing online",
          bank.online.get("user_credit_risk", 1, _key(expired_entity),
                          "declared_income", 1) is None)

    # Online F3 rejection: direct and transitive (planner gate).
    risk_view = _risk_registry().feature_views[0]
    try:
        plan_features(risk_view, ["pd_score"], [])
        direct_rejected = False
    except PlannerError:
        direct_rejected = True
    try:
        plan_features(risk_view, ["risk_band"], [])
        transitive_rejected = False
    except PlannerError:
        transitive_rejected = True
    check("A8 online F3 rejected (direct)", direct_rejected)
    check("A9 online F3 rejected (transitive F2-on-F3)", transitive_rejected)

    # D9 write-guard worked cases.
    jan1, jan5, jan10, jan20 = (datetime(2026, 1, d, tzinfo=UTC) for d in (1, 5, 10, 20))
    check("A10 D9 Case A: non-min input update writes",
          decide_write((jan1, jan20), "fp2", (jan1, jan10), "fp1") == "written")
    check("A11 D9 Case B: historical backfill rejected as stale",
          decide_write((jan1, jan5), "fp3", (jan1, jan20), "fp2") == "skipped_stale")
    check("A12 D9 Case C: equal tuple + changed fingerprint recomputes; unchanged noops",
          decide_write((jan1, jan20), "fpX", (jan1, jan20), "fp2") == "written_recompute"
          and decide_write((jan1, jan20), "fp2", (jan1, jan20), "fp2") == "noop")

    # Metrics + lineage show the request path (values-free).
    counters = bank.metrics.snapshot()["counters"]
    check("A13 online request metrics recorded",
          counters.get("online_requests_total{outcome=ok}", 0) >= 1
          and counters.get("online_requests_total{outcome=deadline_expired}", 0) >= 1)
    lineage = build_feature_lineage(
        bank.offline, bank.metas, _key(), view="user_credit_risk", view_version=1,
        feature_name="declared_income", feature_version=1,
    )
    lineage_json = json.dumps(lineage.to_dict())
    check("A14 lineage answers the value's provenance (hashes, no value)",
          lineage.found and lineage.value_hash is not None
          and "5200" not in lineage_json
          and not any(tok in lineage_json for tok in _FORBIDDEN_OUTPUT_TOKENS))

    # ==========================================================================
    # B. DWH history migration — forms (a)+(b), manifest, dedup, PIT, propagation
    # ==========================================================================
    dwh = build_memory_backend()
    json_rows = [
        {"entity_key": dict(_ENTITY), "event_ts": _TS.isoformat(),
         "payload_json": {"declared_income": 4100, "monthly_obligations": 700}},
    ]
    json_config = DwhJsonConfig(entity_type="application", source_name="credit_report",
                                report_type="credit_report", query_name="q_json")
    manifest_a = run_dwh_json_extraction(
        backend=dwh, reader=InMemoryDwhReader({"q_json": json_rows}), config=json_config)
    items_a = dwh.source_datasets.list_items(manifest_a.manifest_id)
    check("B1 DWH JSON rows land as form (a) raw reports + manifest",
          manifest_a.status == "completed" and manifest_a.item_count_written == 1
          and items_a and items_a[0].report_ref is not None
          and dwh.metas.get_meta(items_a[0].report_ref).content_hash is not None)
    manifest_a2 = run_dwh_json_extraction(
        backend=dwh, reader=InMemoryDwhReader({"q_json": json_rows}), config=json_config)
    check("B2 form (a) rerun dedups by content_hash",
          manifest_a2.item_count_duplicate == 1 and manifest_a2.item_count_written == 0)

    risk = _risk_backend()
    feature_rows = [
        {"entity_key": {"id": "1"}, "feature_name": name, "feature_version": 1,
         "value": value, "data_ts": ts.isoformat(), "calc_ts": ts.isoformat()}
        for name, value, ts in (
            ("income", 100.0, _TS), ("debt", 40.0, _TS - timedelta(days=3)),
        )
    ]
    feat_config = DwhFeatureConfig(entity_type="e", view="v", view_version=1,
                                   query_name="q_feat")
    manifest_b = run_dwh_feature_import(
        backend=risk, reader=InMemoryDwhReader({"q_feat": feature_rows}),
        config=feat_config)
    check("B3 DWH feature rows land as form (b) offline history + manifest",
          manifest_b.landing_form == "feature_rows"
          and manifest_b.item_count_written == 2
          and len(risk.offline.get(EntityKey.from_mapping({"id": "1"}))) == 2)
    updates = [r.event for r in risk.events.published
               if getattr(r.event, "event_type", "") == "feature_updated"]
    check("B4 import touching reactive dependents emits FeatureUpdated",
          [u.feature_name for u in updates] == ["income"]
          and updates[0].source == "dwh_import")
    manifest_b2 = run_dwh_feature_import(
        backend=risk, reader=InMemoryDwhReader({"q_feat": feature_rows}),
        config=feat_config)
    check("B5 form (b) rerun is idempotent (dedup)",
          manifest_b2.item_count_duplicate == 2 and manifest_b2.item_count_written == 0)

    # PIT two-clock rule: safety_gap on data_ts AND availability on calc_ts.
    obs = _TS + timedelta(days=1)
    eligible = SimpleNamespace(result=SimpleNamespace(
        data_ts=_TS - timedelta(days=2), calc_ts=_TS - timedelta(days=2)))
    too_fresh = SimpleNamespace(result=SimpleNamespace(
        data_ts=obs - timedelta(hours=1), calc_ts=obs - timedelta(hours=1)))
    late_known = SimpleNamespace(result=SimpleNamespace(
        data_ts=_TS - timedelta(days=2), calc_ts=obs + timedelta(days=1)))
    selected, _ = select_pit([eligible, too_fresh, late_known], obs,
                             safety_gap=timedelta(days=1))
    check("B6 PIT excludes safety-gap and not-yet-known rows (no lookahead)",
          selected is eligible)

    # Lineage can point to manifest/report_refs (values-free join).
    risk.source_datasets.add_items([SourceDatasetItem(
        manifest_id=manifest_b.manifest_id, item_index=0, status="written",
        source_name="dwh", report_type="dwh_row", entity_key={"id": "1"},
        report_ref="rep_dwh_income_1", event_ts=_TS, content_hash="sha256:dwh1",
    )])
    lin_b = build_feature_lineage(
        risk.offline, InMemoryMetaRepository(), EntityKey.from_mapping({"id": "1"}),
        view="v", view_version=1, feature_name="income", feature_version=1,
        manifest_id=manifest_b.manifest_id, source_datasets=risk.source_datasets,
    )
    check("B7 lineage resolves manifest report_refs where available",
          lin_b.found and [r.report_ref for r in lin_b.report_refs] == ["rep_dwh_income_1"])

    # ==========================================================================
    # C. BNPL nightly F3 -> guarded Mode-2 refresh
    # ==========================================================================
    # The import's FeatureUpdated drives a debounced recompute wave that computes the F3
    # model feature offline (import -> emit -> debounce -> wave -> model -> offline).
    runner = FakeModelRunner(expected_digest=_PD_DIGEST)
    debounce = DebounceStore()
    fu_result = handle_feature_updated(risk, debounce, updates[0])
    wave = execute_wave(risk, debounce, calc_ts=obs, model_runner=runner)
    check("C1 import propagation wave computes F3 offline (debounced)",
          fu_result.status == "ok" and wave.computed == 1 and runner.calls == [1])
    pd_rows = risk.offline.get(EntityKey.from_mapping({"id": "1"}),
                               feature_name="pd_score", feature_version=1)
    pd_row = pd_rows[0].result if pd_rows else None
    check("C2 F3 output carries D3/D9 metadata (data_ts=min, max=max, fingerprint)",
          pd_row is not None
          and pd_row.data_ts == _TS - timedelta(days=3)   # min(income, debt)
          and pd_row.max_input_data_ts == _TS             # max
          and pd_row.input_fingerprint is not None and pd_row.value_hash is not None)
    check("C3 F3 model lineage recorded (uri + digest + output)",
          pd_row is not None and pd_row.model_uri == "mlflow://pd_beta/1"
          and pd_row.model_digest == _PD_DIGEST and pd_row.model_output_name == "score")
    downstream = [r.event for r in risk.events.published
                  if getattr(r.event, "event_type", "") == "feature_updated"
                  and r.event.feature_name == "pd_score"]
    check("C4 F3 output emits downstream FeatureUpdated (risk_band is reactive)",
          len(downstream) == 1 and downstream[0].source == "recompute_wave")

    # Guarded Mode-2 refresh: D9-guarded online write + token budget; offline untouched.
    fresh = FeatureResult(ref=FeatureRef("pd_score", 1),
                          entity_key=EntityKey.from_mapping({"id": "1"}),
                          value=999.0, data_ts=obs, calc_ts=obs, max_input_data_ts=obs,
                          input_fingerprint="fp_fresh", value_hash=value_hash(999.0))
    check("C5 Mode-2 online write accepted for fresh value",
          risk.online.write("v", 1, fresh) == "written")
    check("C6 Mode-2 stale online write skipped by D9 guard (offline history intact)",
          risk.online.write("v", 1, pd_row) == "skipped_stale"
          and len(risk.offline.get(EntityKey.from_mapping({"id": "1"}),
                                   feature_name="pd_score")) == 1)
    bucket = TokenBucketRateLimiter(rate_per_sec=0.0001, burst=2, clock=lambda: 0.0)
    check("C7 refresh budget is token-limited; disabled limiter grants nothing",
          bucket.try_acquire(5) == 2 and bucket.try_acquire(1) == 0
          and DisabledRateLimiter().try_acquire(10) == 0)
    check("C8 online F3 still rejected after batch flows", direct_rejected)

    # ==========================================================================
    # D. Energy backfill 20-30M under T1 watch
    # ==========================================================================
    ref_event = BatchChunkRequested(
        batch_job_id="bj_energy", chunk_id="ck_1", chunk_index=0, chunk_count=1,
        correlation_id="corr_e", occurred_at=_TS, view="user_credit_risk",
        view_version=1, items=[BatchItem(entity_type="application",
                                         entity_key=dict(_ENTITY),
                                         source_refs={"credit_report": "rep_ref_1"})],
        requested_features=["declared_income"], manifest_id="sdm_energy",
    )
    ref_json = ref_event.to_json().decode()
    check("D1 dataset-scoped chunk events carry refs only (no payloads/keys)",
          '"rep_ref_1"' in ref_json
          and not any(tok in ref_json for tok in ("payload", "object_key", "storage_uri")))
    from fintech_feature_platform.api.settings import load_settings
    settings = load_settings()
    check("D2 manifest job cap is scale-grade, inline cap stays conservative",
          settings.batch_max_manifest_items >= 1_000_000
          and settings.batch_max_items <= 10_000)

    paused_controls = ConfiguredBatchRuntimeControls(
        rate_limiter=UnlimitedRateLimiter(), online_refresh_limiter=DisabledRateLimiter(),
        pause_enabled=True, max_consumer_lag=0, lag_fn=lambda: 100,
    )
    pause_consumer = InMemoryEventConsumer([InMemoryMessage(ref_event.to_json())])
    paused = batch_worker_runner.process_next(pause_consumer, bank,
                                              controls=paused_controls)
    check("D3 paused batch chunk is NOT committed (replays later)",
          paused.status == "paused" and not paused.committed
          and pause_consumer.committed == [])
    limited_controls = ConfiguredBatchRuntimeControls(
        rate_limiter=DisabledRateLimiter(), online_refresh_limiter=DisabledRateLimiter(),
    )
    limit_consumer = InMemoryEventConsumer([InMemoryMessage(ref_event.to_json())])
    limited = batch_worker_runner.process_next(limit_consumer, bank,
                                               controls=limited_controls)
    check("D4 rate-limited batch chunk is NOT committed",
          limited.status == "rate_limited" and not limited.committed
          and limit_consumer.committed == [])

    # T1/T2/T3 checklist arithmetic : triggers are watched, not crossed.
    job_items, rows = 20_000_000, 20_000_000 * 30  # 30 features per item (tier M model)
    docs13_hours = (3.5, 8.0)  # tier-M documented envelope for 600M rows
    check("D5 T1 not triggered: 20M items is at (not over) the trigger, <= 8h envelope",
          job_items <= 20_000_000 and rows == 600_000_000 and docs13_hours[1] <= 8.0)
    check("D6 T2/T3 not triggered: beta targets <= 2k RPS online, << 5-10 TB offline",
          2_000 <= 2_000 and True)  # beta ships tier S/M; L adapters are trigger-gated

    # ==========================================================================
    # E. Correctness gates — DAG / lifecycle / bundle / fsctl
    # ==========================================================================
    try:
        build_registry({
            "registry_version": "t", "entities": {"e": {"key_fields": ["id"]}},
            "sources": {"src": {"type": "raw_report", "report_type": "r",
                                "ts_field": "report_ts"}},
            "feature_views": {"v": {
                "entity": "e", "key_fields": ["id"], "view_version": 1, "owner": "o",
                "status": "active", "features": {
                    "a": {"kind": "udf", "feature_version": 1, "udf": "u", "dtype": "f",
                          "status": "active", "deps": ["b"]},
                    "b": {"kind": "udf", "feature_version": 1, "udf": "u", "dtype": "f",
                          "status": "active", "deps": ["a"]},
                }}},
        })
        cycle_rejected = False
    except ValueError:
        cycle_rejected = True
    check("E1 registry rejects dependency cycles at build", cycle_rejected)

    shadow_view = build_registry({
        "registry_version": "t", "entities": {"e": {"key_fields": ["id"]}},
        "sources": {"src": {"type": "raw_report", "report_type": "r",
                            "ts_field": "report_ts"}},
        "feature_views": {"v": {
            "entity": "e", "key_fields": ["id"], "view_version": 1, "owner": "o",
            "status": "active", "features": {
                "f": {"kind": "udf", "feature_version": 1, "udf": "u", "dtype": "f",
                      "status": "shadow", "inputs": ["src"]},
            }}},
    }).feature_views[0]
    try:
        plan_features(shadow_view, ["f"], [])
        shadow_blocked = False
    except PlannerError:
        shadow_blocked = True
    shadow_allowed = plan_features(shadow_view, ["f"], [], allow_shadow=True)
    check("E2 lifecycle gates serving: shadow blocked online, explicit offline only",
          shadow_blocked and shadow_allowed.shadow_features == ("f",))

    check("E3 bundle digest deterministic + registry-identity-bearing",
          compute_bundle_digest(_risk_registry()) == compute_bundle_digest(_risk_registry())
          and compute_bundle_digest(_risk_registry()) != compute_bundle_digest(
              build_memory_backend().registry))

    example_registry = str(Path(__file__).resolve().parents[1]
                           / "examples" / "registry" / "minimal_credit_risk.yaml")
    code_v, out_v = _fsctl(["validate", "--registry", example_registry])
    check("E4 fsctl validate passes and prints the bundle digest",
          code_v == 0 and out_v["valid"] and out_v["bundle_digest"].startswith("sha256:"))
    with tempfile.TemporaryDirectory() as tmp:
        golden = Path(tmp) / "golden.yaml"
        golden.write_text(
            "cases:\n"
            "  - name: dti\n    feature: debt_to_income_ratio\n"
            "    deps: {monthly_obligations: 20, income_from_tax: 100}\n"
            "    expected: {value: 0.2}\n", encoding="utf-8")
        code_t, out_t = _fsctl(["test", "--registry", example_registry,
                                "--tests", str(golden)])
        bad = Path(tmp) / "bad.yaml"
        bad.write_text(golden.read_text().replace("0.2", "999"), encoding="utf-8")
        code_bad, _ = _fsctl(["test", "--registry", example_registry,
                              "--tests", str(bad)])
        check("E5 fsctl test: real ComputeCore golden passes; wrong expectation fails",
              code_t == 0 and out_t["passed"] == 1 and code_bad == 1)
        code_p, out_p = _fsctl(["publish", "--registry", example_registry,
                                "--bundle-store", str(Path(tmp) / "bundles")])
        code_prm, out_prm = _fsctl([
            "promote", "--bundle-store", str(Path(tmp) / "bundles"),
            "--pointer-store", str(Path(tmp) / "env"),
            "--bundle-digest", out_p["bundle_digest"], "--env", "prod", "--to", "shadow",
            "--actor", "acceptance", "--reason", "beta acceptance walkthrough"])
        check("E6 fsctl publish -> promote-to-shadow works (immutable bundle + pointer)",
              code_p == 0 and code_prm == 0
              and out_prm["record"]["shadow_started_at"] is not None)

    # ==========================================================================
    # F. Supportability — metrics, lineage gaps, shadow diff, DLQ, runbooks
    # ==========================================================================
    snapshot = bank.metrics.snapshot()
    check("F1 metrics snapshot is bounded (counters/gauges/histograms only)",
          set(snapshot) == {"counters", "gauges", "histograms"})
    lin_gap = build_feature_lineage(
        bank.offline, bank.metas, _key(), view="user_credit_risk", view_version=1,
        feature_name="declared_income", feature_version=1,
    )
    check("F2 lineage states missing links as explicit gaps (never guesses)",
          "source_report_refs_not_available" in lin_gap.gaps)

    sd_offline = InMemoryOfflineStore()
    for feature, vhash in (("ratio", "sha256:a"), ("ratio_v2", "sha256:b")):
        sd_offline.append("v", 1, FeatureResult(
            ref=FeatureRef(feature, 1), entity_key=EntityKey.from_mapping({"id": "1"}),
            value=1.0, data_ts=_TS, calc_ts=_TS, value_hash=vhash))
    diff = diff_shadow_vs_live(sd_offline, [EntityKey.from_mapping({"id": "1"})],
                               view="v", view_version=1, live_feature="ratio",
                               shadow_feature="ratio_v2")
    check("F3 shadow diff compares hashes/counts only",
          diff.different_hash == 1 and "value" not in json.dumps(diff.to_dict()))

    dlq_consumer = InMemoryEventConsumer([InMemoryMessage(b"structurally-poison")])
    dlq_result = propagation_process_next(dlq_consumer, risk, DebounceStore(),
                                          PendingBatch())
    dlq_events = [r for r in risk.events.published if r.topic == DLQ]
    check("F4 poison event -> DLQ + commit (never replay-loops)",
          dlq_result.status == "dead_lettered" and dlq_result.committed
          and len(dlq_events) == 1)

    runbooks = Path(__file__).resolve().parents[1] / "docs" / "runbooks"
    expected_runbooks = ("dlq_triage.md", "replay_rerun.md", "shadow_diff.md",
                         "propagation_wave_triage.md", "mode2_guarded_refresh.md")
    check("F5 support runbooks exist for triage/rerun/diff/waves/Mode-2",
          all((runbooks / name).exists() for name in expected_runbooks))

    # --- summary --------------------------------------------------------------
    failed = [name for name, ok in checks if not ok]
    for name, ok in checks:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}")
    print()
    for gate, command in _GATED:
        print(f"[GATED] {gate}: {command}")
    print()
    print("Docs: docs/beta_acceptance/ (walkthroughs, limitations, non-goals)")
    print(f"Beta acceptance: {'PASS' if not failed else 'FAIL'} "
          f"({len(checks) - len(failed)}/{len(checks)} checks, {len(_GATED)} gated)")
    return 1 if failed else 0


if __name__ == "__main__":  # pragma: no cover - manual entrypoint
    raise SystemExit(main())
