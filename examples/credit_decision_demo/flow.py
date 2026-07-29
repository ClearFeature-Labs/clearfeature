"""Batch demo flow steps over the REAL platform seams.

Each step is a thin driver around an existing platform path — no feature logic lives here:

    ingest_dataset       -> run_jsonl_ingestion (landing form (a), one manifest per source)
    f2_topological_order -> registry deps (which F2 level computes after which)
    run_f2_batch         -> FeatureStore.compute_dependent_from_offline (real DAG) + bulk append
    run_f3_batch         -> compute_model_feature_batch (PIT reads, D3/D9, model lineage)
    guarded_online_refresh -> D9-guarded OnlineStore.write behind a token bucket (Mode-2)

The steps take an ``AppBackend``, so the same code runs in-memory (tests/fixture) and
against the live local backend (Postgres/MinIO/Valkey) from the host.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from examples.credit_decision_demo.generator import SOURCES
from fintech_feature_platform.api.batch_controls import TokenBucketRateLimiter
from fintech_feature_platform.api.jsonl_ingestion import run_jsonl_ingestion
from fintech_feature_platform.api.model_feature_batch import compute_model_feature_batch
from fintech_feature_platform.fs_core.dedup import partition_new_results
from fintech_feature_platform.fs_core.models import EntityKey
from fintech_feature_platform.fs_core.registry.models import Registry

VIEW = "credit_decision"
VIEW_VERSION = 1

DATA_FILES = {
    "application_request": "applications.jsonl",
    "tax_report": "tax_reports.jsonl",
    "credit_bureau_report": "credit_bureau_reports.jsonl",
    "telco_report": "telco_reports.jsonl",
    "socdem_report": "socdem_reports.jsonl",
}

# One manifest-scoped batch job per source computes exactly that source's F1 group.
SOURCE_FEATURE_GROUPS = {
    "application_request": "application_request_v1",
    "tax_report": "tax_income_v1",
    "credit_bureau_report": "bureau_risk_v1",
    "telco_report": "telco_profile_v1",
    "socdem_report": "socdem_profile_v1",
}


def entity_key(user_id: str, application_id: str) -> EntityKey:
    return EntityKey.from_mapping(
        {"user_id": user_id, "application_id": application_id},
        key_order=["user_id", "application_id"],
    )


def fast_local_backend(backend, settings):
    """Single-connection Postgres stores for the demo's bulk host-side steps.

    The local backend opens a fresh connection per operation (the documented no-pooling
    limitation) — fine for request traffic, prohibitive for a 30k portfolio batch
    (~1M+ connection opens across F2/F3/refresh). This returns a backend view whose
    offline + raw-meta stores hold ONE connection each, and a FeatureStore rebuilt on
    the fast offline store so ``compute_dependent_from_offline`` uses it too. Memory
    mode is returned unchanged. Demo plumbing only — same store classes, no new logic.
    """
    if settings.backend != "local":
        return backend
    from examples.credit_decision_demo.features import build_registry_and_udfs
    from fintech_feature_platform.fs_core.feature_store import FeatureStore
    from fintech_feature_platform.fs_core.raw.postgres_meta_repository import (
        PostgresRawReportMetaRepository,
        connect_postgres,
    )
    from fintech_feature_platform.fs_core.raw.report_resolver import ReportResolver
    from fintech_feature_platform.fs_core.stores.postgres_offline import (
        PostgresOfflineStore,
    )

    offline = PostgresOfflineStore(connect_postgres(settings.postgres_dsn))
    metas = PostgresRawReportMetaRepository(connect_postgres(settings.postgres_dsn))
    registry, udfs = build_registry_and_udfs()
    store = FeatureStore(
        registry, udfs, ReportResolver(backend.payloads, metas), offline, backend.online
    )
    return SimpleNamespace(
        registry=registry, store=store, offline=offline, online=backend.online,
        payloads=backend.payloads, metas=metas, events=backend.events,
        source_datasets=backend.source_datasets, metrics=backend.metrics,
        make_storage_uri=backend.make_storage_uri, raw_format=backend.raw_format,
        raw_compression=backend.raw_compression,
        serialize_payload=backend.serialize_payload,
    )


def ingest_dataset(backend, data_dir: Path) -> dict[str, Any]:
    """Land every source file into form (a); returns {source_name: manifest}."""
    manifests = {}
    for source_name in SOURCES:
        path = data_dir / DATA_FILES[source_name]
        with path.open(encoding="utf-8") as handle:
            manifests[source_name] = run_jsonl_ingestion(
                backend=backend,
                lines=handle,
                entity_type="application",
                source_name=source_name,
                report_type=SOURCES[source_name][1],
                dataset_id=f"credit_demo_{source_name}",
                input_uri=str(path),
            )
    return manifests


def f2_topological_order(registry: Registry, feature_names: list[str]) -> list[str]:
    """Order the F2 features so every dependency level is offline before its dependents."""
    view = next(v for v in registry.feature_views if v.name == VIEW)
    features = {f.name: f for f in view.features}
    ordered: list[str] = []
    visiting: set[str] = set()

    def visit(name: str) -> None:
        if name in ordered or name not in feature_names:
            return
        if name in visiting:  # cycles are rejected at registry build; defensive only
            raise ValueError(f"cycle at {name}")
        visiting.add(name)
        for dep in features[name].deps:
            visit(dep.feature)
        visiting.discard(name)
        ordered.append(name)

    for name in feature_names:
        visit(name)
    return ordered


def run_f2_batch(
    backend,
    entity_keys: list[EntityKey],
    feature_names: list[str],
    calc_ts: datetime,
) -> dict[str, int]:
    """Recompute the F2 layer from offline history via the real DAG, level by level.

    Uses ``compute_dependent_from_offline`` (the earlier recompute seam: PIT-safe inputs,
    D3/D9 metadata, per-node staleness) per entity per feature, bulk-appending each
    feature's results once (COPY on Postgres). Entities missing a required input are
    counted skipped, never fabricated.
    """
    ordered = f2_topological_order(backend.registry, feature_names)
    computed = skipped = 0
    for feature_name in ordered:
        results = []
        for key in entity_keys:
            result = backend.store.compute_dependent_from_offline(
                view=VIEW, view_version=VIEW_VERSION, entity_key=key,
                feature_name=feature_name, calc_ts=calc_ts,
            )
            if result is None:
                skipped += 1
                continue
            results.append(result)
        new_results, _ = partition_new_results(backend.offline, VIEW, VIEW_VERSION, results)
        backend.offline.append_many(VIEW, VIEW_VERSION, new_results)
        computed += len(results)
    return {"features": len(ordered), "computed": computed, "skipped": skipped}


def run_f3_batch(
    backend,
    runner,
    entity_keys: list[EntityKey],
    calc_ts: datetime,
    chunk_size: int = 1000,
) -> dict[str, int]:
    """Score the portfolio with the F3 model-as-feature path, in bounded chunks."""
    totals = {"planned": 0, "computed": 0, "skipped": 0, "duplicates_skipped": 0}
    for start in range(0, len(entity_keys), chunk_size):
        chunk = entity_keys[start:start + chunk_size]
        result = compute_model_feature_batch(
            backend, runner, view=VIEW, view_version=VIEW_VERSION,
            feature_name="pd_score", entity_keys=chunk, observation_ts=calc_ts,
            job_id="credit_demo_f3",
        )
        for key in totals:
            totals[key] += getattr(result, key)
    return totals


def guarded_online_refresh(
    backend,
    entity_keys: list[EntityKey],
    feature_names: list[str],
    observation_ts: datetime,
    *,
    tokens_per_sec: float = 5000.0,
    burst: float = 5000.0,
) -> dict[str, int]:
    """Mode-2: push latest offline values online through the D9 write guard + token bucket.

    Offline history stays the source of truth; every online write goes through
    ``OnlineStore.write`` (the D9 guard), so a stale value is ``skipped_stale`` and an
    identical replay is ``noop`` — never an overwrite of fresher online state.
    """
    limiter = TokenBucketRateLimiter(rate_per_sec=tokens_per_sec, burst=burst)
    counts = {"written": 0, "written_recompute": 0, "skipped_stale": 0, "noop": 0,
              "rate_limited": 0, "missing_offline": 0}
    for key in entity_keys:
        for feature_name in feature_names:
            record = backend.offline.get_pit(
                key, feature_name=feature_name, feature_version=1,
                view=VIEW, view_version=VIEW_VERSION, observation_ts=observation_ts,
            )
            if record is None:
                counts["missing_offline"] += 1
                continue
            if limiter.try_acquire(1) < 1:
                counts["rate_limited"] += 1
                continue
            outcome = backend.online.write(VIEW, VIEW_VERSION, record.result)
            counts[outcome] = counts.get(outcome, 0) + 1
    return counts
