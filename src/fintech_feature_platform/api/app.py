"""FastAPI app over the FeatureStore workflow and the Kafka-first producer.

The HTTP layer only adapts requests to the wired backend (compute, the raw/online/
offline stores, and the event publisher) and adapts results back — no compute or
storage logic lives here. The backend is selected by ``FSP_BACKEND`` (default
``memory``); the ``local`` backend (MinIO + Postgres + Valkey) is fully wired, and
``FSP_EVENTS=kafka`` selects the Kafka event publisher. See ``api/backend.py``,
``api/local_backend.py``, and ``api/settings.py``.

Endpoints: the Kafka-first request API (``POST /v1/feature-requests``,
``/v1/feature-requests/compute``, ``GET /v1/feature-requests/{request_id}``) plus the
read APIs (``/v1/features/latest``, ``history``, ``consistency-check``,
``training-datasets/build``). The legacy synchronous compute routes were removed: the
report_ref subsystem (``/v1/raw-reports`` + ``/v1/features/compute``) in and
``/v1/features/compute-direct`` in. Online compute is Kafka-first only.

Run the server with:

    uv sync --extra api
    uv run uvicorn fintech_feature_platform.api.app:app --reload
"""

from __future__ import annotations

import dataclasses
import hashlib
import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Response
from pydantic import BaseModel, field_validator

from fintech_feature_platform.api.backend import AppBackend, build_backend
from fintech_feature_platform.api.consistency import (
    ConsistencyCheckRequest,
    ConsistencyCheckResponse,
    to_response,
)
from fintech_feature_platform.api.direct_compute import InlineSource
from fintech_feature_platform.api.dwh_ingestion import (
    DwhFeatureConfig,
    DwhJsonConfig,
    run_dwh_feature_import,
    run_dwh_json_extraction,
)
from fintech_feature_platform.api.jsonl_ingestion import run_jsonl_ingestion
from fintech_feature_platform.api.readiness import build_readiness_checker
from fintech_feature_platform.api.security import (
    MODE_API_KEY,
    OPERATOR_ONLY,
    PUBLIC,
    SERVICE_OR_OPERATOR,
    SecurityConfig,
    assert_policy_complete,
    require_role,
    warn_if_disabled,
)
from fintech_feature_platform.api.settings import Settings, load_settings
from fintech_feature_platform.api.training import (
    TrainingDatasetRequest,
    TrainingDatasetResponse,
    dataset_to_response,
    to_core_observations,
)
from fintech_feature_platform.fs_core.consistency import (
    check_online_offline_consistency,
)
from fintech_feature_platform.fs_core.events.models import (
    BatchChunkRequested,
    BatchItem,
    EntityRef,
    FeatureComputeRequested,
    ModelScoreWriteRequested,
    ReportDescriptor,
    ScoreItem,
    batch_chunk_partition_key,
    build_idempotency_key,
)
from fintech_feature_platform.fs_core.events.topics import (
    FEATURE_COMPUTE_BATCH,
    FEATURE_COMPUTE_ONLINE,
    MODEL_SCORE_WRITE,
)
from fintech_feature_platform.fs_core.models import EntityKey
from fintech_feature_platform.fs_core.observability.lineage import build_feature_lineage
from fintech_feature_platform.fs_core.observability.logs import (
    adopt_uvicorn_logging,
    configure_logging,
    get_logger,
    log_event,
)
from fintech_feature_platform.fs_core.planner import PlannerError, plan_features
from fintech_feature_platform.fs_core.registry.models import FeatureViewDef, Registry
from fintech_feature_platform.fs_core.stores.batch_status import (
    BatchChunkStatus,
    BatchJobStatus,
)
from fintech_feature_platform.fs_core.stores.request_status import RequestStatus
from fintech_feature_platform.fs_core.stores.source_dataset import (
    ITEM_DUPLICATE,
    ITEM_WRITTEN,
    LANDING_RAW_REPORTS,
)
from fintech_feature_platform.fs_core.training import build_training_dataset

# --- request / response models ----------------------------------------------

class LatestRequest(BaseModel):
    view: str
    view_version: int
    entity: dict[str, str]
    requested_features: list[str]


class LatestFeatureOut(BaseModel):
    feature_version: int
    value: Any
    data_ts: datetime
    calc_ts: datetime


class LatestResponse(BaseModel):
    view: str
    view_version: int
    entity_key: str
    features: dict[str, LatestFeatureOut]
    missing: list[str]


class HistoryRequest(BaseModel):
    view: str
    view_version: int | None = None
    entity: dict[str, str]
    feature_name: str | None = None
    feature_version: int | None = None


class HistoryRecordOut(BaseModel):
    view: str
    view_version: int
    feature_name: str
    feature_version: int
    value: Any
    data_ts: datetime
    calc_ts: datetime


class HistoryResponse(BaseModel):
    view: str
    view_version: int | None
    entity_key: str
    records: list[HistoryRecordOut]


class InlineReportIn(BaseModel):
    source_name: str
    report_type: str
    report_ts: datetime
    payload: Any
    schema_version: str = "v1"
    report_ref: str | None = None

    @field_validator("report_ts")
    @classmethod
    def _require_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise ValueError("must be timezone-aware")
        return value


class FeatureRequestIn(BaseModel):
    entity_type: str
    entity_key: dict[str, str]
    view: str
    view_version: int
    reports: list[InlineReportIn]
    requested_feature_groups: list[str] = []
    requested_features: list[str] = []
    write_policy: str = "online_first"
    priority: str = "online"
    deadline_ms: int = 1000
    idempotency_key: str | None = None
    correlation_id: str | None = None


class FeatureRequestResponse(BaseModel):
    request_id: str
    job_id: str
    status: str
    status_url: str
    report_refs: list[str]


class FeatureRequestStatusResponse(BaseModel):
    request_id: str
    job_id: str
    status: str
    entity_type: str
    entity_key: dict[str, str]
    view: str
    view_version: int
    requested_features: list[str]
    requested_feature_groups: list[str]
    online_write_status: str | None
    offline_write_status: str | None
    metadata_write_status: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    error: str | None


class ScoreIn(BaseModel):
    feature: str
    value: Any
    data_ts: datetime
    calc_ts: datetime | None = None
    model_name: str
    model_version: str
    source_request_id: str | None = None
    decision_request_id: str | None = None
    context: dict[str, Any] = {}

    @field_validator("data_ts", "calc_ts")
    @classmethod
    def _require_timezone_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and (
            value.tzinfo is None or value.tzinfo.utcoffset(value) is None
        ):
            raise ValueError("must be timezone-aware")
        return value


class ModelScoreWriteIn(BaseModel):
    entity_type: str
    entity_key: dict[str, str]
    view: str
    view_version: int
    scores: list[ScoreIn]
    idempotency_key: str
    write_online: bool = True
    write_offline: bool = True
    correlation_id: str | None = None


class ModelScoreWriteResponse(BaseModel):
    score_write_id: str
    status: str


class BatchItemIn(BaseModel):
    entity_type: str
    entity_key: dict[str, str]
    inline_sources: dict[str, InlineSource]


class BatchScopeIn(BaseModel):
    type: str  # "inline" | "source_dataset_manifest"
    items: list[BatchItemIn] = []           # inline scope
    manifest_id: str | None = None          # source_dataset_manifest scope
    include_duplicate_items: bool = False   # source_dataset_manifest scope


class BatchJobIn(BaseModel):
    view: str
    view_version: int
    scope: BatchScopeIn
    idempotency_key: str
    requested_features: list[str] = []
    requested_feature_groups: list[str] = []
    write_online: bool = False
    # For manifest scope, write_online=true is allowed only as a guarded Mode-2 refresh
    #; None defaults to "guarded".
    online_refresh_mode: str | None = None
    chunk_size: int = 100


class BatchJobResponse(BaseModel):
    job_id: str
    status: str
    chunk_count: int
    total_items: int
    manifest_id: str | None = None


class JsonlIngestIn(BaseModel):
    entity_type: str
    source_name: str
    report_type: str
    lines: list[str]  # one JSON object per string; refs recorded, payloads landed
    dataset_id: str | None = None
    copy_mode: str = "copy"
    input_uri: str | None = None
    created_by: str | None = None


class DwhJsonIngestIn(BaseModel):
    entity_type: str
    source_name: str
    report_type: str
    rows: list[dict[str, Any]]  # already-extracted DWH rows (JSON column), not SQL
    query_name: str = "api_inline"
    dataset_id: str | None = None
    entity_key_column: str = "entity_key"
    event_ts_column: str = "event_ts"
    payload_column: str = "payload_json"


class DwhFeatureImportIn(BaseModel):
    entity_type: str
    view: str
    view_version: int
    rows: list[dict[str, Any]]  # already-extracted DWH tall feature rows, not SQL
    query_name: str = "api_inline"
    dataset_id: str | None = None
    source_name: str | None = None
    entity_key_column: str = "entity_key"
    feature_name_column: str = "feature_name"
    feature_version_column: str = "feature_version"
    value_column: str = "value"
    data_ts_column: str = "data_ts"
    calc_ts_column: str = "calc_ts"


class SourceDatasetManifestOut(BaseModel):
    manifest_id: str
    dataset_id: str
    source_kind: str
    landing_form: str
    entity_type: str
    source_name: str
    report_type: str | None
    view: str | None
    view_version: int | None
    copy_mode: str
    status: str
    item_count_read: int
    item_count_written: int
    item_count_duplicate: int
    item_count_rejected: int
    watermark_min_event_ts: datetime | None
    watermark_max_event_ts: datetime | None
    content_hash: str | None
    detail_url: str


class HybridFeatureRequestIn(FeatureRequestIn):
    wait_timeout_ms: int | None = None


class HybridComputeResponse(BaseModel):
    request_id: str
    job_id: str
    status: str
    status_url: str
    entity_type: str | None = None
    entity_key: dict[str, str] | None = None
    feature_view: str | None = None
    view_version: int | None = None
    features: dict[str, LatestFeatureOut] | None = None
    missing_features: list[str] | None = None
    online_write_status: str | None = None
    offline_write_status: str | None = None
    error: str | None = None


class MetricsResponse(BaseModel):
    metrics: dict
    generated_at: str


class LineageRequest(BaseModel):
    # Values-free lineage lookup for one feature value.
    view: str
    view_version: int
    feature_name: str
    feature_version: int
    entity: dict[str, str]
    data_ts: datetime | None = None
    calc_ts: datetime | None = None
    report_refs: list[str] | None = None
    manifest_id: str | None = None


@dataclass(frozen=True)
class _SubmitResult:
    request_id: str
    job_id: str
    report_refs: list[str]
    entity: EntityRef
    output_features: list[str]  # planner-expanded output feature names
    expires_at: datetime  # absolute deadline stamped on the published event


_DEFAULT_HYBRID_WAIT_MS = 1000

_request_logger = get_logger("api")


def _find_view(registry: Registry, name: str, version: int) -> FeatureViewDef:
    return registry.find_view(name, version)  # ONE authoritative lookup


def _find_view_by_name(
    registry: Registry, name: str, version: int | None
) -> FeatureViewDef:
    if version is not None:
        return _find_view(registry, name, version)
    for view in registry.feature_views:
        if view.name == name:
            return view
    raise ValueError(f"unknown view {name!r}")


def _submit_feature_request(
    backend: AppBackend, request: FeatureRequestIn, settings: Settings
) -> _SubmitResult:
    """Shared Kafka-first submission used by the async and hybrid endpoints.

    Stores inline reports, publishes one self-contained ``FeatureComputeRequested`` to
    ``fp.feature-compute.online``, and initializes ``accepted`` status (best-effort).
    Sets the absolute deadline : ``event_ts`` = accept time, ``expires_at`` =
    ``event_ts + clamp(deadline_ms)``. Never computes, never exposes
    ``object_key``/``storage_uri``.
    """
    try:
        EntityKey.from_mapping(request.entity_key)  # validate the key parts
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not request.reports:
        raise HTTPException(status_code=400, detail="at least one report is required")

    # Validate + expand the request against the exact view version BEFORE any write or
    # publish: unknown view/feature/group -> 400 (nothing is stored, published, or tracked).
    try:
        view_def = _find_view(backend.registry, request.view, request.view_version)
        plan = plan_features(
            view_def, request.requested_features, request.requested_feature_groups
        )
    except (PlannerError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    output_features = list(plan.compute_features)

    request_id = f"freq_{uuid4().hex}"
    job_id = f"job_{uuid4().hex}"

    # Availability trust boundary : ordinary service-role requests can
    # NEVER backdate availability — the API stamps its own accept time on every
    # descriptor. Trusted historical availability exists only on the operator
    # ingestion paths (JSONL/DWH rows, inline batch sources).
    accepted_at = datetime.now(tz=UTC)

    descriptors: list[ReportDescriptor] = []
    report_refs: list[str] = []
    for report in request.reports:
        report_ref = report.report_ref or f"rep_{uuid4().hex}"
        try:
            serialized = backend.serialize_payload(report.payload)
            storage_uri = backend.make_storage_uri(
                report.report_type, report_ref, report.report_ts
            )
            content_hash = "sha256:" + hashlib.sha256(serialized).hexdigest()
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        # Accepted boundary = raw payload stored + Kafka publish acked. raw_reports_meta
        # is projected asynchronously by the Metadata Writer (no sync Postgres write here).
        backend.payloads.put(storage_uri, report.payload)
        descriptors.append(
            ReportDescriptor(
                report_ref=report_ref,
                source_name=report.source_name,
                report_type=report.report_type,
                schema_version=report.schema_version,
                report_ts=report.report_ts,
                object_key=storage_uri,  # internal; never returned to clients
                content_hash=content_hash,
                size_bytes=len(serialized),
                compression=backend.raw_compression,
                format=backend.raw_format,
                available_at=accepted_at,  # server-stamped; never client-supplied
            )
        )
        report_refs.append(report_ref)

    entity = EntityRef(
        entity_type=request.entity_type, entity_key=dict(request.entity_key)
    )
    idempotency_key = request.idempotency_key or build_idempotency_key(
        entity,
        request.requested_feature_groups,
        request.requested_features,
        descriptors,
    )
    # Absolute deadline: clamp the requested deadline_ms to a sane ceiling, then stamp
    # an absolute expires_at. Workers reason from expires_at, never relative deadline_ms.
    event_ts = accepted_at
    deadline_ms = max(1, min(request.deadline_ms, settings.online_max_deadline_ms))
    expires_at = event_ts + timedelta(milliseconds=deadline_ms)
    event = FeatureComputeRequested(
        request_id=request_id,
        job_id=job_id,
        priority=request.priority,
        deadline_ms=deadline_ms,
        entity=entity,
        view=request.view,
        view_version=request.view_version,
        reports=descriptors,
        write_policy=request.write_policy,
        idempotency_key=idempotency_key,
        correlation_id=request.correlation_id or request_id,
        occurred_at=event_ts,
        requested_feature_groups=request.requested_feature_groups,
        requested_features=request.requested_features,
        event_ts=event_ts,
        expires_at=expires_at,
    )
    # Publish failure raises -> request is not reported as accepted.
    backend.events.publish(FEATURE_COMPUTE_ONLINE, entity.encoded(), event)

    # Best-effort: initialize request status. A status-store failure must NOT fail the
    # accepted request (status is observability, not the system of record).
    now = datetime.now(tz=UTC)
    try:
        backend.status.put(
            RequestStatus(
                request_id=request_id,
                job_id=job_id,
                status="accepted",
                entity_type=request.entity_type,
                entity_key=dict(request.entity_key),
                view=request.view,
                view_version=request.view_version,
                requested_features=output_features,  # planner-expanded output set
                requested_feature_groups=request.requested_feature_groups,
                online_write_status=None,
                offline_write_status=None,
                metadata_write_status="pending",
                created_at=now,
                updated_at=now,
            )
        )
    except Exception:  # noqa: BLE001 - status is best-effort
        pass

    return _SubmitResult(
        request_id=request_id,
        job_id=job_id,
        report_refs=report_refs,
        entity=entity,
        output_features=output_features,
        expires_at=expires_at,
    )


def _read_online_features(
    backend: AppBackend,
    view: FeatureViewDef,
    view_name: str,
    view_version: int,
    entity_key: EntityKey,
    requested_features: list[str],
) -> tuple[dict[str, LatestFeatureOut], list[str]]:
    """Read requested latest online values (shared by /features/latest and hybrid)."""
    features_by_name = {feature.name: feature for feature in view.features}
    features: dict[str, LatestFeatureOut] = {}
    missing: list[str] = []
    for name in requested_features:
        feature_def = features_by_name.get(name)
        if feature_def is None:
            raise HTTPException(status_code=400, detail=f"unknown feature {name!r}")
        result = backend.online.get(
            view_name, view_version, entity_key, name, feature_def.feature_version
        )
        if result is None:
            missing.append(name)
        else:
            features[name] = LatestFeatureOut(
                feature_version=result.ref.version,
                value=result.value,
                data_ts=result.data_ts,
                calc_ts=result.calc_ts,
            )
    return features, missing


def _reconcile_metadata_status(backend: AppBackend, status: RequestStatus) -> RequestStatus:
    """Report ``metadata_write_status`` from durable evidence.

    PostgreSQL is the durable source of truth for the metadata projection; the Valkey
    operational status may lag when the writer's best-effort update failed. A
    ``pending`` metadata status is therefore reconciled against the durable request
    projection: a committed terminal projection proves ``written``, and a monotonic
    best-effort read-repair heals the operational store. Legacy statuses (``None``)
    and every other value pass through untouched; failures here never fail the read.
    """
    if status.metadata_write_status != "pending":
        return status
    try:
        durable = backend.metadata.get_request(status.request_id)
    except Exception:  # noqa: BLE001 - reconciliation is best-effort observability
        return status
    if durable is None or durable.status != "completed":
        return status
    repaired = dataclasses.replace(
        status,
        metadata_write_status="written",
        updated_at=datetime.now(tz=UTC),
    )
    try:
        backend.status.put(repaired)  # monotonic read-repair; loss-tolerant
    except Exception:  # noqa: BLE001
        pass
    return repaired


def _read_request_result(
    backend: AppBackend,
    request_id: str,
    output_features: list[str],
) -> tuple[dict[str, LatestFeatureOut], list[str], str | None]:
    """Read the request's OWN computed values from the request-result store.

    Never falls back to the online store's /latest values: if the result is
    missing/expired, the hybrid response says so explicitly (``result_missing``)
    with every output feature reported missing.
    """
    try:
        stored = backend.results.get(request_id)
    except Exception:  # noqa: BLE001 - treat a result-store read failure as missing
        stored = None
    if stored is None:
        return {}, list(output_features), "result_missing"

    features: dict[str, LatestFeatureOut] = {}
    for name in output_features:
        item = stored.features.get(name)
        if item is None:
            continue
        features[name] = LatestFeatureOut(
            feature_version=item["feature_version"],
            value=item["value"],
            data_ts=datetime.fromisoformat(item["data_ts"]),
            calc_ts=datetime.fromisoformat(item["calc_ts"]),
        )
    missing = [name for name in output_features if name not in features]
    return features, missing, None


def _manifest_to_out(manifest) -> SourceDatasetManifestOut:
    """Map a SourceDatasetManifest to the API response — counts/watermarks, no payloads."""
    return SourceDatasetManifestOut(
        manifest_id=manifest.manifest_id,
        dataset_id=manifest.dataset_id,
        source_kind=manifest.source_kind,
        landing_form=manifest.landing_form,
        entity_type=manifest.entity_type,
        source_name=manifest.source_name,
        report_type=manifest.report_type,
        view=manifest.view,
        view_version=manifest.view_version,
        copy_mode=manifest.copy_mode,
        status=manifest.status,
        item_count_read=manifest.item_count_read,
        item_count_written=manifest.item_count_written,
        item_count_duplicate=manifest.item_count_duplicate,
        item_count_rejected=manifest.item_count_rejected,
        watermark_min_event_ts=manifest.watermark_min_event_ts,
        watermark_max_event_ts=manifest.watermark_max_event_ts,
        content_hash=manifest.content_hash,
        detail_url=f"/v1/source-datasets/{manifest.manifest_id}",
    )


def _manifest_batch_items(backend: AppBackend, request: BatchJobIn):
    """Build ref-only batch items from a raw-reports manifest.

    Validates the manifest (exists, landing form (a), not online), then turns each
    eligible item into a ``BatchItem`` carrying ``source_refs`` (source_name ->
    report_ref) only — never a payload. Returns ``(batch_items, manifest_id)``.
    """
    scope = request.scope
    if not scope.manifest_id:
        raise HTTPException(status_code=400, detail="scope.manifest_id is required")
    # write_online=true is allowed ONLY as a guarded Mode-2 refresh : offline
    # stays primary, online refresh is token-bucketed + D9-guarded in the worker.
    if request.write_online and request.online_refresh_mode not in (None, "guarded"):
        raise HTTPException(
            status_code=400,
            detail=(
                "manifest-scoped write_online requires online_refresh_mode='guarded' "
                f"(got {request.online_refresh_mode!r})"
            ),
        )
    manifest = backend.source_datasets.get_manifest(scope.manifest_id)
    if manifest is None:
        raise HTTPException(
            status_code=404, detail=f"unknown manifest {scope.manifest_id!r}"
        )
    if manifest.landing_form != LANDING_RAW_REPORTS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"manifest {scope.manifest_id!r} landing_form "
                f"{manifest.landing_form!r} is not computable by batch; feature-row "
                "datasets already live in offline history"
            ),
        )
    eligible = {ITEM_WRITTEN}
    if scope.include_duplicate_items:
        eligible.add(ITEM_DUPLICATE)
    batch_items = [
        BatchItem(
            entity_type=manifest.entity_type,
            entity_key=dict(item.entity_key),
            source_refs={item.source_name: item.report_ref},
        )
        for item in backend.source_datasets.list_items(scope.manifest_id)
        if item.status in eligible and item.report_ref and item.entity_key
    ]
    return batch_items, manifest.manifest_id


ENDPOINT_POLICY: dict[str, str] = {
    "/health": PUBLIC,
    # Readiness : bounded dependency categories only — no secrets,
    # no exception text — so orchestration can probe it unauthenticated, like /health.
    "/ready": PUBLIC,
    # data plane (service or operator)
    "/v1/features/latest": SERVICE_OR_OPERATOR,
    "/v1/features/history": SERVICE_OR_OPERATOR,
    "/v1/features/consistency-check": SERVICE_OR_OPERATOR,
    "/v1/feature-requests": SERVICE_OR_OPERATOR,
    "/v1/feature-requests/compute": SERVICE_OR_OPERATOR,
    "/v1/feature-requests/{request_id}": SERVICE_OR_OPERATOR,
    "/v1/model-scores": SERVICE_OR_OPERATOR,
    # observability / lineage / ingestion / batch / training (operator only)
    "/v1/observability/metrics": OPERATOR_ONLY,
    "/v1/lineage/feature-value": OPERATOR_ONLY,
    "/v1/batch/jobs": OPERATOR_ONLY,
    "/v1/batch/jobs/{job_id}": OPERATOR_ONLY,
    "/v1/source-datasets/ingest-jsonl": OPERATOR_ONLY,
    "/v1/source-datasets/ingest-dwh-json": OPERATOR_ONLY,
    "/v1/source-datasets/import-dwh-features": OPERATOR_ONLY,
    "/v1/source-datasets/{manifest_id}": OPERATOR_ONLY,
    "/v1/training-datasets/build": OPERATOR_ONLY,
}


def create_app(
    backend: AppBackend | None = None,
    security: SecurityConfig | None = None,
) -> FastAPI:
    # Structured operational logging : one JSON event per line on the
    # 'fsp' namespace logger. Uvicorn lifecycle/error output is re-routed through the
    # same JSON envelope (event=runtime_log) and its plaintext access log is dropped in
    # json mode (api_request_completed + Prometheus cover requests) — so service stdout
    # is uniformly machine-parseable. Text mode leaves uvicorn's dev output untouched.
    configure_logging("api")
    adopt_uvicorn_logging("api")
    settings = load_settings()
    # Validate the security configuration before constructing the backend so the auth gate
    # (and its fail-closed startup refusal) is evaluated first.
    security = security if security is not None else SecurityConfig.from_env()
    security.validate()
    warn_if_disabled(security, "feature-api")
    if backend is None:
        backend = build_backend(settings)
    registry = backend.registry

    # Per-process observability server : the API uses the SAME shared
    # lifecycle helper as the workers. Disabled unless the observability port
    # (FSP_OBSERVABILITY_PORT, compat fallback FSP_METRICS_PORT) is > 0.
    from fintech_feature_platform.api.runner_daemon import maybe_start_metrics_server

    metrics_server = maybe_start_metrics_server("api", backend, settings)
    # API readiness rides the main HTTP server (option C): orchestration probes /ready
    # on the API port regardless of whether the internal metrics port is enabled.
    readiness = build_readiness_checker("api", backend)

    secure = security.mode == MODE_API_KEY
    service_auth = Depends(require_role(security, SERVICE_OR_OPERATOR))
    operator_auth = Depends(require_role(security, OPERATOR_ONLY))

    app = FastAPI(
        title="FinTech Feature Platform (local demo API)",
        # OpenAPI/Swagger/ReDoc are disabled in api_key mode.
        docs_url=None if secure else "/docs",
        redoc_url=None if secure else "/redoc",
        openapi_url=None if secure else "/openapi.json",
    )
    # Kept on state so tests (and an eventual shutdown hook) can close it; the serving
    # thread is a daemon, so it never blocks process exit either way.
    app.state.metrics_server = metrics_server

    # API request metrics. route_class is the matched route's
    # TEMPLATE (bounded by code-defined routes; never the raw path/query), with the
    # brace characters normalized out ("{id}" -> ":id") to satisfy label-value safety.
    @app.middleware("http")
    async def _request_metrics(request, call_next):  # pragma: no branch
        from time import perf_counter as _pc

        started = _pc()
        response = await call_next(request)
        try:
            route = request.scope.get("route")
            route_class = (
                route.path.replace("{", ":").replace("}", "")
                if route is not None else "unmatched"
            )
            method = request.method if request.method in (
                "GET", "POST", "PUT", "DELETE", "PATCH") else "OTHER"
            status_class = f"{response.status_code // 100}xx"
            if status_class not in ("2xx", "3xx", "4xx", "5xx"):
                status_class = "5xx"
            backend.metrics.incr(
                "fsp_api_requests_total",
                {"route_class": route_class, "method": method,
                 "status_class": status_class},
            )
            backend.metrics.observe(
                "fsp_api_request_duration_seconds", _pc() - started,
                {"route_class": route_class},
            )
            # Structured request log : bounded route template + outcome only —
            # never the raw path/query/body (they can carry IDs and payloads). DEBUG
            # for routine success (final review C: counts/latency live in Prometheus);
            # 5xx responses surface at ERROR so failures stay visible at default INFO.
            log_event(
                _request_logger,
                logging.ERROR if status_class == "5xx" else logging.DEBUG,
                "api_request_completed",
                operation=route_class, method=method,
                status_code=response.status_code,
                result=status_class,
                duration_ms=round((_pc() - started) * 1000, 3),
            )
        except Exception:  # noqa: BLE001, S110 - metrics must never break a request
            pass
        return response

    @app.get("/health")
    def health() -> dict[str, str]:
        # LIVENESS only: "the process is up and serving HTTP". Unchanged on purpose —
        # readiness (dependency capability) is the separate /ready contract below.
        return {"status": "ok"}

    @app.get("/ready")
    def ready(response: Response) -> dict:
        # READINESS : "can this process perform its role right now?"
        # Role-aware bounded dependency checks over clients built at startup; NEVER
        # queue depth / last-success / throughput (an idle process is ready). 200 when
        # ready, 503 when a required dependency fails; categories only, no secrets.
        report = readiness.check()
        if not report.ready:
            response.status_code = 503
        return report.to_dict()

    @app.get("/v1/observability/metrics", response_model=MetricsResponse,
             dependencies=[operator_auth])
    def observability_metrics() -> MetricsResponse:
        # Bounded, values-free in-process metrics snapshot.
        return MetricsResponse(
            metrics=backend.metrics.snapshot(),
            generated_at=datetime.now(tz=UTC).isoformat(),
        )

    @app.post("/v1/lineage/feature-value", dependencies=[operator_auth])
    def lineage_feature_value(request: LineageRequest) -> dict:
        # Values-free lineage: refs/hashes/timestamps/model/bundle/report_refs only (O2).
        try:
            view = _find_view_by_name(registry, request.view, request.view_version)
            entity_key = EntityKey.from_mapping(
                {field: request.entity[field] for field in view.key_fields},
                key_order=view.key_fields,
            )
        except KeyError as exc:
            raise HTTPException(
                status_code=400, detail=f"missing entity key field: {exc}"
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        lineage = build_feature_lineage(
            backend.offline, backend.metas, entity_key,
            view=request.view, view_version=request.view_version,
            feature_name=request.feature_name, feature_version=request.feature_version,
            data_ts=request.data_ts, calc_ts=request.calc_ts,
            report_refs=request.report_refs, manifest_id=request.manifest_id,
            source_datasets=backend.source_datasets,
        )
        return lineage.to_dict()

    @app.post("/v1/features/latest", response_model=LatestResponse,
              dependencies=[service_auth])
    def read_latest(request: LatestRequest) -> LatestResponse:
        # Read-only: looks up already-computed latest values in the online store.
        try:
            view = _find_view(registry, request.view, request.view_version)
            entity_key = EntityKey.from_mapping(
                {field: request.entity[field] for field in view.key_fields},
                key_order=view.key_fields,
            )
        except KeyError as exc:
            raise HTTPException(
                status_code=400, detail=f"missing entity key field: {exc}"
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        features, missing = _read_online_features(
            backend, view, request.view, request.view_version, entity_key,
            request.requested_features,
        )
        return LatestResponse(
            view=request.view,
            view_version=request.view_version,
            entity_key=entity_key.encode(),
            features=features,
            missing=missing,
        )

    @app.post("/v1/features/history", response_model=HistoryResponse,
              dependencies=[service_auth])
    def read_history(request: HistoryRequest) -> HistoryResponse:
        # Read-only: returns append-only records from the offline store.
        try:
            view = _find_view_by_name(registry, request.view, request.view_version)
            entity_key = EntityKey.from_mapping(
                {field: request.entity[field] for field in view.key_fields},
                key_order=view.key_fields,
            )
        except KeyError as exc:
            raise HTTPException(
                status_code=400, detail=f"missing entity key field: {exc}"
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        if request.feature_name is not None and request.feature_name not in {
            feature.name for feature in view.features
        }:
            raise HTTPException(
                status_code=400,
                detail=f"unknown feature {request.feature_name!r}",
            )

        records = backend.offline.get(
            entity_key,
            feature_name=request.feature_name,
            feature_version=request.feature_version,
            view=request.view,
            view_version=request.view_version,
        )
        return HistoryResponse(
            view=request.view,
            view_version=request.view_version,
            entity_key=entity_key.encode(),
            records=[
                HistoryRecordOut(
                    view=record.view,
                    view_version=record.view_version,
                    feature_name=record.result.ref.name,
                    feature_version=record.result.ref.version,
                    value=record.result.value,
                    data_ts=record.result.data_ts,
                    calc_ts=record.result.calc_ts,
                )
                for record in records
            ],
        )

    @app.post("/v1/features/consistency-check", response_model=ConsistencyCheckResponse,
              dependencies=[service_auth])
    def consistency_check(request: ConsistencyCheckRequest) -> ConsistencyCheckResponse:
        # Read-only: compares persisted offline history vs online latest. No recompute.
        try:
            view = _find_view(registry, request.view, request.view_version)
            entity_key = EntityKey.from_mapping(
                {field: request.entity[field] for field in view.key_fields},
                key_order=view.key_fields,
            )
        except KeyError as exc:
            raise HTTPException(
                status_code=400, detail=f"missing entity key field: {exc}"
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        try:
            report = check_online_offline_consistency(
                offline=backend.offline,
                online=backend.online,
                view=view,
                view_version=request.view_version,
                entity_key=entity_key,
                feature_names=request.features,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return to_response(report)

    @app.post(
        "/v1/training-datasets/build", response_model=TrainingDatasetResponse,
        dependencies=[operator_auth],
    )
    def build_training_dataset_route(
        request: TrainingDatasetRequest,
    ) -> TrainingDatasetResponse:
        # Leakage-safe PIT join over offline history. Read-only; no recompute.
        try:
            view = _find_view(registry, request.view, request.view_version)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        try:
            dataset = build_training_dataset(
                offline=backend.offline,
                view=view,
                view_version=request.view_version,
                feature_names=request.features,
                observations=to_core_observations(request.observations),
                missing_policy=request.missing_policy,
                safety_gap=timedelta(seconds=request.safety_gap_seconds),
            )
        except KeyError as exc:
            raise HTTPException(
                status_code=400, detail=f"missing entity key field: {exc}"
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return dataset_to_response(dataset)

    @app.post("/v1/feature-requests", response_model=FeatureRequestResponse,
              dependencies=[service_auth])
    def create_feature_request(request: FeatureRequestIn) -> FeatureRequestResponse:
        # Kafka-first producer: store inline reports, then publish a self-contained
        # compute event. This endpoint does NOT compute features — a worker consumes it.
        result = _submit_feature_request(backend, request, settings)
        return FeatureRequestResponse(
            request_id=result.request_id,
            job_id=result.job_id,
            status="accepted",
            status_url=f"/v1/feature-requests/{result.request_id}",
            report_refs=result.report_refs,
        )

    @app.post(
        "/v1/model-scores", response_model=ModelScoreWriteResponse, status_code=202,
        dependencies=[service_auth],
    )
    def write_model_scores(request: ModelScoreWriteIn) -> ModelScoreWriteResponse:
        # Kafka-first writeback: validate externally-computed scores against the exact
        # registry view, then publish. No synchronous store write; the score writer
        # materializes online (CAS) + offline (via the Offline Writer). The platform
        # never executes the model.
        if not request.scores:
            raise HTTPException(status_code=400, detail="at least one score is required")
        if not request.write_online and not request.write_offline:
            raise HTTPException(
                status_code=400,
                detail="at least one of write_online/write_offline must be true",
            )
        try:
            EntityKey.from_mapping(request.entity_key)
            view = _find_view(backend.registry, request.view, request.view_version)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        features_by_name = {feature.name: feature for feature in view.features}
        for score in request.scores:
            feature_def = features_by_name.get(score.feature)
            if feature_def is None:
                raise HTTPException(
                    status_code=400, detail=f"unknown feature {score.feature!r}"
                )
            if feature_def.kind != "model_score":
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"feature {score.feature!r} is not a model_score feature; "
                        "scores can only be written to model_score features"
                    ),
                )

        score_write_id = request.idempotency_key
        now = datetime.now(tz=UTC)
        entity = EntityRef(
            entity_type=request.entity_type, entity_key=dict(request.entity_key)
        )
        event = ModelScoreWriteRequested(
            score_write_id=score_write_id,
            correlation_id=request.correlation_id or score_write_id,
            occurred_at=now,
            entity=entity,
            view=request.view,
            view_version=request.view_version,
            scores=[
                ScoreItem(
                    feature=score.feature,
                    value=score.value,
                    data_ts=score.data_ts,
                    calc_ts=score.calc_ts or now,
                    model_name=score.model_name,
                    model_version=score.model_version,
                    source_request_id=score.source_request_id,
                    decision_request_id=score.decision_request_id,
                    context=dict(score.context),
                )
                for score in request.scores
            ],
            idempotency_key=score_write_id,
            write_online=request.write_online,
            write_offline=request.write_offline,
        )
        backend.events.publish(MODEL_SCORE_WRITE, entity.encoded(), event)
        return ModelScoreWriteResponse(score_write_id=score_write_id, status="accepted")

    @app.post("/v1/batch/jobs", response_model=BatchJobResponse, status_code=202,
              dependencies=[operator_auth])
    def create_batch_job(request: BatchJobIn) -> BatchJobResponse:
        # Kafka-first async batch: validate + planner-expand, deterministically chunk the
        # scope, create job status, then publish one BatchChunkRequested per chunk. No
        # synchronous compute; the batch worker computes each chunk. Two scopes: inline
        # (payloads in the event) and source_dataset_manifest (refs only,).
        if request.chunk_size <= 0 or request.chunk_size > settings.batch_max_chunk_size:
            raise HTTPException(
                status_code=400,
                detail=f"chunk_size must be in 1..{settings.batch_max_chunk_size}",
            )
        try:
            view_def = _find_view(backend.registry, request.view, request.view_version)
            plan_features(
                view_def, request.requested_features, request.requested_feature_groups
            )
        except (PlannerError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        if request.scope.type == "inline":
            batch_items = [
                BatchItem(
                    entity_type=item.entity_type,
                    entity_key=dict(item.entity_key),
                    inline_sources={
                        name: source.model_dump(mode="json")
                        for name, source in item.inline_sources.items()
                    },
                )
                for item in request.scope.items
            ]
            manifest_id = None
        elif request.scope.type == "source_dataset_manifest":
            batch_items, manifest_id = _manifest_batch_items(backend, request)
        else:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"unsupported scope type {request.scope.type!r}; expected "
                    "'inline' or 'source_dataset_manifest'"
                ),
            )

        if not batch_items:
            raise HTTPException(
                status_code=400, detail="scope produced no eligible items"
            )
        # Ref-scoped (manifest) jobs carry refs only + bulk-append per chunk, so they get
        # a far larger cap than inline jobs (whose payloads travel in the Kafka event).
        max_items = (
            settings.batch_max_manifest_items
            if manifest_id is not None
            else settings.batch_max_items
        )
        if len(batch_items) > max_items:
            raise HTTPException(
                status_code=400,
                detail=f"total items {len(batch_items)} exceeds cap {max_items}",
            )

        job_id = request.idempotency_key
        now = datetime.now(tz=UTC)
        chunk_size = request.chunk_size
        chunks = [
            batch_items[i : i + chunk_size]
            for i in range(0, len(batch_items), chunk_size)
        ]
        chunk_count = len(chunks)
        total_items = len(batch_items)

        # Deterministic chunk ids + accepted chunk statuses (pre-populated).
        chunk_statuses: dict[str, BatchChunkStatus] = {}
        events: list[tuple[int, BatchChunkRequested]] = []
        for index, chunk_items in enumerate(chunks):
            chunk_id = f"{job_id}:{index}"
            chunk_statuses[chunk_id] = BatchChunkStatus(
                chunk_id=chunk_id, chunk_index=index, status="accepted",
                item_count=len(chunk_items), updated_at=now,
            )
            events.append(
                (
                    index,
                    BatchChunkRequested(
                        batch_job_id=job_id,
                        chunk_id=chunk_id,
                        chunk_index=index,
                        chunk_count=chunk_count,
                        correlation_id=job_id,
                        occurred_at=now,
                        view=request.view,
                        view_version=request.view_version,
                        items=list(chunk_items),
                        requested_features=request.requested_features,
                        requested_feature_groups=request.requested_feature_groups,
                        write_online=request.write_online,
                        total_items=total_items,
                        manifest_id=manifest_id,
                    ),
                )
            )

        job = BatchJobStatus(
            job_id=job_id,
            status="accepted",
            view=request.view,
            view_version=request.view_version,
            created_at=now,
            updated_at=now,
            total_items=total_items,
            chunk_count=chunk_count,
            write_online=request.write_online,
            requested_features=request.requested_features,
            requested_feature_groups=request.requested_feature_groups,
            chunks=chunk_statuses,
            manifest_id=manifest_id,
        )
        backend.batch_status.put(job)

        published = 0
        for index, event in events:
            try:
                # Keyed per chunk (not job_id) so one job spreads across partitions
                # and multiple batch workers; see batch_chunk_partition_key.
                backend.events.publish(
                    FEATURE_COMPUTE_BATCH, batch_chunk_partition_key(event), event
                )
            except Exception as exc:  # noqa: BLE001 - mark publish_failed, surface 503
                backend.batch_status.put(
                    dataclasses.replace(
                        job, status="publish_failed", updated_at=datetime.now(tz=UTC),
                        error_summary={
                            "published_chunks": published, "failed_chunk_index": index,
                            "error": str(exc),
                        },
                    )
                )
                raise HTTPException(
                    status_code=503, detail=f"failed to publish chunk {index}: {exc}"
                ) from exc
            published += 1

        return BatchJobResponse(
            job_id=job_id, status="accepted", chunk_count=chunk_count,
            total_items=total_items, manifest_id=manifest_id,
        )

    @app.get("/v1/batch/jobs/{job_id}", dependencies=[operator_auth])
    def get_batch_job(job_id: str) -> dict:
        status = backend.batch_status.get(job_id)
        if status is None:
            raise HTTPException(
                status_code=404, detail=f"unknown batch job {job_id!r}"
            )
        return status.to_dict()

    @app.post(
        "/v1/source-datasets/ingest-jsonl",
        response_model=SourceDatasetManifestOut,
        status_code=201,
        dependencies=[operator_auth],
    )
    def ingest_jsonl(request: JsonlIngestIn) -> SourceDatasetManifestOut:
        # Landing form (a): land raw payloads in object storage + raw_reports_meta and
        # record report_refs in a manifest. Never computes features, never returns
        # payloads. copy_mode='register_in_place' is rejected (not supported yet).
        try:
            manifest = run_jsonl_ingestion(
                backend=backend,
                lines=request.lines,
                entity_type=request.entity_type,
                source_name=request.source_name,
                report_type=request.report_type,
                dataset_id=request.dataset_id,
                copy_mode=request.copy_mode,
                input_uri=request.input_uri,
                created_by=request.created_by,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return _manifest_to_out(manifest)

    @app.post(
        "/v1/source-datasets/ingest-dwh-json",
        response_model=SourceDatasetManifestOut,
        status_code=201,
        dependencies=[operator_auth],
    )
    def ingest_dwh_json(request: DwhJsonIngestIn) -> SourceDatasetManifestOut:
        # DWH rows with a JSON column -> landing form (a). The platform never executes
        # SQL here: it materializes the already-extracted rows in the request body.
        from fintech_feature_platform.fs_core.dwh.reader import InMemoryDwhReader

        reader = InMemoryDwhReader({request.query_name: request.rows})
        config = DwhJsonConfig(
            entity_type=request.entity_type,
            source_name=request.source_name,
            report_type=request.report_type,
            query_name=request.query_name,
            dataset_id=request.dataset_id,
            entity_key_column=request.entity_key_column,
            event_ts_column=request.event_ts_column,
            payload_column=request.payload_column,
        )
        try:
            manifest = run_dwh_json_extraction(
                backend=backend, reader=reader, config=config
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return _manifest_to_out(manifest)

    @app.post(
        "/v1/source-datasets/import-dwh-features",
        response_model=SourceDatasetManifestOut,
        status_code=201,
        dependencies=[operator_auth],
    )
    def import_dwh_features(request: DwhFeatureImportIn) -> SourceDatasetManifestOut:
        # DWH tall feature rows -> landing form (b) offline history. No SQL executed here.
        from fintech_feature_platform.fs_core.dwh.reader import InMemoryDwhReader

        reader = InMemoryDwhReader({request.query_name: request.rows})
        config = DwhFeatureConfig(
            entity_type=request.entity_type,
            view=request.view,
            view_version=request.view_version,
            query_name=request.query_name,
            dataset_id=request.dataset_id,
            source_name=request.source_name,
            entity_key_column=request.entity_key_column,
            feature_name_column=request.feature_name_column,
            feature_version_column=request.feature_version_column,
            value_column=request.value_column,
            data_ts_column=request.data_ts_column,
            calc_ts_column=request.calc_ts_column,
        )
        try:
            manifest = run_dwh_feature_import(
                backend=backend, reader=reader, config=config
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return _manifest_to_out(manifest)

    @app.get(
        "/v1/source-datasets/{manifest_id}",
        response_model=SourceDatasetManifestOut,
        dependencies=[operator_auth],
    )
    def get_source_dataset(manifest_id: str) -> SourceDatasetManifestOut:
        manifest = backend.source_datasets.get_manifest(manifest_id)
        if manifest is None:
            raise HTTPException(
                status_code=404, detail=f"unknown manifest {manifest_id!r}"
            )
        return _manifest_to_out(manifest)

    @app.post(
        "/v1/feature-requests/compute", response_model=HybridComputeResponse,
        dependencies=[service_auth],
    )
    def compute_feature_request(
        request: HybridFeatureRequestIn,
    ) -> HybridComputeResponse:
        # Hybrid: submit the same Kafka event, then briefly wait on the status store.
        # Completed values are the REQUEST'S OWN computed write set, served from the
        # request-result store (never a Valkey /latest re-read, never computed here,
        # never bypassing Kafka). skipped_stale is still completed: the request was
        # computed but did not overwrite fresher online values. Offline write may
        # still be pending.
        result = _submit_feature_request(backend, request, settings)
        request_id = result.request_id
        status_url = f"/v1/feature-requests/{request_id}"

        requested = request.wait_timeout_ms
        if requested is None:
            requested = _DEFAULT_HYBRID_WAIT_MS
        wait_ms = max(0, min(requested, settings.hybrid_max_wait_ms))
        poll_s = settings.hybrid_poll_ms / 1000.0
        # Never wait past the request's own deadline: a result produced after expires_at
        # would be deadline_expired anyway, so there is nothing fresh to wait for.
        seconds_to_expiry = (result.expires_at - datetime.now(tz=UTC)).total_seconds()
        deadline = time.monotonic() + min(wait_ms / 1000.0, max(0.0, seconds_to_expiry))

        while True:
            try:
                status = backend.status.get(request_id)
            except Exception:  # noqa: BLE001 - status read failure -> degrade to pending
                status = None

            if status is not None and status.status == "completed":
                # An expired request has no online write and no request-scoped result:
                # report the deadline outcome explicitly, never a result_missing alarm
                # and never a /latest fallback.
                if status.online_write_status == "deadline_expired":
                    return HybridComputeResponse(
                        request_id=request_id,
                        job_id=result.job_id,
                        status="deadline_expired",
                        status_url=status_url,
                        entity_type=request.entity_type,
                        entity_key=dict(request.entity_key),
                        feature_view=request.view,
                        view_version=request.view_version,
                        features={},
                        missing_features=list(result.output_features),
                        online_write_status="deadline_expired",
                        offline_write_status=status.offline_write_status,
                    )
                features, missing, error = _read_request_result(
                    backend, request_id, result.output_features
                )
                return HybridComputeResponse(
                    request_id=request_id,
                    job_id=result.job_id,
                    status="completed",
                    status_url=status_url,
                    entity_type=request.entity_type,
                    entity_key=dict(request.entity_key),
                    feature_view=request.view,
                    view_version=request.view_version,
                    features=features,
                    missing_features=missing,
                    online_write_status=status.online_write_status,
                    offline_write_status=status.offline_write_status,
                    error=error,
                )

            if status is not None and status.status == "failed":
                return HybridComputeResponse(
                    request_id=request_id,
                    job_id=result.job_id,
                    status="failed",
                    status_url=status_url,
                    error=status.error,
                )

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # If we have already passed the absolute deadline, the request cannot
                # produce a fresh online result — report the deadline outcome; otherwise
                # it is simply still pending (worker has not finished within the wait).
                expired = datetime.now(tz=UTC) >= result.expires_at
                return HybridComputeResponse(
                    request_id=request_id,
                    job_id=result.job_id,
                    status="deadline_expired" if expired else "pending",
                    status_url=status_url,
                    online_write_status="deadline_expired" if expired else None,
                )
            time.sleep(min(poll_s, remaining) if poll_s > 0 else remaining)

    @app.get(
        "/v1/feature-requests/{request_id}",
        response_model=FeatureRequestStatusResponse,
        dependencies=[service_auth],
    )
    def get_feature_request(request_id: str) -> FeatureRequestStatusResponse:
        # Read-only request lifecycle. Exposes no object_key/storage_uri/values/payloads.
        status = backend.status.get(request_id)
        if status is None:
            raise HTTPException(
                status_code=404,
                detail=f"unknown or expired request_id {request_id!r}",
            )
        status = _reconcile_metadata_status(backend, status)
        return FeatureRequestStatusResponse(
            request_id=status.request_id,
            job_id=status.job_id,
            status=status.status,
            entity_type=status.entity_type,
            entity_key=dict(status.entity_key),
            view=status.view,
            view_version=status.view_version,
            requested_features=list(status.requested_features),
            requested_feature_groups=list(status.requested_feature_groups),
            online_write_status=status.online_write_status,
            offline_write_status=status.offline_write_status,
            metadata_write_status=status.metadata_write_status,
            created_at=status.created_at,
            updated_at=status.updated_at,
            started_at=status.started_at,
            finished_at=status.finished_at,
            error=status.error,
        )

    # Startup guard: a new route cannot appear without an explicit classification.
    assert_policy_complete(app, ENDPOINT_POLICY, "feature-api")
    return app


# uvicorn entrypoint: `uvicorn fintech_feature_platform.api.app:app`. Gated (mirrors
# examples/credit_decision_demo/model_service.py's FSP_MODEL_SERVICE_AUTOSTART) so a
# tool that only needs a module-level constant (e.g. ENDPOINT_POLICY, a plain dict
# defined above with no side effects) can import this module WITHOUT constructing a
# live app — and therefore without needing security env at all, and without printing
# the disabled-mode startup warning as an import side effect.
app = create_app() if os.environ.get("FSP_API_APP_AUTOSTART", "1") == "1" else None
