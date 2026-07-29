"""Backend bundle and builders for the API.

An ``AppBackend`` bundles the wired pieces the HTTP routes operate over (registry,
FeatureStore, the raw payload/metadata stores, the online/offline stores) plus a
``make_storage_uri`` callable and the raw format/compression to record in metadata.

``build_memory_backend()`` reproduces the in-memory demo wiring (the default).
``build_backend(settings)`` selects the backend by mode; the ``local`` backend is
built in ``api/local_backend.py`` (imported lazily, only when selected). This module
imports no optional extras (no MinIO/Postgres/Valkey clients).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from fintech_feature_platform.api.settings import Settings
from fintech_feature_platform.api.storage_uri import build_memory_storage_uri
from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.events.publisher import (
    EventPublisher,
    InMemoryEventPublisher,
)
from fintech_feature_platform.fs_core.feature_store import FeatureStore
from fintech_feature_platform.fs_core.observability.catalog import (
    fully_qualified_feature_ids,
)
from fintech_feature_platform.fs_core.observability.metrics import MetricsRecorder
from fintech_feature_platform.fs_core.observability.prometheus_recorder import (
    PrometheusMetricsRecorder,
)
from fintech_feature_platform.fs_core.raw.meta_repository import (
    InMemoryMetaRepository,
    RawReportMetaRepository,
)
from fintech_feature_platform.fs_core.raw.payload_store import (
    InMemoryPayloadStore,
    RawPayloadStore,
)
from fintech_feature_platform.fs_core.raw.report_resolver import ReportResolver
from fintech_feature_platform.fs_core.registry.loader import load_registry_file
from fintech_feature_platform.fs_core.registry.models import Registry
from fintech_feature_platform.fs_core.stores.batch_metadata import (
    BatchMetadataStore,
    InMemoryBatchMetadataStore,
)
from fintech_feature_platform.fs_core.stores.batch_status import (
    BatchJobStatusStore,
    InMemoryBatchJobStatusStore,
)
from fintech_feature_platform.fs_core.stores.metadata import (
    InMemoryMetadataStore,
    MetadataStore,
)
from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore
from fintech_feature_platform.fs_core.stores.online import InMemoryOnlineStore
from fintech_feature_platform.fs_core.stores.request_result import (
    InMemoryRequestResultStore,
    RequestResultStore,
)
from fintech_feature_platform.fs_core.stores.request_status import (
    InMemoryRequestStatusStore,
    RequestStatusStore,
)
from fintech_feature_platform.fs_core.stores.source_dataset import (
    InMemorySourceDatasetStore,
    SourceDatasetStore,
)

_REGISTRY_PATH = (
    Path(__file__).resolve().parents[3]
    / "examples"
    / "registry"
    / "minimal_credit_risk.yaml"
)


@dataclass(frozen=True)
class AppBackend:
    registry: Registry
    store: FeatureStore
    payloads: RawPayloadStore
    metas: RawReportMetaRepository
    online: Any
    offline: Any
    make_storage_uri: Callable[[str, str, datetime], str]
    raw_format: str
    raw_compression: str
    serialize_payload: Callable[[Any], bytes]
    make_feature_store: Callable[[ReportResolver], FeatureStore]
    events: EventPublisher
    status: RequestStatusStore
    results: RequestResultStore
    metadata: MetadataStore
    batch_status: BatchJobStatusStore
    batch_meta: BatchMetadataStore
    source_datasets: SourceDatasetStore
    metrics: MetricsRecorder
    # Readiness probes : built ONCE at backend construction over the
    # already-initialized clients. Empty for the memory backend (no external
    # dependencies -> always ready); role filtering happens in api/readiness.py.
    readiness_probes: tuple = ()


# --- sample (local/demo) wiring; domain logic, deliberately not in fs_core ---

def _declared_income(sources, deps):
    return sources["credit_report"]["declared_income"]


def _monthly_obligations(sources, deps):
    return sources["credit_report"]["monthly_obligations"]


def _income_from_tax(sources, deps):
    return sources["tax_report"]["income"]


def _safe_ratio(sources, deps):
    income = deps["income_from_tax"]
    return deps["monthly_obligations"] / income if income else None


def _memory_storage_uri(report_type: str, report_ref: str, report_ts: datetime) -> str:
    return build_memory_storage_uri(report_ref)


def _memory_serialize_payload(payload: Any) -> bytes:
    # Preserve the memory-mode behaviour (tolerant of non-JSON types).
    return json.dumps(payload, sort_keys=True, default=str).encode()


def _load_udf_provider(spec: str) -> UdfRegistry:
    """Load a UDF provider from ``module:callable`` (the fsctl --udfs contract).

    Delegates to the ONE shared loader  with the runtime spec label.
    Only the UDF half of a ``(registry, udfs)`` tuple is used — the registry comes
    from ``registry_path``, so the served contract stays the reviewed YAML file.
    A load failure raises ``UdfProviderLoadError`` (a ``ValueError``) and fails
    worker/API startup loudly — the fail-closed behavior is unchanged.
    """
    from fintech_feature_platform.fs_core.compute.udf_provider import load_udf_provider

    return load_udf_provider(spec, spec_label="FSP_UDF_PROVIDER")


def build_registry_and_udfs(
    settings: Settings | None = None,
    metrics: MetricsRecorder | None = None,
) -> tuple[Registry, UdfRegistry]:
    """Load the registry + UDF map (shared by both backends).

    Defaults (no ``FSP_REGISTRY_PATH``/``FSP_UDF_PROVIDER``) are the built-in demo
    registry + UDFs, exactly as before. Setting both serves a different
    feature contract (e.g. the credit-decision demo) through the same API/workers.
    ``metrics``  lets the artifact gate record verification outcomes.
    """
    if settings is None:
        from fintech_feature_platform.api.settings import load_settings

        settings = load_settings()
    registry = load_registry_file(settings.registry_path or _REGISTRY_PATH)
    # Fail-closed artifact-binding gate. Runs for every role because both
    # backends funnel through here. With default settings (development + legacy-compatible +
    # no promoted bundle configured) it is a no-op, preserving the existing behavior.
    from fintech_feature_platform.api.artifact_binding import enforce_artifact_binding

    enforce_artifact_binding(registry, settings, metrics=metrics)
    if settings.udf_provider:
        return registry, _load_udf_provider(settings.udf_provider)
    udfs = UdfRegistry(
        {
            "udf.credit.declared_income": _declared_income,
            "udf.credit.monthly_obligations": _monthly_obligations,
            "udf.tax.income": _income_from_tax,
            "udf.common.safe_ratio": _safe_ratio,
        }
    )
    return registry, udfs


def build_memory_backend() -> AppBackend:
    """In-memory demo backend (the default).

    Stores start empty; the Kafka-first submit path (and the backfill/import runner)
    populate the payload store / metadata at runtime. (The old ``rep_credit``/``rep_tax``
    sample seeding was removed with the legacy ``/v1/features/compute`` route in.)
    """
    # Recorder first : the artifact gate records outcomes and the
    # per-feature metric domain is bound from the loaded registry right after.
    metrics = PrometheusMetricsRecorder()
    registry, udfs = build_registry_and_udfs(metrics=metrics)
    metrics.bind_dynamic_domain("feature_id", fully_qualified_feature_ids(registry))
    payloads = InMemoryPayloadStore()
    metas = InMemoryMetaRepository()
    resolver = ReportResolver(payloads, metas)
    online = InMemoryOnlineStore()
    offline = InMemoryOfflineStore()
    store = FeatureStore(registry, udfs, resolver, offline, online, metrics=metrics)

    def make_feature_store(request_resolver: ReportResolver) -> FeatureStore:
        return FeatureStore(registry, udfs, request_resolver, offline, online, metrics=metrics)

    return AppBackend(
        registry=registry,
        store=store,
        payloads=payloads,
        metas=metas,
        online=online,
        offline=offline,
        make_storage_uri=_memory_storage_uri,
        raw_format="json",
        raw_compression="none",
        serialize_payload=_memory_serialize_payload,
        make_feature_store=make_feature_store,
        events=InMemoryEventPublisher(),
        status=InMemoryRequestStatusStore(),
        results=InMemoryRequestResultStore(),
        metadata=InMemoryMetadataStore(),
        batch_status=InMemoryBatchJobStatusStore(),
        batch_meta=InMemoryBatchMetadataStore(),
        source_datasets=InMemorySourceDatasetStore(),
        # Process-local Prometheus-backed recorder : one registry per
        # process; snapshot() keeps the legacy JSON schema.
        metrics=metrics,
    )


def build_event_publisher(settings: Settings) -> EventPublisher:
    """Select the event publisher by settings.

    ``memory`` (default) records events in process; ``kafka`` constructs a
    ``KafkaEventPublisher`` (lazy confluent-kafka import, clear error if the extra is
    missing). Kafka is only ever selected when ``FSP_EVENTS=kafka`` is explicit.
    """
    if settings.events == "kafka":
        # Imported lazily so memory mode never imports the Kafka publisher path.
        from fintech_feature_platform.fs_core.events.publisher import KafkaEventPublisher

        return KafkaEventPublisher(
            settings.kafka_bootstrap_servers, settings.kafka_client_id
        )
    return InMemoryEventPublisher()


def build_backend(settings: Settings) -> AppBackend:
    if settings.backend == "memory":
        return build_memory_backend()
    if settings.backend == "local":
        # Imported lazily so memory mode never imports the optional backend clients.
        from fintech_feature_platform.api.local_backend import build_local_backend

        return build_local_backend(settings)
    raise ValueError(f"unknown backend {settings.backend!r}")
