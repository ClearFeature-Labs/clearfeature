"""Inline persist+compute helper for backfill / import (and the milestone acceptance).

`persist_and_compute` persists each inline source (payload + RawReportMeta) to the
backend, then computes through the normal `FeatureStore.compute` using a request-scoped
overlay resolver so the freshly submitted payloads are served from memory rather than
read back from MinIO/Postgres.

This is an **internal library helper**, not an HTTP compute API. The legacy
``POST /v1/features/compute-direct`` route that used to wrap it was removed in;
the online compute path is Kafka-first (``POST /v1/feature-requests``). The remaining
callers are ``api/backfill.py`` (raw-JSON backfill / table import) and the acceptance
script.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, field_validator

from fintech_feature_platform.api.backend import AppBackend
from fintech_feature_platform.fs_core.feature_store import ComputeOutcome
from fintech_feature_platform.fs_core.models import (
    EntityKey,
    RawReportMeta,
    SourceStamp,
)
from fintech_feature_platform.fs_core.raw.overlay import (
    OverlayRawPayloadStore,
    OverlayRawReportMetaRepository,
)
from fintech_feature_platform.fs_core.raw.report_resolver import ReportResolver
from fintech_feature_platform.fs_core.registry.models import FeatureViewDef


class InlineSource(BaseModel):
    """Inline source payload for batch items.

    ``available_at`` : trusted historical availability — inline
    batch submission is an operator-role path, so backdating here is a trusted
    operator action. Absent -> the computation time is recorded as ingestion-time
    availability.
    """

    report_type: str
    report_ts: datetime
    payload: Any
    available_at: datetime | None = None

    @field_validator("report_ts", "available_at")
    @classmethod
    def _require_timezone_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and (
            value.tzinfo is None or value.tzinfo.utcoffset(value) is None
        ):
            raise ValueError("must be timezone-aware")
        return value




def persist_and_compute(
    backend: AppBackend,
    view: FeatureViewDef,
    entity_key: EntityKey,
    inline_sources: Mapping[str, InlineSource],
    requested_features: list[str],
    *,
    write_online: bool = True,
    skip_duplicate_offline: bool = False,
) -> tuple[ComputeOutcome, dict[str, str]]:
    """Persist inline sources, then compute via an overlay-backed FeatureStore.

    Used by the raw-JSON backfill / table import runner (``write_online=False`` for
    offline-only, ``write_online=True`` for offline+online) and the milestone acceptance
    script. Returns the compute outcome and ``{source_name: report_ref}``. Raises
    ``ValueError``/``TypeError`` for user/input errors.
    """
    if not inline_sources:
        raise ValueError("inline_sources must not be empty")

    calc_ts = datetime.now(tz=UTC)

    overlay_payloads: dict[str, Any] = {}
    overlay_metas: dict[str, RawReportMeta] = {}
    report_refs: dict[str, str] = {}
    source_refs: dict[str, str] = {}
    # Per-source stamps: each feature derives its OWN data_ts from its inputs (D3/D9).
    source_stamps: dict[str, SourceStamp] = {}

    for source_name, source in inline_sources.items():
        report_ref = f"rep_{uuid4().hex}"
        serialized = backend.serialize_payload(source.payload)
        storage_uri = backend.make_storage_uri(
            source.report_type, report_ref, source.report_ts
        )
        # Availability : operator-trusted when provided, else this
        # computation's wall time recorded as ingestion-time availability.
        if source.available_at is not None:
            effective_available_at = source.available_at
            availability_source = "source_provided"
        else:
            effective_available_at = calc_ts
            availability_source = "ingestion_time"
        meta = RawReportMeta(
            report_ref=report_ref,
            report_type=source.report_type,
            entity_type=view.entity,
            entity_key=entity_key,
            report_ts=source.report_ts,
            payload_size_bytes=len(serialized),
            content_hash="sha256:" + hashlib.sha256(serialized).hexdigest(),
            storage_uri=storage_uri,
            created_at=calc_ts,
            format=backend.raw_format,
            compression=backend.raw_compression,
            available_at=effective_available_at,
            availability_source=availability_source,
        )
        backend.payloads.put(storage_uri, source.payload)
        backend.metas.add(meta)
        overlay_payloads[storage_uri] = source.payload
        overlay_metas[report_ref] = meta
        report_refs[source_name] = report_ref
        # Bound by source NAME (not report_type) — the registry source key.
        source_refs[source_name] = report_ref
        source_stamps[source_name] = SourceStamp(
            report_ts=source.report_ts,
            content_hash=meta.content_hash,
            available_at=effective_available_at,
            availability_source=availability_source,
        )

    resolver = ReportResolver(
        OverlayRawPayloadStore(overlay_payloads, backend.payloads),
        OverlayRawReportMetaRepository(overlay_metas, backend.metas),
    )
    store = backend.make_feature_store(resolver)
    outcome = store.compute(
        view=view.name,
        view_version=view.view_version,
        entity_key=entity_key,
        requested_features=requested_features,
        source_refs=source_refs,
        source_stamps=source_stamps,
        calc_ts=calc_ts,
        write_online=write_online,
        skip_duplicate_offline=skip_duplicate_offline,
    )
    return outcome, report_refs
