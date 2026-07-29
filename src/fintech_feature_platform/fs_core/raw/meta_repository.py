"""Raw report metadata seam.

A narrow contract for fetching/persisting ``RawReportMeta`` by ``report_ref``, plus a
tiny in-memory implementation for tests. The real implementation (Postgres) satisfies the
same contract.

``upsert`` is the idempotent projection write used by the Metadata Writer: it is a no-op
(returns ``False``) when an identical row already exists for ``report_ref``, and raises
``RawReportMetaConflictError`` when the same ``report_ref`` carries different metadata
(``report_ref`` is immutable; a content-hash/storage-uri mismatch is data corruption, not a
harmless duplicate).
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Protocol

from fintech_feature_platform.fs_core.models import RawReportMeta

# Real correction-capable ingestion paths. Inline batch sources mint
# fresh report_refs per run and can never hit the correction branch.
CORRECTION_ORIGIN_JSONL = "jsonl_ingestion"
CORRECTION_ORIGIN_DWH = "dwh_import"


@dataclass(frozen=True)
class CorrectionContext:
    """Origin evidence for an availability correction — ids only, never secrets."""

    change_origin: str
    manifest_id: str | None = None
    request_id: str | None = None
    actor_key_id: str | None = None


@dataclass(frozen=True)
class AvailabilityChange:
    """One append-only audit record of a trusted availability correction."""

    report_ref: str
    old_available_at: datetime | None
    new_available_at: datetime
    old_availability_source: str | None
    new_availability_source: str
    changed_at: datetime
    change_origin: str
    manifest_id: str | None = None
    request_id: str | None = None
    actor_key_id: str | None = None


def build_availability_change(
    existing: RawReportMeta,
    incoming: RawReportMeta,
    context: CorrectionContext | None,
) -> AvailabilityChange:
    context = context or CorrectionContext(change_origin="unknown")
    return AvailabilityChange(
        report_ref=existing.report_ref,
        old_available_at=existing.available_at,
        new_available_at=incoming.available_at,
        old_availability_source=existing.availability_source,
        new_availability_source=incoming.availability_source,
        changed_at=datetime.now(tz=UTC),
        change_origin=context.change_origin,
        manifest_id=context.manifest_id,
        request_id=context.request_id,
        actor_key_id=context.actor_key_id,
    )

# Identity fields compared to detect a conflicting re-projection (everything except the
# write-time ``created_at``, which legitimately differs between writes).
_IDENTITY_FIELDS = (
    "report_type",
    "entity_type",
    "entity_key",
    "report_ts",
    "payload_size_bytes",
    "content_hash",
    "storage_uri",
    "format",
    "compression",
)


class RawReportMetaConflictError(Exception):
    """Raised when a ``report_ref`` is re-projected with different metadata."""


def raw_meta_conflicts(a: RawReportMeta, b: RawReportMeta) -> bool:
    return any(getattr(a, field) != getattr(b, field) for field in _IDENTITY_FIELDS)


def is_availability_correction(existing: RawReportMeta, incoming: RawReportMeta) -> bool:
    """True when the SAME report (identity matches) arrives with a different trusted
    availability — an auditable metadata correction, never a silent duplicate
. Only a source-provided incoming value corrects; ingestion-time
    availability on a rerun is expected to differ and stays a duplicate no-op."""
    if raw_meta_conflicts(existing, incoming):
        return False
    return (
        incoming.availability_source == "source_provided"
        and incoming.available_at is not None
        and incoming.available_at != existing.available_at
    )


class RawReportMetaRepository(Protocol):
    def get_meta(self, report_ref: str) -> RawReportMeta: ...

    def upsert(
        self,
        meta: RawReportMeta,
        *,
        correction_context: CorrectionContext | None = None,
    ) -> bool: ...


class InMemoryMetaRepository:
    def __init__(self) -> None:
        self._meta: dict[str, RawReportMeta] = {}
        # Append-only audit of trusted availability corrections  —
        # the memory twin of raw_report_availability_changes.
        self.availability_changes: list[AvailabilityChange] = []

    def add(self, meta: RawReportMeta) -> None:
        self._meta[meta.report_ref] = meta

    def upsert(
        self,
        meta: RawReportMeta,
        *,
        correction_context: CorrectionContext | None = None,
    ) -> bool:
        existing = self._meta.get(meta.report_ref)
        if existing is None:
            self._meta[meta.report_ref] = meta
            return True
        if raw_meta_conflicts(existing, meta):
            raise RawReportMetaConflictError(meta.report_ref)
        if is_availability_correction(existing, meta):
            # Correct availability in place; first-ingestion created_at is
            # preserved; exactly one append-only audit record per real change.
            self.availability_changes.append(
                build_availability_change(existing, meta, correction_context)
            )
            self._meta[meta.report_ref] = replace(
                existing,
                available_at=meta.available_at,
                availability_source=meta.availability_source,
            )
            return True
        return False

    def get_meta(self, report_ref: str) -> RawReportMeta:
        return self._meta[report_ref]
