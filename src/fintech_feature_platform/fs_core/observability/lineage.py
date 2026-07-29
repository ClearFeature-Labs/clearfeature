"""Bounded, values-free feature-value lineage.

Given a feature-value reference (entity + feature ref, optionally pinned by data_ts/calc_ts),
build the audit trail that support needs: which run/bundle/model/data produced it — as
**refs, hashes, and timestamps only**. Never a feature value, raw payload, ``object_key``,
``storage_uri``, DWH row, SQL, or model artifact bytes.

The offline row does not persist a value→source link, so source ``report_refs`` are resolved
only from a caller-supplied ``report_refs`` list (via the raw-meta store) and/or a
``manifest_id`` (via source-dataset items). When neither is available, ``report_refs`` is an
explicit ``[]`` with a ``source_report_refs_not_available`` gap — missing links are stated,
never guessed. Auto-deriving report_refs from a value is future lineage hardening.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from fintech_feature_platform.fs_core.models import EntityKey


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


@dataclass(frozen=True)
class ReportRefLineage:
    """A values-free pointer to one source report (never a storage path or payload)."""

    report_ref: str
    report_type: str | None = None
    content_hash: str | None = None
    report_ts: datetime | None = None
    entity_type: str | None = None
    manifest_id: str | None = None
    # Availability provenance  — timestamps/enum only, values-free.
    available_at: datetime | None = None
    availability_source: str | None = None

    def to_dict(self) -> dict:
        return {
            "report_ref": self.report_ref,
            "report_type": self.report_type,
            "content_hash": self.content_hash,
            "report_ts": _iso(self.report_ts),
            "entity_type": self.entity_type,
            "manifest_id": self.manifest_id,
            "available_at": _iso(self.available_at),
            "availability_source": self.availability_source,
        }


@dataclass(frozen=True)
class FeatureLineage:
    """The audit trail for one feature value — refs / hashes / timestamps only."""

    found: bool
    feature_name: str
    feature_version: int
    view: str
    view_version: int
    entity_key: str
    data_ts: datetime | None = None
    calc_ts: datetime | None = None
    max_input_data_ts: datetime | None = None
    value_hash: str | None = None
    input_fingerprint: str | None = None
    bundle_digest: str | None = None
    model_uri: str | None = None
    model_digest: str | None = None
    model_output_name: str | None = None
    report_refs: tuple[ReportRefLineage, ...] = ()
    manifest_id: str | None = None
    gaps: tuple[str, ...] = ()
    # Effective availability clock of this value : trusted available_at
    # when the row carries one, else the conservative calc_ts fallback.
    available_at: datetime | None = None
    availability_effective: datetime | None = None
    availability_source: str | None = None

    def to_dict(self) -> dict:
        return {
            "found": self.found,
            "feature_name": self.feature_name,
            "feature_version": self.feature_version,
            "view": self.view,
            "view_version": self.view_version,
            "entity_key": self.entity_key,
            "data_ts": _iso(self.data_ts),
            "calc_ts": _iso(self.calc_ts),
            "max_input_data_ts": _iso(self.max_input_data_ts),
            "value_hash": self.value_hash,
            "input_fingerprint": self.input_fingerprint,
            "bundle_digest": self.bundle_digest,
            "model_uri": self.model_uri,
            "model_digest": self.model_digest,
            "model_output_name": self.model_output_name,
            "report_refs": [r.to_dict() for r in self.report_refs],
            "manifest_id": self.manifest_id,
            "gaps": list(self.gaps),
            "available_at": _iso(self.available_at),
            "availability_effective": _iso(self.availability_effective),
            "availability_source": self.availability_source,
        }


def _select_record(records, data_ts, calc_ts):
    """Pick the target offline record: filter by data_ts/calc_ts if given, else latest calc_ts."""
    candidates = records
    if data_ts is not None:
        candidates = [r for r in candidates if r.result.data_ts == data_ts]
    if calc_ts is not None:
        candidates = [r for r in candidates if r.result.calc_ts == calc_ts]
    if not candidates:
        return None
    return max(candidates, key=lambda r: r.result.calc_ts)


def _resolve_report_refs(metas, report_refs, gaps) -> list[ReportRefLineage]:
    resolved: list[ReportRefLineage] = []
    for ref in report_refs:
        try:
            meta = metas.get_meta(ref)
        except Exception:  # noqa: BLE001 - unknown ref -> explicit gap, never raise
            gaps.append(f"unknown_report_ref:{ref}")
            continue
        resolved.append(
            ReportRefLineage(
                report_ref=meta.report_ref,
                report_type=meta.report_type,
                content_hash=meta.content_hash,
                report_ts=meta.report_ts,
                entity_type=meta.entity_type,
                available_at=meta.available_at,
                availability_source=meta.availability_source,
            )
        )
    return resolved


def _resolve_manifest_refs(source_datasets, manifest_id, entity_key) -> list[ReportRefLineage]:
    if source_datasets is None:
        return []
    wanted = entity_key.encode()
    key_order = [name for name, _ in entity_key.parts]
    resolved: list[ReportRefLineage] = []
    for item in source_datasets.list_items(manifest_id):
        item_key = item.entity_key
        if item_key is not None:
            try:
                encoded = EntityKey.from_mapping(item_key, key_order=key_order).encode()
            except (KeyError, ValueError):
                continue  # item key doesn't match this entity's shape
            if encoded != wanted:
                continue
        if item.report_ref is None:
            continue
        resolved.append(
            ReportRefLineage(
                report_ref=item.report_ref,
                report_type=item.report_type,
                content_hash=item.content_hash,
                report_ts=item.event_ts,
                manifest_id=manifest_id,
            )
        )
    return resolved


def build_feature_lineage(
    offline,
    metas,
    entity_key: EntityKey,
    *,
    view: str,
    view_version: int,
    feature_name: str,
    feature_version: int,
    data_ts: datetime | None = None,
    calc_ts: datetime | None = None,
    report_refs: list[str] | None = None,
    manifest_id: str | None = None,
    source_datasets=None,
) -> FeatureLineage:
    """Build values-free lineage for one feature value; missing links are explicit gaps."""
    records = offline.get(
        entity_key, feature_name=feature_name, feature_version=feature_version,
        view=view, view_version=view_version,
    )
    record = _select_record(records, data_ts, calc_ts)
    gaps: list[str] = []
    if record is None:
        return FeatureLineage(
            found=False, feature_name=feature_name, feature_version=feature_version,
            view=view, view_version=view_version, entity_key=entity_key.encode(),
            gaps=("feature_value_not_found",),
        )

    result = record.result
    resolved_refs: list[ReportRefLineage] = []
    if report_refs:
        resolved_refs.extend(_resolve_report_refs(metas, report_refs, gaps))
    if manifest_id:
        resolved_refs.extend(
            _resolve_manifest_refs(source_datasets, manifest_id, entity_key)
        )
    if not resolved_refs:
        gaps.append("source_report_refs_not_available")
    if result.bundle_digest is None:
        gaps.append("bundle_digest_not_available")

    return FeatureLineage(
        found=True,
        feature_name=feature_name,
        feature_version=feature_version,
        view=view,
        view_version=view_version,
        entity_key=entity_key.encode(),
        data_ts=result.data_ts,
        calc_ts=result.calc_ts,
        max_input_data_ts=result.max_input_data_ts,
        value_hash=result.value_hash,
        input_fingerprint=result.input_fingerprint,
        available_at=result.available_at,
        availability_effective=result.available_at or result.calc_ts,
        availability_source=result.availability_source,
        bundle_digest=result.bundle_digest,
        model_uri=result.model_uri,
        model_digest=result.model_digest,
        model_output_name=result.model_output_name,
        report_refs=tuple(resolved_refs),
        manifest_id=manifest_id,
        gaps=tuple(gaps),
    )
