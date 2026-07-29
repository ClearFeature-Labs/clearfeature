"""Core domain value objects for the FinTech Feature Platform.

These are small, immutable, dependency-free data holders. They define the
vocabulary (entity keys, feature references, feature results, raw-report
metadata) that later components — registry, ComputeCore, stores, APIs — will
exchange. No I/O, no persistence, no registry logic lives here.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from fintech_feature_platform.fs_core.keycodec import encode_entity_key


def _require_aware(value: datetime, field_name: str) -> None:
    """Reject naive datetimes.

    Freshness/CAS, audit history, and point-in-time retrieval all compare
    timestamps, so every stored datetime must carry an explicit timezone.
    """
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"{field_name} must be timezone-aware")


@dataclass(frozen=True)
class EntityKey:
    """A business entity's composite key, stored in canonical order.

    Parts are kept as an immutable tuple of ``(name, value)`` pairs so the
    object is truly hashable and immutable. Build one with ``from_mapping``.
    """

    parts: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        # Validates parts and (for empty/malformed input) raises ValueError.
        self.encode()

    @classmethod
    def from_mapping(
        cls,
        parts: Mapping[str, str],
        key_order: Sequence[str] | None = None,
    ) -> EntityKey:
        encode_entity_key(parts, key_order)  # validate before storing
        ordered_names = list(key_order) if key_order is not None else sorted(parts)
        return cls(tuple((name, parts[name]) for name in ordered_names))

    def encode(self) -> str:
        names = [name for name, _ in self.parts]
        return encode_entity_key(dict(self.parts), key_order=names)


@dataclass(frozen=True)
class FeatureRef:
    """Identity of a feature: name plus version (e.g. ``declared_income:v1``).

    The version is part of the identity because a meaning change (UDF logic,
    window, dtype) requires a new version, and old and new versions must
    coexist in history and be served distinctly.
    """

    name: str
    version: int

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError("feature name must be non-empty")
        if self.version < 1:
            raise ValueError("feature version must be >= 1")

    def encode(self) -> str:
        return f"{self.name}:v{self.version}"


@dataclass(frozen=True)
class FeatureViewRef:
    """Identity of a feature view: name plus version (e.g. ``user_credit_risk:v2``)."""

    view: str
    view_version: int

    def __post_init__(self) -> None:
        if not self.view or not self.view.strip():
            raise ValueError("view name must be non-empty")
        if self.view_version < 1:
            raise ValueError("view version must be >= 1")

    def encode(self) -> str:
        return f"{self.view}:v{self.view_version}"


@dataclass(frozen=True)
class SourceStamp:
    """Per-source freshness + content identity used to derive per-feature data_ts.

    ``report_ts`` is the source report's own timestamp (never a request-level max);
    ``content_hash`` identifies the payload content and is stable across replays and
    re-submissions (unlike ``report_ref``), so it anchors ``input_fingerprint``.
    ``available_at``  is when the fact became available to the bank —
    trusted ingestion may carry a historical value; ``None`` = unknown (legacy),
    which degrades derived availability to the conservative calc_ts fallback.
    """

    report_ts: datetime
    content_hash: str
    available_at: datetime | None = None
    availability_source: str | None = None  # "source_provided" | "ingestion_time"

    def __post_init__(self) -> None:
        _require_aware(self.report_ts, "report_ts")
        if not self.content_hash or not self.content_hash.strip():
            raise ValueError("content_hash must be non-empty")
        if self.available_at is not None:
            _require_aware(self.available_at, "available_at")


@dataclass(frozen=True)
class FeatureResult:
    """Output of computing one feature for one entity.

    ``data_ts`` is the freshness timestamp of the feature's own inputs (D3:
    ``min`` over input data_ts); ``calc_ts`` is when the platform computed the
    value (audit). ``max_input_data_ts`` / ``input_fingerprint`` / ``value_hash``
    are the D9 write-guard metadata; they are optional so
    externally supplied results (model scores, table imports) degenerate to the
    plain CAS-on-``data_ts`` behavior.
    """

    ref: FeatureRef
    entity_key: EntityKey
    value: Any
    data_ts: datetime
    calc_ts: datetime
    max_input_data_ts: datetime | None = None
    input_fingerprint: str | None = None
    value_hash: str | None = None
    # Availability clock : max over the inputs' available_at, set only
    # when EVERY input carries one; None = legacy/unknown -> PIT falls back to the
    # conservative calc_ts rule. Distinct from data_ts (business time) by contract.
    available_at: datetime | None = None
    # "source_provided" only when EVERY input's availability is a trusted source
    # claim — then (and only then) availability is part of the replay identity, so
    # a corrected historical claim is auditable while ordinary reruns (whose
    # ingestion-time availability legitimately varies) stay exact duplicates.
    availability_source: str | None = None
    # F3 model lineage : which pinned model produced this value. Refs/hashes
    # only — never model artifact bytes. All ``None`` for F1/F2/model_score rows.
    model_uri: str | None = None
    model_digest: str | None = None
    model_output_name: str | None = None
    # Immutable registry snapshot this value was computed against. Optional and
    # backward-compatible; full propagation into events is future lineage work.
    bundle_digest: str | None = None

    def __post_init__(self) -> None:
        _require_aware(self.data_ts, "data_ts")
        _require_aware(self.calc_ts, "calc_ts")
        if self.max_input_data_ts is not None:
            _require_aware(self.max_input_data_ts, "max_input_data_ts")
        if self.available_at is not None:
            _require_aware(self.available_at, "available_at")


def trusted_available_at(result: FeatureResult) -> datetime | None:
    """The availability value that participates in replay identity : only a
    fully source-provided claim; incidental ingestion-time availability never
    distinguishes duplicates (it legitimately varies across reruns)."""
    if result.availability_source == "source_provided":
        return result.available_at
    return None


@dataclass(frozen=True)
class FeatureWriteSet:
    """Pre-persistence result of computing features for one entity.

    Wraps the computed ``FeatureResult`` objects (it does not replace them) plus the
    identifying context needed to persist or publish them later. It performs no I/O.
    The synchronous ``FeatureStore.compute`` persists from this; a future Kafka worker
    will write Valkey-first and emit an offline-write event from the same shape.

    Deliberately minimal: no per-feature write statuses, no materialization policy, no
    per-feature lineage — those are added by the tasks that need them. Serialization
    (added in) carries only the values the Offline Writer needs.
    """

    view: str
    view_version: int
    entity_key: EntityKey
    results: Mapping[str, FeatureResult]
    source_refs: Mapping[str, str]
    request_id: str | None = None
    job_id: str | None = None
    run_id: str | None = None
    correlation_id: str | None = None
    write_policy: str = "online_first"
    # Per-node F2 status (OK/DEGRADED/SKIPPED,). In-process observability only —
    # NOT serialized (the Offline Writer does not need it); reconstructs empty via from_dict.
    node_statuses: Mapping[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "view": self.view,
            "view_version": self.view_version,
            "entity_key": [[name, value] for name, value in self.entity_key.parts],
            "results": {
                name: {
                    "feature_name": result.ref.name,
                    "feature_version": result.ref.version,
                    "value": result.value,
                    "data_ts": result.data_ts.isoformat(),
                    "calc_ts": result.calc_ts.isoformat(),
                    "max_input_data_ts": (
                        result.max_input_data_ts.isoformat()
                        if result.max_input_data_ts is not None
                        else None
                    ),
                    "input_fingerprint": result.input_fingerprint,
                    "value_hash": result.value_hash,
                    "available_at": (
                        result.available_at.isoformat()
                        if result.available_at is not None
                        else None
                    ),
                    "availability_source": result.availability_source,
                    "model_uri": result.model_uri,
                    "model_digest": result.model_digest,
                    "model_output_name": result.model_output_name,
                    "bundle_digest": result.bundle_digest,
                }
                for name, result in self.results.items()
            },
            "source_refs": dict(self.source_refs),
            "request_id": self.request_id,
            "job_id": self.job_id,
            "run_id": self.run_id,
            "correlation_id": self.correlation_id,
            "write_policy": self.write_policy,
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> FeatureWriteSet:
        entity_key = EntityKey(tuple((name, value) for name, value in data["entity_key"]))
        results = {
            name: FeatureResult(
                ref=FeatureRef(item["feature_name"], item["feature_version"]),
                entity_key=entity_key,
                value=item["value"],
                data_ts=datetime.fromisoformat(item["data_ts"]),
                calc_ts=datetime.fromisoformat(item["calc_ts"]),
                # Optional D9 metadata: absent in legacy events (tolerant parse).
                max_input_data_ts=(
                    datetime.fromisoformat(item["max_input_data_ts"])
                    if item.get("max_input_data_ts") is not None
                    else None
                ),
                input_fingerprint=item.get("input_fingerprint"),
                value_hash=item.get("value_hash"),
                # Optional availability clock: absent in legacy events.
                available_at=(
                    datetime.fromisoformat(item["available_at"])
                    if item.get("available_at") is not None
                    else None
                ),
                availability_source=item.get("availability_source"),
                # Optional F3 lineage: absent in legacy events (tolerant parse).
                model_uri=item.get("model_uri"),
                model_digest=item.get("model_digest"),
                model_output_name=item.get("model_output_name"),
                # Optional bundle digest: absent in legacy events (tolerant parse).
                bundle_digest=item.get("bundle_digest"),
            )
            for name, item in data["results"].items()
        }
        return cls(
            view=data["view"],
            view_version=data["view_version"],
            entity_key=entity_key,
            results=results,
            source_refs=dict(data["source_refs"]),
            request_id=data.get("request_id"),
            job_id=data.get("job_id"),
            run_id=data.get("run_id"),
            correlation_id=data.get("correlation_id"),
            write_policy=data.get("write_policy", "online_first"),
        )

    @classmethod
    def from_json(cls, raw: bytes | str) -> FeatureWriteSet:
        return cls.from_dict(json.loads(raw))


@dataclass(frozen=True)
class RawReportMeta:
    """Metadata for a stored raw report (no payload).

    Entity identifiers live inside ``entity_key``; they are intentionally not
    duplicated as separate fields here.
    """

    report_ref: str
    report_type: str
    entity_type: str
    entity_key: EntityKey
    report_ts: datetime
    payload_size_bytes: int
    content_hash: str
    storage_uri: str
    created_at: datetime
    format: str = "json"
    compression: str = "gzip"
    # Availability clock : when the fact became available to the bank.
    # source_provided (trusted operator ingestion) or ingestion_time (= created_at).
    # None on legacy rows -> consumers fall back to created_at.
    available_at: datetime | None = None
    availability_source: str | None = None  # "source_provided" | "ingestion_time"

    def __post_init__(self) -> None:
        if not self.report_ref or not self.report_ref.strip():
            raise ValueError("report_ref must be non-empty")
        if self.payload_size_bytes < 0:
            raise ValueError("payload_size_bytes must be >= 0")
        _require_aware(self.report_ts, "report_ts")
        _require_aware(self.created_at, "created_at")
        if self.available_at is not None:
            _require_aware(self.available_at, "available_at")
            # The impossible-contract check binds TRUSTED claims only: a source
            # cannot have made a fact available before its as-of time. The
            # ingestion_time fallback is exempt (a future-dated report ingested now
            # is odd but legal — availability stays honest as "now").
            if (
                self.availability_source == "source_provided"
                and self.available_at < self.report_ts
            ):
                raise ValueError(
                    "available_at must not precede report_ts (a fact cannot be "
                    "available before its as-of time)"
                )
        if self.availability_source not in (None, "source_provided", "ingestion_time"):
            raise ValueError(
                "availability_source must be source_provided or ingestion_time"
            )
