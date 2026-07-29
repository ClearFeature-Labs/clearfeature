"""Self-contained Kafka event models.

The online compute event must be *self-contained*: a worker can locate and read the
raw reports from object storage using the report descriptors alone, without querying
Postgres. Events carry references and metadata only — never raw payloads.

These are boring stdlib dataclasses with deterministic JSON (``sort_keys=True``) so
tests can assert exact serialization. ``object_key`` is internal event metadata; the
public API exposes ``report_ref`` only.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime

from fintech_feature_platform.fs_core.keycodec import encode_entity_key
from fintech_feature_platform.fs_core.models import FeatureWriteSet

EVENT_TYPE_FEATURE_COMPUTE_REQUESTED = "feature_compute.requested"
EVENT_TYPE_FEATURE_COMPUTE_COMPLETED = "feature_compute.completed"
EVENT_TYPE_FEATURE_OFFLINE_WRITE_REQUESTED = "feature_write.offline.requested"
EVENT_TYPE_FEATURE_UPDATED = "feature_updated"
EVENT_TYPE_MODEL_SCORE_WRITE = "model_score.write.requested"
EVENT_TYPE_BATCH_CHUNK_REQUESTED = "batch.chunk.requested"
EVENT_TYPE_BATCH_CHUNK_PROCESSED = "batch.chunk.processed"
EVENT_TYPE_DLQ = "event.dlq"
EVENT_VERSION = 1

# Who durably wrote the feature that a FeatureUpdated announces.
FEATURE_UPDATE_SOURCE_OFFLINE_WRITER = "offline_writer"
FEATURE_UPDATE_SOURCE_BATCH_WORKER = "batch_worker"
FEATURE_UPDATE_SOURCE_DWH_IMPORT = "dwh_import"
FEATURE_UPDATE_SOURCE_TABLE_IMPORT = "table_import"
FEATURE_UPDATE_SOURCE_RECOMPUTE_WAVE = "recompute_wave"
FEATURE_UPDATE_SOURCES = (
    FEATURE_UPDATE_SOURCE_OFFLINE_WRITER,
    FEATURE_UPDATE_SOURCE_BATCH_WORKER,
    FEATURE_UPDATE_SOURCE_DWH_IMPORT,
    FEATURE_UPDATE_SOURCE_TABLE_IMPORT,
    FEATURE_UPDATE_SOURCE_RECOMPUTE_WAVE,
)


def _iso(value: datetime) -> str:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError("event datetimes must be timezone-aware")
    return value.isoformat()


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


@dataclass(frozen=True)
class EntityRef:
    """The entity an event is about: type plus its key parts."""

    entity_type: str
    entity_key: dict[str, str]

    def encoded(self) -> str:
        """Deterministic encoded key, used as the Kafka partition key."""
        return encode_entity_key(self.entity_key)

    def to_dict(self) -> dict:
        return {"entity_type": self.entity_type, "entity_key": dict(self.entity_key)}

    @classmethod
    def from_dict(cls, data: dict) -> EntityRef:
        return cls(entity_type=data["entity_type"], entity_key=dict(data["entity_key"]))


@dataclass(frozen=True)
class ReportDescriptor:
    """Self-contained pointer to one stored raw report.

    Carries enough for a worker to read and validate the payload from object storage
    without a Postgres lookup. ``object_key`` is internal (the physical storage URI).
    """

    report_ref: str
    source_name: str
    report_type: str
    schema_version: str
    report_ts: datetime
    object_key: str
    content_hash: str
    size_bytes: int
    compression: str
    format: str
    # Availability clock : stamped by the producer (API accept time for
    # online requests; trusted/ingestion-time meta for batch refs). Optional and
    # tolerant-parsed so legacy events replay unchanged.
    available_at: datetime | None = None

    def to_dict(self) -> dict:
        return {
            "report_ref": self.report_ref,
            "source_name": self.source_name,
            "report_type": self.report_type,
            "schema_version": self.schema_version,
            "report_ts": _iso(self.report_ts),
            "object_key": self.object_key,
            "content_hash": self.content_hash,
            "size_bytes": self.size_bytes,
            "compression": self.compression,
            "format": self.format,
            "available_at": _iso(self.available_at) if self.available_at else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> ReportDescriptor:
        # report_type is required (pre-beta strict contract; no source_name fallback).
        return cls(
            report_ref=data["report_ref"],
            source_name=data["source_name"],
            report_type=data["report_type"],
            schema_version=data["schema_version"],
            report_ts=_parse_dt(data["report_ts"]),
            object_key=data["object_key"],
            content_hash=data["content_hash"],
            size_bytes=data["size_bytes"],
            compression=data["compression"],
            format=data["format"],
            available_at=(
                _parse_dt(data["available_at"])
                if data.get("available_at") is not None
                else None
            ),
        )


@dataclass(frozen=True)
class FeatureComputeRequested:
    """A self-contained request to compute feature groups for one entity.

    Published to ``fp.feature-compute.online``. Consumed by online workers.

    Deadline fields : ``event_ts`` is when the API accepted/published the
    request; ``expires_at`` is the absolute deadline after which an online compute
    result must not write Valkey (the online worker skips the write and records a
    ``deadline_expired`` outcome). Both are optional for backward compatibility — an
    event without ``expires_at`` has no deadline and never expires. When present they
    must be timezone-aware and ``expires_at`` must be strictly after ``event_ts``.
    Workers reason from the absolute ``expires_at``, never the relative ``deadline_ms``.
    """

    request_id: str
    job_id: str
    priority: str
    deadline_ms: int
    entity: EntityRef
    view: str
    view_version: int
    reports: list[ReportDescriptor]
    write_policy: str
    idempotency_key: str
    correlation_id: str
    occurred_at: datetime
    requested_feature_groups: list[str] = field(default_factory=list)
    requested_features: list[str] = field(default_factory=list)
    event_ts: datetime | None = None
    expires_at: datetime | None = None
    event_type: str = EVENT_TYPE_FEATURE_COMPUTE_REQUESTED
    event_version: int = EVENT_VERSION

    def __post_init__(self) -> None:
        if self.event_ts is not None:
            _iso(self.event_ts)  # rejects naive datetimes
        if self.expires_at is not None:
            _iso(self.expires_at)
            reference = self.event_ts if self.event_ts is not None else None
            if reference is not None and self.expires_at <= reference:
                raise ValueError("expires_at must be strictly after event_ts")

    def to_dict(self) -> dict:
        return {
            "event_type": self.event_type,
            "event_version": self.event_version,
            "request_id": self.request_id,
            "job_id": self.job_id,
            "priority": self.priority,
            "deadline_ms": self.deadline_ms,
            "entity": self.entity.to_dict(),
            "view": self.view,
            "view_version": self.view_version,
            "requested_feature_groups": list(self.requested_feature_groups),
            "requested_features": list(self.requested_features),
            "reports": [r.to_dict() for r in self.reports],
            "write_policy": self.write_policy,
            "idempotency_key": self.idempotency_key,
            "correlation_id": self.correlation_id,
            "occurred_at": _iso(self.occurred_at),
            "event_ts": _iso(self.event_ts) if self.event_ts is not None else None,
            "expires_at": _iso(self.expires_at) if self.expires_at is not None else None,
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> FeatureComputeRequested:
        event_ts = data.get("event_ts")
        expires_at = data.get("expires_at")
        return cls(
            request_id=data["request_id"],
            job_id=data["job_id"],
            priority=data["priority"],
            deadline_ms=data["deadline_ms"],
            entity=EntityRef.from_dict(data["entity"]),
            view=data["view"],
            view_version=data["view_version"],
            reports=[ReportDescriptor.from_dict(r) for r in data["reports"]],
            write_policy=data["write_policy"],
            idempotency_key=data["idempotency_key"],
            correlation_id=data["correlation_id"],
            occurred_at=_parse_dt(data["occurred_at"]),
            requested_feature_groups=list(data.get("requested_feature_groups", [])),
            requested_features=list(data.get("requested_features", [])),
            event_ts=_parse_dt(event_ts) if event_ts is not None else None,
            expires_at=_parse_dt(expires_at) if expires_at is not None else None,
            event_type=data.get("event_type", EVENT_TYPE_FEATURE_COMPUTE_REQUESTED),
            event_version=data.get("event_version", EVENT_VERSION),
        )

    @classmethod
    def from_json(cls, raw: bytes | str) -> FeatureComputeRequested:
        return cls.from_dict(json.loads(raw))


@dataclass(frozen=True)
class FeatureComputeCompleted:
    """Completion/status signal published after the online (Valkey-first) write.

    Carries references and the names of the features written online — never feature
    values or raw payloads. The values-bearing offline-write event (for the Offline
    Writer) is deliberately deferred to a later task.

    ``online_write_status``  records the aggregate D9 outcome of the online
    write (``written`` / ``skipped_stale`` / ``noop``) or ``deadline_expired`` when the
    request expired before its online write. It lets the Metadata Writer audit "expired
    before online write" without any feature values. Legacy events (legacy) omit it;
    consumers treat a missing value as ``written``.
    """

    request_id: str
    job_id: str
    entity: EntityRef
    view: str
    view_version: int
    correlation_id: str
    occurred_at: datetime
    written_features: list[str] = field(default_factory=list)
    online_write_status: str | None = None
    event_type: str = EVENT_TYPE_FEATURE_COMPUTE_COMPLETED
    event_version: int = EVENT_VERSION

    def to_dict(self) -> dict:
        return {
            "event_type": self.event_type,
            "event_version": self.event_version,
            "request_id": self.request_id,
            "job_id": self.job_id,
            "entity": self.entity.to_dict(),
            "view": self.view,
            "view_version": self.view_version,
            "written_features": list(self.written_features),
            "online_write_status": self.online_write_status,
            "correlation_id": self.correlation_id,
            "occurred_at": _iso(self.occurred_at),
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> FeatureComputeCompleted:
        return cls(
            request_id=data["request_id"],
            job_id=data["job_id"],
            entity=EntityRef.from_dict(data["entity"]),
            view=data["view"],
            view_version=data["view_version"],
            correlation_id=data["correlation_id"],
            occurred_at=_parse_dt(data["occurred_at"]),
            written_features=list(data.get("written_features", [])),
            online_write_status=data.get("online_write_status"),
            event_type=data.get("event_type", EVENT_TYPE_FEATURE_COMPUTE_COMPLETED),
            event_version=data.get("event_version", EVENT_VERSION),
        )

    @classmethod
    def from_json(cls, raw: bytes | str) -> FeatureComputeCompleted:
        return cls.from_dict(json.loads(raw))


@dataclass(frozen=True)
class FeatureUpdated:
    """A values-free announcement that one feature was durably written for one entity.

    Published to ``fp.feature-updates`` after a durable offline write/import. It
    carries references, freshness timestamps, and D9 hash ids only — never
    a feature value, raw payload, ``object_key``/``storage_uri``, DWH row body, or SQL. The
    reactive propagation worker uses it to plan debounced recompute waves for reactive
    dependents.
    """

    update_id: str
    entity: EntityRef
    view: str
    view_version: int
    feature_name: str
    feature_version: int
    data_ts: datetime
    calc_ts: datetime
    source: str
    occurred_at: datetime
    max_input_data_ts: datetime | None = None
    input_fingerprint: str | None = None
    value_hash: str | None = None
    run_id: str | None = None
    job_id: str | None = None
    manifest_id: str | None = None
    event_type: str = EVENT_TYPE_FEATURE_UPDATED
    event_version: int = EVENT_VERSION

    def __post_init__(self) -> None:
        if self.source not in FEATURE_UPDATE_SOURCES:
            raise ValueError(
                f"FeatureUpdated has unknown source {self.source!r}; "
                f"must be one of {list(FEATURE_UPDATE_SOURCES)}"
            )
        _iso(self.data_ts)  # reject naive datetimes
        _iso(self.calc_ts)
        if self.max_input_data_ts is not None:
            _iso(self.max_input_data_ts)

    def to_dict(self) -> dict:
        return {
            "event_type": self.event_type,
            "event_version": self.event_version,
            "update_id": self.update_id,
            "entity": self.entity.to_dict(),
            "view": self.view,
            "view_version": self.view_version,
            "feature_name": self.feature_name,
            "feature_version": self.feature_version,
            "data_ts": _iso(self.data_ts),
            "calc_ts": _iso(self.calc_ts),
            "max_input_data_ts": (
                _iso(self.max_input_data_ts)
                if self.max_input_data_ts is not None
                else None
            ),
            "input_fingerprint": self.input_fingerprint,
            "value_hash": self.value_hash,
            "run_id": self.run_id,
            "job_id": self.job_id,
            "manifest_id": self.manifest_id,
            "source": self.source,
            "occurred_at": _iso(self.occurred_at),
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> FeatureUpdated:
        max_input = data.get("max_input_data_ts")
        return cls(
            update_id=data["update_id"],
            entity=EntityRef.from_dict(data["entity"]),
            view=data["view"],
            view_version=data["view_version"],
            feature_name=data["feature_name"],
            feature_version=data["feature_version"],
            data_ts=_parse_dt(data["data_ts"]),
            calc_ts=_parse_dt(data["calc_ts"]),
            source=data["source"],
            occurred_at=_parse_dt(data["occurred_at"]),
            max_input_data_ts=_parse_dt(max_input) if max_input is not None else None,
            input_fingerprint=data.get("input_fingerprint"),
            value_hash=data.get("value_hash"),
            run_id=data.get("run_id"),
            job_id=data.get("job_id"),
            manifest_id=data.get("manifest_id"),
            event_type=data.get("event_type", EVENT_TYPE_FEATURE_UPDATED),
            event_version=data.get("event_version", EVENT_VERSION),
        )

    @classmethod
    def from_json(cls, raw: bytes | str) -> FeatureUpdated:
        return cls.from_dict(json.loads(raw))


@dataclass(frozen=True)
class FeatureOfflineWriteRequested:
    """Durable, values-bearing event for the Offline Writer.

    Published to ``fp.feature-write.offline`` after the online write succeeds, so the
    computed values are durable (and replayable into offline history) before the source
    offset is committed. Carries a serialized ``FeatureWriteSet`` — values + lineage +
    ids only, never raw payloads. MVP assumes online write sets are small enough for
    Kafka; oversized-write-set object-storage offloading is deferred.
    """

    request_id: str
    job_id: str
    correlation_id: str
    occurred_at: datetime
    write_set: FeatureWriteSet
    event_type: str = EVENT_TYPE_FEATURE_OFFLINE_WRITE_REQUESTED
    event_version: int = EVENT_VERSION

    def to_dict(self) -> dict:
        return {
            "event_type": self.event_type,
            "event_version": self.event_version,
            "request_id": self.request_id,
            "job_id": self.job_id,
            "correlation_id": self.correlation_id,
            "occurred_at": _iso(self.occurred_at),
            "write_set": self.write_set.to_dict(),
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> FeatureOfflineWriteRequested:
        return cls(
            request_id=data["request_id"],
            job_id=data["job_id"],
            correlation_id=data["correlation_id"],
            occurred_at=_parse_dt(data["occurred_at"]),
            write_set=FeatureWriteSet.from_dict(data["write_set"]),
            event_type=data.get("event_type", EVENT_TYPE_FEATURE_OFFLINE_WRITE_REQUESTED),
            event_version=data.get("event_version", EVENT_VERSION),
        )

    @classmethod
    def from_json(cls, raw: bytes | str) -> FeatureOfflineWriteRequested:
        return cls.from_dict(json.loads(raw))


@dataclass(frozen=True)
class ScoreItem:
    """One externally-computed model score to write back as a feature value."""

    feature: str
    value: object
    data_ts: datetime
    calc_ts: datetime
    model_name: str
    model_version: str
    source_request_id: str | None = None
    decision_request_id: str | None = None
    context: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "feature": self.feature,
            "value": self.value,
            "data_ts": _iso(self.data_ts),
            "calc_ts": _iso(self.calc_ts),
            "model_name": self.model_name,
            "model_version": self.model_version,
            "source_request_id": self.source_request_id,
            "decision_request_id": self.decision_request_id,
            "context": dict(self.context),
        }

    @classmethod
    def from_dict(cls, data: dict) -> ScoreItem:
        return cls(
            feature=data["feature"],
            value=data["value"],
            data_ts=_parse_dt(data["data_ts"]),
            calc_ts=_parse_dt(data["calc_ts"]),
            model_name=data["model_name"],
            model_version=data["model_version"],
            source_request_id=data.get("source_request_id"),
            decision_request_id=data.get("decision_request_id"),
            context=dict(data.get("context", {})),
        )


@dataclass(frozen=True)
class ModelScoreWriteRequested:
    """A request to write externally-computed model scores back as feature values.

    Published to ``fp.model-score.write``. The score writer resolves each feature's
    version from the registry, writes online (CAS by ``data_ts``) when ``write_online``,
    and republishes ``FeatureOfflineWriteRequested`` when ``write_offline``. The platform
    never executes the model — it stores/serves/audits the supplied outputs.
    """

    score_write_id: str
    correlation_id: str
    occurred_at: datetime
    entity: EntityRef
    view: str
    view_version: int
    scores: list[ScoreItem]
    idempotency_key: str
    write_online: bool = True
    write_offline: bool = True
    event_type: str = EVENT_TYPE_MODEL_SCORE_WRITE
    event_version: int = EVENT_VERSION

    def to_dict(self) -> dict:
        return {
            "event_type": self.event_type,
            "event_version": self.event_version,
            "score_write_id": self.score_write_id,
            "correlation_id": self.correlation_id,
            "occurred_at": _iso(self.occurred_at),
            "entity": self.entity.to_dict(),
            "view": self.view,
            "view_version": self.view_version,
            "scores": [s.to_dict() for s in self.scores],
            "idempotency_key": self.idempotency_key,
            "write_online": self.write_online,
            "write_offline": self.write_offline,
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> ModelScoreWriteRequested:
        return cls(
            score_write_id=data["score_write_id"],
            correlation_id=data["correlation_id"],
            occurred_at=_parse_dt(data["occurred_at"]),
            entity=EntityRef.from_dict(data["entity"]),
            view=data["view"],
            view_version=data["view_version"],
            scores=[ScoreItem.from_dict(s) for s in data["scores"]],
            idempotency_key=data["idempotency_key"],
            write_online=data.get("write_online", True),
            write_offline=data.get("write_offline", True),
            event_type=data.get("event_type", EVENT_TYPE_MODEL_SCORE_WRITE),
            event_version=data.get("event_version", EVENT_VERSION),
        )

    @classmethod
    def from_json(cls, raw: bytes | str) -> ModelScoreWriteRequested:
        return cls.from_dict(json.loads(raw))


@dataclass(frozen=True)
class BatchItem:
    """One entity for a batch chunk, sourced one of two ways (never both):

    - ``inline_sources`` (inline batch,): ``api.direct_compute.InlineSource``
      shape as plain dicts (``{source_name: {report_type, report_ts(iso), payload}}``);
    - ``source_refs`` (dataset-scoped,): ``{source_name: report_ref}`` — the
      report is already landed, so the chunk event carries a **reference only**, never a
      payload/object_key. The worker resolves it via ``raw_reports_meta`` + payload store.
    """

    entity_type: str
    entity_key: dict[str, str]
    inline_sources: dict[str, dict] = field(default_factory=dict)
    source_refs: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "entity_type": self.entity_type,
            "entity_key": dict(self.entity_key),
            "inline_sources": {
                name: dict(source) for name, source in self.inline_sources.items()
            },
            "source_refs": dict(self.source_refs),
        }

    @classmethod
    def from_dict(cls, data: dict) -> BatchItem:
        return cls(
            entity_type=data["entity_type"],
            entity_key=dict(data["entity_key"]),
            inline_sources={
                name: dict(source)
                for name, source in data.get("inline_sources", {}).items()
            },
            source_refs=dict(data.get("source_refs", {})),
        )


@dataclass(frozen=True)
class BatchChunkRequested:
    """One chunk of a batch job: a bounded list of entities to compute + persist.

    Published to ``fp.feature-compute.batch``. The batch worker plans the output features
    once, then computes each item via the shared ``persist_and_compute`` (offline-first,
    optional online CAS). Payloads are inline and bounded by ``chunk_size``.
    """

    batch_job_id: str
    chunk_id: str
    chunk_index: int
    chunk_count: int
    correlation_id: str
    occurred_at: datetime
    view: str
    view_version: int
    items: list[BatchItem]
    requested_features: list[str] = field(default_factory=list)
    requested_feature_groups: list[str] = field(default_factory=list)
    write_online: bool = False
    total_items: int = 0  # job-wide item count (so audit job snapshot is complete)
    manifest_id: str | None = None  # set for dataset-scoped jobs
    event_type: str = EVENT_TYPE_BATCH_CHUNK_REQUESTED
    event_version: int = EVENT_VERSION

    def to_dict(self) -> dict:
        return {
            "event_type": self.event_type,
            "event_version": self.event_version,
            "batch_job_id": self.batch_job_id,
            "chunk_id": self.chunk_id,
            "chunk_index": self.chunk_index,
            "chunk_count": self.chunk_count,
            "correlation_id": self.correlation_id,
            "occurred_at": _iso(self.occurred_at),
            "view": self.view,
            "view_version": self.view_version,
            "items": [item.to_dict() for item in self.items],
            "requested_features": list(self.requested_features),
            "requested_feature_groups": list(self.requested_feature_groups),
            "write_online": self.write_online,
            "total_items": self.total_items,
            "manifest_id": self.manifest_id,
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> BatchChunkRequested:
        return cls(
            batch_job_id=data["batch_job_id"],
            chunk_id=data["chunk_id"],
            chunk_index=data["chunk_index"],
            chunk_count=data["chunk_count"],
            correlation_id=data["correlation_id"],
            occurred_at=_parse_dt(data["occurred_at"]),
            view=data["view"],
            view_version=data["view_version"],
            items=[BatchItem.from_dict(item) for item in data["items"]],
            requested_features=list(data.get("requested_features", [])),
            requested_feature_groups=list(data.get("requested_feature_groups", [])),
            write_online=data.get("write_online", False),
            total_items=data.get("total_items", 0),
            manifest_id=data.get("manifest_id"),
            event_type=data.get("event_type", EVENT_TYPE_BATCH_CHUNK_REQUESTED),
            event_version=data.get("event_version", EVENT_VERSION),
        )

    @classmethod
    def from_json(cls, raw: bytes | str) -> BatchChunkRequested:
        return cls.from_dict(json.loads(raw))


def batch_chunk_partition_key(event: BatchChunkRequested) -> str:
    """Kafka message key for a batch chunk event.

    Chunk-specific keys distribute one batch job across Kafka partitions so multiple
    batch workers can process the same job concurrently. Job-status events
    (``BatchChunkProcessed`` on ``fp.batch.events``) remain keyed by ``job_id`` to
    preserve job-level locality in the metadata projection.

    ``chunk_id`` is already ``"{job_id}:{chunk_index}"`` (globally unique, assigned once
    at job submission), so the key is deterministic and stable across retries, processes
    and restarts — a redelivered chunk lands on the same partition.
    """
    return event.chunk_id


@dataclass(frozen=True)
class BatchChunkProcessed:
    """A batch chunk's processing result (audit/completion). Published to
    ``fp.batch.events`` after the chunk is computed, before the source offset commits.

    Carries counts + bounded error strings only — no feature values, raw payloads,
    ``inline_sources``, or object keys.
    """

    batch_job_id: str
    chunk_id: str
    chunk_index: int
    chunk_count: int
    correlation_id: str
    processed_at: datetime
    status: str
    item_count: int
    ok_items: int
    failed_items: int
    first_errors: list[str] = field(default_factory=list)
    manifest_id: str | None = None  # set for dataset-scoped jobs
    # Bounded counts of the guarded Mode-2 online refresh; None if not run.
    # Counts only — never feature values or payloads.
    online_refresh_counts: dict[str, int] | None = None
    event_type: str = EVENT_TYPE_BATCH_CHUNK_PROCESSED
    event_version: int = EVENT_VERSION

    def to_dict(self) -> dict:
        return {
            "event_type": self.event_type,
            "event_version": self.event_version,
            "batch_job_id": self.batch_job_id,
            "chunk_id": self.chunk_id,
            "chunk_index": self.chunk_index,
            "chunk_count": self.chunk_count,
            "correlation_id": self.correlation_id,
            "processed_at": _iso(self.processed_at),
            "status": self.status,
            "item_count": self.item_count,
            "ok_items": self.ok_items,
            "failed_items": self.failed_items,
            "first_errors": list(self.first_errors),
            "manifest_id": self.manifest_id,
            "online_refresh_counts": (
                dict(self.online_refresh_counts)
                if self.online_refresh_counts is not None
                else None
            ),
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> BatchChunkProcessed:
        refresh = data.get("online_refresh_counts")
        return cls(
            batch_job_id=data["batch_job_id"],
            chunk_id=data["chunk_id"],
            chunk_index=data["chunk_index"],
            chunk_count=data["chunk_count"],
            correlation_id=data["correlation_id"],
            processed_at=_parse_dt(data["processed_at"]),
            status=data["status"],
            item_count=data["item_count"],
            ok_items=data["ok_items"],
            failed_items=data["failed_items"],
            first_errors=list(data.get("first_errors", [])),
            manifest_id=data.get("manifest_id"),
            online_refresh_counts=dict(refresh) if refresh is not None else None,
            event_type=data.get("event_type", EVENT_TYPE_BATCH_CHUNK_PROCESSED),
            event_version=data.get("event_version", EVENT_VERSION),
        )

    @classmethod
    def from_json(cls, raw: bytes | str) -> BatchChunkProcessed:
        return cls.from_dict(json.loads(raw))


@dataclass(frozen=True)
class DeadLetterEvent:
    """A structurally-unprocessable source message routed to the DLQ.

    Captures the original message bytes (base64) so nothing is dropped, plus enough
    failure metadata for an operator to inspect/replay. Best-effort ids are extracted
    only when the source bytes happen to parse. Carries no raw report payloads.

    Note: ``source_payload_b64`` may contain feature values (for offline-write events);
    do not log it.
    """

    source_topic: str
    failure_stage: str
    failure_status: str
    error: str | None
    occurred_at: datetime
    source_payload_b64: str
    original_request_id: str | None = None
    original_job_id: str | None = None
    original_correlation_id: str | None = None
    # Attempt-limit DLQ; None for structural-poison DLQ.
    attempt_count: int | None = None
    max_attempts: int | None = None
    # event_type of the wrapped source event, when its bytes parse.
    original_event_type: str | None = None
    event_type: str = EVENT_TYPE_DLQ
    event_version: int = EVENT_VERSION

    def to_dict(self) -> dict:
        return {
            "event_type": self.event_type,
            "event_version": self.event_version,
            "source_topic": self.source_topic,
            "failure_stage": self.failure_stage,
            "failure_status": self.failure_status,
            "error": self.error,
            "occurred_at": _iso(self.occurred_at),
            "source_payload_b64": self.source_payload_b64,
            "original_request_id": self.original_request_id,
            "original_job_id": self.original_job_id,
            "original_correlation_id": self.original_correlation_id,
            "attempt_count": self.attempt_count,
            "max_attempts": self.max_attempts,
            "original_event_type": self.original_event_type,
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> DeadLetterEvent:
        return cls(
            source_topic=data["source_topic"],
            failure_stage=data["failure_stage"],
            failure_status=data["failure_status"],
            error=data.get("error"),
            occurred_at=_parse_dt(data["occurred_at"]),
            source_payload_b64=data["source_payload_b64"],
            original_request_id=data.get("original_request_id"),
            original_job_id=data.get("original_job_id"),
            original_correlation_id=data.get("original_correlation_id"),
            attempt_count=data.get("attempt_count"),
            max_attempts=data.get("max_attempts"),
            # Optional: absent in legacy DLQ events (tolerant parse).
            original_event_type=data.get("original_event_type"),
            event_type=data.get("event_type", EVENT_TYPE_DLQ),
            event_version=data.get("event_version", EVENT_VERSION),
        )

    @classmethod
    def from_json(cls, raw: bytes | str) -> DeadLetterEvent:
        return cls.from_dict(json.loads(raw))


def build_idempotency_key(
    entity: EntityRef,
    requested_feature_groups: list[str],
    requested_features: list[str],
    reports: list[ReportDescriptor],
) -> str:
    """Deterministic idempotency key for the logical compute request.

    Covers the entity, the requested outputs, and the exact report contents, so an
    identical resubmission yields the same key.
    """
    digest_src = json.dumps(
        {
            "entity": entity.to_dict(),
            "groups": sorted(requested_feature_groups),
            "features": sorted(requested_features),
            "reports": sorted(r.content_hash for r in reports),
        },
        sort_keys=True,
    )
    short = hashlib.sha256(digest_src.encode("utf-8")).hexdigest()[:16]
    groups = "|".join(sorted(requested_feature_groups)) or "-"
    return f"{entity.entity_type}:{entity.encoded()}:{groups}:{short}"
