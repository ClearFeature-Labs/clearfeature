"""Central metric catalog — the only source of metric names/labels.

Every metric the platform records is declared here: name, type, help, allowed label keys,
closed label-value domains where the set is code-defined, and histogram buckets. Business
code cannot mint arbitrary metric names or labels — the Prometheus recorder rejects
anything outside this catalog fail-fast (see ``metrics_cardinality_policy.md``).

Initial contents: the process-identity metric plus every metric name business code records
today (so existing instrumentation exposes compatibly). Later revisions add the worker/API/
artifact families and tightens the remaining open label domains to closed enums.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

# The ONE canonical bounded set of service roles (compose service names + the API).
# Used for metrics; structured logging must reuse it — never a slightly different set.
WORKER_ROLES: tuple[str, ...] = (
    "api",
    "online-worker",
    "batch-worker",
    "offline-writer",
    "metadata-writer",
    "propagation-worker",
    "model-score-writer",
)

COUNTER = "counter"
GAUGE = "gauge"
HISTOGRAM = "histogram"

# Marker for a label whose closed domain is bound at runtime from the loaded Registry
# (the ONE sanctioned case: feature_id). Until bound, every value is rejected.
DYNAMIC = "__dynamic__"

# Shared bucket ladders (milliseconds / seconds); calibrated from measured latency ranges.
_MS_BUCKETS = (1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0)
_SECONDS_BUCKETS = (0.1, 0.5, 1.0, 5.0, 15.0, 30.0, 60.0, 300.0, 900.0, 3600.0)

# operation-scale ladders. Whole-operation online requests sit in the ms range;
# batch chunks in the 0.1–120 s range; individual feature UDFs in the
# 0.1 ms – 1 s range (cheap F1s are ~µs; heavy model prep can reach 100s of ms).
_ONLINE_OP_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
_BATCH_CHUNK_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0)
_STAGE_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
# 10 buckets for the per-feature family — the cardinality driver (see the series
# estimates in the cardinality analysis: ~28 series/feature across both modes).
_FEATURE_BUCKETS = (0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.25, 1.0)
_WORKER_BUCKETS = (0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 15.0, 60.0)
_API_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 10.0)
_STORE_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 2.5)

EXECUTION_MODES = frozenset({"online", "batch"})

# Pipeline stages (closed; from the execution map). NOT strictly disjoint:
# input_fetch happens lazily inside compute (documented on the metric help).
PIPELINE_STAGES = frozenset({
    "input_fetch", "compute", "online_write", "offline_write", "publish", "result_write",
})

# Worker item results (closed). Derived from the real ProcessResult statuses:
# success <- ok/observed/projected/written; noop <- noop/skipped/deadline_expired;
# retry <- retry_republished; dead_lettered <- dead_lettered; failure <- everything
# else (consume_error, *_failed, unexpected_error, invalid_event, ...). idle/paused/
# rate_limited are NOT items (nothing was consumed-and-resolved in that poll).
WORKER_RESULTS = frozenset({"success", "noop", "retry", "dead_lettered", "failure"})

# D9 write-guard decisions — the real constants from fs_core/write_guard.py.
D9_OUTCOMES = frozenset({"written", "written_recompute", "skipped_stale", "noop"})

# The 10 stable artifact-verification categories (fs_core/runtime/artifact_verifier.py).
ARTIFACT_CATEGORIES = frozenset({
    "feature_artifact_required", "feature_artifact_manifest_missing",
    "feature_artifact_wheel_missing", "feature_artifact_sha_mismatch",
    "feature_artifact_distribution_mismatch", "feature_artifact_installation_mismatch",
    "feature_artifact_provider_mismatch", "feature_artifact_core_incompatible",
    "feature_artifact_metadata_invalid", "feature_artifact_registry_mismatch",
})


@dataclass(frozen=True)
class MetricSpec:
    """One declared metric: identity, type, labels and (closed) label domains."""

    name: str
    kind: str  # counter | gauge | histogram
    help: str
    label_names: tuple[str, ...] = ()
    # label key -> closed value set, or None when the domain is open-but-validated
    # (value-level validation still applies; closes the remaining Nones).
    label_domains: Mapping[str, frozenset[str] | None] = field(default_factory=dict)
    buckets: tuple[float, ...] | None = None


def _spec(name, kind, help_, labels=(), domains=None, buckets=None) -> MetricSpec:
    return MetricSpec(
        name=name, kind=kind, help=help_, label_names=tuple(labels),
        label_domains=dict(domains or {key: None for key in labels}), buckets=buckets,
    )


CATALOG: tuple[MetricSpec, ...] = (
    # --- foundational process identity  ------------------------------
    _spec(
        "fsp_process_info", GAUGE,
        "Static process identity; value is always 1. worker_role is the canonical bounded set.",
        labels=("worker_role",),
        domains={"worker_role": frozenset(WORKER_ROLES)},
    ),
    # --- existing online-worker metrics --------------------------------------
    _spec("online_requests_total", COUNTER,
          "Online compute requests processed, by outcome.", labels=("outcome",)),
    _spec("online_request_errors_total", COUNTER,
          "Online compute requests that ended in an error outcome.", labels=("outcome",)),
    _spec("online_request_latency_ms", HISTOGRAM,
          "Online request processing latency in milliseconds.", buckets=_MS_BUCKETS),
    # --- existing offline-writer metrics -------------------------------------
    _spec("offline_append_rows_total", COUNTER, "Offline history rows appended."),
    _spec("offline_append_errors_total", COUNTER, "Offline append failures."),
    # --- existing batch metrics ----------------------------------------------
    _spec("batch_chunks_total", COUNTER, "Batch chunks processed, by status.", labels=("status",)),
    _spec("batch_items_total", COUNTER, "Batch items processed, by outcome.", labels=("outcome",)),
    _spec("batch_rows_written_total", COUNTER, "Batch rows written offline."),
    _spec("batch_pause_events_total", COUNTER, "Batch consumption pause events."),
    _spec("batch_rate_limited_events_total", COUNTER, "Batch rate-limit backoff events."),
    # --- existing propagation metrics ----------------------------------------
    _spec("feature_updates_total", COUNTER,
          "Feature-update events observed, by source.", labels=("source",)),
    _spec("propagation_debounced_total", COUNTER, "Updates coalesced by the debounce window."),
    _spec("propagation_pending_waves", GAUGE, "Debounced recompute candidates pending flush."),
    _spec("propagation_waves_total", COUNTER, "Recompute waves flushed."),
    _spec("propagation_wave_items_total", COUNTER,
          "Recompute wave items, by outcome.", labels=("outcome",)),
    _spec("propagation_lag_seconds", HISTOGRAM,
          "Staleness of the triggering input at recompute time (seconds).",
          buckets=_SECONDS_BUCKETS),
    # --- existing DLQ metric --------------------------------------------------
    _spec("dlq_events_total", COUNTER,
          "Events routed to the DLQ, by stage and failure status.", labels=("stage", "status")),
    # =========================================================================
    # — pipeline, per-feature, worker, API, D9, artifact, storage
    # =========================================================================
    # LEVEL 1 — whole operation
    _spec("fsp_online_operation_duration_seconds", HISTOGRAM,
          "End-to-end online compute-request handling time in the online worker.",
          buckets=_ONLINE_OP_BUCKETS),
    _spec("fsp_batch_chunk_duration_seconds", HISTOGRAM,
          "End-to-end batch chunk handling time in the batch worker.",
          buckets=_BATCH_CHUNK_BUCKETS),
    # LEVEL 2 — pipeline stages (closed stage domain; see STAGES below)
    _spec("fsp_pipeline_stage_duration_seconds", HISTOGRAM,
          "Time per platform pipeline stage. NOT all additive: stage 'compute' is the "
          "ComputeCore wall-clock interval and INCLUDES the lazy input_fetch loads that "
          "happen inside it (input_fetch is a nested subset, observed once per source). "
          "Exclusive compute may be derived from SUM/COUNT only (sum(compute) - "
          "sum(input_fetch)); histogram QUANTILES must never be subtracted "
          "(p95(compute) - p95(input_fetch) is not a valid quantity).",
          labels=("execution_mode", "stage"),
          domains={"execution_mode": EXECUTION_MODES, "stage": PIPELINE_STAGES},
          buckets=_STAGE_BUCKETS),
    # LEVEL 3 — per registered feature (feature_id = registry-bound DYNAMIC domain)
    _spec("fsp_feature_compute_duration_seconds", HISTOGRAM,
          "EXCLUSIVE per-feature UDF execution time (dependencies and lazy source "
          "fetches are timed as their own nodes/stages). One observation = one entity "
          "evaluation in BOTH modes (the batch worker computes item-by-item).",
          labels=("execution_mode", "feature_id"),
          domains={"execution_mode": EXECUTION_MODES, "feature_id": DYNAMIC},
          buckets=_FEATURE_BUCKETS),
    _spec("fsp_feature_compute_items_total", COUNTER,
          "Entity evaluations per registered feature (matches duration observations).",
          labels=("execution_mode", "feature_id"),
          domains={"execution_mode": EXECUTION_MODES, "feature_id": DYNAMIC}),
    # Generic worker processing (shared ProcessResult seam; result domain closed below)
    _spec("fsp_worker_items_total", COUNTER,
          "Logical work items per worker, by bounded result (idle polls are not items).",
          labels=("worker_role", "result"),
          domains={"worker_role": frozenset(WORKER_ROLES), "result": WORKER_RESULTS}),
    _spec("fsp_worker_processing_duration_seconds", HISTOGRAM,
          "Per-item processing time in each worker's process_next.",
          labels=("worker_role",),
          domains={"worker_role": frozenset(WORKER_ROLES)},
          buckets=_WORKER_BUCKETS),
    _spec("fsp_worker_last_success_unixtime_seconds", GAUGE,
          "Unix time of the last successfully processed item per worker.",
          labels=("worker_role",),
          domains={"worker_role": frozenset(WORKER_ROLES)}),
    # API requests (route_class = brace-free normalized route template; bounded by code)
    _spec("fsp_api_requests_total", COUNTER,
          "API requests, by normalized route template, method and status class.",
          labels=("route_class", "method", "status_class"),
          domains={"route_class": None,
                    "method": frozenset({"GET", "POST", "PUT", "DELETE", "PATCH", "OTHER"}),
                    "status_class": frozenset({"2xx", "3xx", "4xx", "5xx"})}),
    _spec("fsp_api_request_duration_seconds", HISTOGRAM,
          "API request handling time by normalized route template.",
          labels=("route_class",), buckets=_API_BUCKETS),
    # D9 freshness-guard outcomes (closed domain finalized from write_guard semantics)
    _spec("fsp_online_write_outcomes_total", COUNTER,
          "Per-feature online write-guard decisions (D9).",
          labels=("execution_mode", "outcome"),
          domains={"execution_mode": EXECUTION_MODES, "outcome": D9_OUTCOMES}),
    # Artifact verification ( gate; categories from the verifier's stable set)
    _spec("fsp_artifact_verification_total", COUNTER,
          "Artifact-bound bundle verifications, by result.",
          labels=("result",),
          domains={"result": frozenset({"success", "failure"})}),
    _spec("fsp_artifact_verification_failure_total", COUNTER,
          "Artifact verification failures, by stable category.",
          labels=("category",), domains={"category": ARTIFACT_CATEGORIES}),
    # Storage boundary operations (platform-owned external calls only)
    _spec("fsp_store_operation_duration_seconds", HISTOGRAM,
          "Platform-boundary storage operation time.",
          labels=("store", "operation", "result"),
          domains={"store": frozenset({"postgres", "valkey", "minio"}),
                    "operation": frozenset({"read", "write", "append", "delete"}),
                    "result": frozenset({"ok", "error"})},
          buckets=_STORE_BUCKETS),
)


def catalog_by_name() -> dict[str, MetricSpec]:
    return {spec.name: spec for spec in CATALOG}


def fully_qualified_feature_ids(registry) -> set[str]:
    """The bundle-format identities (``view:vN:feature:vN``) for every registered
    feature — the ONLY values admissible for the ``feature_id`` metric label."""
    return {
        f"{view.name}:v{view.view_version}:{feature.name}:v{feature.feature_version}"
        for view in registry.feature_views
        for feature in view.features
    }
