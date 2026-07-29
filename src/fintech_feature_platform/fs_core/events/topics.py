"""Kafka topic name constants for the Kafka-first MVP.

These are the canonical, fully-qualified topic names (already namespaced with the
``fp`` prefix). Only ``FEATURE_COMPUTE_ONLINE`` is published in; the others
are declared here so producers/consumers share one source of truth.
"""

from __future__ import annotations

FEATURE_COMPUTE_ONLINE = "fp.feature-compute.online"
FEATURE_COMPUTE_BATCH = "fp.feature-compute.batch"
FEATURE_WRITE_OFFLINE = "fp.feature-write.offline"
MODEL_SCORE_WRITE = "fp.model-score.write"
FEATURE_COMPUTE_COMPLETED = "fp.feature-compute.completed"
# Values-free feature-update changelog. Published after a durable
# offline write/import touches a feature; consumed by the reactive propagation worker.
FEATURE_UPDATES = "fp.feature-updates"
# Batch job lifecycle events (chunk processed/completion). Published by the batch worker,
# consumed by the Metadata Writer only — the batch worker must NOT consume this topic.
BATCH_JOB_EVENTS = "fp.batch.events"
DLQ = "fp.dlq"

# Canonical platform topic list. Deployment-owned
# provisioning (api/kafka_init_runner.py) creates exactly these; a test pins this tuple
# to the constants above so the two can never drift.
ALL_TOPICS: tuple[str, ...] = (
    FEATURE_COMPUTE_ONLINE,
    FEATURE_COMPUTE_BATCH,
    FEATURE_WRITE_OFFLINE,
    MODEL_SCORE_WRITE,
    FEATURE_COMPUTE_COMPLETED,
    FEATURE_UPDATES,
    BATCH_JOB_EVENTS,
    DLQ,
)
