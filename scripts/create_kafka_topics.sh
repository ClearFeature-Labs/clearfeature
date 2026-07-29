#!/usr/bin/env bash
#
# Idempotent Kafka topic provisioning.
#
# Runs the canonical provisioner (api/kafka_init_runner.py, topic list imported from
# fs_core/events/topics.py — no second hand-written list) as a one-shot container on
# the compose network, so it uses the internal bootstrap (redpanda:19092) and the image
# that already ships the kafka extra. Compose runs the same service automatically as
# `topic-init` before the API/workers start; this wrapper is for operators re-running
# it by hand (e.g. after raising FSP_KAFKA_TOPIC_PARTITIONS).
#
# Grow-only: creates missing topics, expands smaller ones, never reduces partitions.
#
# Usage:
#   bash scripts/create_kafka_topics.sh                            # desired = env/default 4
#   FSP_KAFKA_TOPIC_PARTITIONS=8 bash scripts/create_kafka_topics.sh   # expand to 8

set -euo pipefail

cd "$(dirname "$0")/.."

if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker daemon not available — topic provisioning targets the compose stack." >&2
    exit 1
fi

docker compose run --rm --no-deps topic-init
