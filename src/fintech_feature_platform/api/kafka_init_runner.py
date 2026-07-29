"""One-shot Kafka topic provisioning.

Deployment-owned: creates every canonical ``fp.*`` topic (``fs_core/events/topics.py``
``ALL_TOPICS``) with the configured partition count and replication factor, BEFORE the
API/workers start consuming (compose ``topic-init`` service — renamed from ``kafka-init``
in; this module keeps its name, it uses the Kafka admin protocol — gated with
``service_completed_successfully``). Idempotent and grow-only:

- missing topic            -> create with ``FSP_KAFKA_TOPIC_PARTITIONS`` partitions
- fewer partitions than desired -> expand (``create_partitions``)
- equal                    -> no-op
- more than desired        -> left unchanged and reported (partitions cannot shrink)

Growing partitions re-maps keys, so for one window two events for the same key may sit
on different partitions and be consumed out of order. That is a designed-for condition:
D9 CAS absorbs online disorder, offline dedup absorbs duplicates, and the metadata
projection is idempotent. Existing topic data is never rewritten.

Replication factor must stay 1 on the single-broker local stack; production guidance
(docs/deployment/docker_compose.md): partitions ≈ 2× worker ceiling, RF=3 with ≥3
brokers. This module never runs inside API/worker processes — topic capacity is operator
configuration, like pool sizes, not application logic.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from fintech_feature_platform.api.settings import Settings, load_settings
from fintech_feature_platform.fs_core.events.topics import ALL_TOPICS


@dataclass(frozen=True)
class TopicAction:
    """Planned provisioning step for one topic (pure; no broker connection)."""

    topic: str
    action: str  # "create" | "expand" | "unchanged"
    current_partitions: int | None  # None = topic does not exist yet
    target_partitions: int
    note: str = ""


def validate_topic_config(partitions: int, replication_factor: int) -> None:
    """Fail clearly on impossible capacity configuration (zero/negative)."""
    if partitions < 1:
        raise ValueError(f"FSP_KAFKA_TOPIC_PARTITIONS must be >= 1, got {partitions}")
    if replication_factor < 1:
        raise ValueError(
            f"FSP_KAFKA_REPLICATION_FACTOR must be >= 1, got {replication_factor}"
        )


def plan_topic_actions(
    existing: Mapping[str, int],
    topics: Sequence[str] = ALL_TOPICS,
    *,
    partitions: int,
) -> list[TopicAction]:
    """Grow-only plan: create missing, expand smaller, never reduce. Idempotent."""
    plan: list[TopicAction] = []
    for topic in topics:
        current = existing.get(topic)
        if current is None:
            plan.append(TopicAction(topic, "create", None, partitions))
        elif current < partitions:
            plan.append(TopicAction(topic, "expand", current, partitions))
        elif current == partitions:
            plan.append(TopicAction(topic, "unchanged", current, partitions))
        else:
            plan.append(
                TopicAction(
                    topic,
                    "unchanged",
                    current,
                    current,
                    note=f"has {current} > desired {partitions}; partitions cannot be reduced",
                )
            )
    return plan


def provision_topics(settings: Settings) -> list[TopicAction]:
    """Apply the plan against the broker. Imports confluent-kafka lazily (kafka extra)."""
    from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic

    validate_topic_config(settings.kafka_topic_partitions, settings.kafka_replication_factor)
    admin = AdminClient(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": f"{settings.kafka_client_id}-topic-init",
        }
    )
    metadata = admin.list_topics(timeout=30)
    existing = {
        name: len(meta.partitions)
        for name, meta in metadata.topics.items()
        if meta.error is None
    }
    plan = plan_topic_actions(existing, partitions=settings.kafka_topic_partitions)

    creates = [
        NewTopic(
            action.topic,
            num_partitions=action.target_partitions,
            replication_factor=settings.kafka_replication_factor,
        )
        for action in plan
        if action.action == "create"
    ]
    if creates:
        for topic, future in admin.create_topics(creates).items():
            try:
                future.result(timeout=30)
            except Exception as exc:  # noqa: BLE001 - re-raised unless benign race
                # Another init racing us created it first: still the desired end state.
                if "already exists" not in str(exc).lower():
                    raise RuntimeError(f"creating topic {topic!r} failed: {exc}") from exc

    expands = [
        NewPartitions(action.topic, action.target_partitions)
        for action in plan
        if action.action == "expand"
    ]
    if expands:
        for topic, future in admin.create_partitions(expands).items():
            try:
                future.result(timeout=30)
            except Exception as exc:  # noqa: BLE001 - clear operator-facing failure
                raise RuntimeError(f"expanding topic {topic!r} failed: {exc}") from exc
    return plan


def main() -> int:
    settings = load_settings()
    plan = provision_topics(settings)
    print(
        f"topic-init: {settings.kafka_bootstrap_servers} "
        f"partitions={settings.kafka_topic_partitions} "
        f"rf={settings.kafka_replication_factor}"
    )
    for action in plan:
        detail = (
            "created"
            if action.action == "create"
            else f"{action.action} ({action.current_partitions}->{action.target_partitions})"
        )
        suffix = f" [{action.note}]" if action.note else ""
        print(f"  {action.topic}: {detail}{suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
