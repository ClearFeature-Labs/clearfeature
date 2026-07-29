"""Docker-free tests for deployment-owned topic provisioning.

The planner is pure (no confluent-kafka import), so create/expand/no-reduce and
idempotency are provable without a broker; the live path is exercised by the compose
topic-init service (renamed from kafka-init in) in the smoke/demo flows.
"""

from __future__ import annotations

import pytest

from fintech_feature_platform.api.kafka_init_runner import (
    plan_topic_actions,
    validate_topic_config,
)
from fintech_feature_platform.api.settings import load_settings
from fintech_feature_platform.fs_core.events import topics


def test_canonical_topic_list_matches_module_constants():
    # ALL_TOPICS is the single provisioning source; it must exactly equal the set of
    # topic-name constants so the two can never drift.
    constants = {
        value
        for name, value in vars(topics).items()
        if name.isupper() and isinstance(value, str)
    }
    assert len(topics.ALL_TOPICS) >= 8
    assert set(topics.ALL_TOPICS) == constants
    assert len(set(topics.ALL_TOPICS)) == len(topics.ALL_TOPICS), "duplicate topic"
    assert all(t.startswith("fp.") for t in topics.ALL_TOPICS)


def test_settings_defaults_and_env(monkeypatch):
    settings = load_settings()
    assert settings.kafka_topic_partitions == 4  # local default: up to 4 consumers
    assert settings.kafka_replication_factor == 1  # single-broker compose stack

    monkeypatch.setenv("FSP_KAFKA_TOPIC_PARTITIONS", "8")
    monkeypatch.setenv("FSP_KAFKA_REPLICATION_FACTOR", "3")
    settings = load_settings()
    assert settings.kafka_topic_partitions == 8
    assert settings.kafka_replication_factor == 3

    monkeypatch.setenv("FSP_KAFKA_TOPIC_PARTITIONS", "four")
    with pytest.raises(ValueError):
        load_settings()  # non-integer capacity config must fail loudly


@pytest.mark.parametrize(("partitions", "rf"), [(0, 1), (-2, 1), (4, 0), (4, -1)])
def test_invalid_topic_config_rejected(partitions, rf):
    with pytest.raises(ValueError):
        validate_topic_config(partitions, rf)
    validate_topic_config(1, 1)  # minimal valid config passes


def test_plan_creates_missing_topics_with_desired_partitions():
    plan = plan_topic_actions({}, partitions=4)
    assert [a.topic for a in plan] == list(topics.ALL_TOPICS)
    assert all(a.action == "create" and a.target_partitions == 4 for a in plan)


def test_plan_expands_smaller_and_keeps_equal_topics():
    existing = {t: 1 for t in topics.ALL_TOPICS}
    existing[topics.DLQ] = 4
    plan = {a.topic: a for a in plan_topic_actions(existing, partitions=4)}
    assert plan[topics.FEATURE_COMPUTE_BATCH].action == "expand"
    assert plan[topics.FEATURE_COMPUTE_BATCH].current_partitions == 1
    assert plan[topics.FEATURE_COMPUTE_BATCH].target_partitions == 4
    assert plan[topics.DLQ].action == "unchanged"


def test_plan_is_idempotent_and_never_reduces():
    desired = {t: 4 for t in topics.ALL_TOPICS}
    assert all(a.action == "unchanged" for a in plan_topic_actions(desired, partitions=4))

    # Existing topic already has MORE partitions than desired: reduction is impossible,
    # so it is left unchanged (target stays at the current count) and reported.
    wide = {t: 8 for t in topics.ALL_TOPICS}
    plan = plan_topic_actions(wide, partitions=4)
    assert all(a.action == "unchanged" and a.target_partitions == 8 for a in plan)
    assert all("cannot be reduced" in a.note for a in plan)
