"""Reactive propagation consumer runner: DLQ, commit discipline, debounce."""

from datetime import UTC, datetime
from types import SimpleNamespace

from fintech_feature_platform.api import propagation_worker_runner as runner
from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.events.consumer import (
    InMemoryEventConsumer,
    InMemoryMessage,
)
from fintech_feature_platform.fs_core.events.models import EntityRef, FeatureUpdated
from fintech_feature_platform.fs_core.events.publisher import InMemoryEventPublisher
from fintech_feature_platform.fs_core.events.topics import DLQ
from fintech_feature_platform.fs_core.feature_store import FeatureStore
from fintech_feature_platform.fs_core.hashing import value_hash
from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult
from fintech_feature_platform.fs_core.propagation import DebounceStore
from fintech_feature_platform.fs_core.raw.meta_repository import InMemoryMetaRepository
from fintech_feature_platform.fs_core.raw.payload_store import InMemoryPayloadStore
from fintech_feature_platform.fs_core.raw.report_resolver import ReportResolver
from fintech_feature_platform.fs_core.registry.loader import build_registry
from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore
from fintech_feature_platform.fs_core.stores.online import InMemoryOnlineStore

_SEED_TS = datetime(2026, 1, 10, tzinfo=UTC)


def _registry():
    data = {
        "registry_version": "test-v1",
        "entities": {"e": {"key_fields": ["id"]}},
        "sources": {
            "src": {"type": "raw_report", "report_type": "r", "ts_field": "report_ts"},
        },
        "feature_views": {
            "v": {"entity": "e", "key_fields": ["id"], "view_version": 1,
                  "owner": "o", "status": "active", "features": {
                      "a": {"kind": "udf", "feature_version": 1, "udf": "udf.a",
                            "dtype": "int", "status": "active", "inputs": ["src"]},
                      "b": {"kind": "udf", "feature_version": 1, "udf": "udf.b",
                            "dtype": "int", "status": "active", "inputs": ["src"]},
                      "c": {"kind": "udf", "feature_version": 1, "udf": "udf.c",
                            "dtype": "int", "status": "active", "deps": [
                                {"feature": "a", "version": 1, "propagation": "reactive"},
                                {"feature": "b", "version": 1, "propagation": "reactive"}]},
                      # d makes c a reactive input, so the wave attempts a downstream publish.
                      "d": {"kind": "udf", "feature_version": 1, "udf": "udf.d",
                            "dtype": "int", "status": "active", "deps": [
                                {"feature": "c", "version": 1, "propagation": "reactive"}]},
                  }}
        },
    }
    return build_registry(data)


def _backend(publisher=None):
    registry = _registry()
    udfs = UdfRegistry({
        "udf.a": lambda s, d: s["src"]["v"],
        "udf.b": lambda s, d: s["src"]["v"],
        "udf.c": lambda s, d: d["a"] + d["b"],
        "udf.d": lambda s, d: d["c"] * 10,
    })
    resolver = ReportResolver(InMemoryPayloadStore(), InMemoryMetaRepository())
    online = InMemoryOnlineStore()
    offline = InMemoryOfflineStore()
    store = FeatureStore(registry, udfs, resolver, offline, online)
    return SimpleNamespace(
        registry=registry, store=store, offline=offline, online=online,
        events=publisher or InMemoryEventPublisher(),
    )


def _key(id_="1"):
    return EntityKey.from_mapping({"id": id_}, key_order=["id"])


def _seed(offline, name, value, id_="1"):
    offline.append("v", 1, FeatureResult(
        ref=FeatureRef(name, 1), entity_key=_key(id_), value=value,
        data_ts=_SEED_TS, calc_ts=_SEED_TS, max_input_data_ts=_SEED_TS,
        input_fingerprint=f"fp_{name}_{id_}", value_hash=value_hash(value),
    ))


def _msg(feature_name, *, id_="1", update_id="u1"):
    event = FeatureUpdated(
        update_id=update_id, entity=EntityRef("e", {"id": id_}),
        view="v", view_version=1, feature_name=feature_name, feature_version=1,
        data_ts=_SEED_TS, calc_ts=_SEED_TS, source="offline_writer", occurred_at=_SEED_TS,
    )
    return InMemoryMessage(event.to_json())


class _FailingPublisher:
    """Records nothing; raises on every publish (simulates broker unavailable)."""

    def publish(self, *args, **kwargs):
        raise RuntimeError("broker unavailable")


# --- DLQ / poison ------------------------------------------------------------

def test_invalid_event_dead_lettered_and_committed():
    backend = _backend()
    consumer = InMemoryEventConsumer([InMemoryMessage(b"not-json")])
    debounce = DebounceStore()
    result = runner.process_next(consumer, backend, debounce, runner.PendingBatch())
    assert result.status == "dead_lettered"
    assert result.committed is True
    assert len(consumer.committed) == 1
    dlq = [r for r in backend.events.published if r.topic == DLQ]
    assert len(dlq) == 1


# --- happy path: observe -> flush -> commit ----------------------------------

def test_debounced_updates_produce_one_wave_and_commit_all():
    backend = _backend()
    _seed(backend.offline, "a", 3)
    _seed(backend.offline, "b", 4)
    consumer = InMemoryEventConsumer([
        _msg("a", update_id="u1"), _msg("a", update_id="u2"),
    ])
    results, flush = runner.run(backend=backend, consumer=consumer, max_messages=2)

    assert [r.status for r in results] == ["observed", "observed"]
    assert flush.status == "ok"
    assert flush.wave.computed == 1  # two updates for same key -> one recompute
    assert flush.committed == 2      # both source offsets committed after durable wave
    assert len(consumer.committed) == 2
    assert [rec.result.value for rec in
            backend.offline.get(_key(), feature_name="c", feature_version=1)] == [7]


# --- transient failure: no commit -> replay ----------------------------------

def test_downstream_publish_failure_leaves_offsets_uncommitted():
    # Offline append succeeds inside the wave, then the downstream publish fails: the flush
    # must NOT commit, so the source offsets replay (offline dedup keeps recompute safe).
    backend = _backend(publisher=_FailingPublisher())
    _seed(backend.offline, "a", 3)
    _seed(backend.offline, "b", 4)
    # add a reactive dependent for c so the wave attempts a downstream publish.
    consumer = InMemoryEventConsumer([_msg("a")])
    debounce = DebounceStore()
    pending = runner.PendingBatch()
    runner.process_next(consumer, backend, debounce, pending)
    flush = runner.flush(consumer, backend, debounce, pending)

    assert flush.status == "wave_failed"
    assert flush.committed == 0
    assert consumer.committed == []


def test_offline_store_failure_leaves_offsets_uncommitted():
    backend = _backend()
    _seed(backend.offline, "a", 3)
    _seed(backend.offline, "b", 4)
    consumer = InMemoryEventConsumer([_msg("a")])
    debounce = DebounceStore()
    pending = runner.PendingBatch()
    runner.process_next(consumer, backend, debounce, pending)

    # Make the durable offline append fail (transient store outage) during the wave.
    def _boom(*args, **kwargs):
        raise RuntimeError("offline store unavailable")

    backend.offline.append_many = _boom
    flush = runner.flush(consumer, backend, debounce, pending)

    assert flush.status == "wave_failed"
    assert flush.committed == 0
    assert consumer.committed == []
