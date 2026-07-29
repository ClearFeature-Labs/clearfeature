"""F2 DAG compute : topological order, ephemerality, staleness status, D3/D9.

Uses ``ComputeCore`` directly with a two-source registry (sa -> a, sb -> b, c = f(a, b))
so we control each input's ``data_ts`` (via source stamps) and the eval time (``calc_ts``).
"""

from datetime import UTC, datetime, timedelta

from fintech_feature_platform.fs_core.compute.context import (
    STATUS_DEGRADED,
    STATUS_OK,
    STATUS_SKIPPED,
    RequestContext,
)
from fintech_feature_platform.fs_core.compute.engine import ComputeCore
from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.models import EntityKey, SourceStamp
from fintech_feature_platform.fs_core.registry.loader import build_registry

_A_TS = datetime(2026, 1, 10, tzinfo=UTC)
_B_TS = datetime(2026, 1, 5, tzinfo=UTC)


def _dep(feature, *, required=True, max_age=None):
    d = {"feature": feature, "version": 1, "required": required}
    if max_age is not None:
        d["max_input_age_seconds"] = max_age
    return d


def _registry(c_deps):
    data = {
        "registry_version": "test-v1",
        "entities": {"e": {"key_fields": ["id"]}},
        "sources": {
            "sa": {"type": "raw_report", "report_type": "ra", "ts_field": "report_ts"},
            "sb": {"type": "raw_report", "report_type": "rb", "ts_field": "report_ts"},
        },
        "feature_views": {
            "v": {
                "entity": "e", "key_fields": ["id"], "view_version": 1,
                "owner": "t", "status": "active",
                "features": {
                    "a": {"kind": "udf", "feature_version": 1, "udf": "udf.a",
                          "dtype": "int", "status": "active", "inputs": ["sa"]},
                    "b": {"kind": "udf", "feature_version": 1, "udf": "udf.b",
                          "dtype": "int", "status": "active", "inputs": ["sb"]},
                    "c": {"kind": "udf", "feature_version": 1, "udf": "udf.c",
                          "dtype": "int", "status": "active", "deps": c_deps},
                },
            }
        },
    }
    return build_registry(data)


def _core(c_deps, order=None):
    def _rec(name, fn):
        def wrapped(sources, deps):
            if order is not None:
                order.append(name)
            return fn(sources, deps)
        return wrapped

    udfs = UdfRegistry({
        "udf.a": _rec("a", lambda s, d: s["sa"]["v"]),
        "udf.b": _rec("b", lambda s, d: s["sb"]["v"]),
        # c reads both deps; tolerates a missing optional dep.
        "udf.c": _rec("c", lambda s, d: d.get("a", 0) + d.get("b", 0)),
    })
    return ComputeCore(_registry(c_deps), udfs)


def _loader(a_v=10, b_v=5):
    payloads = {"sa": {"v": a_v}, "sb": {"v": b_v}}
    return lambda name: payloads[name]


def _stamps(a_ts=_A_TS, b_ts=_B_TS):
    return {
        "sa": SourceStamp(report_ts=a_ts, content_hash=f"sha256:a@{a_ts.isoformat()}"),
        "sb": SourceStamp(report_ts=b_ts, content_hash=f"sha256:b@{b_ts.isoformat()}"),
    }


def _key():
    return EntityKey.from_mapping({"id": "1"}, key_order=["id"])


def _run(core, requested, *, calc_ts, stamps=None, loader=None):
    ctx = RequestContext(loader or _loader())
    results = core.compute(
        view="v", view_version=1, entity_key=_key(),
        requested_features=requested, context=ctx,
        source_stamps=stamps or _stamps(), calc_ts=calc_ts,
    )
    return results, ctx


# --- topological order / ephemerality ----------------------------------------

def test_dependencies_computed_before_dependent():
    order: list[str] = []
    core = _core([_dep("a"), _dep("b")], order=order)
    _run(core, ["c"], calc_ts=_A_TS)
    assert order.index("a") < order.index("c")
    assert order.index("b") < order.index("c")


def test_requested_derived_does_not_materialize_dependencies():
    core = _core([_dep("a"), _dep("b")])
    results, _ = _run(core, ["c"], calc_ts=_A_TS)
    assert set(results) == {"c"}  # a, b computed but not returned/materialized


def test_requesting_dependency_and_derived_materializes_both():
    core = _core([_dep("a"), _dep("b")])
    results, _ = _run(core, ["a", "c"], calc_ts=_A_TS)
    assert set(results) == {"a", "c"}


# --- D3 / D9 derived metadata ------------------------------------------------

def test_derived_data_ts_and_max_input_data_ts():
    core = _core([_dep("a"), _dep("b")])
    results, _ = _run(core, ["c"], calc_ts=_A_TS)
    c = results["c"]
    assert c.data_ts == _B_TS          # min(inputs)
    assert c.max_input_data_ts == _A_TS  # max(inputs)


def test_fingerprint_changes_when_non_min_input_changes():
    # Change b's VALUE (keeping every data_ts the same). min data_ts stays _B_TS, but the
    # fingerprint must change (D9 written_recompute can distinguish it).
    core = _core([_dep("a"), _dep("b")])
    r1, _ = _run(core, ["c"], calc_ts=_A_TS, loader=_loader(b_v=5))
    r2, _ = _run(core, ["c"], calc_ts=_A_TS, loader=_loader(b_v=999))
    assert r1["c"].data_ts == r2["c"].data_ts == _B_TS
    assert r1["c"].input_fingerprint != r2["c"].input_fingerprint


# --- staleness status --------------------------------------------------------

def test_fresh_inputs_status_ok():
    core = _core([_dep("a", max_age=3600), _dep("b", max_age=3600)])
    fresh = datetime(2026, 3, 1, 12, tzinfo=UTC)
    stamps = _stamps(a_ts=fresh - timedelta(minutes=10), b_ts=fresh - timedelta(minutes=5))
    results, ctx = _run(core, ["c"], calc_ts=fresh, stamps=stamps)
    assert ctx.statuses()["c"] == STATUS_OK
    assert "c" in results


def test_required_stale_input_skips_dependent():
    core = _core([_dep("a", required=True, max_age=3600)])
    # a.data_ts (_A_TS, Jan 10) is far older than calc_ts (Mar 1) -> required stale.
    results, ctx = _run(core, ["c"], calc_ts=datetime(2026, 3, 1, tzinfo=UTC))
    assert ctx.statuses()["c"] == STATUS_SKIPPED
    assert "c" not in results  # no FeatureResult for a SKIPPED node
    assert ctx.statuses()["a"] == STATUS_OK  # the input itself computed fine


def test_optional_stale_input_degrades_dependent():
    core = _core([_dep("a", required=True, max_age=None),
                  _dep("b", required=False, max_age=3600)])
    # b is optional + stale -> c is DEGRADED but still computed (stale value passed).
    results, ctx = _run(core, ["c"], calc_ts=datetime(2026, 3, 1, tzinfo=UTC))
    assert ctx.statuses()["c"] == STATUS_DEGRADED
    assert "c" in results
    # a fresh + b stale-but-present: value still uses both.
    assert results["c"].value == 10 + 5


def test_degradation_propagates_to_further_dependents():
    # c is DEGRADED (optional-stale b); d = f(c) must also be DEGRADED (fresh edge, but a
    # degraded input propagates up).
    data = {
        "registry_version": "test-v1",
        "entities": {"e": {"key_fields": ["id"]}},
        "sources": {
            "sa": {"type": "raw_report", "report_type": "ra", "ts_field": "report_ts"},
            "sb": {"type": "raw_report", "report_type": "rb", "ts_field": "report_ts"},
        },
        "feature_views": {"v": {
            "entity": "e", "key_fields": ["id"], "view_version": 1,
            "owner": "t", "status": "active",
            "features": {
                "a": {"kind": "udf", "feature_version": 1, "udf": "udf.a",
                      "dtype": "int", "status": "active", "inputs": ["sa"]},
                "b": {"kind": "udf", "feature_version": 1, "udf": "udf.b",
                      "dtype": "int", "status": "active", "inputs": ["sb"]},
                "c": {"kind": "udf", "feature_version": 1, "udf": "udf.c",
                      "dtype": "int", "status": "active",
                      "deps": [_dep("a", max_age=None), _dep("b", required=False,
                                                             max_age=3600)]},
                "d": {"kind": "udf", "feature_version": 1, "udf": "udf.d",
                      "dtype": "int", "status": "active", "deps": [_dep("c")]},
            },
        }},
    }
    udfs = UdfRegistry({
        "udf.a": lambda s, dp: s["sa"]["v"], "udf.b": lambda s, dp: s["sb"]["v"],
        "udf.c": lambda s, dp: dp.get("a", 0) + dp.get("b", 0),
        "udf.d": lambda s, dp: dp["c"],
    })
    core = ComputeCore(build_registry(data), udfs)
    ctx = RequestContext(_loader())
    core.compute(
        view="v", view_version=1, entity_key=_key(), requested_features=["d"],
        context=ctx, source_stamps=_stamps(), calc_ts=datetime(2026, 3, 1, tzinfo=UTC),
    )
    assert ctx.statuses()["c"] == STATUS_DEGRADED
    assert ctx.statuses()["d"] == STATUS_DEGRADED  # propagated upward
