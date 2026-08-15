"""Test fixture: a 3-part-key multi-source view (mirrors the multi-source snapshot scenario).

Entity key (user_id, application_id, report_id) — several report_id values for one
application are DISTINCT logical snapshots. Non-alphabetical key order on purpose.
"""

from types import SimpleNamespace

from fintech_feature_platform.api.backend import AppBackend, build_memory_backend
from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.feature_store import FeatureStore
from fintech_feature_platform.fs_core.raw.report_resolver import ReportResolver
from fintech_feature_platform.fs_core.registry.loader import build_registry

_REGISTRY_DATA = {
    "registry_version": "snapshot-test-v1",
    "entities": {"application_snapshot": {
        "key_fields": ["user_id", "application_id", "report_id"]}},
    "sources": {
        "credit_bureau_report": {"type": "raw_report",
                                 "report_type": "credit_bureau_report",
                                 "ts_field": "report_ts"},
        "socdem_report": {"type": "raw_report", "report_type": "socdem_report",
                          "ts_field": "report_ts"},
    },
    "feature_views": {"credit_model_inputs": {
        "entity": "application_snapshot",
        "key_fields": ["user_id", "application_id", "report_id"],
        "view_version": 1, "owner": "o", "status": "active",
        "features": {
            "active_monthly_payment": {
                "kind": "udf", "feature_version": 1, "udf": "udf.amp",
                "dtype": "float", "status": "live",
                "inputs": ["credit_bureau_report"]},
            "monthly_income": {
                "kind": "udf", "feature_version": 1, "udf": "udf.mi",
                "dtype": "float", "status": "live", "inputs": ["socdem_report"]},
            "payment_to_income_ratio": {
                "kind": "udf", "feature_version": 1, "udf": "udf.ratio",
                "dtype": "float", "status": "live", "inputs": [],
                "deps": [{"feature": "active_monthly_payment", "version": 1},
                         {"feature": "monthly_income", "version": 1}]},
        },
    }},
}

_UDFS = UdfRegistry({
    "udf.amp": lambda s, d: sum(
        loan["monthly_payment"] for loan in s["credit_bureau_report"]["loans"]
        if loan["status"] == "active"),
    "udf.mi": lambda s, d: float(s["socdem_report"]["monthly_income"]),
    "udf.ratio": lambda s, d: d["active_monthly_payment"] / d["monthly_income"],
})


def build_snapshot_backend() -> AppBackend:
    """The memory backend rewired onto the 3-part-key snapshot registry."""
    base = build_memory_backend()
    registry = build_registry(_REGISTRY_DATA)
    resolver = ReportResolver(base.payloads, base.metas)
    store = FeatureStore(registry, _UDFS, resolver, base.offline, base.online)

    def make_feature_store(request_resolver):
        return FeatureStore(registry, _UDFS, request_resolver, base.offline, base.online)

    fields = {name: getattr(base, name) for name in base.__dataclass_fields__}
    fields.update(registry=registry, store=store, make_feature_store=make_feature_store)
    return AppBackend(**fields)


__all__ = ["build_snapshot_backend", "SimpleNamespace"]
