"""Local credit-risk demo for the FinTech Feature Platform MVP.

Runs the full in-memory workflow end to end, outside the test suite:
registry -> example UDFs -> raw report resolution -> ComputeCore ->
offline/online stores, via FeatureStore. Run it with:

    uv run python examples/local_credit_risk_demo.py

The example UDFs below are domain feature logic and deliberately live here, not
in fs_core: the platform core stays free of use-case-specific feature code.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

_REGISTRY_PATH = (
    Path(__file__).resolve().parent / "registry" / "minimal_credit_risk.yaml"
)
_VIEW = "user_credit_risk"
_VIEW_VERSION = 1
_TS = datetime(2024, 8, 26, 10, tzinfo=UTC)
_STALE_TS = datetime(2024, 8, 25, 10, tzinfo=UTC)

_CREDIT_REF = "rep_credit"
_TAX_REF = "rep_tax"
_SOURCE_REFS = {"credit_report": _CREDIT_REF, "tax_report": _TAX_REF}


def _stamps(report_ts):
    # Lazy import mirror of the in-function imports below (demo style).
    from fintech_feature_platform.fs_core.models import SourceStamp

    return {
        "credit_report": SourceStamp(report_ts=report_ts, content_hash="sha256:demo"),
        "tax_report": SourceStamp(report_ts=report_ts, content_hash="sha256:demo"),
    }
_REQUESTED = [
    "declared_income",
    "monthly_obligations",
    "income_from_tax",
    "debt_to_income_ratio",
]
_ENTITY_VALUES = {"user_id": "1", "application_id": "A1"}


# --- example UDFs (domain feature logic; not part of fs_core) -----------------

def declared_income(sources, deps):
    return sources["credit_report"]["declared_income"]


def monthly_obligations(sources, deps):
    return sources["credit_report"]["monthly_obligations"]


def income_from_tax(sources, deps):
    return sources["tax_report"]["income"]


def safe_ratio(sources, deps):
    income = deps["income_from_tax"]
    return deps["monthly_obligations"] / income if income else None


def run_demo():
    """Wire the in-memory components, compute features, print, and return the outcome."""
    from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
    from fintech_feature_platform.fs_core.feature_store import FeatureStore
    from fintech_feature_platform.fs_core.models import EntityKey, RawReportMeta
    from fintech_feature_platform.fs_core.raw.meta_repository import InMemoryMetaRepository
    from fintech_feature_platform.fs_core.raw.payload_store import InMemoryPayloadStore
    from fintech_feature_platform.fs_core.raw.report_resolver import ReportResolver
    from fintech_feature_platform.fs_core.registry.loader import load_registry_file
    from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore
    from fintech_feature_platform.fs_core.stores.online import InMemoryOnlineStore

    registry = load_registry_file(_REGISTRY_PATH)
    view = next(v for v in registry.feature_views if v.name == _VIEW)

    udfs = UdfRegistry(
        {
            "udf.credit.declared_income": declared_income,
            "udf.credit.monthly_obligations": monthly_obligations,
            "udf.tax.income": income_from_tax,
            "udf.common.safe_ratio": safe_ratio,
        }
    )

    # Payloads are stored under their storage_uri (mem://<report_ref> here), which
    # the resolver derives from each report's metadata.
    payloads = InMemoryPayloadStore()
    payloads.put(
        f"mem://{_CREDIT_REF}",
        {"declared_income": 3_500_000, "monthly_obligations": 700_000},
    )
    payloads.put(f"mem://{_TAX_REF}", {"income": 3_000_000})

    def make_meta(report_ref: str, report_type: str) -> RawReportMeta:
        return RawReportMeta(
            report_ref=report_ref,
            report_type=report_type,
            entity_type="application",
            entity_key=EntityKey.from_mapping({"user_id": _ENTITY_VALUES["user_id"]}),
            report_ts=_TS,
            payload_size_bytes=10,
            content_hash="sha256:demo",
            storage_uri=f"mem://{report_ref}",
            created_at=_TS,
        )

    metas = InMemoryMetaRepository()
    metas.add(make_meta(_CREDIT_REF, "credit_report"))
    metas.add(make_meta(_TAX_REF, "tax_report"))

    resolver = ReportResolver(payloads, metas)
    offline = InMemoryOfflineStore()
    online = InMemoryOnlineStore()
    store = FeatureStore(registry, udfs, resolver, offline, online)

    # Build the entity key from exactly the fields the view declares.
    entity_key = EntityKey.from_mapping(
        {field: _ENTITY_VALUES[field] for field in view.key_fields},
        key_order=view.key_fields,
    )

    outcome = store.compute(
        view=_VIEW,
        view_version=_VIEW_VERSION,
        entity_key=entity_key,
        requested_features=_REQUESTED,
        source_refs=_SOURCE_REFS,
        source_stamps=_stamps(_TS),
        calc_ts=_TS,
    )

    _print_report(entity_key, outcome, offline)
    _demonstrate_cas_skip(store, entity_key)

    return outcome


def _print_report(entity_key, outcome, offline) -> None:
    print("=== Local credit-risk demo ===")
    print(f"entity_key: {entity_key.encode()}")
    print()
    print("computed features:")
    for name in _REQUESTED:
        result = outcome.results[name]
        written = outcome.online_written.get(result.ref.encode())
        print(f"  {result.ref.encode()} = {result.value}  (online_written={written})")
    print()
    dti = outcome.results["debt_to_income_ratio"]
    print(f"debt_to_income_ratio:v{dti.ref.version} = {dti.value:.4f}")
    print()
    rows = len(offline.get(entity_key, view=_VIEW, view_version=_VIEW_VERSION))
    print(f"offline history rows for this entity/view: {rows}")


def _demonstrate_cas_skip(store, entity_key) -> None:
    # A stale recompute must be skipped by the online CAS (older data_ts).
    stale = store.compute(
        view=_VIEW,
        view_version=_VIEW_VERSION,
        entity_key=entity_key,
        requested_features=["declared_income"],
        source_refs=_SOURCE_REFS,
        source_stamps=_stamps(_STALE_TS),
        calc_ts=_TS,
    )
    print()
    print(f"stale recompute online_written: {stale.online_written}")
    print("(online CAS skipped the stale write because its data_ts is older)")


def main() -> None:
    run_demo()


if __name__ == "__main__":
    main()
