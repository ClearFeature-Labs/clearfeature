from datetime import UTC, datetime
from pathlib import Path

import pytest

from fintech_feature_platform.fs_core.compute.context import RequestContext
from fintech_feature_platform.fs_core.compute.engine import ComputeCore
from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.models import EntityKey, RawReportMeta, SourceStamp
from fintech_feature_platform.fs_core.raw.meta_repository import InMemoryMetaRepository
from fintech_feature_platform.fs_core.raw.payload_store import InMemoryPayloadStore
from fintech_feature_platform.fs_core.raw.report_resolver import ReportResolver
from fintech_feature_platform.fs_core.registry.loader import load_registry_file

_EXAMPLE = (
    Path(__file__).resolve().parents[3]
    / "examples"
    / "registry"
    / "minimal_credit_risk.yaml"
)
_TS = datetime(2024, 8, 26, 10, tzinfo=UTC)


def _meta(report_ref: str, report_type: str, storage_uri: str | None = None) -> RawReportMeta:
    return RawReportMeta(
        report_ref=report_ref,
        report_type=report_type,
        entity_type="application",
        entity_key=EntityKey.from_mapping({"user_id": "1"}),
        report_ts=_TS,
        payload_size_bytes=10,
        content_hash="sha256:test",
        storage_uri=storage_uri or f"mem://{report_ref}",
        created_at=_TS,
    )


class _CountingPayloadStore:
    def __init__(self) -> None:
        self._payloads: dict = {}
        self.calls: dict[str, int] = {}

    def put(self, storage_uri: str, payload) -> None:
        self._payloads[storage_uri] = payload

    def get_payload(self, storage_uri: str):
        self.calls[storage_uri] = self.calls.get(storage_uri, 0) + 1
        return self._payloads[storage_uri]


def test_resolve_source_by_name():
    payloads = InMemoryPayloadStore()
    payloads.put("mem://rep_1", {"declared_income": 3_500_000})
    metas = InMemoryMetaRepository()
    metas.add(_meta("rep_1", "credit_report"))

    resolver = ReportResolver(payloads, metas)
    load = resolver.make_source_loader({"credit_report": "rep_1"})

    assert load("credit_report") == {"declared_income": 3_500_000}


def test_resolver_loads_payload_via_storage_uri_when_it_differs_from_ref():
    payloads = InMemoryPayloadStore()
    payloads.put("mem://obj_abc", {"declared_income": 4_200_000})
    metas = InMemoryMetaRepository()
    metas.add(_meta("rep_1", "credit_report", storage_uri="mem://obj_abc"))

    resolver = ReportResolver(payloads, metas)
    load = resolver.make_source_loader({"credit_report": "rep_1"})

    # report_ref ("rep_1") and storage_uri ("mem://obj_abc") differ; resolution
    # must use the storage_uri from metadata to load the payload.
    assert load("credit_report") == {"declared_income": 4_200_000}


def test_payload_store_get_by_storage_uri():
    payloads = InMemoryPayloadStore()
    payloads.put("mem://rep_1", {"x": 1})
    assert payloads.get_payload("mem://rep_1") == {"x": 1}


def test_payload_loaded_once_through_request_context():
    payloads = _CountingPayloadStore()
    payloads.put("mem://rep_1", {"declared_income": 1})
    metas = InMemoryMetaRepository()
    metas.add(_meta("rep_1", "credit_report"))

    resolver = ReportResolver(payloads, metas)
    ctx = RequestContext(resolver.make_source_loader({"credit_report": "rep_1"}))

    ctx.get_source("credit_report")
    ctx.get_source("credit_report")

    assert payloads.calls["mem://rep_1"] == 1


def _declared_income(sources, deps):
    return sources["credit_report"]["declared_income"]


def test_full_path_with_compute_core():
    registry = load_registry_file(_EXAMPLE)
    payloads = InMemoryPayloadStore()
    payloads.put("mem://rep_credit", {"declared_income": 3_500_000})
    metas = InMemoryMetaRepository()
    metas.add(_meta("rep_credit", "credit_report"))

    resolver = ReportResolver(payloads, metas)
    expected_types = {s.name: s.report_type for s in registry.sources}
    loader = resolver.make_source_loader({"credit_report": "rep_credit"}, expected_types)
    ctx = RequestContext(loader)

    core = ComputeCore(
        registry, UdfRegistry({"udf.credit.declared_income": _declared_income})
    )
    key = EntityKey.from_mapping(
        {"user_id": "1", "application_id": "A1", "report_id": "R9"},
        key_order=["user_id", "application_id", "report_id"],
    )

    results = core.compute(
        view="user_credit_risk",
        view_version=1,
        entity_key=key,
        requested_features=["declared_income"],
        context=ctx,
        source_stamps={
            "credit_report": SourceStamp(report_ts=_TS, content_hash="sha256:test")
        },
        calc_ts=_TS,
    )

    assert results["declared_income"].value == 3_500_000


def test_missing_source_mapping_raises():
    resolver = ReportResolver(InMemoryPayloadStore(), InMemoryMetaRepository())
    load = resolver.make_source_loader({"credit_report": "rep_1"})
    with pytest.raises(ValueError):
        load("tax_report")


def test_missing_metadata_raises():
    payloads = InMemoryPayloadStore()
    payloads.put("mem://rep_1", {"x": 1})
    resolver = ReportResolver(payloads, InMemoryMetaRepository())
    load = resolver.make_source_loader({"credit_report": "rep_1"})
    with pytest.raises(ValueError):
        load("credit_report")


def test_missing_payload_raises():
    # Metadata exists (so storage_uri is known) but no payload is stored there.
    metas = InMemoryMetaRepository()
    metas.add(_meta("rep_1", "credit_report"))
    resolver = ReportResolver(InMemoryPayloadStore(), metas)
    load = resolver.make_source_loader({"credit_report": "rep_1"})
    with pytest.raises(ValueError):
        load("credit_report")


def test_wrong_report_type_raises():
    payloads = InMemoryPayloadStore()
    payloads.put("mem://rep_1", {"x": 1})
    metas = InMemoryMetaRepository()
    metas.add(_meta("rep_1", "tax_report"))

    resolver = ReportResolver(payloads, metas)
    load = resolver.make_source_loader(
        {"credit_report": "rep_1"}, {"credit_report": "credit_report"}
    )
    with pytest.raises(ValueError):
        load("credit_report")
