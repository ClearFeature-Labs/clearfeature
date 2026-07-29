from datetime import UTC, datetime

import pytest

from fintech_feature_platform.fs_core.models import EntityKey, RawReportMeta
from fintech_feature_platform.fs_core.raw.meta_repository import InMemoryMetaRepository
from fintech_feature_platform.fs_core.raw.overlay import (
    OverlayRawPayloadStore,
    OverlayRawReportMetaRepository,
)
from fintech_feature_platform.fs_core.raw.payload_store import InMemoryPayloadStore

_TS = datetime(2024, 8, 26, 10, tzinfo=UTC)


def _meta(report_ref: str, report_type: str = "credit_report") -> RawReportMeta:
    return RawReportMeta(
        report_ref=report_ref,
        report_type=report_type,
        entity_type="application",
        entity_key=EntityKey.from_mapping({"user_id": "1"}),
        report_ts=_TS,
        payload_size_bytes=1,
        content_hash="sha256:x",
        storage_uri=f"mem://{report_ref}",
        created_at=_TS,
    )


def test_payload_overlay_first():
    base = InMemoryPayloadStore()
    base.put("mem://k", {"src": "base"})
    overlay = OverlayRawPayloadStore({"mem://k": {"src": "overlay"}}, base)
    assert overlay.get_payload("mem://k") == {"src": "overlay"}


def test_payload_falls_back_to_base():
    base = InMemoryPayloadStore()
    base.put("mem://k", {"src": "base"})
    overlay = OverlayRawPayloadStore({}, base)
    assert overlay.get_payload("mem://k") == {"src": "base"}


def test_payload_does_not_mutate_base():
    base = InMemoryPayloadStore()
    overlay = OverlayRawPayloadStore({"mem://k": {"src": "overlay"}}, base)
    overlay.get_payload("mem://k")
    with pytest.raises(KeyError):  # base never received the overlay entry
        base.get_payload("mem://k")


def test_payload_missing_behaves_like_base():
    overlay = OverlayRawPayloadStore({}, InMemoryPayloadStore())
    with pytest.raises(KeyError):
        overlay.get_payload("mem://missing")


def test_meta_overlay_first():
    base = InMemoryMetaRepository()
    base.add(_meta("rep_1", "tax_report"))
    overlay = OverlayRawReportMetaRepository({"rep_1": _meta("rep_1", "credit_report")}, base)
    assert overlay.get_meta("rep_1").report_type == "credit_report"


def test_meta_falls_back_to_base():
    base = InMemoryMetaRepository()
    base.add(_meta("rep_1", "tax_report"))
    overlay = OverlayRawReportMetaRepository({}, base)
    assert overlay.get_meta("rep_1").report_type == "tax_report"


def test_meta_does_not_mutate_base():
    base = InMemoryMetaRepository()
    overlay = OverlayRawReportMetaRepository({"rep_1": _meta("rep_1")}, base)
    overlay.get_meta("rep_1")
    with pytest.raises(KeyError):
        base.get_meta("rep_1")


def test_meta_missing_behaves_like_base():
    overlay = OverlayRawReportMetaRepository({}, InMemoryMetaRepository())
    with pytest.raises(KeyError):
        overlay.get_meta("missing")
