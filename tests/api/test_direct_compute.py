"""Unit tests for the internal ``persist_and_compute`` helper (backfill/import engine).

The legacy ``POST /v1/features/compute-direct`` HTTP route and its ``run_direct_compute``
wrapper were removed in; these tests exercise the remaining library function
directly. (The route's 404 guard lives in ``test_app.py``.)
"""

import dataclasses
from datetime import UTC, datetime

import pytest

from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.direct_compute import InlineSource, persist_and_compute
from fintech_feature_platform.fs_core.models import EntityKey

_ENTITY = {"user_id": "1", "application_id": "A1"}


def _view(backend):
    return next(
        v for v in backend.registry.feature_views if v.name == "user_credit_risk"
    )


def _entity_key(view) -> EntityKey:
    return EntityKey.from_mapping(_ENTITY, key_order=view.key_fields)


def _credit(income=4_000_000, monthly=700_000) -> InlineSource:
    return InlineSource(
        report_type="credit_report",
        report_ts=datetime(2026, 6, 22, 10, tzinfo=UTC),
        payload={"declared_income": income, "monthly_obligations": monthly},
    )


def test_persist_and_compute_returns_features_and_report_refs():
    backend = build_memory_backend()
    view = _view(backend)
    outcome, report_refs = persist_and_compute(
        backend, view, _entity_key(view), {"credit_report": _credit()},
        ["declared_income"],
    )
    assert outcome.results["declared_income"].value == 4_000_000
    assert report_refs["credit_report"].startswith("rep_")  # keyed by source name


def test_persist_and_compute_derived_feature_two_sources():
    backend = build_memory_backend()
    view = _view(backend)
    inline_sources = {
        "credit_report": _credit(monthly=800_000),
        "tax_report": InlineSource(
            report_type="tax_report",
            report_ts=datetime(2026, 6, 22, 10, tzinfo=UTC),
            payload={"income": 2_000_000},
        ),
    }
    outcome, report_refs = persist_and_compute(
        backend, view, _entity_key(view), inline_sources, ["debt_to_income_ratio"],
    )
    assert outcome.results["debt_to_income_ratio"].value == pytest.approx(
        800_000 / 2_000_000
    )
    assert set(report_refs) == {"credit_report", "tax_report"}


def test_persist_and_compute_write_online_true_writes_online():
    backend = build_memory_backend()
    view = _view(backend)
    entity_key = _entity_key(view)
    outcome, _ = persist_and_compute(
        backend, view, entity_key, {"credit_report": _credit()}, ["declared_income"],
        write_online=True,
    )
    assert outcome.online_written["declared_income:v1"] == "written"
    assert backend.online.get(
        "user_credit_risk", 1, entity_key, "declared_income", 1
    ) is not None


def test_persist_and_compute_write_online_false_skips_online():
    backend = build_memory_backend()
    view = _view(backend)
    entity_key = _entity_key(view)
    outcome, _ = persist_and_compute(
        backend, view, entity_key, {"credit_report": _credit()}, ["declared_income"],
        write_online=False,
    )
    assert outcome.online_written["declared_income:v1"] == "disabled"
    # offline history written, online latest skipped
    assert len(backend.offline.get(entity_key, feature_name="declared_income")) == 1
    assert backend.online.get(
        "user_credit_risk", 1, entity_key, "declared_income", 1
    ) is None


def test_persist_and_compute_empty_sources_raises():
    backend = build_memory_backend()
    view = _view(backend)
    with pytest.raises(ValueError):
        persist_and_compute(
            backend, view, _entity_key(view), {}, ["declared_income"]
        )


def test_persist_and_compute_wrong_report_type_raises():
    backend = build_memory_backend()
    view = _view(backend)
    bad = {
        "credit_report": InlineSource(
            report_type="tax_report",  # bound under the credit_report source name
            report_ts=datetime(2026, 6, 22, 10, tzinfo=UTC),
            payload={"declared_income": 4_000_000},
        )
    }
    with pytest.raises((ValueError, KeyError, TypeError)):
        persist_and_compute(
            backend, view, _entity_key(view), bad, ["declared_income"]
        )


class _NoReadPayloads:
    """Payload store that persists but fails loudly if a base read-back is attempted."""

    def __init__(self, base) -> None:
        self._base = base

    def put(self, storage_uri, payload) -> None:
        self._base.put(storage_uri, payload)

    def get_payload(self, storage_uri):
        raise AssertionError("base payload read-back occurred")


def test_persist_and_compute_overlay_avoids_base_payload_read_back():
    backend = build_memory_backend()
    backend = dataclasses.replace(backend, payloads=_NoReadPayloads(backend.payloads))
    view = _view(backend)
    # If the resolver read the payload back from the base store, _NoReadPayloads raises.
    outcome, report_refs = persist_and_compute(
        backend, view, _entity_key(view),
        {"credit_report": _credit(income=4_000_000, monthly=700_000)},
        ["declared_income"],
    )
    assert outcome.results["declared_income"].value == 4_000_000
    assert "credit_report" in report_refs
