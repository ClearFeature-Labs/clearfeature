"""Tests for DWH extraction/materialization into landing forms (a) and (b)."""

from datetime import UTC, datetime

import pytest

from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.dwh_ingestion import (
    DwhFeatureConfig,
    DwhFeatureRow,
    DwhJsonConfig,
    run_dwh_feature_import,
    run_dwh_json_extraction,
)
from fintech_feature_platform.fs_core.dwh.reader import InMemoryDwhReader
from fintech_feature_platform.fs_core.models import EntityKey
from fintech_feature_platform.fs_core.stores.source_dataset import (
    ITEM_REJECTED,
    LANDING_FEATURE_ROWS,
    LANDING_RAW_REPORTS,
    SOURCE_KIND_DWH_JSON_REPORTS,
    SOURCE_KIND_DWH_TABLE,
)
from fintech_feature_platform.fs_core.training import (
    TrainingObservation,
    build_training_dataset,
)

_Q = "q1"

# The example registry's view/feature used across offline tests.
_VIEW = "user_credit_risk"
_KEY_ORDER = ["user_id", "application_id"]


# --- DWH JSON -> landing form (a) ---------------------------------------------

def _json_row(customer_id="c1", *, event_ts="2026-07-01T10:00:00Z", score=710):
    return {
        "entity_key": {"customer_id": customer_id},
        "event_ts": event_ts,
        "payload_json": {"bureau_score": score},
    }


def _json_config(**kw):
    return DwhJsonConfig(
        entity_type="customer", source_name="bureau",
        report_type="credit_report", query_name=_Q, **kw,
    )


def _run_json(backend, rows, **kw):
    reader = InMemoryDwhReader({_Q: rows})
    return run_dwh_json_extraction(backend=backend, reader=reader, config=_json_config(**kw))


def test_dwh_json_lands_raw_report_and_meta_and_manifest():
    backend = build_memory_backend()
    manifest = _run_json(backend, [_json_row()])
    assert manifest.source_kind == SOURCE_KIND_DWH_JSON_REPORTS
    assert manifest.landing_form == LANDING_RAW_REPORTS
    assert manifest.input_uri == _Q
    assert manifest.item_count_written == 1
    item = backend.source_datasets.list_items(manifest.manifest_id)[0]
    assert item.report_ref  # ref recorded for a later batch job
    meta = backend.metas.get_meta(item.report_ref)
    assert backend.payloads.get_payload(meta.storage_uri) == {"bureau_score": 710}


def test_dwh_json_counts_and_watermarks():
    backend = build_memory_backend()
    manifest = _run_json(
        backend,
        [
            _json_row("c1", event_ts="2026-07-01T00:00:00Z"),
            _json_row("c2", event_ts="2026-07-05T00:00:00Z"),
        ],
    )
    assert manifest.item_count_read == 2
    assert manifest.item_count_written == 2
    assert manifest.watermark_min_event_ts == datetime(2026, 7, 1, tzinfo=UTC)
    assert manifest.watermark_max_event_ts == datetime(2026, 7, 5, tzinfo=UTC)


def test_dwh_json_idempotent_rerun_no_duplicate_rows():
    backend = build_memory_backend()
    _run_json(backend, [_json_row("c1"), _json_row("c2")])
    meta_count = len(backend.metas._meta)  # noqa: SLF001
    payload_count = len(backend.payloads._payloads)  # noqa: SLF001
    second = _run_json(backend, [_json_row("c1"), _json_row("c2")])
    assert second.item_count_written == 0
    assert second.item_count_duplicate == 2
    assert len(backend.metas._meta) == meta_count  # noqa: SLF001
    assert len(backend.payloads._payloads) == payload_count  # noqa: SLF001


def test_dwh_json_bad_rows_rejected_without_aborting():
    backend = build_memory_backend()
    rows = [
        _json_row("c1"),                                       # ok
        {"event_ts": "2026-07-01T10:00:00Z", "payload_json": {}},  # no entity_key
        {"entity_key": {"customer_id": "c2"}, "payload_json": {}},  # no event_ts
        {"entity_key": {"customer_id": "c3"},                  # naive event_ts
         "event_ts": "2026-07-01T10:00:00", "payload_json": {}},
    ]
    manifest = _run_json(backend, rows)
    assert manifest.item_count_read == 4
    assert manifest.item_count_written == 1
    assert manifest.item_count_rejected == 3
    rejected = [
        i for i in backend.source_datasets.list_items(manifest.manifest_id)
        if i.status == ITEM_REJECTED
    ]
    assert all(i.error for i in rejected)


def test_dwh_json_missing_payload_column_rejected():
    backend = build_memory_backend()
    manifest = _run_json(backend, [{"entity_key": {"customer_id": "c1"},
                                    "event_ts": "2026-07-01T10:00:00Z"}])
    assert manifest.item_count_rejected == 1
    assert manifest.item_count_written == 0


# --- DWH feature rows -> landing form (b) -------------------------------------

def _key(user_id="1"):
    return EntityKey.from_mapping(
        {"user_id": user_id, "application_id": "A1"}, key_order=_KEY_ORDER
    )


def _feat_row(user_id="1", *, value=1000, data_ts="2026-07-01T00:00:00Z",
              calc_ts="2026-07-01T01:00:00Z", feature="declared_income", version=1):
    return {
        "entity_key": {"user_id": user_id, "application_id": "A1"},
        "feature_name": feature,
        "feature_version": version,
        "value": value,
        "data_ts": data_ts,
        "calc_ts": calc_ts,
    }


def _feat_config(**kw):
    return DwhFeatureConfig(
        entity_type="application", view=_VIEW, view_version=1, query_name=_Q, **kw,
    )


def _run_feat(backend, rows, **kw):
    reader = InMemoryDwhReader({_Q: rows})
    return run_dwh_feature_import(backend=backend, reader=reader, config=_feat_config(**kw))


def test_feature_row_parser_rejects_naive_and_missing():
    with pytest.raises(ValueError):
        DwhFeatureRow.model_validate({**_feat_row(), "data_ts": "2026-07-01T00:00:00"})
    with pytest.raises(ValueError):
        bad = _feat_row()
        del bad["calc_ts"]
        DwhFeatureRow.model_validate(bad)


def test_dwh_feature_import_writes_offline_rows():
    backend = build_memory_backend()
    manifest = _run_feat(backend, [_feat_row("1", value=1000)])
    assert manifest.source_kind == SOURCE_KIND_DWH_TABLE
    assert manifest.landing_form == LANDING_FEATURE_ROWS
    assert manifest.view == _VIEW
    assert manifest.item_count_written == 1
    records = backend.offline.get(_key("1"), feature_name="declared_income")
    assert len(records) == 1
    assert records[0].result.value == 1000
    assert records[0].result.data_ts == datetime(2026, 7, 1, tzinfo=UTC)
    assert records[0].result.calc_ts == datetime(2026, 7, 1, 1, tzinfo=UTC)


def test_dwh_feature_import_dq_counts_and_watermarks():
    backend = build_memory_backend()
    rows = [
        _feat_row("1", data_ts="2026-07-01T00:00:00Z"),
        _feat_row("2", data_ts="2026-07-03T00:00:00Z"),
        {**_feat_row("3"), "data_ts": "2026-07-01T00:00:00"},  # naive -> rejected
        _feat_row("4", feature="nope"),                        # unknown feature -> rejected
    ]
    manifest = _run_feat(backend, rows)
    assert manifest.item_count_read == 4
    assert manifest.item_count_written == 2
    assert manifest.item_count_rejected == 2
    assert manifest.watermark_min_event_ts == datetime(2026, 7, 1, tzinfo=UTC)
    assert manifest.watermark_max_event_ts == datetime(2026, 7, 3, tzinfo=UTC)


def test_dwh_feature_import_idempotent_rerun_dedups():
    backend = build_memory_backend()
    _run_feat(backend, [_feat_row("1"), _feat_row("2")])
    second = _run_feat(backend, [_feat_row("1"), _feat_row("2")])
    assert second.item_count_written == 0
    assert second.item_count_duplicate == 2
    # No duplicate offline rows.
    assert len(backend.offline.get(_key("1"), feature_name="declared_income")) == 1


def test_dwh_feature_import_wrong_version_rejected():
    backend = build_memory_backend()
    manifest = _run_feat(backend, [_feat_row("1", version=99)])
    assert manifest.item_count_rejected == 1
    assert manifest.item_count_written == 0


def test_imported_feature_row_respects_pit_availability():
    # calc_ts after the observation -> PIT builder must not select it.
    backend = build_memory_backend()
    _run_feat(
        backend,
        [_feat_row("1", data_ts="2026-07-01T00:00:00Z",
                   calc_ts="2026-07-20T00:00:00Z")],  # computed after the observation
    )
    view = next(v for v in backend.registry.feature_views if v.name == _VIEW)
    dataset = build_training_dataset(
        offline=backend.offline, view=view, view_version=1,
        feature_names=["declared_income"],
        observations=[TrainingObservation(
            entity={"user_id": "1", "application_id": "A1"},
            observation_ts=datetime(2026, 7, 10, tzinfo=UTC),
        )],
    )
    assert dataset.rows[0].features["declared_income"] is None  # calc_ts > observation


def test_imported_feature_row_selected_when_available():
    backend = build_memory_backend()
    _run_feat(
        backend,
        [_feat_row("1", value=1234, data_ts="2026-07-01T00:00:00Z",
                   calc_ts="2026-07-01T00:00:00Z")],
    )
    view = next(v for v in backend.registry.feature_views if v.name == _VIEW)
    dataset = build_training_dataset(
        offline=backend.offline, view=view, view_version=1,
        feature_names=["declared_income"],
        observations=[TrainingObservation(
            entity={"user_id": "1", "application_id": "A1"},
            observation_ts=datetime(2026, 7, 10, tzinfo=UTC),
        )],
    )
    assert dataset.rows[0].features["declared_income"] == 1234


def test_unknown_query_raises():
    backend = build_memory_backend()
    reader = InMemoryDwhReader({"other": []})
    with pytest.raises(KeyError):
        run_dwh_json_extraction(backend=backend, reader=reader, config=_json_config())
