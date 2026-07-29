"""Values-free feature-value lineage."""

import json
from datetime import UTC, datetime

from fintech_feature_platform.fs_core.models import (
    EntityKey,
    FeatureRef,
    FeatureResult,
    RawReportMeta,
)
from fintech_feature_platform.fs_core.observability.lineage import build_feature_lineage
from fintech_feature_platform.fs_core.raw.meta_repository import InMemoryMetaRepository
from fintech_feature_platform.fs_core.stores.offline import InMemoryOfflineStore
from fintech_feature_platform.fs_core.stores.source_dataset import (
    InMemorySourceDatasetStore,
    SourceDatasetItem,
)

_TS = datetime(2026, 1, 10, tzinfo=UTC)


def _key(id_="1"):
    return EntityKey.from_mapping({"id": id_}, key_order=["id"])


def _offline_with(**result_kw):
    offline = InMemoryOfflineStore()
    result = FeatureResult(
        ref=FeatureRef("f", 1), entity_key=_key(), value=42.0,
        data_ts=_TS, calc_ts=_TS, max_input_data_ts=_TS,
        input_fingerprint="sha256:fp", value_hash="sha256:vh", **result_kw,
    )
    offline.append("v", 1, result)
    return offline


def _lineage(offline, metas=None, **kw):
    return build_feature_lineage(
        offline, metas or InMemoryMetaRepository(), _key(),
        view="v", view_version=1, feature_name="f", feature_version=1, **kw,
    )


def test_basic_f1_f2_metadata():
    lin = _lineage(_offline_with())
    assert lin.found is True
    assert lin.feature_name == "f" and lin.feature_version == 1
    assert lin.entity_key == _key().encode()
    assert lin.data_ts == _TS and lin.calc_ts == _TS
    assert lin.value_hash == "sha256:vh"
    assert lin.input_fingerprint == "sha256:fp"
    assert lin.max_input_data_ts == _TS


def test_f3_model_metadata():
    offline = _offline_with(
        model_uri="mlflow://pd/17", model_digest="sha256:model", model_output_name="score"
    )
    lin = _lineage(offline)
    assert lin.model_uri == "mlflow://pd/17"
    assert lin.model_digest == "sha256:model"
    assert lin.model_output_name == "score"


def test_bundle_digest_present():
    lin = _lineage(_offline_with(bundle_digest="sha256:bundle"))
    assert lin.bundle_digest == "sha256:bundle"
    assert "bundle_digest_not_available" not in lin.gaps


def test_missing_bundle_digest_is_explicit_gap():
    lin = _lineage(_offline_with())
    assert lin.bundle_digest is None
    assert "bundle_digest_not_available" in lin.gaps


def test_report_refs_resolved_from_supplied_refs():
    metas = InMemoryMetaRepository()
    metas.add(RawReportMeta(
        report_ref="rep_1", report_type="credit_report", entity_type="e",
        entity_key=_key(), report_ts=_TS, payload_size_bytes=10,
        content_hash="sha256:c", storage_uri="mem://secret", created_at=_TS,
    ))
    lin = _lineage(_offline_with(), metas=metas, report_refs=["rep_1"])
    assert len(lin.report_refs) == 1
    ref = lin.report_refs[0]
    assert ref.report_ref == "rep_1"
    assert ref.content_hash == "sha256:c"
    assert "source_report_refs_not_available" not in lin.gaps


def test_report_refs_resolved_from_manifest_items():
    source_datasets = InMemorySourceDatasetStore()
    source_datasets.add_items([
        SourceDatasetItem(
            manifest_id="sdm_1", item_index=0, status="written",
            source_name="credit", report_type="credit_report",
            entity_key={"id": "1"}, report_ref="rep_9", event_ts=_TS,
            content_hash="sha256:h",
        ),
        SourceDatasetItem(
            manifest_id="sdm_1", item_index=1, status="written",
            source_name="credit", report_type="credit_report",
            entity_key={"id": "2"}, report_ref="rep_other", event_ts=_TS,
        ),
    ])
    lin = _lineage(_offline_with(), manifest_id="sdm_1", source_datasets=source_datasets)
    refs = [r.report_ref for r in lin.report_refs]
    assert refs == ["rep_9"]  # only the matching entity's item
    assert lin.manifest_id == "sdm_1"


def test_unknown_supplied_ref_is_explicit_gap():
    lin = _lineage(_offline_with(), report_refs=["rep_missing"])
    assert lin.report_refs == ()
    assert "unknown_report_ref:rep_missing" in lin.gaps


def test_missing_value_is_explicit_not_fabricated():
    lin = build_feature_lineage(
        InMemoryOfflineStore(), InMemoryMetaRepository(), _key(),
        view="v", view_version=1, feature_name="nope", feature_version=1,
    )
    assert lin.found is False
    assert lin.value_hash is None
    assert "feature_value_not_found" in lin.gaps


def test_lineage_response_has_no_values_or_storage_paths():
    metas = InMemoryMetaRepository()
    metas.add(RawReportMeta(
        report_ref="rep_1", report_type="credit_report", entity_type="e",
        entity_key=_key(), report_ts=_TS, payload_size_bytes=10,
        content_hash="sha256:c", storage_uri="mem://secret-path", created_at=_TS,
    ))
    lin = _lineage(
        _offline_with(bundle_digest="sha256:b", model_uri="mlflow://m/1"),
        metas=metas, report_refs=["rep_1"],
    )
    blob = json.dumps(lin.to_dict())
    for forbidden in ("42.0", "storage_uri", "mem://secret", "object_key", "payload", "SQL"):
        assert forbidden not in blob
