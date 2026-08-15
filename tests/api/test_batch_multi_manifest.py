"""Multi-manifest batch scope ``source_dataset_manifests`` — .

One existing BatchJob materializes a complete multi-source feature DAG from several
already-landed SourceDatasetManifests. Items are joined by the COMPLETE canonical
entity key (view ``key_fields`` order), each source keeps its own stamps, Kafka stays
refs-only, ambiguity fails closed before publish, and provenance (all manifest_ids)
is durable. The singular ``source_dataset_manifest`` scope is unchanged.
"""

import json
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from fintech_feature_platform.api.app import create_app
from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.api.batch_worker import handle_batch_chunk
from fintech_feature_platform.fs_core.models import EntityKey, RawReportMeta
from fintech_feature_platform.fs_core.stores.source_dataset import (
    ITEM_DUPLICATE,
    ITEM_WRITTEN,
    LANDING_FEATURE_ROWS,
    LANDING_RAW_REPORTS,
    SourceDatasetItem,
    SourceDatasetManifest,
)

_TS = datetime(2026, 5, 1, 9, 0, tzinfo=UTC)
_AVAIL = _TS + timedelta(hours=1)

# The built-in demo registry: view user_credit_risk v1, key_fields
# [user_id, application_id] (NON-alphabetical), sources credit_report + tax_report,
# F2 debt_to_income_ratio spanning both sources.
_VIEW = "user_credit_risk"
_CREDIT = "credit_report"
_TAX = "tax_report"


def _mk_backend():
    return build_memory_backend()


def _client(backend):
    return TestClient(create_app(backend=backend))


def _seed_report(backend, key: dict, source: str, payload: dict, *, ref: str,
                 report_ts=_TS, available_at=_AVAIL):
    uri = f"memory://{ref}"
    backend.payloads.put(uri, payload)
    backend.metas.add(RawReportMeta(
        report_ref=ref, report_type=source, entity_type="application",
        entity_key=EntityKey.from_mapping(key, key_order=["user_id", "application_id"]),
        report_ts=report_ts, payload_size_bytes=1, content_hash=f"sha256:{ref}",
        storage_uri=uri, created_at=report_ts, format="json", compression="none",
        available_at=available_at, availability_source="source_provided",
    ))


def _mk_manifest(backend, manifest_id: str, source: str, bindings, *,
                 entity_type="application", landing_form=LANDING_RAW_REPORTS,
                 statuses=None):
    """bindings: list of (entity_key_dict, report_ref)."""
    backend.source_datasets.upsert_manifest(SourceDatasetManifest(
        manifest_id=manifest_id, dataset_id=f"ds_{manifest_id}", source_kind="jsonl",
        entity_type=entity_type, source_name=source, copy_mode="copy",
        created_at=_TS, report_type=source, landing_form=landing_form,
        status="completed", item_count_read=len(bindings),
        item_count_written=len(bindings),
    ))
    items = []
    for index, (key, ref) in enumerate(bindings):
        status = (statuses or {}).get(index, ITEM_WRITTEN)
        items.append(SourceDatasetItem(
            manifest_id=manifest_id, item_index=index, status=status,
            source_name=source, report_type=source, entity_key=dict(key),
            report_ref=ref, event_ts=_TS, content_hash=f"sha256:{ref}",
        ))
    backend.source_datasets.add_items(items)


def _key(i: int) -> dict:
    return {"user_id": f"u{i}", "application_id": f"a{i}"}


def _credit_payload(income=5000, obligations=1000):
    return {"declared_income": income, "monthly_obligations": obligations}


def _tax_payload(income=4000):
    return {"income": income}


def _seed_pair(backend, i: int, *, credit_ref=None, tax_ref=None):
    key = _key(i)
    credit_ref = credit_ref or f"rep_c{i}"
    tax_ref = tax_ref or f"rep_t{i}"
    _seed_report(backend, key, _CREDIT, _credit_payload(), ref=credit_ref)
    _seed_report(backend, key, _TAX, _tax_payload(), ref=tax_ref)
    return key, credit_ref, tax_ref


def _submit(client, *, manifest_ids, job_id="job_mm", features=None, groups=None,
            chunk_size=100):
    body = {
        "view": _VIEW, "view_version": 1, "idempotency_key": job_id,
        "requested_features": features or [], "requested_feature_groups": groups or [],
        "chunk_size": chunk_size,
        "scope": {"type": "source_dataset_manifests", "manifest_ids": manifest_ids},
    }
    return client.post("/v1/batch/jobs", json=body)


def _published_chunks(backend, job_id):
    return [r.event for r in backend.events.published
            if getattr(r.event, "batch_job_id", None) == job_id]


# --- include_duplicate_items is rejected for the multi-manifest scope --------

def test_include_duplicate_items_rejected_before_job_or_publish():
    # The multi-manifest join ALWAYS admits exact-duplicate re-ingested bindings; a
    # true here would be a silently-ignored option, so it fails closed before any job or publish.
    backend = _mk_backend()
    key, credit_ref, tax_ref = _seed_pair(backend, 40)
    _mk_manifest(backend, "m_credit", _CREDIT, [(key, credit_ref)])
    _mk_manifest(backend, "m_tax", _TAX, [(key, tax_ref)])
    client = _client(backend)
    body = {
        "view": _VIEW, "view_version": 1, "idempotency_key": "job_dup_flag",
        "requested_features": ["debt_to_income_ratio"], "chunk_size": 100,
        "scope": {"type": "source_dataset_manifests",
                  "manifest_ids": ["m_credit", "m_tax"],
                  "include_duplicate_items": True},
    }
    response = client.post("/v1/batch/jobs", json=body)
    assert response.status_code == 400
    assert "include_duplicate_items" in response.json()["detail"]
    assert client.get("/v1/batch/jobs/job_dup_flag").status_code == 404  # no job
    assert _published_chunks(backend, "job_dup_flag") == []  # nothing published


# --- idempotency-key request identity ----------------------------------------

def _seed_two_manifests(backend, i=50):
    key, credit_ref, tax_ref = _seed_pair(backend, i)
    _mk_manifest(backend, "m_credit", _CREDIT, [(key, credit_ref)])
    _mk_manifest(backend, "m_tax", _TAX, [(key, tax_ref)])
    return key


def test_identical_replay_returns_existing_job_without_republish():
    backend = _mk_backend()
    _seed_two_manifests(backend)
    client = _client(backend)
    first = _submit(client, manifest_ids=["m_credit", "m_tax"],
                    features=["debt_to_income_ratio"])
    assert first.status_code == 202
    published_before = len(backend.events.published)
    status_before = client.get("/v1/batch/jobs/job_mm").json()

    second = _submit(client, manifest_ids=["m_credit", "m_tax"],
                     features=["debt_to_income_ratio"])
    assert second.status_code == 202
    assert second.json() == first.json()  # the existing job, unchanged
    assert len(backend.events.published) == published_before  # no re-publish
    assert client.get("/v1/batch/jobs/job_mm").json() == status_before  # no mutation


def test_reversed_manifest_order_replay_is_the_same_request():
    backend = _mk_backend()
    _seed_two_manifests(backend)
    client = _client(backend)
    first = _submit(client, manifest_ids=["m_credit", "m_tax"],
                    features=["debt_to_income_ratio"])
    published_before = len(backend.events.published)
    second = _submit(client, manifest_ids=["m_tax", "m_credit"],
                     features=["debt_to_income_ratio"])
    assert second.status_code == 202
    assert second.json()["job_id"] == first.json()["job_id"]
    assert len(backend.events.published) == published_before


def test_same_key_with_different_manifests_conflicts_409_without_mutation():
    backend = _mk_backend()
    key = _seed_two_manifests(backend)
    _mk_manifest(backend, "m_credit_v2", _CREDIT, [(key, "rep_c50")])  # same ref, new manifest
    client = _client(backend)
    assert _submit(client, manifest_ids=["m_credit", "m_tax"],
                   features=["debt_to_income_ratio"]).status_code == 202
    published_before = len(backend.events.published)
    status_before = client.get("/v1/batch/jobs/job_mm").json()

    conflict = _submit(client, manifest_ids=["m_credit_v2", "m_tax"],
                       features=["debt_to_income_ratio"])
    assert conflict.status_code == 409
    assert "idempotency_key" in conflict.json()["detail"]
    # Neither operational status nor provenance mutated; nothing published, so the
    # durable projection (event-driven) cannot have changed either.
    status_after = client.get("/v1/batch/jobs/job_mm").json()
    assert status_after == status_before
    assert status_after["manifest_ids"] == ["m_credit", "m_tax"]  # no merge/replace
    assert len(backend.events.published) == published_before


def test_same_key_across_singular_and_multi_scope_conflicts_409():
    backend = _mk_backend()
    _seed_two_manifests(backend)
    client = _client(backend)
    assert _submit(client, manifest_ids=["m_credit", "m_tax"],
                   features=["debt_to_income_ratio"]).status_code == 202
    singular = {
        "view": _VIEW, "view_version": 1, "idempotency_key": "job_mm",
        "requested_features": ["declared_income"], "chunk_size": 100,
        "scope": {"type": "source_dataset_manifest", "manifest_id": "m_credit"},
    }
    response = client.post("/v1/batch/jobs", json=singular)
    assert response.status_code == 409


def test_same_key_with_different_feature_plan_conflicts_409():
    backend = _mk_backend()
    _seed_two_manifests(backend)
    client = _client(backend)
    assert _submit(client, manifest_ids=["m_credit", "m_tax"],
                   features=["debt_to_income_ratio"]).status_code == 202
    conflict = _submit(client, manifest_ids=["m_credit", "m_tax"],
                       features=["declared_income"])
    assert conflict.status_code == 409


# --- 1. two manifests, two sources, one entity ---------------------------------

def test_join_two_manifests_into_multi_ref_items_and_compute_full_dag():
    backend = _mk_backend()
    key, credit_ref, tax_ref = _seed_pair(backend, 1)
    _mk_manifest(backend, "m_credit", _CREDIT, [(key, credit_ref)])
    _mk_manifest(backend, "m_tax", _TAX, [(key, tax_ref)])
    client = _client(backend)

    response = _submit(client, manifest_ids=["m_credit", "m_tax"],
                       features=["declared_income", "income_from_tax",
                                 "debt_to_income_ratio"])
    assert response.status_code == 202, response.text
    accepted = response.json()
    assert accepted["total_items"] == 1
    assert sorted(accepted["manifest_ids"]) == ["m_credit", "m_tax"]

    chunks = _published_chunks(backend, "job_mm")
    assert len(chunks) == 1
    item = chunks[0].items[0]
    assert item.source_refs == {_CREDIT: credit_ref, _TAX: tax_ref}
    assert not item.inline_sources  # refs-only: no payloads through Kafka
    event_json = json.dumps(chunks[0].to_dict())
    assert "5000" not in event_json and "4000" not in event_json  # no payload VALUES

    result = handle_batch_chunk(backend, chunks[0])
    assert result.status == "ok"
    entity = EntityKey.from_mapping(key, key_order=["user_id", "application_id"])
    for name, expected in (("declared_income", 5000),
                           ("income_from_tax", 4000),
                           ("debt_to_income_ratio", 1000 / 4000)):
        records = backend.offline.get(entity, feature_name=name, feature_version=1)
        assert [r.result.value for r in records] == [expected], name


# --- 2. non-alphabetical key_fields --------------------------------------------

def test_join_uses_view_key_fields_order_not_dict_order():
    backend = _mk_backend()
    key, credit_ref, tax_ref = _seed_pair(backend, 2)
    alphabetized = dict(sorted(key.items()))  # application_id first (serialized shape)
    _mk_manifest(backend, "m_credit", _CREDIT, [(alphabetized, credit_ref)])
    _mk_manifest(backend, "m_tax", _TAX, [(key, tax_ref)])
    client = _client(backend)

    response = _submit(client, manifest_ids=["m_credit", "m_tax"],
                       features=["debt_to_income_ratio"])
    assert response.status_code == 202, response.text
    assert response.json()["total_items"] == 1  # ONE logical entity, not two

    item = _published_chunks(backend, "job_mm")[0].items[0]
    assert len(item.source_refs) == 2


# --- 3. manifest order determinism ---------------------------------------------

def test_manifest_ids_order_does_not_change_items_or_chunks():
    backend_a = _mk_backend()
    backend_b = _mk_backend()
    for backend in (backend_a, backend_b):
        for i in range(5):
            key, credit_ref, tax_ref = _seed_pair(backend, i)
            _mk_manifest(backend, "m_credit", _CREDIT, [(key, credit_ref)]) \
                if i == 0 else None
    # seed manifests with all five bindings at once
    backend_a, backend_b = _mk_backend(), _mk_backend()
    for backend in (backend_a, backend_b):
        pairs = [_seed_pair(backend, i) for i in range(5)]
        _mk_manifest(backend, "m_credit", _CREDIT, [(k, c) for k, c, _ in pairs])
        _mk_manifest(backend, "m_tax", _TAX, [(k, t) for k, _, t in pairs])
    ra = _submit(_client(backend_a), manifest_ids=["m_credit", "m_tax"],
                 features=["debt_to_income_ratio"], chunk_size=2)
    rb = _submit(_client(backend_b), manifest_ids=["m_tax", "m_credit"],
                 features=["debt_to_income_ratio"], chunk_size=2)
    assert ra.status_code == rb.status_code == 202
    items_a = [[(i.entity_key, i.source_refs) for i in c.items]
               for c in _published_chunks(backend_a, "job_mm")]
    items_b = [[(i.entity_key, i.source_refs) for i in c.items]
               for c in _published_chunks(backend_b, "job_mm")]
    assert items_a == items_b  # identical joined items and chunking


# --- 4/5. missing required source ----------------------------------------------

def test_missing_required_source_is_deterministic_per_item_error():
    backend = _mk_backend()
    pairs = [_seed_pair(backend, i) for i in range(3)]
    # entity 1 has NO tax report in the tax manifest
    _mk_manifest(backend, "m_credit", _CREDIT, [(k, c) for k, c, _ in pairs])
    _mk_manifest(backend, "m_tax", _TAX,
                 [(k, t) for j, (k, _, t) in enumerate(pairs) if j != 1])
    client = _client(backend)
    response = _submit(client, manifest_ids=["m_credit", "m_tax"],
                       features=["debt_to_income_ratio"])
    assert response.status_code == 202
    assert response.json()["total_items"] == 3  # the incomplete entity is NOT dropped

    chunk = _published_chunks(backend, "job_mm")[0]
    result = handle_batch_chunk(backend, chunk)
    assert result.status == "ok"
    assert result.ok_items == 2
    assert result.failed_items == 1
    assert any("tax_report" in e for e in result.first_errors)  # visible accounting


def test_source_not_required_by_the_requested_dag_may_be_absent():
    # Only sources in the planned requested-output closure are mandatory; an empty
    # manifest for an unrelated source neither blocks the job nor fails items.
    backend = _mk_backend()
    key, credit_ref, _ = _seed_pair(backend, 7)
    _mk_manifest(backend, "m_credit", _CREDIT, [(key, credit_ref)])
    _mk_manifest(backend, "m_tax", _TAX, [])  # no tax binding at all
    client = _client(backend)
    response = _submit(client, manifest_ids=["m_credit", "m_tax"],
                       features=["declared_income"])
    assert response.status_code == 202
    chunk = _published_chunks(backend, "job_mm")[0]
    assert chunk.items[0].source_refs == {_CREDIT: credit_ref}
    result = handle_batch_chunk(backend, chunk)
    assert result.status == "ok" and result.ok_items == 1 and result.failed_items == 0


# --- 6. several report_id values = distinct snapshots (3-part key) --------------

def test_multiple_report_ids_for_one_application_stay_distinct():
    from tests.api._snapshot_registry import build_snapshot_backend

    from fintech_feature_platform.api.backend import AppBackend  # noqa: F401

    backend = build_snapshot_backend()
    keys = [
        {"user_id": "u1", "application_id": "a1", "report_id": "r1"},
        {"user_id": "u1", "application_id": "a1", "report_id": "r2"},
    ]
    for j, key in enumerate(keys):
        for source, ref in (("credit_bureau_report", f"rep_c{j}"),
                            ("socdem_report", f"rep_s{j}")):
            payload = ({"loans": [{"status": "active", "monthly_payment": 100.0 + j}]}
                       if source.startswith("credit") else {"monthly_income": 1000.0})
            uri = f"memory://{ref}"
            backend.payloads.put(uri, payload)
            backend.metas.add(RawReportMeta(
                report_ref=ref, report_type=source, entity_type="application_snapshot",
                entity_key=EntityKey.from_mapping(
                    key, key_order=["user_id", "application_id", "report_id"]),
                report_ts=_TS, payload_size_bytes=1, content_hash=f"sha256:{ref}",
                storage_uri=uri, created_at=_TS, format="json", compression="none",
                available_at=_AVAIL, availability_source="source_provided",
            ))
    _mk_manifest(backend, "m_credit", "credit_bureau_report",
                 [(keys[0], "rep_c0"), (keys[1], "rep_c1")],
                 entity_type="application_snapshot")
    _mk_manifest(backend, "m_socdem", "socdem_report",
                 [(keys[0], "rep_s0"), (keys[1], "rep_s1")],
                 entity_type="application_snapshot")
    client = _client(backend)
    body = {"view": "credit_model_inputs", "view_version": 1,
            "idempotency_key": "job_snap", "requested_features": ["payment_to_income_ratio"],
            "chunk_size": 100,
            "scope": {"type": "source_dataset_manifests",
                      "manifest_ids": ["m_credit", "m_socdem"]}}
    response = client.post("/v1/batch/jobs", json=body)
    assert response.status_code == 202, response.text
    assert response.json()["total_items"] == 2  # two snapshots, NOT merged

    chunk = _published_chunks(backend, "job_snap")[0]
    result = handle_batch_chunk(backend, chunk)
    assert result.ok_items == 2


# --- 7/8. per-source stamps and availability ------------------------------------

def test_source_stamps_and_trusted_availability_survive_the_join():
    backend = _mk_backend()
    key = _key(9)
    credit_ts, tax_ts = _TS, _TS + timedelta(days=2)
    credit_avail, tax_avail = _AVAIL, _TS + timedelta(days=2, hours=3)
    _seed_report(backend, key, _CREDIT, _credit_payload(), ref="rep_c9",
                 report_ts=credit_ts, available_at=credit_avail)
    _seed_report(backend, key, _TAX, _tax_payload(), ref="rep_t9",
                 report_ts=tax_ts, available_at=tax_avail)
    _mk_manifest(backend, "m_credit", _CREDIT, [(key, "rep_c9")])
    _mk_manifest(backend, "m_tax", _TAX, [(key, "rep_t9")])
    client = _client(backend)
    response = _submit(client, manifest_ids=["m_credit", "m_tax"],
                       features=["debt_to_income_ratio"])
    assert response.status_code == 202  # unequal report_ts across sources is FINE
    chunk = _published_chunks(backend, "job_mm")[0]
    assert handle_batch_chunk(backend, chunk).status == "ok"

    entity = EntityKey.from_mapping(key, key_order=["user_id", "application_id"])
    record = backend.offline.get(entity, feature_name="debt_to_income_ratio",
                                 feature_version=1)[0].result
    # D3/D9 from the INDIVIDUAL source stamps: min for data_ts, max for the watermark.
    assert record.data_ts == credit_ts
    assert record.max_input_data_ts == tax_ts
    # availability: max of the trusted per-source available_at values.
    assert record.available_at == tax_avail


# --- 9/10/11/12. fail-closed validation -----------------------------------------

def test_duplicate_manifest_id_rejected_before_publish():
    backend = _mk_backend()
    key, credit_ref, _ = _seed_pair(backend, 3)
    _mk_manifest(backend, "m_credit", _CREDIT, [(key, credit_ref)])
    response = _submit(_client(backend), manifest_ids=["m_credit", "m_credit"],
                       features=["declared_income"])
    assert response.status_code == 400
    assert "duplicate" in response.json()["detail"].lower()
    assert _published_chunks(backend, "job_mm") == []


def test_ambiguous_distinct_refs_for_same_entity_source_fail_closed():
    backend = _mk_backend()
    key, credit_ref, tax_ref = _seed_pair(backend, 4)
    _seed_report(backend, key, _CREDIT, _credit_payload(income=1), ref="rep_c4_other")
    _mk_manifest(backend, "m_credit", _CREDIT, [(key, credit_ref)])
    _mk_manifest(backend, "m_credit2", _CREDIT, [(key, "rep_c4_other")])
    _mk_manifest(backend, "m_tax", _TAX, [(key, tax_ref)])
    response = _submit(_client(backend),
                       manifest_ids=["m_credit", "m_credit2", "m_tax"],
                       features=["declared_income"])
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "ambiguous" in detail.lower()
    assert "rep_c4" not in detail  # bounded, values/ref-free response
    assert _published_chunks(backend, "job_mm") == []


def test_exact_duplicate_binding_collapses_safely():
    backend = _mk_backend()
    key, credit_ref, tax_ref = _seed_pair(backend, 5)
    # re-ingestion manifest: SAME content-addressed ref, marked duplicate
    _mk_manifest(backend, "m_credit", _CREDIT, [(key, credit_ref)])
    _mk_manifest(backend, "m_credit_rerun", _CREDIT, [(key, credit_ref)],
                 statuses={0: ITEM_DUPLICATE})
    _mk_manifest(backend, "m_tax", _TAX, [(key, tax_ref)])
    response = _submit(_client(backend),
                       manifest_ids=["m_credit", "m_credit_rerun", "m_tax"],
                       features=["debt_to_income_ratio"])
    assert response.status_code == 202, response.text
    assert response.json()["total_items"] == 1


def test_global_incompatibilities_rejected_before_publish():
    backend = _mk_backend()
    key, credit_ref, _ = _seed_pair(backend, 6)
    _mk_manifest(backend, "m_credit", _CREDIT, [(key, credit_ref)])
    _mk_manifest(backend, "m_rows", _CREDIT, [(key, credit_ref)],
                 landing_form=LANDING_FEATURE_ROWS)
    _mk_manifest(backend, "m_other_entity", _CREDIT, [(key, credit_ref)],
                 entity_type="merchant")
    _mk_manifest(backend, "m_unknown_source", "unregistered_source",
                 [(key, credit_ref)])
    client = _client(backend)
    cases = [
        (["m_missing", "m_credit"], 404),
        (["m_rows", "m_credit"], 400),
        (["m_other_entity", "m_credit"], 400),
        (["m_unknown_source", "m_credit"], 400),
        ([], 400),
    ]
    for manifest_ids, expected in cases:
        response = _submit(client, manifest_ids=manifest_ids,
                           features=["declared_income"])
        assert response.status_code == expected, (manifest_ids, response.text)
    assert _published_chunks(backend, "job_mm") == []


# --- 13. replay idempotency ------------------------------------------------------

def test_replay_creates_no_duplicate_offline_rows():
    backend = _mk_backend()
    key, credit_ref, tax_ref = _seed_pair(backend, 8)
    _mk_manifest(backend, "m_credit", _CREDIT, [(key, credit_ref)])
    _mk_manifest(backend, "m_tax", _TAX, [(key, tax_ref)])
    client = _client(backend)
    first = _submit(client, manifest_ids=["m_credit", "m_tax"],
                    features=["debt_to_income_ratio"])
    second = _submit(client, manifest_ids=["m_tax", "m_credit"],
                     features=["debt_to_income_ratio"])
    assert first.status_code == second.status_code == 202
    assert first.json()["job_id"] == second.json()["job_id"] == "job_mm"

    chunks = _published_chunks(backend, "job_mm")
    assert {c.chunk_id for c in chunks} == {"job_mm:0"}  # same deterministic ids
    for chunk in chunks:  # process the SAME chunk twice = replay
        handle_batch_chunk(backend, chunk)
    entity = EntityKey.from_mapping(key, key_order=["user_id", "application_id"])
    records = backend.offline.get(entity, feature_name="debt_to_income_ratio",
                                  feature_version=1)
    assert len(records) == 1  # offline dedup: no duplicate logical rows


# --- 14. backward compatibility --------------------------------------------------

def test_singular_manifest_scope_response_unchanged():
    backend = _mk_backend()
    key, credit_ref, _ = _seed_pair(backend, 10)
    _mk_manifest(backend, "m_credit", _CREDIT, [(key, credit_ref)])
    client = _client(backend)
    response = client.post("/v1/batch/jobs", json={
        "view": _VIEW, "view_version": 1, "idempotency_key": "job_single",
        "requested_features": ["declared_income"], "chunk_size": 100,
        "scope": {"type": "source_dataset_manifest", "manifest_id": "m_credit"}})
    assert response.status_code == 202, response.text
    body = response.json()
    assert body["manifest_id"] == "m_credit"  # legacy singular field unchanged


# --- 15. durable provenance ------------------------------------------------------

def test_provenance_all_manifest_ids_durable_and_values_free():
    backend = _mk_backend()
    key, credit_ref, tax_ref = _seed_pair(backend, 11)
    _mk_manifest(backend, "m_credit", _CREDIT, [(key, credit_ref)])
    _mk_manifest(backend, "m_tax", _TAX, [(key, tax_ref)])
    client = _client(backend)
    response = _submit(client, manifest_ids=["m_tax", "m_credit"],
                       features=["debt_to_income_ratio"])
    assert response.status_code == 202
    assert sorted(response.json()["manifest_ids"]) == ["m_credit", "m_tax"]

    # operational status store retains the full set
    status = backend.batch_status.get("job_mm")
    assert sorted(status.manifest_ids) == ["m_credit", "m_tax"]
    assert sorted(status.to_dict()["manifest_ids"]) == ["m_credit", "m_tax"]
    # chunk events carry it for the durable projection; refs only, no payload values
    chunk = _published_chunks(backend, "job_mm")[0]
    assert sorted(chunk.manifest_ids) == ["m_credit", "m_tax"]
    assert "5000" not in json.dumps(status.to_dict())  # values-free job metadata

    # durable projection round-trip (record model)
    from fintech_feature_platform.fs_core.stores.batch_metadata import BatchJobRecord
    record = BatchJobRecord(
        job_id="job_mm", status="accepted", created_at=_TS, updated_at=_TS,
        manifest_ids=["m_credit", "m_tax"])
    assert BatchJobRecord.from_dict(record.to_dict()).manifest_ids == [
        "m_credit", "m_tax"]
