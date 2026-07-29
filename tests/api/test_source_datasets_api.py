"""Tests for the /v1/source-datasets ingestion API."""

import json

from fastapi.testclient import TestClient

from fintech_feature_platform.api.app import create_app
from fintech_feature_platform.api.backend import build_memory_backend


def _row(customer_id="c1", *, event_ts="2026-07-01T10:00:00Z", income=1000):
    return json.dumps(
        {
            "entity_key": {"customer_id": customer_id},
            "event_ts": event_ts,
            "payload": {"income": income},
        }
    )


def _body(lines, **overrides):
    body = {
        "entity_type": "customer",
        "source_name": "bureau",
        "report_type": "credit_report",
        "lines": lines,
    }
    body.update(overrides)
    return body


def _client(backend=None):
    return TestClient(create_app(backend or build_memory_backend()))


def test_ingest_jsonl_returns_manifest_with_counts_and_watermarks():
    resp = _client().post(
        "/v1/source-datasets/ingest-jsonl", json=_body([_row("c1"), _row("c2")])
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["manifest_id"].startswith("sdm_")
    assert body["status"] == "completed"
    assert body["item_count_read"] == 2
    assert body["item_count_written"] == 2
    assert body["item_count_duplicate"] == 0
    assert body["item_count_rejected"] == 0
    assert body["watermark_min_event_ts"] is not None
    assert body["detail_url"] == f"/v1/source-datasets/{body['manifest_id']}"


def test_ingest_response_contains_no_raw_payload():
    resp = _client().post(
        "/v1/source-datasets/ingest-jsonl", json=_body([_row(income="SENTINEL_INCOME")])
    )
    # Alphabetic sentinel can't collide with the random manifest/dataset UUID hex.
    assert "SENTINEL_INCOME" not in resp.text  # no payload values in the response
    assert "storage_uri" not in resp.text
    assert "object_key" not in resp.text
    assert "payload" not in resp.json()


def test_get_source_dataset_returns_manifest():
    backend = build_memory_backend()
    client = _client(backend)
    manifest_id = client.post(
        "/v1/source-datasets/ingest-jsonl", json=_body([_row()])
    ).json()["manifest_id"]

    resp = client.get(f"/v1/source-datasets/{manifest_id}")
    assert resp.status_code == 200
    assert resp.json()["manifest_id"] == manifest_id
    assert resp.json()["item_count_written"] == 1


def test_get_unknown_manifest_returns_404():
    resp = _client().get("/v1/source-datasets/sdm_nope")
    assert resp.status_code == 404


def test_ingest_rejects_register_in_place_with_400():
    resp = _client().post(
        "/v1/source-datasets/ingest-jsonl",
        json=_body([_row()], copy_mode="register_in_place"),
    )
    assert resp.status_code == 400


def test_ingest_bad_row_counts_rejected_but_returns_201():
    resp = _client().post(
        "/v1/source-datasets/ingest-jsonl", json=_body([_row("c1"), "not-json{"])
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["item_count_written"] == 1
    assert body["item_count_rejected"] == 1


# --- DWH endpoints  ------------------------------------------------

def test_ingest_dwh_json_endpoint_lands_and_returns_manifest():
    backend = build_memory_backend()
    resp = _client(backend).post(
        "/v1/source-datasets/ingest-dwh-json",
        json={
            "entity_type": "customer",
            "source_name": "bureau",
            "report_type": "credit_report",
            "rows": [
                {"entity_key": {"customer_id": "c1"},
                 "event_ts": "2026-07-01T10:00:00Z",
                 "payload_json": {"bureau_score": "SENTINEL_SCORE"}},
            ],
        },
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["source_kind"] == "dwh_json_reports"
    assert body["landing_form"] == "raw_reports"
    assert body["item_count_written"] == 1
    # no raw payload values in the response (alphabetic sentinel — no UUID-hex collision)
    assert "SENTINEL_SCORE" not in resp.text
    assert "payload" not in body


def test_import_dwh_features_endpoint_lands_offline():
    resp = _client().post(
        "/v1/source-datasets/import-dwh-features",
        json={
            "entity_type": "application",
            "view": "user_credit_risk",
            "view_version": 1,
            "rows": [
                {"entity_key": {"user_id": "1", "application_id": "A1"},
                 "feature_name": "declared_income", "feature_version": 1,
                 "value": "SENTINEL_VALUE", "data_ts": "2026-07-01T00:00:00Z",
                 "calc_ts": "2026-07-01T01:00:00Z"},
            ],
        },
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["source_kind"] == "dwh_table"
    assert body["landing_form"] == "feature_rows"
    assert body["view"] == "user_credit_risk"
    assert body["item_count_written"] == 1
    # No feature values in the response (alphabetic sentinel can't collide with the
    # random manifest/dataset UUID hex, unlike a numeric value).
    assert "SENTINEL_VALUE" not in resp.text


def test_import_dwh_features_unknown_view_returns_400():
    resp = _client().post(
        "/v1/source-datasets/import-dwh-features",
        json={"entity_type": "application", "view": "nope", "view_version": 1, "rows": []},
    )
    assert resp.status_code == 400
