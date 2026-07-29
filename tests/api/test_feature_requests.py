"""Tests for the POST /v1/feature-requests producer + hybrid compute endpoints."""

import dataclasses
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from fintech_feature_platform.api.app import create_app
from fintech_feature_platform.api.backend import build_memory_backend
from fintech_feature_platform.fs_core.events.topics import FEATURE_COMPUTE_ONLINE
from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult
from fintech_feature_platform.fs_core.stores.request_result import RequestResult
from fintech_feature_platform.fs_core.stores.request_status import RequestStatus

_TS = datetime(2026, 6, 27, 10, tzinfo=UTC)


class _StubStatus:
    """Status store whose get() returns a configured terminal state (or None)."""

    def __init__(self, status=None, online=None, offline=None, error=None):
        self._cfg = (status, online, offline, error)
        self.puts: list = []

    def put(self, status):
        self.puts.append(status)

    def get(self, request_id):
        st, online, offline, error = self._cfg
        if st is None:
            return None
        return RequestStatus(
            request_id=request_id,
            job_id="job_x",
            status=st,
            entity_type="application",
            entity_key={"user_id": "1", "application_id": "A1"},
            view="user_credit_risk",
            view_version=1,
            requested_features=["declared_income"],
            online_write_status=online,
            offline_write_status=offline,
            created_at=_TS,
            updated_at=_TS,
            error=error,
        )

    def update(self, request_id, **changes):
        return None


def _seed_online(backend, value=4_200_000, name="declared_income", version=1):
    entity_key = EntityKey.from_mapping(
        {"user_id": "1", "application_id": "A1"}, key_order=["user_id", "application_id"]
    )
    backend.online.write(
        "user_credit_risk",
        1,
        FeatureResult(
            ref=FeatureRef(name, version),
            entity_key=entity_key,
            value=value,
            data_ts=_TS,
            calc_ts=_TS,
        ),
    )

class _StubResultStore:
    """Result store whose get() returns the given features for any request_id."""

    def __init__(self, features=None, statuses=None):
        self._features = features or {}
        self._statuses = statuses or {}

    def put(self, result):
        return None

    def get(self, request_id):
        if not self._features:
            return None
        return RequestResult(
            request_id=request_id,
            view="user_credit_risk",
            view_version=1,
            entity_key={"user_id": "1", "application_id": "A1"},
            features={
                name: {
                    "feature_version": 1,
                    "value": value,
                    "data_ts": _TS.isoformat(),
                    "max_input_data_ts": None,
                    "calc_ts": _TS.isoformat(),
                    "input_fingerprint": None,
                    "value_hash": None,
                    "online_write_status": self._statuses.get(name, "written"),
                }
                for name, value in self._features.items()
            },
            created_at=_TS,
        )


class _NeverReadOnline:
    """Online store stand-in: any read on the hybrid completed path is a bug."""

    def get(self, *args, **kwargs):
        raise AssertionError("hybrid completed path must not read online /latest")

    def write(self, *args, **kwargs):
        raise AssertionError("unexpected online write in this test")

    def write_many(self, *args, **kwargs):
        raise AssertionError("unexpected online write in this test")


_REQUEST = {
    "entity_type": "application",
    "entity_key": {"user_id": "1", "application_id": "A1"},
    "view": "user_credit_risk",
    "view_version": 1,
    "requested_feature_groups": ["pd_model_input_v1"],
    "requested_features": ["declared_income"],
    "reports": [
        {
            "source_name": "credit_report",
            "report_type": "credit_report",
            "report_ts": "2026-06-27T10:00:00Z",
            "payload": {"declared_income": 4_200_000, "monthly_obligations": 800_000},
        }
    ],
}


def test_feature_request_returns_accepted_and_report_refs():
    client = TestClient(create_app(build_memory_backend()))
    resp = client.post("/v1/feature-requests", json=_REQUEST)

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "accepted"
    assert body["request_id"].startswith("freq_")
    assert body["job_id"].startswith("job_")
    assert body["status_url"] == f"/v1/feature-requests/{body['request_id']}"
    assert len(body["report_refs"]) == 1


def test_feature_request_publishes_exactly_one_online_event():
    backend = build_memory_backend()
    client = TestClient(create_app(backend))

    client.post("/v1/feature-requests", json=_REQUEST)

    assert len(backend.events.published) == 1
    record = backend.events.published[0]
    assert record.topic == FEATURE_COMPUTE_ONLINE
    # partition key is the deterministic encoded entity key
    assert record.key == "application_id=A1|user_id=1"
    event = record.event
    assert event.event_type == "feature_compute.requested"
    # view/view_version are forwarded from the request into the event
    assert event.view == "user_credit_risk"
    assert event.view_version == 1
    assert event.requested_feature_groups == ["pd_model_input_v1"]
    # the event is self-contained: object_key present internally for workers
    assert event.reports[0].object_key
    assert event.reports[0].report_ref == backend.events.published[0].event.reports[0].report_ref
    # report_type is carried on the descriptor (so the Metadata Writer can project it)
    assert event.reports[0].report_type == "credit_report"


def test_submit_sets_timezone_aware_deadline_fields():
    backend = build_memory_backend()
    client = TestClient(create_app(backend))

    client.post("/v1/feature-requests", json={**_REQUEST, "deadline_ms": 2000})

    event = backend.events.published[0].event
    assert event.event_ts is not None and event.event_ts.tzinfo is not None
    assert event.expires_at is not None and event.expires_at.tzinfo is not None
    # expires_at = event_ts + deadline_ms (translated from the relative deadline).
    assert event.expires_at > event.event_ts
    assert (event.expires_at - event.event_ts).total_seconds() == 2.0
    assert event.deadline_ms == 2000


def test_submit_clamps_deadline_ms_to_ceiling(monkeypatch):
    monkeypatch.setenv("FSP_ONLINE_MAX_DEADLINE_MS", "5000")
    backend = build_memory_backend()
    client = TestClient(create_app(backend))

    client.post("/v1/feature-requests", json={**_REQUEST, "deadline_ms": 999_999})

    event = backend.events.published[0].event
    assert event.deadline_ms == 5000  # clamped
    assert (event.expires_at - event.event_ts).total_seconds() == 5.0


def test_feature_request_does_not_write_raw_metadata_synchronously():
    backend = build_memory_backend()
    client = TestClient(create_app(backend))
    resp = client.post("/v1/feature-requests", json=_REQUEST)
    report_ref = resp.json()["report_refs"][0]

    # raw_reports_meta is now projected asynchronously by the Metadata Writer; the
    # submit path must NOT have written it synchronously.
    with pytest.raises(KeyError):
        backend.metas.get_meta(report_ref)


def test_feature_request_stores_raw_payload_before_publish():
    backend = build_memory_backend()
    client = TestClient(create_app(backend))
    client.post("/v1/feature-requests", json=_REQUEST)

    # The raw payload is stored (accepted boundary) under the descriptor's object_key.
    object_key = backend.events.published[0].event.reports[0].object_key
    assert backend.payloads.get_payload(object_key) == {
        "declared_income": 4_200_000,
        "monthly_obligations": 800_000,
    }


def test_hybrid_compute_publishes_descriptor_with_report_type():
    backend = build_memory_backend()
    client = TestClient(create_app(backend))
    client.post(
        "/v1/feature-requests/compute", json={**_REQUEST, "wait_timeout_ms": 0}
    )
    assert backend.events.published[0].event.reports[0].report_type == "credit_report"


def test_feature_request_response_does_not_expose_internal_pointers():
    client = TestClient(create_app(build_memory_backend()))
    resp = client.post("/v1/feature-requests", json=_REQUEST)

    assert "object_key" not in resp.text
    assert "storage_uri" not in resp.text
    assert "declared_income" not in resp.text  # no raw payload echoed back


def test_feature_request_requires_at_least_one_report():
    client = TestClient(create_app(build_memory_backend()))
    bad = {**_REQUEST, "reports": []}
    resp = client.post("/v1/feature-requests", json=bad)
    assert resp.status_code == 400


def test_feature_request_does_not_compute_or_write_stores():
    backend = build_memory_backend()
    client = TestClient(create_app(backend))
    client.post("/v1/feature-requests", json=_REQUEST)

    # The producer stores the raw payload (metadata is projected async) but must NOT
    # compute features:
    # no offline history and no online latest are written by this endpoint.
    from fintech_feature_platform.fs_core.models import EntityKey

    entity_key = EntityKey.from_mapping(
        {"user_id": "1", "application_id": "A1"}, key_order=["user_id", "application_id"]
    )
    assert backend.offline.get(entity_key) == []
    assert (
        backend.online.get("user_credit_risk", 1, entity_key, "declared_income", 1)
        is None
    )


# --- request status  ---------------------------------------------

def test_feature_request_initializes_accepted_status():
    backend = build_memory_backend()
    client = TestClient(create_app(backend))
    resp = client.post("/v1/feature-requests", json=_REQUEST)

    request_id = resp.json()["request_id"]
    status = backend.status.get(request_id)
    assert status is not None
    assert status.status == "accepted"
    assert status.metadata_write_status == "pending"
    assert status.online_write_status is None
    assert status.view == "user_credit_risk"


def test_get_feature_request_returns_status():
    backend = build_memory_backend()
    client = TestClient(create_app(backend))
    request_id = client.post("/v1/feature-requests", json=_REQUEST).json()["request_id"]

    resp = client.get(f"/v1/feature-requests/{request_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["request_id"] == request_id
    assert body["status"] == "accepted"
    assert body["view"] == "user_credit_risk"


def test_get_feature_request_missing_returns_404():
    client = TestClient(create_app(build_memory_backend()))
    resp = client.get("/v1/feature-requests/freq_does_not_exist")
    assert resp.status_code == 404


def test_get_feature_request_does_not_expose_internal_fields():
    backend = build_memory_backend()
    client = TestClient(create_app(backend))
    request_id = client.post("/v1/feature-requests", json=_REQUEST).json()["request_id"]

    resp = client.get(f"/v1/feature-requests/{request_id}")
    assert "object_key" not in resp.text
    assert "storage_uri" not in resp.text
    assert "source_payload_b64" not in resp.text
    # requested_features (names) may appear, but no feature *values* / raw payload
    assert "4200000" not in resp.text


def test_feature_request_status_write_failure_does_not_fail_accept():
    class _FailingStatus:
        def put(self, status):
            raise RuntimeError("valkey down")

        def get(self, request_id):
            return None

        def update(self, request_id, **changes):
            return None

    backend = dataclasses.replace(build_memory_backend(), status=_FailingStatus())
    client = TestClient(create_app(backend))
    resp = client.post("/v1/feature-requests", json=_REQUEST)

    assert resp.status_code == 200
    assert resp.json()["status"] == "accepted"


# --- hybrid endpoint  --------------------------------------------

def test_hybrid_pending_on_timeout():
    # real backend -> status stays "accepted"; wait_timeout_ms=0 -> immediate pending
    client = TestClient(create_app(build_memory_backend()))
    resp = client.post(
        "/v1/feature-requests/compute", json={**_REQUEST, "wait_timeout_ms": 0}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "pending"
    assert body["request_id"].startswith("freq_")
    assert body["status_url"].endswith(body["request_id"])
    assert body.get("features") is None


def test_hybrid_completed_returns_request_scoped_values():
    backend = build_memory_backend()
    # Seed a DIFFERENT (fresher) /latest value: the hybrid response must NOT return it.
    _seed_online(backend, value=9_999_999)
    backend = dataclasses.replace(
        backend,
        status=_StubStatus(status="completed", online="written", offline="pending"),
        results=_StubResultStore({"declared_income": 4_200_000}),
    )
    client = TestClient(create_app(backend))
    resp = client.post(
        "/v1/feature-requests/compute",
        json={**_REQUEST, "requested_feature_groups": [], "wait_timeout_ms": 1000},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    # Request-scoped write-set value, not the seeded Valkey /latest value.
    assert body["features"]["declared_income"]["value"] == 4_200_000
    assert body["missing_features"] == []
    # completion is online-only; offline write may still be pending
    assert body["offline_write_status"] == "pending"


def test_hybrid_completed_never_reads_online_latest():
    backend = dataclasses.replace(
        build_memory_backend(),
        online=_NeverReadOnline(),
        status=_StubStatus(status="completed", online="written", offline="pending"),
        results=_StubResultStore({"declared_income": 4_200_000}),
    )
    client = TestClient(create_app(backend))
    resp = client.post(
        "/v1/feature-requests/compute",
        json={**_REQUEST, "requested_feature_groups": [], "wait_timeout_ms": 1000},
    )
    assert resp.status_code == 200
    assert resp.json()["features"]["declared_income"]["value"] == 4_200_000


def test_hybrid_skipped_stale_is_still_completed_with_request_values():
    backend = build_memory_backend()
    _seed_online(backend, value=9_999_999)  # fresher online value stays untouched
    backend = dataclasses.replace(
        backend,
        status=_StubStatus(
            status="completed", online="skipped_stale", offline="pending"
        ),
        results=_StubResultStore(
            {"declared_income": 4_200_000},
            statuses={"declared_income": "skipped_stale"},
        ),
    )
    client = TestClient(create_app(backend))
    resp = client.post(
        "/v1/feature-requests/compute",
        json={**_REQUEST, "requested_feature_groups": [], "wait_timeout_ms": 1000},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    assert body["online_write_status"] == "skipped_stale"
    # The caller still gets its own computed value.
    assert body["features"]["declared_income"]["value"] == 4_200_000


def test_hybrid_completed_result_missing_is_explicit():
    # status=completed but the request result expired -> explicit result_missing,
    # never a fallback to /latest (which holds a value here).
    backend = build_memory_backend()
    _seed_online(backend, value=9_999_999)
    backend = dataclasses.replace(
        backend,
        status=_StubStatus(status="completed", online="written", offline="pending"),
        results=_StubResultStore(),  # empty -> get() returns None
    )
    client = TestClient(create_app(backend))
    resp = client.post(
        "/v1/feature-requests/compute",
        json={**_REQUEST, "requested_feature_groups": [], "wait_timeout_ms": 1000},
    )
    body = resp.json()
    assert body["status"] == "completed"
    assert body["features"] == {}
    assert body["missing_features"] == ["declared_income"]
    assert body["error"] == "result_missing"


def test_hybrid_deadline_expired_reports_outcome_without_latest_or_result():
    # status=completed + online_write_status=deadline_expired: report the deadline
    # outcome explicitly. Must not read /latest and must not need a result record.
    backend = dataclasses.replace(
        build_memory_backend(),
        online=_NeverReadOnline(),
        status=_StubStatus(status="completed", online="deadline_expired", offline=None),
        results=_StubResultStore(),  # no result values exist for an expired request
    )
    client = TestClient(create_app(backend))
    resp = client.post(
        "/v1/feature-requests/compute",
        json={**_REQUEST, "requested_feature_groups": [], "wait_timeout_ms": 1000},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "deadline_expired"
    assert body["online_write_status"] == "deadline_expired"
    assert body["features"] == {}
    # not a result_missing false alarm
    assert body["error"] is None


def test_hybrid_completed_with_missing_features():
    # status completed but no online value seeded -> missing
    backend = dataclasses.replace(
        build_memory_backend(),
        status=_StubStatus(status="completed", online="written", offline="pending"),
    )
    client = TestClient(create_app(backend))
    resp = client.post(
        "/v1/feature-requests/compute",
        json={**_REQUEST, "requested_feature_groups": [], "wait_timeout_ms": 1000},
    )
    body = resp.json()
    assert body["status"] == "completed"
    assert body["features"] == {}
    assert body["missing_features"] == ["declared_income"]


def test_hybrid_failed_when_status_failed():
    backend = dataclasses.replace(
        build_memory_backend(), status=_StubStatus(status="failed", error="boom")
    )
    client = TestClient(create_app(backend))
    resp = client.post(
        "/v1/feature-requests/compute", json={**_REQUEST, "wait_timeout_ms": 1000}
    )
    body = resp.json()
    assert body["status"] == "failed"
    assert body["error"] == "boom"


def test_hybrid_response_does_not_expose_internal_pointers():
    backend = build_memory_backend()
    _seed_online(backend)
    backend = dataclasses.replace(
        backend, status=_StubStatus(status="completed", online="written", offline="pending")
    )
    client = TestClient(create_app(backend))
    resp = client.post(
        "/v1/feature-requests/compute", json={**_REQUEST, "wait_timeout_ms": 1000}
    )
    assert "object_key" not in resp.text
    assert "storage_uri" not in resp.text
    assert "source_payload_b64" not in resp.text


def test_hybrid_clamps_wait_timeout(monkeypatch):
    # cap to 0 -> even a huge requested wait returns pending immediately (no long wait)
    monkeypatch.setenv("FSP_HYBRID_MAX_WAIT_MS", "0")
    client = TestClient(create_app(build_memory_backend()))
    resp = client.post(
        "/v1/feature-requests/compute", json={**_REQUEST, "wait_timeout_ms": 9_999_999}
    )
    assert resp.json()["status"] == "pending"


def test_hybrid_status_store_failure_during_wait_returns_pending():
    class _GetRaisesStatus:
        def put(self, status):
            return None

        def get(self, request_id):
            raise RuntimeError("valkey down")

        def update(self, request_id, **changes):
            return None

    backend = dataclasses.replace(build_memory_backend(), status=_GetRaisesStatus())
    client = TestClient(create_app(backend))
    resp = client.post(
        "/v1/feature-requests/compute", json={**_REQUEST, "wait_timeout_ms": 0}
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "pending"


def test_hybrid_unknown_group_returns_400_and_does_not_publish():
    backend = build_memory_backend()
    client = TestClient(create_app(backend))
    bad = {**_REQUEST, "requested_features": [], "requested_feature_groups": ["nope"]}
    resp = client.post("/v1/feature-requests/compute", json=bad)
    assert resp.status_code == 400
    assert backend.events.published == []  # validation failed before publish


def test_hybrid_group_only_returns_expanded_output_values():
    backend = build_memory_backend()
    # pd_model_input_v1 expands to [declared_income, debt_to_income_ratio]
    backend = dataclasses.replace(
        backend,
        status=_StubStatus(status="completed", online="written", offline="pending"),
        results=_StubResultStore(
            {"declared_income": 4_200_000, "debt_to_income_ratio": 0.25}
        ),
    )
    client = TestClient(create_app(backend))
    resp = client.post(
        "/v1/feature-requests/compute",
        json={
            **_REQUEST,
            "requested_features": [],
            "requested_feature_groups": ["pd_model_input_v1"],
            "wait_timeout_ms": 1000,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    assert body["features"]["declared_income"]["value"] == 4_200_000
    assert body["features"]["debt_to_income_ratio"]["value"] == 0.25
    assert body["missing_features"] == []


def test_async_group_only_publishes_event_with_raw_groups():
    backend = build_memory_backend()
    client = TestClient(create_app(backend))
    resp = client.post(
        "/v1/feature-requests",
        json={
            **_REQUEST,
            "requested_features": [],
            "requested_feature_groups": ["pd_model_input_v1"],
        },
    )
    assert resp.status_code == 200
    event = backend.events.published[0].event
    # event carries the RAW groups (not the expanded plan)
    assert event.requested_feature_groups == ["pd_model_input_v1"]
    assert event.requested_features == []


def test_async_unknown_feature_returns_400_and_does_not_publish():
    backend = build_memory_backend()
    client = TestClient(create_app(backend))
    bad = {**_REQUEST, "requested_features": ["nope"], "requested_feature_groups": []}
    resp = client.post("/v1/feature-requests", json=bad)
    assert resp.status_code == 400
    assert backend.events.published == []


def test_status_requested_features_are_expanded_outputs():
    backend = build_memory_backend()
    client = TestClient(create_app(backend))
    request_id = client.post(
        "/v1/feature-requests",
        json={
            **_REQUEST,
            "requested_features": [],
            "requested_feature_groups": ["pd_model_input_v1"],
        },
    ).json()["request_id"]
    status = backend.status.get(request_id)
    # expanded output set in status; raw groups preserved
    assert status.requested_features == ["declared_income", "debt_to_income_ratio"]
    assert status.requested_feature_groups == ["pd_model_input_v1"]


def test_hybrid_publishes_same_event_shape_as_async():
    backend_async = build_memory_backend()
    backend_hybrid = build_memory_backend()
    TestClient(create_app(backend_async)).post("/v1/feature-requests", json=_REQUEST)
    TestClient(create_app(backend_hybrid)).post(
        "/v1/feature-requests/compute", json={**_REQUEST, "wait_timeout_ms": 0}
    )

    a = backend_async.events.published[0]
    h = backend_hybrid.events.published[0]
    assert a.topic == h.topic == FEATURE_COMPUTE_ONLINE
    assert a.event.event_type == h.event.event_type == "feature_compute.requested"
    assert a.event.view == h.event.view
    assert a.event.view_version == h.event.view_version
    assert a.event.entity.entity_key == h.event.entity.entity_key
    assert a.event.requested_features == h.event.requested_features
    assert len(a.event.reports) == len(h.event.reports) == 1
    assert a.event.reports[0].source_name == h.event.reports[0].source_name
