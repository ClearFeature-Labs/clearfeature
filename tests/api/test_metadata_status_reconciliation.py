"""Durable metadata-status reconciliation.

PostgreSQL is the durable source of truth for the metadata projection; Valkey is
operational state that may lag. A `pending` metadata status is reconciled against the
durable request projection by GET /v1/feature-requests/{id}, with a monotonic
best-effort read-repair. Failure cases use faithful failing fakes, never only the
happy path.
"""

import dataclasses
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from fintech_feature_platform.api.app import create_app
from fintech_feature_platform.api.backend import build_backend
from fintech_feature_platform.api.security import SecurityConfig
from fintech_feature_platform.api.settings import load_settings
from fintech_feature_platform.fs_core.stores.metadata import RequestMetadata
from fintech_feature_platform.fs_core.stores.request_status import RequestStatus

_NOW = datetime(2026, 7, 19, 12, tzinfo=UTC)


def _status(metadata_write_status="pending") -> RequestStatus:
    return RequestStatus(
        request_id="freq_rec_1", job_id="job_1", status="completed",
        entity_type="application",
        entity_key={"user_id": "1", "application_id": "A1"},
        view="user_credit_risk", view_version=1,
        created_at=_NOW, updated_at=_NOW,
        online_write_status="written", offline_write_status="written",
        metadata_write_status=metadata_write_status,
    )


def _durable(status="completed") -> RequestMetadata:
    return RequestMetadata(request_id="freq_rec_1", updated_at=_NOW, status=status)


class _FailingPutStatusStore:
    """Valkey down for writes, up for reads — the lagging-operational-store case."""

    def __init__(self, inner) -> None:
        self._inner = inner
        self.put_attempts = 0

    def put(self, status) -> None:
        self.put_attempts += 1
        raise RuntimeError("valkey down")

    def get(self, request_id):
        return self._inner.get(request_id)


def _client(backend) -> TestClient:
    return TestClient(create_app(
        backend,
        security=SecurityConfig(mode="disabled", environment="development", keys=()),
    ))


def _backend():
    return build_backend(load_settings({"FSP_BACKEND": "memory"}))


def _get(client):
    return client.get("/v1/feature-requests/freq_rec_1").json()


def test_pending_with_durable_terminal_projection_reports_written_and_repairs():
    backend = _backend()
    backend.status.put(_status("pending"))
    backend.metadata.upsert_request(_durable("completed"))
    body = _get(_client(backend))
    assert body["metadata_write_status"] == "written"
    # Read-repair persisted: the operational store now agrees with durable evidence.
    assert backend.status.get("freq_rec_1").metadata_write_status == "written"


def test_valkey_write_failure_still_reports_written_from_durable_evidence():
    backend = _backend()
    backend.status.put(_status("pending"))
    backend.metadata.upsert_request(_durable("completed"))
    failing = _FailingPutStatusStore(backend.status)
    body = _get(_client(dataclasses.replace(backend, status=failing)))
    assert body["metadata_write_status"] == "written"  # durable truth wins
    assert failing.put_attempts == 1  # repair attempted, failure swallowed
    # Operational store still lags — acceptable; a later read repairs it (below).


def test_read_repair_after_valkey_recovery_is_monotonic_and_idempotent():
    backend = _backend()
    backend.status.put(_status("pending"))
    backend.metadata.upsert_request(_durable("completed"))
    client = _client(backend)
    first = _get(client)
    second = _get(client)  # already repaired: reconciliation is a no-op passthrough
    assert first["metadata_write_status"] == second["metadata_write_status"] == "written"
    assert backend.status.get("freq_rec_1").metadata_write_status == "written"


def test_written_never_regresses():
    backend = _backend()
    backend.status.put(_status("written"))
    backend.metadata.upsert_request(_durable("accepted"))  # even with stale durable row
    assert _get(_client(backend))["metadata_write_status"] == "written"


def test_no_durable_projection_stays_pending():
    backend = _backend()
    backend.status.put(_status("pending"))
    assert _get(_client(backend))["metadata_write_status"] == "pending"


def test_non_terminal_durable_projection_stays_pending():
    backend = _backend()
    backend.status.put(_status("pending"))
    backend.metadata.upsert_request(_durable("accepted"))
    assert _get(_client(backend))["metadata_write_status"] == "pending"


def test_legacy_status_without_metadata_field_passes_through():
    backend = _backend()
    backend.status.put(_status(None))
    backend.metadata.upsert_request(_durable("completed"))
    assert _get(_client(backend))["metadata_write_status"] is None


def test_metadata_store_read_failure_degrades_to_pending():
    class _BrokenMetadata:
        def get_request(self, request_id):
            raise RuntimeError("postgres down")

    backend = _backend()
    backend.status.put(_status("pending"))
    body = _get(_client(dataclasses.replace(backend, metadata=_BrokenMetadata())))
    assert body["metadata_write_status"] == "pending"  # degrade, never fail the read


def test_api_and_durable_projection_agree_after_recovery():
    backend = _backend()
    backend.status.put(_status("pending"))
    backend.metadata.upsert_request(_durable("completed"))
    client = _client(backend)
    body = _get(client)
    durable = backend.metadata.get_request("freq_rec_1")
    assert body["metadata_write_status"] == "written"
    assert durable.status == "completed"
    assert backend.status.get("freq_rec_1").metadata_write_status == "written"
