"""Scripted fake for the fsctl data-workflow HTTP seam.

The data-workflow commands route every HTTP call through
``fintech_feature_platform.cli.data_workflow._urlopen(request, timeout)``.
``install`` replaces that seam with a transport that records every request and
replays a scripted response list, and pins a clean auth environment.
"""

from __future__ import annotations

import io
import json
import urllib.error

TEST_API_KEY = "sentinel-test-key-1234567890"


class FakeResponse:
    def __init__(self, body: dict | list | bytes):
        self._body = body if isinstance(body, bytes) else json.dumps(body).encode()

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def http_error(code: int, detail, url: str = "http://api.test/x") -> urllib.error.HTTPError:
    body = json.dumps({"detail": detail}).encode()
    return urllib.error.HTTPError(url, code, "error", {}, io.BytesIO(body))


class FakeTransport:
    """Replays scripted responses in order; records every (Request, timeout) pair."""

    def __init__(self, *scripted):
        self.scripted = list(scripted)
        self.requests: list[tuple] = []

    def __call__(self, request, timeout):
        self.requests.append((request, timeout))
        item = self.scripted.pop(0)
        if isinstance(item, Exception):
            raise item
        return item if isinstance(item, FakeResponse) else FakeResponse(item)

    def bodies(self) -> list:
        return [
            json.loads(request.data.decode("utf-8"))
            for request, _ in self.requests
            if request.data is not None
        ]

    def urls(self) -> list[str]:
        return [request.full_url for request, _ in self.requests]


def install(monkeypatch, *scripted, key: str | None = TEST_API_KEY) -> FakeTransport:
    transport = FakeTransport(*scripted)
    monkeypatch.setattr(
        "fintech_feature_platform.cli.data_workflow._urlopen", transport
    )
    for var in (
        "CLEARFEATURE_API_URL",
        "FSP_API_URL",
        "CLEARFEATURE_API_KEY",
        "FSP_CLIENT_API_KEY",
    ):
        monkeypatch.delenv(var, raising=False)
    if key is not None:
        monkeypatch.setenv("CLEARFEATURE_API_KEY", key)
    return transport


# --- canonical server responses (mirroring the current public contracts) -------

def manifest_response(**over) -> dict:
    out = {
        "manifest_id": "sdm_0123456789abcdef0123456789abcdef",
        "dataset_id": "ds_0123456789abcdef0123456789abcdef",
        "source_kind": "object_storage_jsonl",
        "landing_form": "raw_reports",
        "entity_type": "application_snapshot",
        "source_name": "socdem_report",
        "report_type": "socdem_report",
        "view": None,
        "view_version": None,
        "copy_mode": "copy",
        "status": "completed",
        "item_count_read": 2,
        "item_count_written": 2,
        "item_count_duplicate": 0,
        "item_count_rejected": 0,
        "watermark_min_event_ts": "2026-05-01T09:00:00+00:00",
        "watermark_max_event_ts": "2026-05-02T09:00:00+00:00",
        "content_hash": "sha256:00",
        "detail_url": "/v1/source-datasets/sdm_0123456789abcdef0123456789abcdef",
    }
    out.update(over)
    return out


def accept_response(job_id: str, **over) -> dict:
    out = {
        "job_id": job_id,
        "status": "accepted",
        "chunk_count": 10,
        "total_items": 1000,
        "manifest_id": None,
        "manifest_ids": ["sdm_a", "sdm_b"],
    }
    out.update(over)
    return out


def job_status(job_id: str, status: str = "completed", chunks: dict | None = None, **over) -> dict:
    if chunks is None:
        chunks = {
            f"{job_id}:0": {
                "chunk_id": f"{job_id}:0",
                "chunk_index": 0,
                "status": status,
                "item_count": 1000,
                "ok_items": 1000,
                "failed_items": 0,
                "first_errors": [],
                "updated_at": "2026-08-05T10:00:00+00:00",
                "finished_at": "2026-08-05T10:00:01+00:00",
            }
        }
    out = {
        "job_id": job_id,
        "status": status,
        "view": "credit_model_inputs",
        "view_version": 1,
        "requested_features": [],
        "requested_feature_groups": ["model_inputs"],
        "total_items": 1000,
        "chunk_count": len(chunks),
        "write_online": False,
        "completed_chunks": sum(1 for c in chunks.values() if c["status"] == "completed"),
        "failed_chunks": sum(1 for c in chunks.values() if c["status"] == "failed"),
        "failed_items": sum(c["failed_items"] for c in chunks.values()),
        "chunks": chunks,
        "created_at": "2026-08-05T10:00:00+00:00",
        "updated_at": "2026-08-05T10:00:01+00:00",
        "finished_at": "2026-08-05T10:00:01+00:00",
        "error_summary": {},
        "manifest_id": None,
        "manifest_ids": ["sdm_a", "sdm_b"],
    }
    out.update(over)
    return out


def training_response(rows: list | None = None, **summary_over) -> dict:
    if rows is None:
        rows = [
            {
                "entity": {"user_id": "u0000", "application_id": "a0000", "report_id": "r0000"},
                "observation_ts": "2026-05-01T11:00:00+00:00",
                "context": None,
                "features": {"monthly_income": 2148.95},
                "feature_metadata": {
                    "monthly_income": {
                        "status": "ok",
                        "feature_version": 1,
                        "data_ts": "2026-05-01T09:00:00+00:00",
                    }
                },
            }
        ]
    summary = {
        "rows": len(rows),
        "features": 1,
        "missing_values": 0,
        "future_records_ignored": 0,
        "safety_gap_seconds": 0,
    }
    summary.update(summary_over)
    return {"rows": rows, "summary": summary}
