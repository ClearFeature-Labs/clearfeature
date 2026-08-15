"""Public data-workflow verbs for ``fsctl``.

``ingest``, ``materialize`` and ``build-training-dataset`` are THIN adapters over
the public HTTP API:

    ingest                 -> POST /v1/source-datasets/ingest-jsonl
    materialize            -> POST /v1/batch/jobs (+ GET /v1/batch/jobs/{job_id})
    build-training-dataset -> POST /v1/training-datasets/build

The server owns ingestion persistence, manifest creation, multi-manifest join,
feature planning, DAG compute, offline writes, job status, and PIT training
construction. This module may only read files, validate transport shape, submit
requests, poll status, and save public responses. It must stay stdlib-only
(base installs carry no HTTP client library) and must never import platform
runtime modules — enforced by tests/cli/test_fsctl_data_workflow_boundary.py.

Authentication is environment-only: CLEARFEATURE_API_KEY (or the established
FSP_CLIENT_API_KEY). The key travels as ``Authorization: Bearer`` and is never
echoed in output, errors, or saved files.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

API_URL_ENV = "CLEARFEATURE_API_URL"
API_URL_ENV_FALLBACK = "FSP_API_URL"
API_KEY_ENV = "CLEARFEATURE_API_KEY"
API_KEY_ENV_FALLBACK = "FSP_CLIENT_API_KEY"
DEFAULT_API_URL = "http://127.0.0.1:8000"

# Pinned client-side so the derived idempotency key cannot drift with a server
# default change; matches the server default for POST /v1/batch/jobs.
DEFAULT_CHUNK_SIZE = 100

# Versioned so a future change to the derivation input set cannot silently
# collide with keys derived by older fsctl builds.
IDEMPOTENCY_CONTRACT = "fsctl-materialize-v1"

TERMINAL_JOB_STATUSES = ("completed", "completed_with_errors", "failed", "publish_failed")
_DETAIL_LIMIT = 500
_FIRST_ERRORS_LIMIT = 5


class ApiClientError(ValueError):
    """Expected user/config/API failure — mapped to {"ok": false, "errors": [...]}."""


# --- configuration ------------------------------------------------------------

def resolve_api_url(flag_value: str | None) -> str:
    url = (
        flag_value
        or os.environ.get(API_URL_ENV)
        or os.environ.get(API_URL_ENV_FALLBACK)
        or DEFAULT_API_URL
    )
    return url.rstrip("/")


def resolve_api_key() -> str:
    key = os.environ.get(API_KEY_ENV) or os.environ.get(API_KEY_ENV_FALLBACK)
    if not key:
        raise ApiClientError(
            f"no API key configured: set {API_KEY_ENV} (or {API_KEY_ENV_FALLBACK}) in the "
            "environment; keys are never read from flags or feature_project.yaml"
        )
    return key


# --- HTTP boundary ------------------------------------------------------------

def _urlopen(request: urllib.request.Request, timeout: float):  # pragma: no cover - seam
    return urllib.request.urlopen(request, timeout=timeout)  # noqa: S310 (http API only)


def _bounded(text: str, limit: int = _DETAIL_LIMIT) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _bounded_detail(body: bytes) -> str:
    try:
        detail = json.loads(body.decode("utf-8", errors="replace")).get("detail")
    except (ValueError, AttributeError):
        return "(non-JSON error body)"
    if detail is None:
        return "(no detail)"
    if isinstance(detail, str):
        return _bounded(detail)
    return _bounded(json.dumps(detail, sort_keys=True))


def api_request(
    api_url: str,
    path: str,
    *,
    api_key: str,
    method: str = "GET",
    payload: dict | None = None,
    timeout: float = 60.0,
) -> Any:
    """One bounded JSON round-trip. Raises ApiClientError; never echoes the key."""
    headers = {"Accept": "application/json", "Authorization": f"Bearer {api_key}"}
    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        api_url + path, data=body, headers=headers, method=method
    )
    try:
        with _urlopen(request, timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        detail = _bounded_detail(exc.read())
        hint = ""
        if exc.code in (401, 403):
            hint = f" — check {API_KEY_ENV} / {API_KEY_ENV_FALLBACK}"
        raise ApiClientError(
            f"API {method} {path} failed with HTTP {exc.code}: {detail}{hint}"
        ) from None
    except TimeoutError:
        raise ApiClientError(
            f"API request timed out after {timeout}s: {method} {path}"
        ) from None
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, TimeoutError):
            raise ApiClientError(
                f"API request timed out after {timeout}s: {method} {path}"
            ) from None
        raise ApiClientError(f"cannot reach the API at {api_url}: {exc.reason}") from None
    try:
        return json.loads(raw)
    except ValueError:
        raise ApiClientError(
            f"non-JSON response from the API for {method} {path} "
            f"({len(raw)} bytes) — is {api_url} the ClearFeature API?"
        ) from None


# --- local file helpers --------------------------------------------------------

def _read_jsonl(path_str: str, *, what: str) -> tuple[list[str], list[dict]]:
    """Read a JSONL file; validate transport shape only (each line = JSON object).

    Returns (verbatim non-blank lines, parsed objects) in deterministic file order.
    Error messages name the line number, never echo line content.
    """
    try:
        text = Path(path_str).read_text(encoding="utf-8")
    except OSError as exc:
        raise ApiClientError(f"cannot read {what} file {path_str!r}: {exc}") from None
    lines: list[str] = []
    rows: list[dict] = []
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue  # the ingestion API skips blank lines; mirror that locally
        try:
            row = json.loads(line)
        except ValueError as exc:
            raise ApiClientError(
                f"invalid JSONL in {what} file {path_str!r} at line {number}: {exc}"
                " — nothing was submitted"
            ) from None
        if not isinstance(row, dict):
            raise ApiClientError(
                f"invalid JSONL in {what} file {path_str!r} at line {number}: each line "
                "must be a JSON object — nothing was submitted"
            )
        lines.append(line)
        rows.append(row)
    if not lines:
        raise ApiClientError(f"{what} file {path_str!r} contains no rows")
    return lines, rows


def _write_json_atomic(path_str: str, obj: Any) -> None:
    """Write JSON to path via a same-directory temp file + rename; no partial files.

    EVERY filesystem step (mkdir, temp-file creation, write, rename, cleanup) maps
    to the bounded ApiClientError contract — an unwritable path must never leak a
    traceback or leave a misleading output file behind.
    """
    target = Path(path_str)
    tmp_name: str | None = None
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            dir=target.parent, prefix=f".{target.name}.", suffix=".tmp"
        )
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(obj, handle, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_name, target)
    except OSError as exc:
        if tmp_name is not None:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
        raise ApiClientError(f"cannot write output file {path_str!r}: {exc}") from None


# --- ingest --------------------------------------------------------------------

def run_ingest(args: argparse.Namespace) -> tuple[dict, int]:
    api_url = resolve_api_url(args.api_url)
    api_key = resolve_api_key()
    lines, _rows = _read_jsonl(args.input, what="input")

    # ONE file -> ONE request -> ONE manifest. The server contract has no manifest
    # append, so client-side chunking would fabricate a virtual manifest; the API
    # itself enforces no item cap on this endpoint.
    payload: dict[str, Any] = {
        "entity_type": args.entity_type,
        "source_name": args.source_name,
        "report_type": args.report_type,
        "lines": lines,
    }
    if args.dataset_id is not None:
        payload["dataset_id"] = args.dataset_id
    if args.created_by is not None:
        payload["created_by"] = args.created_by

    response = api_request(
        api_url,
        "/v1/source-datasets/ingest-jsonl",
        api_key=api_key,
        method="POST",
        payload=payload,
        timeout=args.http_timeout_seconds,
    )

    summary = {
        "ok": True,
        "manifest_id": response.get("manifest_id"),
        "dataset_id": response.get("dataset_id"),
        "entity_type": response.get("entity_type"),
        "source_name": response.get("source_name"),
        "report_type": response.get("report_type"),
        "status": response.get("status"),
        "item_count_read": response.get("item_count_read"),
        "item_count_written": response.get("item_count_written"),
        "item_count_duplicate": response.get("item_count_duplicate"),
        "item_count_rejected": response.get("item_count_rejected"),
        "watermark_min_event_ts": response.get("watermark_min_event_ts"),
        "watermark_max_event_ts": response.get("watermark_max_event_ts"),
        "detail_url": response.get("detail_url"),
    }
    if args.output is not None:
        _write_json_atomic(args.output, response)
        summary["output"] = args.output
    return summary, 0


# --- materialize ----------------------------------------------------------------

def derive_idempotency_key(
    *,
    view: str,
    view_version: int,
    manifest_ids: list[str],
    requested_features: list[str],
    requested_feature_groups: list[str],
    write_online: bool,
    online_refresh_mode: str | None,
    chunk_size: int,
) -> str:
    """Stable bounded key over the normalized request — same submission, same key.

    Deliberately excludes API URL, key, wall-clock time, local paths, and the
    input order of manifests/features.
    """
    canonical = {
        "contract": IDEMPOTENCY_CONTRACT,
        "view": view,
        "view_version": view_version,
        "manifest_ids": sorted(set(manifest_ids)),
        "requested_features": sorted(set(requested_features)),
        "requested_feature_groups": sorted(set(requested_feature_groups)),
        "write_online": write_online,
        "online_refresh_mode": online_refresh_mode,
        "chunk_size": chunk_size,
    }
    digest = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"job_{digest[:32]}"


def _job_summary(status: dict, status_path: str, idempotency_key: str) -> dict:
    chunks = sorted(
        (status.get("chunks") or {}).values(), key=lambda c: c.get("chunk_index", 0)
    )
    first_errors: list[str] = []
    for chunk in chunks:
        for error in chunk.get("first_errors") or []:
            if len(first_errors) >= _FIRST_ERRORS_LIMIT:
                break
            first_errors.append(_bounded(str(error)))
    return {
        "job_id": status.get("job_id"),
        "status": status.get("status"),
        "manifest_ids": status.get("manifest_ids"),
        "total_items": status.get("total_items"),
        "chunk_count": status.get("chunk_count"),
        "completed_chunks": status.get("completed_chunks"),
        "failed_chunks": status.get("failed_chunks"),
        "completed_items": sum(int(c.get("ok_items") or 0) for c in chunks),
        "failed_items": status.get("failed_items"),
        "first_errors": first_errors,
        "status_url": status_path,
        "idempotency_key": idempotency_key,
    }


def run_materialize(args: argparse.Namespace) -> tuple[dict, int]:
    api_url = resolve_api_url(args.api_url)
    api_key = resolve_api_key()

    manifest_ids = sorted(set(args.manifest_ids))
    features = sorted(set(args.features or []))
    groups = sorted(set(args.feature_groups or []))
    if not features and not groups:
        raise ApiClientError(
            "select features to materialize: pass --feature and/or --feature-group"
        )

    idempotency_key = args.idempotency_key or derive_idempotency_key(
        view=args.view,
        view_version=args.view_version,
        manifest_ids=manifest_ids,
        requested_features=features,
        requested_feature_groups=groups,
        write_online=args.write_online,
        online_refresh_mode=args.online_refresh_mode,
        chunk_size=args.chunk_size,
    )

    payload: dict[str, Any] = {
        "view": args.view,
        "view_version": args.view_version,
        "idempotency_key": idempotency_key,
        "requested_features": features,
        "requested_feature_groups": groups,
        "write_online": args.write_online,
        "chunk_size": args.chunk_size,
        "scope": {
            "type": "source_dataset_manifests",
            "manifest_ids": manifest_ids,
        },
    }
    if args.online_refresh_mode is not None:
        payload["online_refresh_mode"] = args.online_refresh_mode

    accepted = api_request(
        api_url,
        "/v1/batch/jobs",
        api_key=api_key,
        method="POST",
        payload=payload,
        timeout=args.http_timeout_seconds,
    )
    job_id = accepted.get("job_id")
    status_path = f"/v1/batch/jobs/{job_id}"

    if not args.wait:
        return {
            "ok": True,
            "job_id": job_id,
            "status": accepted.get("status"),
            "manifest_ids": accepted.get("manifest_ids"),
            "total_items": accepted.get("total_items"),
            "chunk_count": accepted.get("chunk_count"),
            "status_url": status_path,
            "idempotency_key": idempotency_key,
        }, 0

    deadline = time.monotonic() + args.timeout_seconds
    while True:
        status = api_request(
            api_url, status_path, api_key=api_key, timeout=args.http_timeout_seconds
        )
        if status.get("status") in TERMINAL_JOB_STATUSES:
            break
        if time.monotonic() >= deadline:
            raise ApiClientError(
                f"timed out after {args.timeout_seconds}s waiting for batch job "
                f"{job_id} (last status: {status.get('status')}); the job keeps "
                f"running server-side — poll GET {status_path}"
            )
        time.sleep(args.poll_interval_seconds)

    summary = _job_summary(status, status_path, idempotency_key)
    final = status.get("status")
    if final == "completed":
        return {"ok": True, **summary}, 0
    if final == "completed_with_errors":
        # The API run finished; some items deterministically failed. Honest JSON,
        # pinned exit code 2 (0 completed / 2 completed_with_errors / 1 otherwise).
        errors = [
            f"batch job {job_id} completed_with_errors: "
            f"{summary['failed_items']} of {summary['total_items']} items failed "
            "(see first_errors)"
        ]
        return {"ok": False, "errors": errors, **summary}, 2
    errors = [f"batch job {job_id} finished with status {final!r}"]
    error_summary = status.get("error_summary") or {}
    if error_summary.get("error"):
        errors.append(_bounded(str(error_summary["error"])))
    return {"ok": False, "errors": errors, **summary}, 1


# --- build-training-dataset ------------------------------------------------------

def run_build_training_dataset(args: argparse.Namespace) -> tuple[dict, int]:
    api_url = resolve_api_url(args.api_url)
    api_key = resolve_api_key()
    _lines, observations = _read_jsonl(args.observations, what="observations")
    for number, row in enumerate(observations, start=1):
        for field in ("entity", "observation_ts"):
            if field not in row:
                raise ApiClientError(
                    f"observation row {number} is missing required field {field!r} "
                    "(expected the public API shape: entity, observation_ts[, context])"
                )

    features = list(dict.fromkeys(args.features))  # dedupe, preserve user order
    payload = {
        "view": args.view,
        "view_version": args.view_version,
        "features": features,
        "observations": observations,
        "missing_policy": args.missing_policy,
        "safety_gap_seconds": args.safety_gap_seconds,
    }
    response = api_request(
        api_url,
        "/v1/training-datasets/build",
        api_key=api_key,
        method="POST",
        payload=payload,
        timeout=args.http_timeout_seconds,
    )
    _write_json_atomic(args.output, response)
    summary = response.get("summary") or {}
    return {
        "ok": True,
        "output": args.output,
        "rows": summary.get("rows"),
        "features": summary.get("features"),
        "missing_values": summary.get("missing_values"),
        "future_records_ignored": summary.get("future_records_ignored"),
        "safety_gap_seconds": summary.get("safety_gap_seconds"),
        "view": args.view,
        "view_version": args.view_version,
    }, 0
