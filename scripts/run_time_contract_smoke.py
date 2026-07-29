#!/usr/bin/env python
"""Live time-contract smoke  — the availability contract on real containers.

Proves against the running secure compose stack (existing volumes + migration 010):

  1. operator ingestion with a trusted historical ``available_at`` builds a row that IS
     PIT-eligible at a historical observation (computed today);
  2. the same report ingested WITHOUT ``available_at`` stays blocked for that
     observation (conservative ingestion-time/calc_ts fallback);
  3. an ordinary online request cannot backdate availability (accept-time stamped);
  4. exact batch replay remains a no-op (no new offline rows);
  5. a corrected trusted availability is an auditable recompute, not a silent no-op;
  6. ``metadata_write_status`` reaches its truthful ``written`` terminal state;
  7. lineage answers availability provenance values-free.

Requires FSP_CLIENT_API_KEY (operator). Wrapper: scripts/run_time_contract_smoke.sh.
"""

from __future__ import annotations

# ruff: noqa: E402  (CLI bootstrap: make the repo root importable for `python <script>.py`)
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import json
import os
import subprocess
import time
import urllib.request
from datetime import UTC, datetime

API = os.environ.get("FSP_API_URL", "http://127.0.0.1:8000")
KEY = os.environ["FSP_CLIENT_API_KEY"]
RUN_ID = datetime.now(tz=UTC).strftime("%H%M%S")
OLD = "2024-01-01T00:00:00+00:00"
HIST_AVAILABLE = "2024-02-01T00:00:00+00:00"
HIST_OBS = "2024-06-01T00:00:00+00:00"

_checks: list[tuple[str, bool]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    _checks.append((name, ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def api(path: str, payload: dict | None = None) -> dict:
    request = urllib.request.Request(
        API + path,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {KEY}"},
        method="POST" if payload is not None else "GET")
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read())


def psql(sql: str) -> str:
    return subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres",
         "psql", "-U", "fsp", "-d", "fsp", "-t", "-A", "-c", sql],
        capture_output=True, text=True, timeout=60).stdout.strip()


def bureau_row(user: str, available_at: str | None) -> str:
    row = {
        "entity_key": {"user_id": user, "application_id": f"app_{user}"},
        "event_ts": OLD,
        "payload": {"bureau_score": 700, "active_loans": 1, "inquiries_30d": 0,
                    "total_outstanding_amount": 100, "total_monthly_payment": 10,
                    "max_dpd_12m": 0, "last_delinquency_date": None,
                    "report_ts": OLD, "currency_code": "USD"},
    }
    if available_at is not None:
        row["available_at"] = available_at
    return json.dumps(row)


def submit_job(manifest_id: str, idem: str) -> dict:
    job = api("/v1/batch/jobs", {
        "view": "credit_decision", "view_version": 1,
        "scope": {"type": "source_dataset_manifest", "manifest_id": manifest_id,
                  "include_duplicate_items": True},
        "requested_feature_groups": ["bureau_risk_v1"],
        "idempotency_key": idem, "chunk_size": 10})
    deadline = time.time() + 90
    while time.time() < deadline:
        status = api(f"/v1/batch/jobs/{job['job_id']}")
        if status["status"] in ("completed", "completed_with_errors", "failed"):
            return status
        time.sleep(0.5)
    raise TimeoutError(job["job_id"])


def pit_value(user: str) -> object:
    ds = api("/v1/training-datasets/build", {
        "view": "credit_decision", "view_version": 1,
        "features": ["bureau_score"],
        "observations": [{"entity": {"user_id": user, "application_id": f"app_{user}"},
                          "observation_ts": HIST_OBS}]})
    return ds["rows"][0]["features"].get("bureau_score")


def main() -> int:
    trusted_user = f"tc_trusted_{RUN_ID}"
    legacy_user = f"tc_legacy_{RUN_ID}"

    # 1+2: ingest one report WITH trusted availability, one WITHOUT.
    trusted = api("/v1/source-datasets/ingest-jsonl", {
        "entity_type": "application", "source_name": "credit_bureau_report",
        "report_type": "credit_bureau_report", "dataset_id": f"tc_t_{RUN_ID}",
        "lines": [bureau_row(trusted_user, HIST_AVAILABLE)]})
    legacy = api("/v1/source-datasets/ingest-jsonl", {
        "entity_type": "application", "source_name": "credit_bureau_report",
        "report_type": "credit_bureau_report", "dataset_id": f"tc_l_{RUN_ID}",
        "lines": [bureau_row(legacy_user, None)]})
    check("operator ingestion landed trusted + legacy reports",
          trusted["item_count_written"] == 1 and legacy["item_count_written"] == 1)

    status = submit_job(trusted["manifest_id"], f"tc_job_t_{RUN_ID}")
    status2 = submit_job(legacy["manifest_id"], f"tc_job_l_{RUN_ID}")
    check("both batch jobs completed",
          status["status"] == "completed" and status2["status"] == "completed")

    # 1: trusted backfill computed TODAY is eligible at the 2024 observation.
    check("trusted historical available_at makes the row PIT-eligible at 2024",
          pit_value(trusted_user) == 700)
    # 2: without a trusted claim the same shape stays blocked (no leak).
    check("report without available_at stays blocked at 2024 (calc/ingestion fallback)",
          pit_value(legacy_user) is None)

    # 3: an online request cannot backdate availability — the row it produces is NOT
    # eligible at 2024 even though report_ts is 2024 (accept-time availability).
    online_user = f"tc_online_{RUN_ID}"
    api("/v1/feature-requests/compute", {
        "entity_type": "application",
        "entity_key": {"user_id": online_user, "application_id": f"app_{online_user}"},
        "view": "credit_decision", "view_version": 1,
        "deadline_ms": 10000, "wait_timeout_ms": 5000,
        "requested_features": ["bureau_score"],
        "reports": [{"source_name": "credit_bureau_report",
                     "report_type": "credit_bureau_report", "report_ts": OLD,
                     "payload": {"bureau_score": 700, "active_loans": 1,
                                 "inquiries_30d": 0, "total_outstanding_amount": 100,
                                 "total_monthly_payment": 10, "max_dpd_12m": 0,
                                 "last_delinquency_date": None, "report_ts": OLD}}]})
    time.sleep(2)  # offline projection
    check("online request cannot backdate availability (blocked at 2024)",
          pit_value(online_user) is None)

    # 4: exact replay of the trusted job -> zero new offline rows.
    rows_before = psql(
        "SELECT count(*) FROM features_offline WHERE entity_key_encoded LIKE "
        f"'user_id={trusted_user}%'")
    replay = submit_job(trusted["manifest_id"], f"tc_job_t_{RUN_ID}")
    rows_after = psql(
        "SELECT count(*) FROM features_offline WHERE entity_key_encoded LIKE "
        f"'user_id={trusted_user}%'")
    check("exact replay is a no-op (no new offline rows)",
          replay["status"] == "completed" and rows_before == rows_after,
          f"rows {rows_before} -> {rows_after}")

    # 5: corrected trusted availability -> auditable recompute (one new row).
    corrected = api("/v1/source-datasets/ingest-jsonl", {
        "entity_type": "application", "source_name": "credit_bureau_report",
        "report_type": "credit_bureau_report", "dataset_id": f"tc_t_{RUN_ID}",
        "lines": [bureau_row(trusted_user, "2024-03-01T00:00:00+00:00")]})
    submit_job(corrected["manifest_id"], f"tc_job_c_{RUN_ID}")
    rows_corrected = psql(
        "SELECT count(*) FROM features_offline WHERE entity_key_encoded LIKE "
        f"'user_id={trusted_user}%'")
    check("corrected trusted availability is an auditable recompute (new rows kept)",
          int(rows_corrected) > int(rows_after),
          f"rows {rows_after} -> {rows_corrected}")
    availability_rows = psql(
        "SELECT count(DISTINCT available_at) FROM features_offline WHERE "
        f"entity_key_encoded LIKE 'user_id={trusted_user}%'")
    check("both availability versions are preserved in history",
          int(availability_rows) >= 2, f"distinct available_at={availability_rows}")

    # 5b : the correction left EXACTLY ONE durable append-only audit
    # row with old/new values and origin; an exact replay adds none.
    report_ref = psql(
        "SELECT report_ref FROM raw_reports_meta WHERE entity_key::text LIKE "
        f"'%{trusted_user}%' LIMIT 1")
    audit_row = psql(
        "SELECT old_available_at, new_available_at, old_availability_source, "
        "new_availability_source, change_origin, manifest_id FROM "
        f"raw_report_availability_changes WHERE report_ref='{report_ref}'")
    audit_count = psql(
        "SELECT count(*) FROM raw_report_availability_changes "
        f"WHERE report_ref='{report_ref}'")
    check("trusted correction wrote exactly one durable audit row",
          audit_count == "1", f"rows={audit_count}")
    check("audit row carries old/new values and jsonl_ingestion origin + manifest",
          "2024-02-01" in audit_row and "2024-03-01" in audit_row
          and "source_provided" in audit_row and "jsonl_ingestion" in audit_row
          and "sdm_" in audit_row, audit_row[:120])
    api("/v1/source-datasets/ingest-jsonl", {
        "entity_type": "application", "source_name": "credit_bureau_report",
        "report_type": "credit_bureau_report", "dataset_id": f"tc_t_{RUN_ID}",
        "lines": [bureau_row(trusted_user, "2024-03-01T00:00:00+00:00")]})
    audit_count_replay = psql(
        "SELECT count(*) FROM raw_report_availability_changes "
        f"WHERE report_ref='{report_ref}'")
    check("replaying the identical correction adds no audit row",
          audit_count_replay == "1", f"rows={audit_count_replay}")
    check("audit row contains no payload or credential material",
          "bureau_score" not in audit_row and "700" not in audit_row
          and "Bearer" not in audit_row)

    # 6: metadata_write_status reaches its truthful terminal state.
    response = api("/v1/feature-requests/compute", {
        "entity_type": "application",
        "entity_key": {"user_id": f"tc_meta_{RUN_ID}", "application_id": "m1"},
        "view": "credit_decision", "view_version": 1,
        "deadline_ms": 10000, "wait_timeout_ms": 5000,
        "requested_features": ["bureau_score"],
        "reports": [{"source_name": "credit_bureau_report",
                     "report_type": "credit_bureau_report",
                     "report_ts": datetime.now(tz=UTC).isoformat(),
                     "payload": {"bureau_score": 650, "active_loans": 0,
                                 "inquiries_30d": 0, "total_outstanding_amount": 0,
                                 "total_monthly_payment": 0, "max_dpd_12m": 0,
                                 "last_delinquency_date": None,
                                 "report_ts": datetime.now(tz=UTC).isoformat()}}]})
    request_id = response["request_id"]
    deadline = time.time() + 30
    metadata_status = None
    while time.time() < deadline:
        metadata_status = api(f"/v1/feature-requests/{request_id}").get(
            "metadata_write_status")
        if metadata_status == "written":
            break
        time.sleep(0.5)
    check("metadata_write_status transitions to written after projection",
          metadata_status == "written", str(metadata_status))

    # 6b : simulate a LOST operational status update (the metadata
    # writer's Valkey write failed after the durable Postgres commit) by rewinding
    # the Valkey record to pending — the status API must reconcile against the
    # durable projection, report written, and read-repair the operational store.
    if metadata_status == "written":
        key = f"fs:request-status:{request_id}"
        raw = subprocess.run(
            ["docker", "compose", "exec", "-T", "valkey", "redis-cli", "GET", key],
            capture_output=True, text=True, timeout=30).stdout.strip()
        rewound = json.dumps({**json.loads(raw), "metadata_write_status": "pending"})
        subprocess.run(
            ["docker", "compose", "exec", "-T", "valkey", "redis-cli",
             "SET", key, rewound, "KEEPTTL"],
            capture_output=True, text=True, timeout=30)
        reconciled = api(f"/v1/feature-requests/{request_id}").get(
            "metadata_write_status")
        check("status API reconciles a lost operational update against durable "
              "Postgres evidence", reconciled == "written", str(reconciled))
        repaired = subprocess.run(
            ["docker", "compose", "exec", "-T", "valkey", "redis-cli", "GET", key],
            capture_output=True, text=True, timeout=30).stdout
        check("read-repair healed the operational store",
              '"metadata_write_status": "written"' in repaired)
    else:
        check("status API reconciles a lost operational update against durable "
              "Postgres evidence", False, "precondition failed")

    # 7: lineage exposes availability provenance, values-free.
    lineage = api("/v1/lineage/feature-value", {
        "view": "credit_decision", "view_version": 1,
        "feature_name": "bureau_score", "feature_version": 1,
        "entity": {"user_id": trusted_user, "application_id": f"app_{trusted_user}"}})
    # Values-free is structural: lineage never carries value/payload keys (a bare
    # substring check would false-positive on timestamp microseconds).
    serialized = json.dumps(lineage)
    check("lineage exposes availability provenance values-free",
          lineage.get("availability_source") == "source_provided"
          and lineage.get("available_at") is not None
          and '"value"' not in serialized and '"payload"' not in serialized
          and '"value_json"' not in serialized)

    failed = [name for name, ok in _checks if not ok]
    print()
    print(f"Time-contract smoke: {'PASS' if not failed else 'FAIL'} "
          f"({len(_checks) - len(failed)}/{len(_checks)} checks)")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
