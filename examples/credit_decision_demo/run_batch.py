#!/usr/bin/env python
"""Credit-decision batch demo driver.

Drives the full portfolio batch against a REAL backend:

  1. ingest the five report files (landing form (a): MinIO payloads + raw_reports_meta +
     one SourceDatasetManifest per source) — host-side bulk, no per-row HTTP;
  2. submit five manifest-scoped batch jobs through the Feature API (Kafka-first: the
     compose batch-worker computes each source's F1 group and bulk-appends offline);
  3. recompute the F2 affordability layer through the real DAG (offline PIT inputs);
  4. score the portfolio with the F3 PD model-as-feature (digest-verified runner);
  5. guarded Mode-2 online refresh (D9 write guard + token bucket), run twice to show
     the noop/skipped-stale semantics;
  6. verify latest values through the Feature API and print a bounded lineage summary.

Requires FSP_BACKEND=local (+ FSP_REGISTRY_PATH / FSP_UDF_PROVIDER for the credit view)
on the host, and the compose stack running with the same registry env. The wrapper
`scripts/run_credit_batch_demo.sh` sets all of this up.
"""

from __future__ import annotations

# ruff: noqa: E402  (CLI bootstrap: make the repo root importable for `python <script>.py`)
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import argparse
import csv
import json
import os
import time
import urllib.request
from datetime import UTC, datetime

from examples.credit_decision_demo.flow import (
    SOURCE_FEATURE_GROUPS,
    VIEW,
    VIEW_VERSION,
    entity_key,
    fast_local_backend,
    guarded_online_refresh,
    ingest_dataset,
    run_f2_batch,
    run_f3_batch,
)
from examples.credit_decision_demo.model_runner import DemoPdModelRunner
from fintech_feature_platform.api.backend import build_backend
from fintech_feature_platform.api.settings import load_settings

F2_FEATURES = [
    "requested_monthly_payment", "existing_payment_to_income", "total_payment_to_income",
    "income_stability_index", "recent_delinquency_flag", "thin_file_flag",
    "affordability_index", "combined_risk_index",
]
REFRESH_FEATURES = ["pd_score", "combined_risk_index", "affordability_index"]

_checks: list[tuple[str, bool]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    _checks.append((name, ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def api_json(api: str, path: str, payload: dict | None = None) -> dict:
    headers = {"Content-Type": "application/json"}
    #: the demo driver authenticates like any client (operator key).
    api_key = os.environ.get("FSP_CLIENT_API_KEY")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    request = urllib.request.Request(
        api + path,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers=headers,
        method="POST" if payload is not None else "GET",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read())


def wait_for_job(api: str, job_id: str, timeout_s: int) -> dict:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        status = api_json(api, f"/v1/batch/jobs/{job_id}")
        done = status.get("completed_chunks", 0) + status.get("failed_chunks", 0)
        if done >= status.get("chunk_count", 1):
            return status
        time.sleep(2)
    raise TimeoutError(f"batch job {job_id} did not finish in {timeout_s}s")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the credit batch demo flow.")
    parser.add_argument("--data-dir", default=".demo-data/credit_decision")
    parser.add_argument("--api-url", default="http://127.0.0.1:8000")
    # Must respect the platform cap (settings.batch_max_chunk_size, default 100).
    parser.add_argument("--chunk-size", type=int, default=100)
    # Global budget for ALL five F1 jobs (a 30k portfolio is ~660k offline rows through
    # a single worker with per-op connections — the documented no-pooling limitation).
    parser.add_argument("--job-timeout-seconds", type=int, default=7200)
    args = parser.parse_args(argv)

    started = time.time()
    settings = load_settings()
    # Single-connection stores for the bulk host-side steps (see fast_local_backend).
    backend = fast_local_backend(build_backend(settings), settings)
    data_dir = Path(args.data_dir)

    entities = []
    with (data_dir / "labels.csv").open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            entities.append(entity_key(row["user_id"], row["application_id"]))
    print(f"==> portfolio: {len(entities)} applications (backend={settings.backend})")

    # 1. Bulk ingestion (form a) --------------------------------------------------
    manifests = ingest_dataset(backend, data_dir)
    landed = {name: m.item_count_written + m.item_count_duplicate
              for name, m in manifests.items()}
    check("ingestion landed all five sources",
          all(count == len(entities) for count in landed.values()), str(landed))

    # 2. Five manifest-scoped F1 batch jobs through the API (Kafka-first). Submit ALL
    # first (they queue on one topic; the worker drains them in order), then wait for
    # each under one global deadline — a per-job window would misfire on queued jobs.
    jobs: list[tuple[str, str]] = []
    for source_name, manifest in manifests.items():
        job = api_json(args.api_url, "/v1/batch/jobs", {
            "view": VIEW, "view_version": VIEW_VERSION,
            "scope": {"type": "source_dataset_manifest", "manifest_id": manifest.manifest_id,
                      "include_duplicate_items": True},
            "requested_feature_groups": [SOURCE_FEATURE_GROUPS[source_name]],
            "idempotency_key": f"credit_demo_f1_{source_name}_{manifest.manifest_id}",
            "chunk_size": args.chunk_size,
        })
        jobs.append((source_name, job["job_id"]))
        print(f"==> submitted F1 job {source_name}: {job['job_id']}")
    deadline = time.time() + args.job_timeout_seconds
    for source_name, job_id in jobs:
        status = wait_for_job(args.api_url, job_id,
                              max(60, int(deadline - time.time())))
        check(f"F1 batch job {source_name} ({job_id})",
              status.get("failed_chunks", 1) == 0,
              f"{status.get('completed_chunks')}/{status.get('chunk_count')} chunks")

    # 3. F2 affordability layer through the real DAG --------------------------------
    calc_ts = datetime.now(tz=UTC)
    f2 = run_f2_batch(backend, entities, F2_FEATURES, calc_ts)
    check("F2 DAG recompute", f2["skipped"] == 0,
          f"{f2['computed']} values across {f2['features']} features")

    # 4. F3 portfolio PD scoring (digest-verified model) ------------------------------
    runner = DemoPdModelRunner()
    f3 = run_f3_batch(backend, runner, entities, calc_ts)
    check("F3 portfolio scoring", f3["computed"] == len(entities),
          f"computed={f3['computed']} skipped={f3['skipped']} "
          f"(model digest {runner.digest[:18]}...)")

    # 5. Guarded Mode-2 refresh, twice (second pass must be all noop) -----------------
    refresh_ts = datetime.now(tz=UTC)
    first = guarded_online_refresh(backend, entities, REFRESH_FEATURES, refresh_ts)
    second = guarded_online_refresh(backend, entities, REFRESH_FEATURES, refresh_ts)
    check("Mode-2 refresh wrote latest values",
          first["written"] + first["written_recompute"] > 0, str(first))
    check("Mode-2 replay is D9-idempotent (all noop, nothing overwritten)",
          second["written"] == 0 and second["noop"] > 0, str(second))

    # 6. Verify through the Feature API + bounded lineage ------------------------------
    sample = entities[0]
    sample_key = dict(sample.parts)
    latest = api_json(args.api_url, "/v1/features/latest", {
        "entity": sample_key, "view": VIEW, "view_version": VIEW_VERSION,
        "requested_features": REFRESH_FEATURES,
    })
    pd_value = latest["features"].get("pd_score", {}).get("value")
    check("Feature API serves refreshed online values",
          pd_value is not None and 0.0 <= float(pd_value) <= 1.0,
          f"pd_score={pd_value} for {sample_key['user_id']}")

    lineage = api_json(args.api_url, "/v1/lineage/feature-value", {
        "view": VIEW, "view_version": VIEW_VERSION,
        "feature_name": "pd_score", "feature_version": 1, "entity": sample_key,
        "manifest_id": manifests["credit_bureau_report"].manifest_id,
    })
    check("lineage answers pd_score provenance (model digest + fingerprint, values-free)",
          lineage["found"] and lineage["model_digest"] == runner.digest
          and lineage["input_fingerprint"] is not None
          and str(pd_value) not in json.dumps(lineage),
          f"report_refs={len(lineage['report_refs'])} gaps={lineage['gaps']}")

    failed = [name for name, ok in _checks if not ok]
    print()
    print(f"Credit batch demo: {'PASS' if not failed else 'FAIL'} "
          f"({len(_checks) - len(failed)}/{len(_checks)} checks, "
          f"{time.time() - started:.0f}s)")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
