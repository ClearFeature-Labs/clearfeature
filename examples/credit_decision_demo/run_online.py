#!/usr/bin/env python
"""Online credit-decision demo driver.

Validates the demo-model-service against the LIVE stack, after the earlier batch flow has
populated offline history:

  1. guarded Mode-2 refresh pushes the 10 model input features online for the entities
     this driver will score (the normal platform path: D9 write guard + token bucket);
  2. seven golden segment clients: online decision vs the batch F3 pd_score from the
     offline store — exact equality required (pure-Python artifact, 6dp rounding);
  3. >= 50 sequential + a concurrent burst of decisions: latency, errors, timeouts;
  4. restart demo-model-service: same digest, same score after reload;
  5. Kafka high-watermark check: the decision path publishes NOTHING to fp.* topics;
  6. evidence written to artifacts/credit_online_demo/ (report.json + samples).

Driver-only file: it may use host-side backend seams for EXPECTED values (offline
reads), like every other demo driver. The model service itself never touches stores.
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
import statistics
import subprocess
import threading
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime

from examples.credit_decision_demo.features import MODEL_FEATURES
from examples.credit_decision_demo.flow import (
    VIEW,
    VIEW_VERSION,
    entity_key,
    fast_local_backend,
    guarded_online_refresh,
)
from examples.credit_decision_demo.model_runner import DemoPdModelRunner
from examples.credit_decision_demo.model_service import decide
from fintech_feature_platform.api.backend import build_backend
from fintech_feature_platform.api.settings import load_settings

SEGMENTS = ["LOW_RISK", "MEDIUM_RISK", "HIGH_RISK", "THIN_FILE",
            "RECENT_DELINQUENCY", "HIGH_INCOME_HIGH_DEBT", "UNSTABLE_INCOME"]
TOPICS_PREFIX = "fp."

_checks: list[tuple[str, bool]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    _checks.append((name, ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def http_json(url: str, payload: dict | None = None, timeout: int = 30) -> dict:
    headers = {"Content-Type": "application/json"}
    #: the driver authenticates to demo-model-service like any client.
    api_key = os.environ.get("FSP_MODEL_CLIENT_API_KEY")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers=headers,
        method="POST" if payload is not None else "GET",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


def decision(model_url: str, user_id: str, application_id: str) -> tuple[dict, float]:
    started = time.monotonic()
    body = http_json(model_url + "/v1/credit/decision",
                     {"user_id": user_id, "application_id": application_id}, timeout=15)
    return body, (time.monotonic() - started) * 1000.0


def kafka_high_watermarks(project: str) -> dict[str, int] | None:
    """{topic: sum of partition high watermarks} for all fp.* topics, via rpk."""
    try:
        out = subprocess.run(
            ["docker", "compose", "-p", project, "exec", "-T", "redpanda",
             "rpk", "topic", "list"],
            capture_output=True, text=True, timeout=60,
        ).stdout
        topics = [line.split()[0] for line in out.splitlines()
                  if line.startswith(TOPICS_PREFIX)]
        marks: dict[str, int] = {}
        for topic in topics:
            described = subprocess.run(
                ["docker", "compose", "-p", project, "exec", "-T", "redpanda",
                 "rpk", "topic", "describe", topic, "-p"],
                capture_output=True, text=True, timeout=60,
            ).stdout
            total = 0
            for line in described.splitlines():
                parts = line.split()
                # partition rows: PARTITION LEADER EPOCH REPLICAS LOG-START-OFFSET HIGH-WATERMARK
                if parts and parts[0].isdigit() and parts[-1].isdigit():
                    total += int(parts[-1])
            marks[topic] = total
        return marks
    except Exception as exc:  # noqa: BLE001 - evidence helper, not the platform
        print(f"    (kafka watermark capture unavailable: {exc})")
        return None


def service_container(project: str) -> str | None:
    out = subprocess.run(
        ["docker", "ps", "--filter", f"label=com.docker.compose.project={project}",
         "--filter", "label=com.docker.compose.service=demo-model-service",
         "--format", "{{.Names}}"],
        capture_output=True, text=True, timeout=30,
    ).stdout.strip()
    return out.splitlines()[0] if out else None


def restart_counts(project: str) -> dict[str, int]:
    out = subprocess.run(
        ["docker", "ps", "--filter", f"label=com.docker.compose.project={project}",
         "--format", "{{.Names}}"],
        capture_output=True, text=True, timeout=30,
    ).stdout.split()
    counts = {}
    for name in out:
        value = subprocess.run(
            ["docker", "inspect", "--format", "{{.RestartCount}}", name],
            capture_output=True, text=True, timeout=30,
        ).stdout.strip()
        counts[name] = int(value or 0)
    return counts


def latency_summary(latencies: list[float]) -> dict:
    ordered = sorted(latencies)
    if len(ordered) < 2:
        return {"n": len(ordered)}
    quantiles = statistics.quantiles(ordered, n=100, method="inclusive")
    return {
        "n": len(ordered),
        "p50_ms": round(statistics.median(ordered), 1),
        "p95_ms": round(quantiles[94], 1),
        "p99_ms": round(quantiles[98], 1),
        "max_ms": round(max(ordered), 1),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=".demo-data/credit_decision")
    parser.add_argument("--api-url", default="http://127.0.0.1:8000")
    parser.add_argument("--model-url", default="http://127.0.0.1:8090")
    parser.add_argument("--compose-project", default="fsp-dev",
                        help="for restart/rpk/restart-count evidence; '' disables")
    parser.add_argument("--sequential-requests", type=int, default=50)
    parser.add_argument("--concurrent-requests", type=int, default=20)
    parser.add_argument("--concurrent-threads", type=int, default=4)
    parser.add_argument("--output-dir", default="artifacts/credit_online_model_service")
    args = parser.parse_args(argv)

    started = time.time()
    settings = load_settings()
    backend = fast_local_backend(build_backend(settings), settings)
    data_dir = Path(args.data_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    project = args.compose_project or None

    # --- entities: 7 segment goldens + a deterministic scoring slice ------------------
    rows = list(csv.DictReader((data_dir / "labels.csv").open(encoding="utf-8")))
    goldens: dict[str, dict] = {}
    for row in rows:
        if row["segment"] in SEGMENTS and row["segment"] not in goldens:
            goldens[row["segment"]] = row
    missing_segments = [s for s in SEGMENTS if s not in goldens]
    check("all seven segments present in the portfolio", not missing_segments,
          str(missing_segments))
    slice_rows = rows[:max(args.sequential_requests, 50)]
    scored_rows = {r["user_id"]: r for r in slice_rows}
    for row in goldens.values():
        scored_rows[row["user_id"]] = row

    # --- health + digest --------------------------------------------------------------
    # /health is minimal by policy since; the expected digest comes from the
    # committed artifact and is asserted against every decision response instead.
    health = http_json(args.model_url + "/health")
    check("model service healthy", health.get("status") == "ok", str(health))
    expected_digest = DemoPdModelRunner().digest

    # --- Mode-2: make the model features available online (normal platform path) ------
    refresh_keys = [entity_key(r["user_id"], r["application_id"])
                    for r in scored_rows.values()]
    refresh = guarded_online_refresh(
        backend, refresh_keys, list(MODEL_FEATURES), datetime.now(tz=UTC))
    check("guarded Mode-2 refresh made model features available online",
          refresh["missing_offline"] == 0, str(refresh))

    marks_before = kafka_high_watermarks(project) if project else None

    # --- golden segment cases: batch F3 vs online ------------------------------------
    golden_results = []
    request_samples: list[dict] = []
    response_samples: list[dict] = []
    observation = datetime.now(tz=UTC)
    for segment in SEGMENTS:
        row = goldens[segment]
        key = entity_key(row["user_id"], row["application_id"])
        body, elapsed = decision(args.model_url, row["user_id"], row["application_id"])
        request_samples.append({"segment": segment, "user_id": row["user_id"],
                                "application_id": row["application_id"]})
        response_samples.append(body)

        batch_record = backend.offline.get_pit(
            key, feature_name="pd_score", feature_version=1,
            view=VIEW, view_version=VIEW_VERSION, observation_ts=observation,
        )
        batch_pd = batch_record.result.value if batch_record else None
        feature_match = True
        mismatches = []
        for name in MODEL_FEATURES:
            expected = backend.offline.get_pit(
                key, feature_name=name, feature_version=1,
                view=VIEW, view_version=VIEW_VERSION, observation_ts=observation,
            )
            got = (body.get("features") or {}).get(name, {}).get("value")
            if expected is None or got != expected.result.value:
                feature_match = False
                mismatches.append(
                    f"{name}: online={got} expected="
                    f"{expected.result.value if expected else None}")
        ok = (feature_match and batch_pd is not None
              and body.get("pd_score") == batch_pd
              and body.get("decision") == decide(batch_pd)
              and body.get("model_digest") == expected_digest)
        golden_results.append({
            "segment": segment, "user_id": row["user_id"],
            "batch_pd": batch_pd, "online_pd": body.get("pd_score"),
            "decision": body.get("decision"), "latency_ms": round(elapsed, 1),
            "ok": ok,
        })
        check(f"golden {segment} ({row['user_id']}): online == batch F3 "
              f"(pd={body.get('pd_score')}, {body.get('decision')})",
              ok, "; ".join(mismatches[:3]))

    # --- sequential + concurrent load -------------------------------------------------
    latencies: list[float] = []
    errors = timeouts = 0
    for row in list(scored_rows.values())[:args.sequential_requests]:
        try:
            body, elapsed = decision(args.model_url, row["user_id"], row["application_id"])
            if body.get("status") == "completed":
                latencies.append(elapsed)
            else:
                timeouts += 1
        except Exception:  # noqa: BLE001 - an error is a counted result here
            errors += 1
    lock = threading.Lock()

    def concurrent_worker(assigned: list[dict]) -> None:
        nonlocal errors, timeouts
        for row in assigned:
            try:
                body, elapsed = decision(
                    args.model_url, row["user_id"], row["application_id"])
                with lock:
                    if body.get("status") == "completed":
                        latencies.append(elapsed)
                    else:
                        timeouts += 1
            except Exception:  # noqa: BLE001
                with lock:
                    errors += 1

    concurrent_rows = list(scored_rows.values())[:args.concurrent_requests]
    threads = []
    for i in range(args.concurrent_threads):
        assigned = concurrent_rows[i::args.concurrent_threads]
        t = threading.Thread(target=concurrent_worker, args=(assigned,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    summary = latency_summary(latencies)
    total_requests = len(SEGMENTS) + args.sequential_requests + len(concurrent_rows)
    check(f"{total_requests} decisions: 0 errors, 0 timeouts",
          errors == 0 and timeouts == 0, f"errors={errors} timeouts={timeouts}")
    check("latency p95 <= 300 ms",
          summary.get("p95_ms") is not None and summary["p95_ms"] <= 300.0,
          str(summary))

    # --- nothing entered Kafka on the decision path -----------------------------------
    marks_after = kafka_high_watermarks(project) if project else None
    if marks_before is not None and marks_after is not None:
        grew = {t: (marks_before.get(t), marks_after.get(t))
                for t in marks_after if marks_after[t] != marks_before.get(t)}
        check("no event entered any fp.* topic during the decision phase",
              not grew, str(grew))
    else:
        print("    (kafka watermark check skipped: no compose project/rpk)")

    # --- restart safety ---------------------------------------------------------------
    restart_result = {}
    if project:
        container = service_container(project)
        golden_row = goldens[SEGMENTS[0]]
        before_body, _ = decision(args.model_url, golden_row["user_id"],
                                  golden_row["application_id"])
        subprocess.run(["docker", "restart", container],
                       capture_output=True, text=True, timeout=120)
        deadline = time.time() + 60
        healthy = False
        while time.time() < deadline:
            try:
                healthy = http_json(args.model_url + "/health").get("status") == "ok"
                if healthy:
                    break
            except Exception:  # noqa: BLE001 - still restarting
                time.sleep(1)
        after_body, _ = decision(args.model_url, golden_row["user_id"],
                                 golden_row["application_id"])
        restart_result = {
            "container": container, "healthy_after_restart": healthy,
            "same_digest": after_body.get("model_digest") == before_body.get("model_digest"),
            "same_score": after_body.get("pd_score") == before_body.get("pd_score"),
        }
        check("service restart reloads the same artifact and returns the same score",
              healthy and restart_result["same_digest"] and restart_result["same_score"],
              str(restart_result))

    # Manual `docker restart` does not increment RestartCount, so every container —
    # including the deliberately restarted model service — must still read 0.
    counts = restart_counts(project) if project else {}
    unexpected = {name: n for name, n in counts.items() if n > 0}
    check("no unexpected container restarts (docker RestartCount all 0)",
          not unexpected, str(unexpected or f"{len(counts)} containers at 0"))

    # --- evidence ---------------------------------------------------------------------
    commit = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True,
                            text=True, timeout=30).stdout.strip()
    failed = [name for name, ok in _checks if not ok]
    report = {
        "demo": "credit_online",
        "timestamp": datetime.now(tz=UTC).isoformat(),
        "git_commit": commit,
        "model_digest": expected_digest,
        "feature_view": {"view": VIEW, "view_version": VIEW_VERSION,
                         "model_features": list(MODEL_FEATURES)},
        "golden_cases": golden_results,
        "batch_vs_online": {
            "comparison": "exact equality (pure-Python artifact, 6dp rounding)",
            "all_equal": all(g["ok"] for g in golden_results),
        },
        "latency": {**summary, "errors": errors, "timeouts": timeouts,
                    "total_requests": total_requests},
        "restart": restart_result,
        "container_restart_counts": counts,
        "checks": [{"name": name, "ok": ok} for name, ok in _checks],
        "verdict": "PASS" if not failed else "FAIL",
    }
    (out_dir / "report.json").write_text(json.dumps(report, indent=2, default=str))
    with (out_dir / "sample_requests.jsonl").open("w") as handle:
        for sample in request_samples:
            handle.write(json.dumps(sample) + "\n")
    with (out_dir / "sample_responses.jsonl").open("w") as handle:
        for sample in response_samples[:10]:
            handle.write(json.dumps(sample, default=str) + "\n")
    (out_dir / "README.md").write_text(
        "# Credit online model-service demo evidence\n\n"
        "Deterministic, synthetic-only evidence from `scripts/run_credit_online_demo.sh`:\n"
        "`report.json` (full run report), `sample_requests.jsonl` /\n"
        "`sample_responses.jsonl` (bounded request/response samples — synthetic data,\n"
        "no secrets, no raw reports). Regenerate any time by re-running the script.\n"
    )

    print()
    print(f"Credit online demo: {'PASS' if not failed else 'FAIL'} "
          f"({len(_checks) - len(failed)}/{len(_checks)} checks, "
          f"{time.time() - started:.0f}s) -> {out_dir / 'report.json'}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
