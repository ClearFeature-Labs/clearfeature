#!/usr/bin/env python
"""Thin CLI wrapper around the raw JSON backfill runner (idempotent reruns).

Reads a JSONL file of historical raw reports and appends offline feature history
(skipping exact duplicates). Backend via FSP_BACKEND (memory default; local needs extras
+ services). Reruns are safe: exact-duplicate offline rows are skipped.

Usage:
    uv run python scripts/backfill_raw_reports.py \
        --view user_credit_risk --view-version 1 \
        --features declared_income --input raw_reports.jsonl \
        [--write-mode offline_only] [--run-id RID] [--expected-rows N] \
        [--source-name NAME] [--summary-output summary.json] [--fail-fast]
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import sys
from pathlib import Path

from fintech_feature_platform.api.backend import build_backend
from fintech_feature_platform.api.backfill import run_raw_json_backfill
from fintech_feature_platform.api.settings import load_settings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Raw JSON backfill (idempotent).")
    parser.add_argument("--view", required=True)
    parser.add_argument("--view-version", type=int, default=1)
    parser.add_argument("--features", required=True, help="comma-separated names")
    parser.add_argument("--input", required=True, help="path to raw reports JSONL")
    parser.add_argument(
        "--write-mode",
        choices=["offline_only", "offline_and_online"],
        default="offline_only",
    )
    parser.add_argument("--run-id")
    parser.add_argument("--expected-rows", type=int)
    parser.add_argument("--source-name")
    parser.add_argument("--summary-output", help="write full summary JSON to this path")
    parser.add_argument("--fail-fast", action="store_true")
    args = parser.parse_args(argv)

    features = [name.strip() for name in args.features.split(",") if name.strip()]
    backend = build_backend(load_settings())

    data = Path(args.input).read_bytes()
    checksum = hashlib.sha256(data).hexdigest()
    lines = data.decode("utf-8").splitlines()

    summary = run_raw_json_backfill(
        backend=backend,
        view=args.view,
        view_version=args.view_version,
        requested_features=features,
        lines=lines,
        write_mode=args.write_mode,
        fail_fast=args.fail_fast,
        run_id=args.run_id,
        expected_rows=args.expected_rows,
        source_name=args.source_name,
        source_file=args.input,
        source_checksum=checksum,
    )

    if args.summary_output:
        Path(args.summary_output).write_text(
            json.dumps(dataclasses.asdict(summary), default=str, indent=2),
            encoding="utf-8",
        )

    print(
        f"run_id={summary.run_id} "
        f"expected_rows={summary.expected_rows} "
        f"rows_read={summary.rows_read} rows_total={summary.rows_total} "
        f"features_written={summary.features_written} "
        f"duplicates_skipped={summary.duplicates_skipped} "
        f"online_written={summary.online_written} "
        f"warnings={summary.warnings}"
    )
    for error in summary.errors:
        print(f"  row {error['row_index']}: {error['error']}", file=sys.stderr)

    return 1 if summary.rows_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
