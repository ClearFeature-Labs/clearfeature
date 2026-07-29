#!/usr/bin/env python
"""Thin CLI wrapper around the table feature import runner (idempotent reruns).

Imports precomputed feature values from a DWH/SQL export (JSONL/CSV/optional Parquet)
into offline history (and optionally online latest), skipping exact duplicates. Backend
via FSP_BACKEND.

Usage:
    uv run python scripts/import_table_features.py \
        --config table_features_config.json \
        --input dwh_features.jsonl --format jsonl \
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
from fintech_feature_platform.api.settings import load_settings
from fintech_feature_platform.api.table_import import (
    TableFeatureImportConfig,
    read_rows,
    run_table_feature_import,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Import precomputed table features.")
    parser.add_argument("--config", required=True, help="path to import config JSON")
    parser.add_argument("--input", required=True, help="path to exported rows file")
    parser.add_argument("--format", choices=["jsonl", "csv", "parquet"], default="jsonl")
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

    with open(args.config, encoding="utf-8") as handle:
        config = TableFeatureImportConfig.model_validate(json.load(handle))

    backend = build_backend(load_settings())
    checksum = hashlib.sha256(Path(args.input).read_bytes()).hexdigest()
    rows = read_rows(args.input, args.format)
    summary = run_table_feature_import(
        backend=backend,
        config=config,
        rows=rows,
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
        f"missing_values={summary.missing_values} "
        f"online_written={summary.online_written} "
        f"warnings={summary.warnings}"
    )
    for error in summary.errors:
        print(f"  row {error['row_index']}: {error['error']}", file=sys.stderr)

    return 1 if summary.rows_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
