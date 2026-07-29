#!/usr/bin/env python
"""Thin CLI wrapper: build a PIT feature dataset and export it.

Reads observations JSONL, calls the existing PIT builder over the current backend, and
writes the dataset to JSONL/CSV/(optional) Parquet. Backend via FSP_BACKEND.

Usage:
    uv run python scripts/build_feature_dataset.py \
        --view user_credit_risk --view-version 1 \
        --features backend_age,risk_flag,pd_model_score_v1 \
        --observations observations.jsonl \
        --output feature_dataset.jsonl --format jsonl
"""

from __future__ import annotations

import argparse
import json

from fintech_feature_platform.api.backend import build_backend
from fintech_feature_platform.api.backfill import resolve_view
from fintech_feature_platform.api.feature_dataset_export import write_dataset
from fintech_feature_platform.api.settings import load_settings
from fintech_feature_platform.api.training import ObservationIn, to_core_observations
from fintech_feature_platform.fs_core.training import build_training_dataset


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build and export a PIT feature dataset.")
    parser.add_argument("--view", required=True)
    parser.add_argument("--view-version", type=int, default=1)
    parser.add_argument("--features", required=True, help="comma-separated names")
    parser.add_argument("--observations", required=True, help="observations JSONL path")
    parser.add_argument("--output", required=True)
    parser.add_argument("--format", choices=["jsonl", "csv", "parquet"], default="jsonl")
    args = parser.parse_args(argv)

    features = [name.strip() for name in args.features.split(",") if name.strip()]
    backend = build_backend(load_settings())
    view = resolve_view(backend, args.view, args.view_version)

    with open(args.observations, encoding="utf-8") as handle:
        observations = to_core_observations(
            [ObservationIn.model_validate(json.loads(line)) for line in handle if line.strip()]
        )

    dataset = build_training_dataset(
        offline=backend.offline,
        view=view,
        view_version=args.view_version,
        feature_names=features,
        observations=observations,
        missing_policy="keep_null",
    )
    write_dataset(dataset, args.output, args.format)
    print(
        f"rows={dataset.summary['rows']} "
        f"features={dataset.summary['features']} "
        f"missing_values={dataset.summary['missing_values']} "
        f"-> {args.output} ({args.format})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
