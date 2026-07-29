#!/usr/bin/env python
"""Train the deterministic demo PD model.

Generates a deterministic training population, computes the model input features through
the REAL ComputeCore + demo registry, trains the pure-Python logistic regression, compares
it to a bureau-score-only baseline, and writes the artifact + metadata. Same arguments ->
bit-identical artifact -> stable sha256 digest (pinned in the registry YAML).

Usage:
    uv run python examples/credit_decision_demo/train_model.py
"""

from __future__ import annotations

# ruff: noqa: E402  (CLI bootstrap: make the repo root importable for `python <script>.py`)
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import argparse
import json

from examples.credit_decision_demo.features import MODEL_FEATURES, build_registry_and_udfs
from examples.credit_decision_demo.generator import GENERATOR_VERSION, generate_population
from examples.credit_decision_demo.model_lib import (
    METADATA_PATH,
    MODEL_URI,
    brier,
    compute_client_features,
    log_loss,
    predict_proba,
    roc_auc,
    save_artifact,
    train_logistic_regression,
)
from fintech_feature_platform.fs_core.compute.engine import ComputeCore

TRAIN_SEED = 4242  # distinct from demo-data seeds: the model never sees demo entities
HOLDOUT_FRACTION = 0.25


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Train the demo PD model.")
    parser.add_argument("--clients", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=TRAIN_SEED)
    args = parser.parse_args(argv)

    registry, udfs = build_registry_and_udfs()
    core = ComputeCore(registry, udfs)
    population = generate_population(args.clients, seed=args.seed)

    rows: list[list[float]] = []
    labels: list[int] = []
    for client in population:
        values = compute_client_features(core, client, list(MODEL_FEATURES))
        rows.append([float(values[name]) for name in MODEL_FEATURES])
        labels.append(client.label_default)

    split = int(len(rows) * (1 - HOLDOUT_FRACTION))
    model = train_logistic_regression(rows[:split], labels[:split])
    holdout_scores = [predict_proba(model, row) for row in rows[split:]]
    holdout_labels = labels[split:]

    # Naive baseline: bureau score alone, same training procedure.
    score_index = MODEL_FEATURES.index("bureau_score")
    baseline_rows = [[row[score_index]] for row in rows]
    baseline = train_logistic_regression(baseline_rows[:split], labels[:split])
    baseline_scores = [predict_proba(baseline, row) for row in baseline_rows[split:]]

    metrics = {
        "model": {
            "roc_auc": roc_auc(holdout_labels, holdout_scores),
            "log_loss": log_loss(holdout_labels, holdout_scores),
            "brier": brier(holdout_labels, holdout_scores),
        },
        "baseline_bureau_score_only": {
            "roc_auc": roc_auc(holdout_labels, baseline_scores),
            "log_loss": log_loss(holdout_labels, baseline_scores),
            "brier": brier(holdout_labels, baseline_scores),
        },
        "holdout_size": len(holdout_labels),
        "holdout_default_rate": round(sum(holdout_labels) / len(holdout_labels), 4),
    }

    artifact = {
        "model_type": "logistic_regression",
        "model_uri": MODEL_URI,
        "feature_order": list(MODEL_FEATURES),
        "weights": model["weights"],
        "bias": model["bias"],
        "means": model["means"],
        "stds": model["stds"],
        "training": {
            "generator_version": GENERATOR_VERSION,
            "clients": args.clients,
            "seed": args.seed,
            "holdout_fraction": HOLDOUT_FRACTION,
            "learning_rate": 0.5,
            "epochs": 400,
        },
        "note": "SYNTHETIC demo model; not suitable for real lending decisions.",
    }
    digest = save_artifact(artifact)
    METADATA_PATH.write_text(
        json.dumps({"digest": digest, "metrics": metrics}, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    print(json.dumps(metrics, indent=2))
    print(f"artifact digest: {digest}")
    print("pin this digest in registry/credit_decision_v1.yaml (pd_score.model.digest)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
