"""Pure-Python deterministic logistic regression for the demo PD model.

Zero ML dependencies: standardized features, full-batch gradient descent with fixed
hyperparameters — identical training data yields a bit-identical JSON artifact, so the
sha256 digest pinned in the registry is reproducible. The same artifact serves the F3
batch runner here and the external demo-model-service.

Also holds the shared "compute a client's registry features in memory" helper so the
trainer, goldens, and tests all go through the REAL ComputeCore (no duplicated feature
logic anywhere in the demo).
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from examples.credit_decision_demo.generator import SOURCES, Client
from fintech_feature_platform.fs_core.compute.context import RequestContext
from fintech_feature_platform.fs_core.compute.engine import ComputeCore
from fintech_feature_platform.fs_core.models import EntityKey, SourceStamp

ARTIFACT_PATH = Path(__file__).resolve().parent / "model" / "artifact.json"
METADATA_PATH = Path(__file__).resolve().parent / "model" / "metadata.json"
MODEL_URI = "mlflow://credit_pd_demo/1"


# --- feature computation through the real engine --------------------------------


def compute_client_features(
    core: ComputeCore, client: Client, features: list[str]
) -> dict[str, Any]:
    """Compute the requested registry features for one client via ComputeCore."""
    payloads = {name: getattr(client, attr) for name, (attr, _) in SOURCES.items()}
    stamps = {}
    for name, payload in payloads.items():
        ts = datetime.fromisoformat(payload.get("application_ts") or payload["report_ts"])
        stamps[name] = SourceStamp(
            report_ts=ts,
            content_hash="sha256:"
            + hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest(),
        )
    context = RequestContext(lambda name: payloads[name])
    entity_key = EntityKey.from_mapping(
        client.entity_key(), key_order=["user_id", "application_id"]
    )
    results = core.compute(
        view="credit_decision", view_version=1, entity_key=entity_key,
        requested_features=features, context=context, source_stamps=stamps,
        calc_ts=datetime(2026, 6, 2, tzinfo=UTC),
    )
    return {name: result.value for name, result in results.items()}


# --- logistic regression (deterministic, dependency-free) ------------------------


def _sigmoid(x: float) -> float:
    if x < -60:
        return 0.0
    if x > 60:
        return 1.0
    return 1.0 / (1.0 + math.exp(-x))


def _standardizer(rows: list[list[float]]) -> tuple[list[float], list[float]]:
    n, k = len(rows), len(rows[0])
    means = [sum(row[j] for row in rows) / n for j in range(k)]
    stds = []
    for j in range(k):
        var = sum((row[j] - means[j]) ** 2 for row in rows) / n
        stds.append(math.sqrt(var) or 1.0)
    return means, stds


def _standardize(row: list[float], means: list[float], stds: list[float]) -> list[float]:
    return [(value - mean) / std for value, mean, std in zip(row, means, stds, strict=True)]


def train_logistic_regression(
    rows: list[list[float]],
    labels: list[int],
    *,
    learning_rate: float = 0.5,
    epochs: int = 400,
) -> dict[str, Any]:
    """Full-batch GD on standardized features; returns weights/bias/scaler (raw floats)."""
    means, stds = _standardizer(rows)
    standardized = [_standardize(row, means, stds) for row in rows]
    n, k = len(rows), len(rows[0])
    weights = [0.0] * k
    bias = 0.0
    for _ in range(epochs):
        grad_w = [0.0] * k
        grad_b = 0.0
        for row, label in zip(standardized, labels, strict=True):
            error = _sigmoid(sum(w * x for w, x in zip(weights, row, strict=True)) + bias) - label
            for j in range(k):
                grad_w[j] += error * row[j]
            grad_b += error
        for j in range(k):
            weights[j] -= learning_rate * grad_w[j] / n
        bias -= learning_rate * grad_b / n
    return {"weights": weights, "bias": bias, "means": means, "stds": stds}


def predict_proba(model: dict[str, Any], row: list[float]) -> float:
    standardized = _standardize(row, model["means"], model["stds"])
    z = sum(w * x for w, x in zip(model["weights"], standardized, strict=True))
    return round(_sigmoid(z + model["bias"]), 6)


# --- metrics -----------------------------------------------------------------------


def roc_auc(labels: list[int], scores: list[float]) -> float:
    """Rank-based AUC (ties get midranks)."""
    pairs = sorted(zip(scores, labels, strict=True))
    n_pos = sum(labels)
    n_neg = len(labels) - n_pos
    if not n_pos or not n_neg:
        return 0.5
    rank_sum = 0.0
    i = 0
    while i < len(pairs):
        j = i
        while j < len(pairs) and pairs[j][0] == pairs[i][0]:
            j += 1
        midrank = (i + 1 + j) / 2  # average of ranks i+1..j
        rank_sum += midrank * sum(1 for _, label in pairs[i:j] if label == 1)
        i = j
    return round((rank_sum - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg), 6)


def log_loss(labels: list[int], scores: list[float]) -> float:
    eps = 1e-9
    total = sum(
        -(label * math.log(max(score, eps)) + (1 - label) * math.log(max(1 - score, eps)))
        for label, score in zip(labels, scores, strict=True)
    )
    return round(total / len(labels), 6)


def brier(labels: list[int], scores: list[float]) -> float:
    total = sum((score - label) ** 2 for label, score in zip(labels, scores, strict=True))
    return round(total / len(labels), 6)


# --- artifact IO ---------------------------------------------------------------------


def artifact_bytes(artifact: dict[str, Any]) -> bytes:
    return json.dumps(artifact, sort_keys=True, separators=(",", ":")).encode("utf-8")


def artifact_digest(artifact: dict[str, Any]) -> str:
    return "sha256:" + hashlib.sha256(artifact_bytes(artifact)).hexdigest()


def load_artifact(path: Path = ARTIFACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_artifact(artifact: dict[str, Any], path: Path = ARTIFACT_PATH) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(artifact_bytes(artifact))
    return artifact_digest(artifact)
