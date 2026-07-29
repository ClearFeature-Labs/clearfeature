"""Digest-verifying ModelRunner for the demo PD artifact.

Implements the platform's vector-first ``ModelRunner`` protocol over the committed JSON
artifact: one ``predict`` call scores the whole batch, and a ``model_ref.digest`` that
does not match the loaded artifact fails loudly (the registry pin is enforced, exactly
like the platform's ``FakeModelRunner`` test seam — but with the real trained weights).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from examples.credit_decision_demo.model_lib import (
    ARTIFACT_PATH,
    artifact_digest,
    load_artifact,
    predict_proba,
)
from fintech_feature_platform.fs_core.model_runner import ModelRef


class DemoPdModelRunner:
    """Loads the demo artifact once; scores batches; enforces the registry digest pin."""

    def __init__(self, artifact_path: Path = ARTIFACT_PATH) -> None:
        self._artifact = load_artifact(artifact_path)
        self._digest = artifact_digest(self._artifact)
        self.calls: list[int] = []  # batch sizes, for vector-first assertions

    @property
    def digest(self) -> str:
        return self._digest

    @property
    def feature_order(self) -> list[str]:
        """The artifact-owned model input order."""
        return list(self._artifact["feature_order"])

    def predict(self, model_ref: ModelRef, rows: list[dict[str, Any]]) -> list[float]:
        if model_ref.digest != self._digest:
            raise ValueError(
                f"model digest mismatch: registry pins {model_ref.digest!r}, "
                f"loaded artifact is {self._digest!r} — retrain or re-pin"
            )
        self.calls.append(len(rows))
        order = self._artifact["feature_order"]
        return [
            predict_proba(self._artifact, [float(row[name]) for name in order])
            for row in rows
        ]
