"""Model runner seam for F3 batch model-as-feature.

F3 is **vector-first**: the scoring function receives a frame of input rows and returns a
frame of predictions (online would call it with a 1-row frame — but online F3 is out of
scope for beta). The platform loads the model from its pinned ``uri`` at ``digest`` and
calls it in batch. This module is the narrow contract; a real MLflow-backed runner is a
later deployment concern (never a live-server dependency for tests).

``FakeModelRunner`` is deterministic, makes no network call, validates the pinned digest,
and records each ``predict`` call so tests can prove the model is called **once per batch**,
not once per row.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class ModelRef:
    """A pinned model to score with: artifact ``uri`` + ``digest`` + prediction column."""

    uri: str
    digest: str
    output_name: str


class ModelRunner(Protocol):
    def predict(self, model_ref: ModelRef, rows: list[dict[str, Any]]) -> list[Any]:
        """Score a batch: ``rows`` is a vector of input-feature dicts, one per entity.

        Returns one prediction per input row, in order. Implementations must not call
        external services at inference time beyond loading the pinned artifact.
        """
        ...


class FakeModelRunner:
    """Deterministic in-memory runner for tests — no network, no MLflow server.

    ``score_fn(row) -> value`` defaults to the sum of the row's numeric inputs. When
    ``expected_digest`` is set, a mismatched ``model_ref.digest`` fails loudly (simulating
    digest verification). ``calls`` records each batch size so a test can assert the model
    was invoked once with a frame rather than once per row.
    """

    def __init__(
        self,
        score_fn: Callable[[dict[str, Any]], Any] | None = None,
        *,
        expected_digest: str | None = None,
    ) -> None:
        self._score_fn = score_fn or _default_score
        self._expected_digest = expected_digest
        self.calls: list[int] = []

    def predict(self, model_ref: ModelRef, rows: list[dict[str, Any]]) -> list[Any]:
        if self._expected_digest is not None and model_ref.digest != self._expected_digest:
            raise ValueError(
                f"model digest mismatch for {model_ref.uri!r}: expected "
                f"{self._expected_digest!r}, got {model_ref.digest!r}"
            )
        self.calls.append(len(rows))
        return [self._score_fn(row) for row in rows]


def _default_score(row: dict[str, Any]) -> float:
    return float(sum(v for v in row.values() if isinstance(v, (int, float))))
