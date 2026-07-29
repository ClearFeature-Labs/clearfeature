"""In-process metrics recorder for golden operational metrics.

A tiny, Docker-free metrics seam — no Prometheus/Grafana dependency. Workers record counters
(``incr``), gauges (``gauge``), and observations/histograms (``observe``) through
``recorder_of(backend)``, which falls back to a shared no-op when a backend has no recorder, so
**metrics never affect business logic** and a lightweight test backend never breaks.

Labels are bounded and sanitized: a label value that looks like a raw payload / storage path /
SQL / feature value is rejected, so metric series can never leak values or high-cardinality
free text. ``snapshot()`` returns a plain JSON-able dict for the metrics endpoint / CLI.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

# Label *values* containing these substrings are rejected — metrics must stay values-free and
# low-cardinality (never a payload, object key, storage uri, SQL, or a serialized value).
_FORBIDDEN_LABEL_SUBSTRINGS = (
    "payload", "object_key", "storage_uri", "storage://", "s3://", "select ", "insert ",
    "{", "}", "\n",
)
_MAX_LABEL_LEN = 64


class MetricLabelError(ValueError):
    """Raised when a metric label value is unbounded or looks like forbidden content."""


def _validate_labels(labels: Mapping[str, str] | None) -> tuple[tuple[str, str], ...]:
    if not labels:
        return ()
    validated: list[tuple[str, str]] = []
    for key, value in sorted(labels.items()):
        text = str(value)
        if len(text) > _MAX_LABEL_LEN:
            raise MetricLabelError(
                f"metric label {key!r} value is too long ({len(text)} > {_MAX_LABEL_LEN})"
            )
        low = text.lower()
        if any(bad in low for bad in _FORBIDDEN_LABEL_SUBSTRINGS):
            raise MetricLabelError(
                f"metric label {key!r} value looks like forbidden content and was rejected"
            )
        validated.append((key, text))
    return tuple(validated)


class MetricsRecorder(Protocol):
    def incr(
        self, name: str, labels: Mapping[str, str] | None = None, value: float = 1
    ) -> None: ...

    def observe(
        self, name: str, value: float, labels: Mapping[str, str] | None = None
    ) -> None: ...

    def gauge(
        self, name: str, value: float, labels: Mapping[str, str] | None = None
    ) -> None: ...

    def snapshot(self) -> dict: ...


class NoopMetricsRecorder:
    """Records nothing. The default fallback so instrumentation is always safe to call."""

    def incr(self, name: str, labels: Mapping[str, str] | None = None, value: float = 1) -> None:
        return None

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        return None

    def gauge(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        return None

    def snapshot(self) -> dict:
        return {"counters": {}, "gauges": {}, "histograms": {}}


class InMemoryMetricsRecorder:
    """Deterministic in-process recorder (counters + gauges + histogram summaries)."""

    def __init__(self) -> None:
        self._counters: dict[str, float] = {}
        self._gauges: dict[str, float] = {}
        self._hist: dict[str, dict[str, float]] = {}

    @staticmethod
    def _key(name: str, labels: tuple[tuple[str, str], ...]) -> str:
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in labels)
        return f"{name}{{{label_str}}}"

    def incr(self, name: str, labels: Mapping[str, str] | None = None, value: float = 1) -> None:
        key = self._key(name, _validate_labels(labels))
        self._counters[key] = self._counters.get(key, 0) + value

    def gauge(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        self._gauges[self._key(name, _validate_labels(labels))] = value

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        key = self._key(name, _validate_labels(labels))
        summary = self._hist.get(key)
        if summary is None:
            self._hist[key] = {"count": 1, "sum": value, "min": value, "max": value}
            return
        summary["count"] += 1
        summary["sum"] += value
        summary["min"] = min(summary["min"], value)
        summary["max"] = max(summary["max"], value)

    def snapshot(self) -> dict:
        return {
            "counters": dict(sorted(self._counters.items())),
            "gauges": dict(sorted(self._gauges.items())),
            "histograms": {k: dict(v) for k, v in sorted(self._hist.items())},
        }


_NOOP = NoopMetricsRecorder()


def recorder_of(backend: object) -> MetricsRecorder:
    """Return the backend's metrics recorder, or a shared no-op if it has none.

    Instrumentation calls this so it is always safe: a backend (or test double) without a
    ``metrics`` attribute records nothing rather than raising.
    """
    return getattr(backend, "metrics", None) or _NOOP
