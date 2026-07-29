"""Prometheus-backed MetricsRecorder over a process-local registry.

Implements the existing ``MetricsRecorder`` protocol so business code keeps depending on
the recorder seam — never on ``prometheus_client`` directly. Each recorder owns its own
``CollectorRegistry`` and creates every metric object ONCE at construction from the
central catalog; nothing is created dynamically on the hot path, and repeated runtime
construction (tests) can never duplicate-register.

Fail-fast contract (matches the existing value-safe label behavior): an unknown metric
name, a wrong metric kind, a missing/extra label key, or a value outside a closed domain
raises — it never silently creates a time series and never silently drops a label.

``snapshot()`` mirrors into an ``InMemoryMetricsRecorder`` so the legacy
``/v1/observability/metrics`` JSON schema is byte-compatible with Step 0.
"""

from __future__ import annotations

from collections.abc import Mapping

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, disable_created_metrics

from fintech_feature_platform.fs_core.observability.catalog import (
    COUNTER,
    DYNAMIC,
    GAUGE,
    HISTOGRAM,
    MetricSpec,
    catalog_by_name,
)
from fintech_feature_platform.fs_core.observability.metrics import (
    InMemoryMetricsRecorder,
    MetricLabelError,
    _validate_labels,
)

# `*_created` companion series add ~25% cardinality with no analytical value for this
# platform (we never reset counters mid-process). Disabled once, module-wide.
disable_created_metrics()


class MetricCatalogError(ValueError):
    """Raised for programming errors against the catalog (unknown name / wrong kind)."""


class PrometheusMetricsRecorder:
    """``MetricsRecorder`` implementation over a process-local ``CollectorRegistry``."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()
        self._mirror = InMemoryMetricsRecorder()
        self._specs = catalog_by_name()
        self._metrics: dict[str, Counter | Gauge | Histogram] = {}
        # Runtime-bound closed domains (the ONE sanctioned case: feature_id from the
        # loaded Registry). Until bound, a DYNAMIC label rejects every value.
        self._dynamic_domains: dict[str, frozenset[str]] = {}
        # Hot-path handle cache: (name, sorted label items) -> (child, mirror_key).
        # A label set is fully validated ONCE (fail-fast preserved); afterwards the
        # prebuilt Prometheus child + prevalidated mirror key are reused — no per-call
        # validation, no labels() lookup, no re-registration. Bounded by the closed
        # domains (incl. the registry-bound feature set), so it can never grow with
        # request volume. Required by the performance budget.
        self._handles: dict[tuple, tuple] = {}
        for spec in self._specs.values():
            self._metrics[spec.name] = self._build(spec)

    def bind_dynamic_domain(self, label: str, values) -> None:
        """Bind a runtime-derived closed domain (e.g. registry feature ids).

        Values are validated with the same value-level safety as any label; the domain
        replaces any previous binding (a re-built backend re-binds its registry's set),
        and cached handles are dropped so removed identities cannot linger.
        """
        validated = frozenset(str(v) for v in values)
        for value in validated:
            _validate_labels({label: value})
        self._dynamic_domains[label] = validated
        self._handles.clear()

    def _build(self, spec: MetricSpec):
        kwargs = {"labelnames": spec.label_names, "registry": self.registry}
        if spec.kind == COUNTER:
            return Counter(spec.name, spec.help, **kwargs)
        if spec.kind == GAUGE:
            return Gauge(spec.name, spec.help, **kwargs)
        if spec.kind == HISTOGRAM:
            if spec.buckets:
                kwargs["buckets"] = spec.buckets
            return Histogram(spec.name, spec.help, **kwargs)
        raise MetricCatalogError(f"unknown metric kind {spec.kind!r} for {spec.name!r}")

    def _resolve(self, name: str, labels: Mapping[str, str] | None, kind: str):
        cache_key = (name, tuple(sorted(labels.items())) if labels else ())
        cached = self._handles.get(cache_key)
        if cached is not None:
            child, mirror_key, cached_kind = cached
            if cached_kind != kind:
                raise MetricCatalogError(
                    f"metric {name!r} is declared as a {cached_kind}, not a {kind}"
                )
            return child, mirror_key
        spec = self._specs.get(name)
        if spec is None:
            raise MetricCatalogError(
                f"metric {name!r} is not in the central catalog "
                "(fs_core/observability/catalog.py); arbitrary metric names are not allowed"
            )
        if spec.kind != kind:
            raise MetricCatalogError(
                f"metric {name!r} is declared as a {spec.kind}, not a {kind}"
            )
        given = dict(labels or {})
        if set(given) != set(spec.label_names):
            raise MetricLabelError(
                f"metric {name!r} requires labels {sorted(spec.label_names)}, "
                f"got {sorted(given)}"
            )
        _validate_labels(given)  # existing value-level safety (length + forbidden content)
        for key, domain in spec.label_domains.items():
            if domain == DYNAMIC:
                bound = self._dynamic_domains.get(key, frozenset())
                if str(given[key]) not in bound:
                    raise MetricLabelError(
                        f"metric {name!r} label {key!r} value {given[key]!r} is not a "
                        f"registry-bound identity (unbound or unregistered)"
                    )
            elif domain is not None and str(given[key]) not in domain:
                raise MetricLabelError(
                    f"metric {name!r} label {key!r} value {given[key]!r} is outside "
                    f"its closed domain"
                )
        metric = self._metrics[name]
        child = metric.labels(**given) if given else metric
        mirror_key = InMemoryMetricsRecorder._key(name, tuple(sorted(given.items())))
        self._handles[cache_key] = (child, mirror_key, kind)
        return child, mirror_key

    # --- MetricsRecorder protocol -------------------------------------------

    def incr(self, name: str, labels: Mapping[str, str] | None = None, value: float = 1) -> None:
        child, mirror_key = self._resolve(name, labels, COUNTER)
        child.inc(value)
        counters = self._mirror._counters
        counters[mirror_key] = counters.get(mirror_key, 0) + value

    def gauge(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        child, mirror_key = self._resolve(name, labels, GAUGE)
        child.set(value)
        self._mirror._gauges[mirror_key] = value

    def observe(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        child, mirror_key = self._resolve(name, labels, HISTOGRAM)
        child.observe(value)
        hist = self._mirror._hist
        summary = hist.get(mirror_key)
        if summary is None:
            hist[mirror_key] = {"count": 1, "sum": value, "min": value, "max": value}
        else:
            summary["count"] += 1
            summary["sum"] += value
            summary["min"] = min(summary["min"], value)
            summary["max"] = max(summary["max"], value)

    def snapshot(self) -> dict:
        return self._mirror.snapshot()
