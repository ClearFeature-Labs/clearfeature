"""Storage-boundary timing proxy.

Wraps a store object so that a small named set of PLATFORM-BOUNDARY methods records
``fsp_store_operation_duration_seconds{store, operation, result}``. Deliberately not an
SDK-level interceptor: only the platform's own store entry points are timed, with the
closed store/operation/result dimensions from the catalog. Everything else is forwarded
untouched, and a metrics failure can never affect the store call itself (the operation's
outcome — including its exception — is always propagated exactly as without the proxy).
"""

from __future__ import annotations

from time import perf_counter

from fintech_feature_platform.fs_core.observability.metrics import MetricsRecorder


class TimedStore:
    """Transparent proxy timing selected methods of a wrapped store object."""

    def __init__(
        self,
        inner: object,
        metrics: MetricsRecorder,
        store: str,
        operations: dict[str, str],
    ) -> None:
        self._inner = inner
        self._metrics = metrics
        self._store = store
        self._operations = dict(operations)

    def __getattr__(self, name: str):
        attribute = getattr(self._inner, name)
        operation = self._operations.get(name)
        if operation is None or not callable(attribute):
            return attribute

        def timed(*args, **kwargs):
            started = perf_counter()
            try:
                value = attribute(*args, **kwargs)
            except Exception:
                self._record(operation, started, "error")
                raise
            self._record(operation, started, "ok")
            return value

        return timed

    def _record(self, operation: str, started: float, result: str) -> None:
        try:
            self._metrics.observe(
                "fsp_store_operation_duration_seconds",
                perf_counter() - started,
                {"store": self._store, "operation": operation, "result": result},
            )
        except Exception:  # noqa: BLE001, S110 - recording must never mask the store call
            pass
