"""Batch runtime isolation controls: pause, rate limit, guarded online-refresh budget.

Runtime isolation is a product feature, not only infrastructure : batch is
only safe next to online serving if it cannot silently steal capacity. These are the
knobs the batch runner/worker consult — the online runtime never pauses and is never rate
limited by this module.

Everything is non-blocking and clock-injectable so tests are deterministic (no sleeps):
``TokenBucketRateLimiter.try_acquire`` returns how many of ``n`` were granted right now.
``NoopBatchRuntimeControls`` never pauses and grants everything (the safe default for
memory mode / tests / callers that pass no controls).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class PauseDecision:
    paused: bool
    reason: str | None = None


class RateLimiter(Protocol):
    def try_acquire(self, n: int = 1) -> int:
        """Grant up to ``n`` tokens now; return how many were granted (0..n)."""
        ...


class UnlimitedRateLimiter:
    """Grants everything — the default when no limit is configured."""

    def try_acquire(self, n: int = 1) -> int:
        return n


class DisabledRateLimiter:
    """Grants nothing — used when a capability (e.g. online refresh) is turned off."""

    def try_acquire(self, n: int = 1) -> int:
        return 0


class TokenBucketRateLimiter:
    """Non-blocking token bucket. Refills at ``rate_per_sec`` up to ``burst``.

    ``clock`` is injectable (monotonic seconds) so tests advance time deterministically
    instead of sleeping. ``try_acquire(n)`` grants ``min(n, floor(available))`` tokens.
    """

    def __init__(
        self,
        rate_per_sec: float,
        burst: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if rate_per_sec < 0 or burst < 0:
            raise ValueError("rate_per_sec and burst must be non-negative")
        self._rate = float(rate_per_sec)
        self._burst = float(burst)
        self._clock = clock
        self._tokens = float(burst)
        self._last = clock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = max(0.0, now - self._last)
        self._last = now
        self._tokens = min(self._burst, self._tokens + elapsed * self._rate)

    def try_acquire(self, n: int = 1) -> int:
        if n <= 0:
            return 0
        self._refill()
        granted = min(n, int(self._tokens))
        self._tokens -= granted
        return granted


class BatchRuntimeControls(Protocol):
    rate_limiter: RateLimiter
    online_refresh_limiter: RateLimiter
    online_refresh_max_features: int  # 0 = unbounded per chunk

    def should_pause(self) -> PauseDecision: ...


class NoopBatchRuntimeControls:
    """Never pauses; unlimited rate and online-refresh budget (default)."""

    def __init__(self) -> None:
        self.rate_limiter: RateLimiter = UnlimitedRateLimiter()
        self.online_refresh_limiter: RateLimiter = UnlimitedRateLimiter()
        self.online_refresh_max_features = 0

    def should_pause(self) -> PauseDecision:
        return PauseDecision(paused=False)


@dataclass
class ConfiguredBatchRuntimeControls:
    """Lag-based pause + rate limiters wired from settings.

    ``lag_fn`` returns the current online consumer lag (or None if unknown). Pause is only
    enabled when both ``pause_enabled`` and a ``lag_fn`` are provided — memory mode / tests
    have no real lag source, so they stay Noop unless a fake ``lag_fn`` is injected.
    """

    rate_limiter: RateLimiter
    online_refresh_limiter: RateLimiter
    online_refresh_max_features: int = 0
    pause_enabled: bool = False
    max_consumer_lag: int = 0
    lag_fn: Callable[[], int | None] | None = None

    def should_pause(self) -> PauseDecision:
        if not self.pause_enabled or self.lag_fn is None:
            return PauseDecision(paused=False)
        lag = self.lag_fn()
        if lag is not None and lag > self.max_consumer_lag:
            return PauseDecision(
                paused=True,
                reason=f"online consumer lag {lag} > max {self.max_consumer_lag}",
            )
        return PauseDecision(paused=False)


def _limiter(rate_per_sec: float, burst: float) -> RateLimiter:
    if rate_per_sec and rate_per_sec > 0:
        return TokenBucketRateLimiter(rate_per_sec, burst or rate_per_sec)
    return UnlimitedRateLimiter()


def build_batch_runtime_controls(
    settings, lag_fn: Callable[[], int | None] | None = None
) -> ConfiguredBatchRuntimeControls:
    """Wire ``ConfiguredBatchRuntimeControls`` from Settings.

    Rate limits of 0 mean unlimited. Pause needs both ``batch_pause_enabled`` and a
    ``lag_fn`` (a real lag source is deployment-provided; absent it, never pauses).
    """
    if settings.batch_online_refresh_enabled:
        online_limiter: RateLimiter = _limiter(
            settings.batch_online_refresh_tokens_per_sec,
            settings.batch_online_refresh_burst,
        )
    else:
        # Off by default: a job may request guarded refresh, but nothing is written online
        # until an operator enables it (batch cannot silently touch online serving).
        online_limiter = DisabledRateLimiter()
    return ConfiguredBatchRuntimeControls(
        rate_limiter=_limiter(
            settings.batch_rate_limit_chunks_per_sec,
            settings.batch_rate_limit_chunks_per_sec,
        ),
        online_refresh_limiter=online_limiter,
        online_refresh_max_features=settings.batch_online_refresh_max_features_per_chunk,
        pause_enabled=settings.batch_pause_enabled,
        max_consumer_lag=settings.batch_max_consumer_lag,
        lag_fn=lag_fn,
    )
