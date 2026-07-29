"""Tests for batch runtime controls: rate limiter, pause, control factory."""

from fintech_feature_platform.api.batch_controls import (
    ConfiguredBatchRuntimeControls,
    DisabledRateLimiter,
    NoopBatchRuntimeControls,
    TokenBucketRateLimiter,
    UnlimitedRateLimiter,
    build_batch_runtime_controls,
)
from fintech_feature_platform.api.settings import load_settings


class _FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def test_token_bucket_grants_up_to_burst_then_refills():
    clock = _FakeClock()
    bucket = TokenBucketRateLimiter(rate_per_sec=2.0, burst=3, clock=clock)
    # Starts full at burst=3.
    assert bucket.try_acquire(2) == 2
    assert bucket.try_acquire(2) == 1  # only 1 left
    assert bucket.try_acquire(1) == 0  # empty, no time passed
    clock.advance(1.0)  # +2 tokens/sec
    assert bucket.try_acquire(5) == 2  # refilled 2 (capped at burst)


def test_token_bucket_deterministic_without_sleep():
    clock = _FakeClock()
    bucket = TokenBucketRateLimiter(rate_per_sec=1.0, burst=1, clock=clock)
    assert bucket.try_acquire(1) == 1
    assert bucket.try_acquire(1) == 0
    clock.advance(0.5)
    assert bucket.try_acquire(1) == 0  # half a token, not enough
    clock.advance(0.5)
    assert bucket.try_acquire(1) == 1


def test_unlimited_and_disabled_limiters():
    assert UnlimitedRateLimiter().try_acquire(1000) == 1000
    assert DisabledRateLimiter().try_acquire(1000) == 0


def test_noop_controls_never_pause_unlimited():
    controls = NoopBatchRuntimeControls()
    assert controls.should_pause().paused is False
    assert controls.rate_limiter.try_acquire(9) == 9
    assert controls.online_refresh_limiter.try_acquire(9) == 9


def test_configured_pause_on_high_lag():
    controls = ConfiguredBatchRuntimeControls(
        rate_limiter=UnlimitedRateLimiter(),
        online_refresh_limiter=UnlimitedRateLimiter(),
        pause_enabled=True,
        max_consumer_lag=100,
        lag_fn=lambda: 250,
    )
    decision = controls.should_pause()
    assert decision.paused is True
    assert "250" in decision.reason


def test_configured_no_pause_when_lag_low_or_disabled():
    low = ConfiguredBatchRuntimeControls(
        rate_limiter=UnlimitedRateLimiter(),
        online_refresh_limiter=UnlimitedRateLimiter(),
        pause_enabled=True, max_consumer_lag=100, lag_fn=lambda: 5,
    )
    assert low.should_pause().paused is False
    # pause disabled -> never pauses even with high lag.
    off = ConfiguredBatchRuntimeControls(
        rate_limiter=UnlimitedRateLimiter(),
        online_refresh_limiter=UnlimitedRateLimiter(),
        pause_enabled=False, max_consumer_lag=1, lag_fn=lambda: 999,
    )
    assert off.should_pause().paused is False


def test_build_controls_parses_runtime_knobs(monkeypatch):
    monkeypatch.setenv("FSP_BATCH_RATE_LIMIT_CHUNKS_PER_SEC", "4")
    monkeypatch.setenv("FSP_BATCH_ONLINE_REFRESH_ENABLED", "true")
    monkeypatch.setenv("FSP_BATCH_ONLINE_REFRESH_TOKENS_PER_SEC", "10")
    monkeypatch.setenv("FSP_BATCH_ONLINE_REFRESH_BURST", "10")
    monkeypatch.setenv("FSP_BATCH_ONLINE_REFRESH_MAX_FEATURES_PER_CHUNK", "50")
    settings = load_settings()
    controls = build_batch_runtime_controls(settings)
    assert controls.rate_limiter.try_acquire(100) == 4  # bucket burst=rate=4
    assert controls.online_refresh_max_features == 50
    assert controls.online_refresh_limiter.try_acquire(100) == 10


def test_db_pool_size_setting(monkeypatch):
    # : real process pool size, default 4.
    assert load_settings().db_pool_size == 4
    monkeypatch.setenv("FSP_DB_POOL_SIZE", "10")
    assert load_settings().db_pool_size == 10
    # 0 is the documented rollback lever (legacy connection-per-operation).
    monkeypatch.setenv("FSP_DB_POOL_SIZE", "0")
    assert load_settings().db_pool_size == 0


def test_build_controls_online_refresh_disabled_by_default():
    controls = build_batch_runtime_controls(load_settings())  # default env
    # Disabled globally -> the limiter grants nothing (batch cannot touch online).
    assert controls.online_refresh_limiter.try_acquire(5) == 0
