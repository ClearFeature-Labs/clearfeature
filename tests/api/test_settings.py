import importlib.util

import pytest

from fintech_feature_platform.api.backend import (
    AppBackend,
    build_backend,
    build_memory_backend,
)
from fintech_feature_platform.api.settings import Settings, load_settings


def test_default_settings_use_memory_backend():
    settings = load_settings({})
    assert settings.backend == "memory"


def test_local_backend_parses():
    settings = load_settings({"FSP_BACKEND": "local"})
    assert settings.backend == "local"


def test_invalid_backend_raises_value_error():
    with pytest.raises(ValueError):
        load_settings({"FSP_BACKEND": "bogus"})


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("true", True), ("false", False), ("TRUE", True), ("0", False), ("1", True)],
)
def test_minio_secure_parses(raw, expected):
    settings = load_settings({"FSP_MINIO_SECURE": raw})
    assert settings.minio_secure is expected


def test_valkey_port_and_db_parse_as_integers():
    settings = load_settings({"FSP_VALKEY_PORT": "6380", "FSP_VALKEY_DB": "2"})
    assert settings.valkey_port == 6380
    assert settings.valkey_db == 2


def test_overrides_apply():
    settings = load_settings(
        {"FSP_POSTGRES_DSN": "postgresql://x@h/db", "FSP_MINIO_BUCKET": "other"}
    )
    assert settings.postgres_dsn == "postgresql://x@h/db"
    assert settings.minio_bucket == "other"


def test_build_backend_memory_returns_bundle():
    backend = build_backend(load_settings({}))
    assert isinstance(backend, AppBackend)
    assert backend.raw_compression == "none"


def test_local_backend_fails_clearly_without_extras(monkeypatch):
    # Simulate the optional packages being absent, regardless of the dev venv state.
    real_find_spec = importlib.util.find_spec

    def fake_find_spec(name, *args, **kwargs):
        if name in {"minio", "psycopg", "redis"}:
            return None
        return real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    with pytest.raises(RuntimeError, match="local backend requires"):
        build_backend(Settings(backend="local"))


def test_memory_serializer_preserves_default_str_behavior():
    backend = build_memory_backend()
    # default=str behaviour: a non-JSON-native value is stringified, not rejected.
    assert backend.serialize_payload({"x": 1}) == b'{"x": 1}'


def test_local_serializer_is_strict_canonical():
    from fintech_feature_platform.api.local_backend import _local_serialize_payload

    assert _local_serialize_payload({"b": 1, "a": 2}) == b'{"a":2,"b":1}'
    with pytest.raises(TypeError):
        _local_serialize_payload({"x": object()})
