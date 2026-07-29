"""Replay-stability of canonical hashing (value_hash / input_fingerprint)."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from fintech_feature_platform.fs_core.hashing import (
    canonical_value_json,
    input_fingerprint,
    value_hash,
)

_TS = datetime(2026, 1, 10, 12, 0, tzinfo=UTC)


def test_canonical_json_sorts_keys_and_is_compact():
    assert canonical_value_json({"b": 1, "a": [1, 2]}) == '{"a":[1,2],"b":1}'


def test_value_hash_is_order_independent_for_dict_keys():
    assert value_hash({"a": 1, "b": 2}) == value_hash({"b": 2, "a": 1})


def test_value_hash_distinguishes_int_and_float():
    assert value_hash(1) != value_hash(1.0)


def test_value_hash_of_none_is_stable():
    assert value_hash(None) == value_hash(None)


def test_non_json_value_fails_loudly():
    with pytest.raises(ValueError):
        value_hash(object())
    with pytest.raises(ValueError):
        value_hash(float("nan"))  # allow_nan=False


def test_fingerprint_is_input_order_independent():
    a = ("source:credit", _TS, "sha256:x")
    b = ("income:v1", _TS, "sha256:y")
    assert input_fingerprint([a, b]) == input_fingerprint([b, a])


def test_fingerprint_normalizes_timezone_representation():
    plus2 = _TS.astimezone(timezone(timedelta(hours=2)))
    assert input_fingerprint([("a", _TS, "h")]) == input_fingerprint([("a", plus2, "h")])


def test_fingerprint_changes_when_any_component_changes():
    base = input_fingerprint([("a", _TS, "h1"), ("b", _TS, "h2")])
    assert input_fingerprint([("a", _TS, "h1"), ("b", _TS, "CHANGED")]) != base
    assert (
        input_fingerprint([("a", _TS + timedelta(days=1), "h1"), ("b", _TS, "h2")])
        != base
    )
    assert input_fingerprint([("a2", _TS, "h1"), ("b", _TS, "h2")]) != base


def test_fingerprint_rejects_naive_timestamps():
    with pytest.raises(ValueError):
        input_fingerprint([("a", datetime(2026, 1, 1), "h")])  # noqa: DTZ001
