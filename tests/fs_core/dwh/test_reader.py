"""Tests for the DWH reader seam."""

import pytest

from fintech_feature_platform.fs_core.dwh.reader import InMemoryDwhReader


def test_reads_rows_for_named_query():
    reader = InMemoryDwhReader({"q1": [{"a": 1}, {"a": 2}]})
    assert list(reader.read_rows("q1")) == [{"a": 1}, {"a": 2}]


def test_empty_query_returns_empty():
    reader = InMemoryDwhReader({"q1": []})
    assert list(reader.read_rows("q1")) == []


def test_unknown_query_raises():
    reader = InMemoryDwhReader({"q1": []})
    with pytest.raises(KeyError):
        list(reader.read_rows("nope"))
