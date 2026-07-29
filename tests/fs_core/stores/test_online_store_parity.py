"""D9 parity contract: InMemoryOnlineStore and ValkeyOnlineStore behave identically.

One scenario suite runs against both stores (Valkey via the fake client, whose
decision path delegates to the shared ``write_guard`` helper; the real Lua is covered
by the gated integration test in ``test_valkey_online_store.py``).
"""

from datetime import UTC, datetime

import pytest
from tests.fs_core.stores.test_valkey_online_store import _FakeValkeyClient

from fintech_feature_platform.fs_core.models import EntityKey, FeatureRef, FeatureResult
from fintech_feature_platform.fs_core.stores.online import InMemoryOnlineStore
from fintech_feature_platform.fs_core.stores.valkey_online import ValkeyOnlineStore
from fintech_feature_platform.fs_core.write_guard import (
    NOOP,
    SKIPPED_STALE,
    WRITTEN,
    WRITTEN_RECOMPUTE,
)

_JAN1 = datetime(2026, 1, 1, tzinfo=UTC)
_JAN5 = datetime(2026, 1, 5, tzinfo=UTC)
_JAN10 = datetime(2026, 1, 10, tzinfo=UTC)
_JAN20 = datetime(2026, 1, 20, tzinfo=UTC)


def _key() -> EntityKey:
    return EntityKey.from_mapping({"user_id": "1"})


def _result(value, data_ts, max_input_data_ts=None, fingerprint=None):
    return FeatureResult(
        ref=FeatureRef("c", 1),
        entity_key=_key(),
        value=value,
        data_ts=data_ts,
        calc_ts=_JAN1,
        max_input_data_ts=max_input_data_ts,
        input_fingerprint=fingerprint,
    )


def _stores():
    return [
        pytest.param(InMemoryOnlineStore(), id="inmemory"),
        pytest.param(ValkeyOnlineStore(_FakeValkeyClient()), id="valkey-fake"),
    ]


@pytest.mark.parametrize("store", _stores())
def test_d9_scenario_sequence_matches(store):
    # Case A setup: C=(Jan1, Jan10).
    assert store.write("v", 1, _result(1, _JAN1, _JAN10, "fp1")) == WRITTEN
    # Case A: non-min input update -> (Jan1, Jan20) accepted.
    assert store.write("v", 1, _result(2, _JAN1, _JAN20, "fp2")) == WRITTEN
    # Case B: historical wave -> (Jan1, Jan5) rejected as stale.
    assert store.write("v", 1, _result(0, _JAN1, _JAN5, "fp0")) == SKIPPED_STALE
    # Identical replay: equal tuple + same fingerprint -> noop.
    assert store.write("v", 1, _result(2, _JAN1, _JAN20, "fp2")) == NOOP
    # Case C: equal tuple + changed fingerprint -> same-freshness recompute.
    assert store.write("v", 1, _result(3, _JAN1, _JAN20, "fp3")) == WRITTEN_RECOMPUTE
    got = store.get("v", 1, _key(), "c", 1)
    assert got.value == 3
    assert got.max_input_data_ts == _JAN20
    assert got.input_fingerprint == "fp3"


@pytest.mark.parametrize("store", _stores())
def test_f1_degenerate_sequence_matches(store):
    later = datetime(2026, 2, 1, tzinfo=UTC)
    assert store.write("v", 1, _result(1, _JAN1)) == WRITTEN
    assert store.write("v", 1, _result(2, later)) == WRITTEN
    assert store.write("v", 1, _result(0, _JAN1)) == SKIPPED_STALE
    assert store.write("v", 1, _result(2, later)) == NOOP
    assert store.get("v", 1, _key(), "c", 1).value == 2
