"""Promotion governance: approval profiles, shadow soak, rollback repoint."""

import json
from datetime import UTC, datetime, timedelta

import pytest

from fintech_feature_platform.fs_core.registry.promotion import (
    InMemoryPointerStore,
    PromotionError,
    promote,
    rollback,
)

_D0 = datetime(2026, 1, 1, tzinfo=UTC)
_DIGEST = "sha256:bundleA"


def _shadow(store, *, digest=_DIGEST, now=_D0, env="prod"):
    return promote(
        bundle_exists=True, pointer_store=store, bundle_digest=digest, env=env,
        stage="shadow", actor="carol", reason="start shadow", now=now,
    )


# --- shadow ------------------------------------------------------------------

def test_promote_to_shadow_records_shadow_started_at():
    store = InMemoryPointerStore()
    record = _shadow(store)
    assert record.stage == "shadow"
    assert record.shadow_started_at == _D0
    pointer = store.get_pointer("prod", "shadow")
    assert pointer.bundle_digest == _DIGEST
    assert pointer.shadow_started_at == _D0


def test_unknown_bundle_rejected():
    with pytest.raises(PromotionError, match="unknown bundle"):
        promote(bundle_exists=False, pointer_store=InMemoryPointerStore(),
                bundle_digest=_DIGEST, env="prod", stage="shadow", actor="g",
                reason="x", now=_D0)


# --- live: approval profiles -------------------------------------------------

def _live(store, *, approvers, now, profile="bank", override=False, reason="promote",
          override_reason=None):
    return promote(
        bundle_exists=True, pointer_store=store, bundle_digest=_DIGEST, env="prod",
        stage="live", actor="carol", reason=reason, now=now, profile=profile,
        approved_by=approvers, shadow_age_override=override, override_reason=override_reason,
    )


def test_bank_live_fails_with_one_approver():
    store = InMemoryPointerStore()
    _shadow(store)
    with pytest.raises(PromotionError, match="requires 2 unique approver"):
        _live(store, approvers=["alice"], now=_D0 + timedelta(days=8))


def test_bank_live_succeeds_with_two_unique_approvers_after_soak():
    store = InMemoryPointerStore()
    _shadow(store)
    record = _live(store, approvers=["alice", "bob"], now=_D0 + timedelta(days=8))
    assert record.stage == "live"
    assert record.approved_by == ("alice", "bob")
    assert record.profile == "bank"
    assert store.get_pointer("prod", "live").bundle_digest == _DIGEST


def test_bank_live_duplicate_approver_fails():
    store = InMemoryPointerStore()
    _shadow(store)
    with pytest.raises(PromotionError, match="unique"):
        _live(store, approvers=["alice", "alice"], now=_D0 + timedelta(days=8))


def test_energy_live_succeeds_with_single_approver_no_soak():
    store = InMemoryPointerStore()
    _shadow(store)
    record = _live(store, approvers=["alice"], now=_D0, profile="energy")  # 0-day soak
    assert record.stage == "live"
    assert record.profile == "energy"


# --- live: shadow soak -------------------------------------------------------

def test_bank_live_before_shadow_min_days_fails():
    store = InMemoryPointerStore()
    _shadow(store)
    with pytest.raises(PromotionError, match="shadow soak"):
        _live(store, approvers=["alice", "bob"], now=_D0 + timedelta(days=1))


def test_shadow_age_override_bypasses_soak_with_reason():
    store = InMemoryPointerStore()
    _shadow(store)
    record = _live(store, approvers=["alice", "bob"], now=_D0 + timedelta(days=1),
                   override=True, override_reason="hotfix approved by risk")
    assert record.shadow_age_override is True
    assert record.override_reason == "hotfix approved by risk"


def test_shadow_age_override_requires_reason():
    store = InMemoryPointerStore()
    _shadow(store)
    with pytest.raises(PromotionError, match="requires an explicit reason"):
        _live(store, approvers=["alice", "bob"], now=_D0 + timedelta(days=1), override=True)


def test_live_requires_profile():
    store = InMemoryPointerStore()
    _shadow(store)
    with pytest.raises(PromotionError, match="--profile is required"):
        promote(bundle_exists=True, pointer_store=store, bundle_digest=_DIGEST, env="prod",
                stage="live", actor="g", reason="x", now=_D0 + timedelta(days=8),
                approved_by=["alice", "bob"])


# --- rollback ----------------------------------------------------------------

def test_rollback_to_previous_repoints_and_records():
    store = InMemoryPointerStore()
    _shadow(store)
    _live(store, approvers=["alice", "bob"], now=_D0 + timedelta(days=8))
    # promote a second bundle to live so there is a previous digest.
    promote(bundle_exists=True, pointer_store=store, bundle_digest="sha256:bundleB",
            env="prod", stage="shadow", actor="g", reason="s2", now=_D0 + timedelta(days=9))
    promote(bundle_exists=True, pointer_store=store, bundle_digest="sha256:bundleB",
            env="prod", stage="live", actor="g", reason="p2", now=_D0 + timedelta(days=20),
            profile="bank", approved_by=["alice", "bob"])
    assert store.get_pointer("prod", "live").bundle_digest == "sha256:bundleB"

    record = rollback(pointer_store=store, env="prod", actor="carol",
                      reason="bundleB is bad", now=_D0 + timedelta(days=21), to_previous=True)
    assert record.action == "rollback"
    assert record.bundle_digest == _DIGEST          # rolled back to A
    assert record.previous_digest == "sha256:bundleB"
    assert store.get_pointer("prod", "live").bundle_digest == _DIGEST


def test_rollback_to_specified_digest():
    store = InMemoryPointerStore()
    _shadow(store)
    _live(store, approvers=["alice", "bob"], now=_D0 + timedelta(days=8))
    record = rollback(pointer_store=store, env="prod", actor="g", reason="known good",
                      now=_D0 + timedelta(days=9), bundle_digest="sha256:knownGood",
                      bundle_exists=True)
    assert record.bundle_digest == "sha256:knownGood"
    assert store.get_pointer("prod", "live").bundle_digest == "sha256:knownGood"


def test_rollback_requires_reason_and_target():
    store = InMemoryPointerStore()
    _shadow(store)
    _live(store, approvers=["alice", "bob"], now=_D0 + timedelta(days=8))
    with pytest.raises(PromotionError, match="reason is required"):
        rollback(pointer_store=store, env="prod", actor="g", reason="",
                 now=_D0, to_previous=True)
    with pytest.raises(PromotionError, match="--bundle-digest or --to-previous"):
        rollback(pointer_store=store, env="prod", actor="g", reason="x", now=_D0)


# --- records are values-free -------------------------------------------------

def test_promotion_record_is_values_free():
    store = InMemoryPointerStore()
    _shadow(store)
    record = _live(store, approvers=["alice", "bob"], now=_D0 + timedelta(days=8))
    blob = json.dumps(record.to_dict())
    for forbidden in ("value", "payload", "object_key", "storage_uri", "sql"):
        assert forbidden not in blob
    # It carries only digests / actors / reasons / approvals / timestamps.
    assert record.to_dict()["bundle_digest"] == _DIGEST
