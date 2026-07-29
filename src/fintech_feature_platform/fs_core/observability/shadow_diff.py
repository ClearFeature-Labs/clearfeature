"""Shadow-vs-live diff by hashes/counts only.

Compares a live feature and its shadow candidate across a set of entities using ``value_hash``
and presence — **never** by exposing feature values. The output is bounded counts plus a small
sample of entity refs, so support can judge "did the new logic change outputs, and for how
many entities" without reading any value. This is a support helper, not an experimentation
platform.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from fintech_feature_platform.fs_core.models import EntityKey

_SAMPLE_LIMIT = 10


@dataclass(frozen=True)
class ShadowDiff:
    live_feature: str
    shadow_feature: str
    total_compared: int
    same_hash: int
    different_hash: int
    missing_live: int
    missing_shadow: int
    sample_different: tuple[str, ...] = ()
    sample_missing_live: tuple[str, ...] = ()
    sample_missing_shadow: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "live_feature": self.live_feature,
            "shadow_feature": self.shadow_feature,
            "total_compared": self.total_compared,
            "same_hash": self.same_hash,
            "different_hash": self.different_hash,
            "missing_live": self.missing_live,
            "missing_shadow": self.missing_shadow,
            "sample_different": list(self.sample_different),
            "sample_missing_live": list(self.sample_missing_live),
            "sample_missing_shadow": list(self.sample_missing_shadow),
        }


def _latest_hash(offline, entity_key, *, feature_name, feature_version, view, view_version):
    records = offline.get(
        entity_key, feature_name=feature_name, feature_version=feature_version,
        view=view, view_version=view_version,
    )
    if not records:
        return None
    latest = max(records, key=lambda r: r.result.calc_ts)
    return latest.result.value_hash


def diff_shadow_vs_live(
    offline,
    entity_keys: Sequence[EntityKey],
    *,
    view: str,
    view_version: int,
    live_feature: str,
    shadow_feature: str,
    live_version: int = 1,
    shadow_version: int = 1,
) -> ShadowDiff:
    """Compare live vs shadow offline outputs for the given entities by ``value_hash`` only."""
    same = different = missing_live = missing_shadow = 0
    sample_diff: list[str] = []
    sample_ml: list[str] = []
    sample_ms: list[str] = []
    for entity_key in entity_keys:
        live_hash = _latest_hash(
            offline, entity_key, feature_name=live_feature, feature_version=live_version,
            view=view, view_version=view_version,
        )
        shadow_hash = _latest_hash(
            offline, entity_key, feature_name=shadow_feature, feature_version=shadow_version,
            view=view, view_version=view_version,
        )
        encoded = entity_key.encode()
        if live_hash is None and shadow_hash is None:
            continue  # neither computed for this entity — nothing to compare
        if live_hash is None:
            missing_live += 1
            _append_capped(sample_ml, encoded)
            continue
        if shadow_hash is None:
            missing_shadow += 1
            _append_capped(sample_ms, encoded)
            continue
        if live_hash == shadow_hash:
            same += 1
        else:
            different += 1
            _append_capped(sample_diff, encoded)
    return ShadowDiff(
        live_feature=live_feature,
        shadow_feature=shadow_feature,
        total_compared=same + different,
        same_hash=same,
        different_hash=different,
        missing_live=missing_live,
        missing_shadow=missing_shadow,
        sample_different=tuple(sample_diff),
        sample_missing_live=tuple(sample_ml),
        sample_missing_shadow=tuple(sample_ms),
    )


def _append_capped(sample: list[str], value: str) -> None:
    if len(sample) < _SAMPLE_LIMIT:
        sample.append(value)
