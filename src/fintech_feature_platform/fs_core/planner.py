"""Feature Planner V1: expand requested groups + explicit features into an output set.

``plan_features`` is a **pure** function: given a ``FeatureViewDef`` and the raw request
(``requested_features`` + ``requested_feature_groups``), it returns a ``FeaturePlan``
whose ``compute_features`` is the deduplicated, deterministically ordered set of output
feature names to pass to the existing compute path.

It does **not** compute values, call stores, or call ``ComputeCore`` — ``ComputeCore``
already resolves transitive dependencies, caches intermediates, and returns only the
requested names, so dependencies stay ephemeral (computed, not materialized) in V1.
Dependency cycles / unknown dependencies are validated cheaply from ``FeatureDef.deps``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from fintech_feature_platform.fs_core.models import FeatureRef
from fintech_feature_platform.fs_core.registry.models import (
    LIFECYCLE_DRAFT,
    LIFECYCLE_LIVE,
    LIFECYCLE_SHADOW,
    FeatureDef,
    FeatureViewDef,
)


class PlannerError(ValueError):
    """Raised for an invalid feature/group request (unknown name, cycle, empty set)."""


@dataclass(frozen=True)
class FeaturePlan:
    view: str
    view_version: int
    requested_outputs: tuple[FeatureRef, ...]
    compute_features: tuple[str, ...]
    expanded_groups: dict[str, tuple[str, ...]]
    # Which computed outputs are shadow features; empty unless allow_shadow.
    shadow_features: tuple[str, ...] = ()


def plan_features(
    view_def: FeatureViewDef,
    requested_features: Sequence[str],
    requested_feature_groups: Sequence[str],
    *,
    allow_shadow: bool = False,
) -> FeaturePlan:
    """Expand groups + explicit features into a validated, ordered output set.

    Lifecycle gating : only ``live`` features are computable by default;
    ``shadow`` features require ``allow_shadow=True`` (offline/shadow runs); ``draft`` and
    ``deprecated`` are never computable here. The online worker keeps the default
    (``allow_shadow=False``), so online serves ``live`` only.
    """
    features_by_name = {feature.name: feature for feature in view_def.features}
    groups = dict(view_def.feature_groups)

    ordered: list[str] = []
    seen: set[str] = set()

    def _add(name: str) -> None:
        if name not in seen:
            seen.add(name)
            ordered.append(name)

    def _check_computable(name: str) -> None:
        kind = features_by_name[name].kind
        # model_score features are externally written, never computed.
        if kind == "model_score":
            raise PlannerError(
                f"feature {name!r} in view {view_def.name!r} is a model_score feature "
                f"and cannot be computed (write it via /v1/model-scores)"
            )
        # F3 model features are batch-only: computed by the batch model path, never by this
        # UDF compute path (so online and raw-source batch chunks reject them). The dedicated
        # F3 batch path and the offline recompute wave do not call plan_features.
        if kind == "model":
            raise PlannerError(
                f"feature {name!r} in view {view_def.name!r} is a batch-only model "
                f"feature (F3) and cannot be computed online or in a raw-source chunk; "
                f"compute it via the batch model path"
            )
        # Lifecycle gating: only live (always) and shadow (with allow_shadow) are computable.
        lifecycle = features_by_name[name].lifecycle
        if lifecycle == LIFECYCLE_DRAFT:
            raise PlannerError(
                f"feature {name!r} in view {view_def.name!r} is a draft feature "
                f"(validation-only) and is not served/computed"
            )
        if lifecycle == LIFECYCLE_SHADOW and not allow_shadow:
            raise PlannerError(
                f"feature {name!r} in view {view_def.name!r} is a shadow feature and is "
                f"not served online; compute it only in an offline/shadow run (allow_shadow)"
            )
        if lifecycle not in (LIFECYCLE_LIVE, LIFECYCLE_SHADOW):  # e.g. deprecated
            raise PlannerError(
                f"feature {name!r} in view {view_def.name!r} has lifecycle "
                f"{lifecycle!r} and cannot be computed (only live, or shadow with "
                f"allow_shadow)"
            )

    # Explicit requested features first (in request order).
    for name in requested_features:
        if name not in features_by_name:
            raise PlannerError(
                f"unknown feature {name!r} in view {view_def.name!r} "
                f"v{view_def.view_version}"
            )
        _check_computable(name)
        _add(name)

    # Then group-expanded features (group order, then list order); skip duplicates.
    expanded_groups: dict[str, tuple[str, ...]] = {}
    for group_name in requested_feature_groups:
        if group_name not in groups:
            raise PlannerError(
                f"unknown feature group {group_name!r} in view {view_def.name!r} "
                f"v{view_def.view_version}"
            )
        members = tuple(groups[group_name])
        expanded_groups[group_name] = members
        for name in members:
            if name not in features_by_name:
                raise PlannerError(
                    f"feature group {group_name!r} references unknown feature {name!r} "
                    f"in view {view_def.name!r}"
                )
            _check_computable(name)
            _add(name)

    if not ordered:
        raise PlannerError(
            f"request expands to no output features for view {view_def.name!r} "
            f"v{view_def.view_version}"
        )

    _validate_deps(ordered, features_by_name, view_def)

    outputs = tuple(features_by_name[name].ref() for name in ordered)
    shadow_features = tuple(
        name for name in ordered
        if features_by_name[name].lifecycle == LIFECYCLE_SHADOW
    )
    return FeaturePlan(
        view=view_def.name,
        view_version=view_def.view_version,
        requested_outputs=outputs,
        compute_features=tuple(ordered),
        expanded_groups=expanded_groups,
        shadow_features=shadow_features,
    )


def _validate_deps(
    names: Sequence[str],
    features_by_name: dict[str, FeatureDef],
    view_def: FeatureViewDef,
) -> None:
    """DFS over ``FeatureDef.deps`` detecting unknown deps and cycles (pre-compute)."""
    done: set[str] = set()
    visiting: list[str] = []

    def _visit(name: str) -> None:
        if name in done:
            return
        if name in visiting:
            cycle = " -> ".join([*visiting, name])
            raise PlannerError(
                f"dependency cycle detected in view {view_def.name!r}: {cycle}"
            )
        feature = features_by_name.get(name)
        if feature is None:
            raise PlannerError(
                f"unknown feature {name!r} in view {view_def.name!r}"
            )
        visiting.append(name)
        for dep in feature.deps:
            if dep.feature not in features_by_name:
                raise PlannerError(
                    f"feature {name!r} depends on unknown feature {dep.feature!r} "
                    f"in view {view_def.name!r}"
                )
            # A feature that (transitively) depends on a batch-only F3 model feature cannot
            # be computed by this UDF path — online must reject it, and a raw-source chunk
            # cannot resolve it either (F3 is materialized offline first, then read by a
            # recompute wave). The wave/offline path bypasses plan_features.
            if features_by_name[dep.feature].kind == "model":
                raise PlannerError(
                    f"feature {name!r} in view {view_def.name!r} depends on batch-only "
                    f"model feature {dep.feature!r} (F3), which cannot be computed online "
                    f"or in a raw-source chunk"
                )
            _visit(dep.feature)
        visiting.pop()
        done.add(name)

    for name in names:
        _visit(name)
