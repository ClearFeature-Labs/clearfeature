"""Cross-object validation for the registry contract.

Per-object field validity is enforced by the dataclasses themselves. This
module checks relationships *between* objects:

- entity, source, and view names are unique;
- feature names are unique within a view;
- each view references a known entity;
- each view's key_fields exactly match (same fields, same order) the referenced
  entity's key_fields;
- each feature input references a known source;
- each feature dependency references a known feature in the same view, pins its exact
  version, forms no cycle, and stays within the view's dependency depth cap (F2).

Validation raises ``ValueError`` on the first problem found.
"""

from __future__ import annotations

from collections.abc import Sequence

from fintech_feature_platform.fs_core.registry.models import (
    ALLOWED_DEPENDENCY_LIFECYCLES,
    DEFAULT_DEPENDENCY_DEPTH_CAP,
    FeatureDef,
    FeatureViewDef,
    Registry,
)


def validate_registry(registry: Registry) -> None:
    _unique_names([e.name for e in registry.entities], "entity")
    source_names = _unique_names([s.name for s in registry.sources], "source")
    _unique_names([v.name for v in registry.feature_views], "feature view")

    entities_by_name = {e.name: e for e in registry.entities}

    for view in registry.feature_views:
        entity = entities_by_name.get(view.entity)
        if entity is None:
            raise ValueError(
                f"feature view {view.name!r} references unknown entity {view.entity!r}"
            )
        if view.key_fields != entity.key_fields:
            raise ValueError(
                f"feature view {view.name!r} key_fields {list(view.key_fields)!r} must "
                f"exactly match entity {entity.name!r} key_fields "
                f"{list(entity.key_fields)!r}"
            )
        feature_names = _unique_names(
            [f.name for f in view.features], f"feature in view {view.name!r}"
        )
        features_by_name = {f.name: f for f in view.features}
        for feature in view.features:
            for source in feature.inputs:
                if source not in source_names:
                    raise ValueError(
                        f"feature {feature.name!r} in view {view.name!r} references "
                        f"unknown source {source!r}"
                    )
            for dep in feature.deps:
                target = features_by_name.get(dep.feature)
                if target is None:
                    raise ValueError(
                        f"feature {feature.name!r} in view {view.name!r} depends on "
                        f"unknown feature {dep.feature!r}"
                    )
                # Explicit version pin: if declared it must match the target's version
                # (no implicit *latest*). Absent, it pins the target's single version.
                if dep.version is not None and dep.version != target.feature_version:
                    raise ValueError(
                        f"feature {feature.name!r} in view {view.name!r} pins "
                        f"{dep.feature!r} v{dep.version}, but the registry has "
                        f"v{target.feature_version}"
                    )
                # Lifecycle dependency rule : a live feature may depend only on
                # live inputs; shadow on live/shadow; draft on live/shadow/draft; and no
                # non-deprecated feature may depend on a deprecated one (no new dependents).
                allowed = ALLOWED_DEPENDENCY_LIFECYCLES.get(feature.lifecycle, frozenset())
                if target.lifecycle not in allowed:
                    raise ValueError(
                        f"feature {feature.name!r} ({feature.lifecycle}) in view "
                        f"{view.name!r} may not depend on {dep.feature!r} "
                        f"({target.lifecycle}); allowed dependency lifecycles are "
                        f"{sorted(allowed)}"
                    )
        _validate_dependency_graph(view, features_by_name)
        for group_name, group_features in view.feature_groups.items():
            for member in group_features:
                if member not in feature_names:
                    raise ValueError(
                        f"feature group {group_name!r} in view {view.name!r} references "
                        f"unknown feature {member!r}"
                    )


def _validate_dependency_graph(
    view: FeatureViewDef, features_by_name: dict[str, FeatureDef]
) -> None:
    """Reject dependency cycles and chains deeper than the view's cap (F2,).

    ``depth(feature)`` = 0 for a feature with no feature-dependencies, else
    ``1 + max(depth(dep))`` — the longest dependency chain rooted at the feature. The
    effective cap is the view's ``max_dependency_depth`` override or the default (3).
    """
    cap = view.max_dependency_depth or DEFAULT_DEPENDENCY_DEPTH_CAP
    depth: dict[str, int] = {}
    visiting: list[str] = []

    def _depth(name: str) -> int:
        if name in depth:
            return depth[name]
        if name in visiting:
            cycle = " -> ".join([*visiting, name])
            raise ValueError(
                f"dependency cycle detected in view {view.name!r}: {cycle}"
            )
        visiting.append(name)
        deps = features_by_name[name].deps
        d = 0 if not deps else 1 + max(_depth(dep.feature) for dep in deps)
        visiting.pop()
        if d > cap:
            raise ValueError(
                f"feature {name!r} in view {view.name!r} has dependency depth {d} "
                f"exceeding cap {cap}"
            )
        depth[name] = d
        return d

    for feature in view.features:
        _depth(feature.name)


def _unique_names(names: Sequence[str], ctx: str) -> set[str]:
    seen: set[str] = set()
    for name in names:
        if name in seen:
            raise ValueError(f"duplicate {ctx} name {name!r}")
        seen.add(name)
    return seen
