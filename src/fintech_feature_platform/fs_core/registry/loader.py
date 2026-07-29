"""Load and build the registry contract.

``build_registry`` is the pure core: it converts an already-parsed mapping (the
shape of the registry YAML) into validated ``Registry`` models, rejecting
unknown/extra keys to catch typos early. It performs no file I/O.

``load_registry_file`` is a thin adapter on top of it: read a YAML file, parse
it, confirm the top-level document is a mapping, then delegate to
``build_registry`` so all schema and cross-reference validation lives in one
place.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

from fintech_feature_platform.fs_core.registry.models import (
    DEFAULT_PROPAGATION,
    EntityDef,
    FeatureDef,
    FeatureDependency,
    FeatureViewDef,
    ModelSpec,
    Registry,
    SourceDef,
)
from fintech_feature_platform.fs_core.registry.validator import validate_registry

# Each spec is (required_keys, optional_keys).
_REGISTRY_KEYS = ({"registry_version", "entities", "sources", "feature_views"}, set())
_ENTITY_KEYS = ({"key_fields"}, set())
_SOURCE_KEYS = ({"type", "report_type", "ts_field"}, set())
_VIEW_KEYS = (
    {"entity", "key_fields", "view_version", "owner", "status", "features"},
    {"feature_groups", "max_dependency_depth"},
)
_FEATURE_KEYS = (
    # ``udf`` is optional at the schema level: required for kind="udf" (enforced by
    # FeatureDef), omitted for kind="model_score" (externally written features).
    # ``model`` is the F3 model spec (required for kind="model", enforced by FeatureDef).
    {"kind", "feature_version", "dtype", "status"},
    {"udf", "inputs", "deps", "model", "description"},
)

_MODEL_KEYS = ({"uri", "digest", "output_name"}, {"runner", "batch_only"})


def load_registry_file(path: str | Path) -> Registry:
    """Load a registry from a YAML file and build a validated ``Registry``.

    Raises ``FileNotFoundError`` if the file is missing and ``ValueError`` if the
    content is not valid YAML or its top-level document is not a mapping. All
    schema and cross-reference validation is delegated to ``build_registry``.
    """
    text = Path(path).read_text(encoding="utf-8")
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ValueError(f"registry file {path} is not valid YAML: {exc}") from exc
    if not isinstance(data, Mapping):
        raise ValueError(
            f"registry file {path} must contain a mapping at the top level"
        )
    return build_registry(data)


def build_registry(data: Mapping[str, Any]) -> Registry:
    """Build a validated ``Registry`` from a parsed mapping (no file I/O)."""
    _check_keys(data, _REGISTRY_KEYS, "registry")

    entities = tuple(
        _build_entity(name, body)
        for name, body in _as_mapping(data["entities"], "entities").items()
    )
    sources = tuple(
        _build_source(name, body)
        for name, body in _as_mapping(data["sources"], "sources").items()
    )
    views = tuple(
        _build_view(name, body)
        for name, body in _as_mapping(data["feature_views"], "feature_views").items()
    )

    registry = Registry(
        registry_version=data["registry_version"],
        entities=entities,
        sources=sources,
        feature_views=views,
    )
    validate_registry(registry)
    return registry


def _build_entity(name: str, body: Any) -> EntityDef:
    _check_keys(body, _ENTITY_KEYS, f"entity {name!r}")
    return EntityDef(
        name=name,
        key_fields=_as_str_tuple(body["key_fields"], f"entity {name!r} key_fields"),
    )


def _build_source(name: str, body: Any) -> SourceDef:
    _check_keys(body, _SOURCE_KEYS, f"source {name!r}")
    return SourceDef(
        name=name,
        type=body["type"],
        report_type=body["report_type"],
        ts_field=body["ts_field"],
    )


def _build_view(name: str, body: Any) -> FeatureViewDef:
    _check_keys(body, _VIEW_KEYS, f"feature view {name!r}")
    features = tuple(
        _build_feature(fname, fbody)
        for fname, fbody in _as_mapping(body["features"], f"view {name!r} features").items()
    )
    feature_groups = {
        group_name: _as_str_tuple(
            group_features, f"view {name!r} feature group {group_name!r}"
        )
        for group_name, group_features in _as_mapping(
            body.get("feature_groups", {}), f"view {name!r} feature_groups"
        ).items()
    }
    return FeatureViewDef(
        name=name,
        entity=body["entity"],
        key_fields=_as_str_tuple(body["key_fields"], f"view {name!r} key_fields"),
        view_version=body["view_version"],
        owner=body["owner"],
        status=body["status"],
        features=features,
        feature_groups=feature_groups,
        max_dependency_depth=body.get("max_dependency_depth"),
    )


def _build_feature(name: str, body: Any) -> FeatureDef:
    _check_keys(body, _FEATURE_KEYS, f"feature {name!r}")
    return FeatureDef(
        name=name,
        kind=body["kind"],
        feature_version=body["feature_version"],
        udf=body.get("udf", ""),
        dtype=body["dtype"],
        status=body["status"],
        inputs=_as_str_tuple(body.get("inputs", ()), f"feature {name!r} inputs"),
        deps=_build_deps(body.get("deps", ()), name),
        model=_build_model(body.get("model"), name),
        description=body.get("description"),
    )


def _build_model(value: Any, feature_name: str) -> ModelSpec | None:
    """Build the F3 ``model`` spec (uri + digest + output_name + runner/batch_only)."""
    if value is None:
        return None
    _check_keys(value, _MODEL_KEYS, f"feature {feature_name!r} model")
    return ModelSpec(
        uri=value["uri"],
        digest=value["digest"],
        output_name=value["output_name"],
        runner=value.get("runner"),
        batch_only=value.get("batch_only", True),
    )


_DEP_KEYS = (
    {"feature"},
    {"version", "required", "max_input_age_seconds", "propagation"},
)


def _build_deps(value: Any, feature_name: str) -> tuple[FeatureDependency, ...]:
    """Build F2 edges: each entry is a bare feature name (legacy) or a mapping."""
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        raise ValueError(f"feature {feature_name!r} deps must be a list")
    deps: list[FeatureDependency] = []
    for entry in value:
        if isinstance(entry, str):
            deps.append(FeatureDependency(feature=entry))
        elif isinstance(entry, Mapping):
            _check_keys(entry, _DEP_KEYS, f"feature {feature_name!r} dep")
            deps.append(
                FeatureDependency(
                    feature=entry["feature"],
                    version=entry.get("version"),
                    required=entry.get("required", True),
                    max_input_age_seconds=entry.get("max_input_age_seconds"),
                    propagation=entry.get("propagation", DEFAULT_PROPAGATION),
                )
            )
        else:
            raise ValueError(
                f"feature {feature_name!r} dep must be a name or mapping, got {entry!r}"
            )
    return tuple(deps)


def _check_keys(mapping: Any, spec: tuple[set[str], set[str]], ctx: str) -> None:
    required, optional = spec
    if not isinstance(mapping, Mapping):
        raise ValueError(f"{ctx} must be a mapping")
    keys = set(mapping)
    unknown = keys - (required | optional)
    if unknown:
        raise ValueError(f"{ctx} has unknown keys: {sorted(unknown)}")
    missing = required - keys
    if missing:
        raise ValueError(f"{ctx} is missing required keys: {sorted(missing)}")


def _as_mapping(value: Any, ctx: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{ctx} must be a mapping")
    return value


def _as_str_tuple(value: Any, ctx: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"{ctx} must be a list")
    result = tuple(value)
    for item in result:
        if not isinstance(item, str):
            raise ValueError(f"{ctx} must contain only strings")
    return result
