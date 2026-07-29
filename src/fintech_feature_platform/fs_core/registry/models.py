"""Registry contract value objects.

Immutable dataclasses describing the registry contract: entities, raw-report
sources, feature views, and their UDF features. Per-object field validity is
enforced here in ``__post_init__``; cross-object referential checks (name
uniqueness and references pointing at defined objects) live in
``validator.validate_registry``.

Collections are stored as tuples to stay immutable, consistent with the domain
models. Feature and view identities reuse ``FeatureRef`` and
``FeatureViewRef`` from ``fs_core.models``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from fintech_feature_platform.fs_core.models import FeatureRef, FeatureViewRef

# F2 dependency-depth caps : default 3, override up to 5.
DEFAULT_DEPENDENCY_DEPTH_CAP = 3
MAX_DEPENDENCY_DEPTH_CAP = 5

# Per-edge propagation policy. ``lazy`` (default): no
# immediate recompute — the dependent recomputes on its own trigger, staleness bounded by
# ``max_input_age_seconds``. ``reactive``: an upstream write publishes to ``feature_updates``
# and the dependent is recomputed (debounced) in an offline wave. ``scheduled``: accepted
# but treated as deferred/no immediate recompute. ``none``: never propagates.
PROPAGATION_LAZY = "lazy"
PROPAGATION_REACTIVE = "reactive"
PROPAGATION_SCHEDULED = "scheduled"
PROPAGATION_NONE = "none"
PROPAGATION_POLICIES = (
    PROPAGATION_LAZY,
    PROPAGATION_REACTIVE,
    PROPAGATION_SCHEDULED,
    PROPAGATION_NONE,
)
DEFAULT_PROPAGATION = PROPAGATION_LAZY

# Feature lifecycle states. ``draft`` = validation-only, never served;
# ``shadow`` = computes offline/shadow but is not served online; ``live`` = served normally;
# ``deprecated`` = readable historically but not a new dependency target. The existing free-form
# ``status`` field carries the lifecycle: legacy ``active`` maps to ``live`` and
# ``inactive``/``disabled`` to ``deprecated`` (backward compatible); an unknown value is rejected.
LIFECYCLE_DRAFT = "draft"
LIFECYCLE_SHADOW = "shadow"
LIFECYCLE_LIVE = "live"
LIFECYCLE_DEPRECATED = "deprecated"
FEATURE_LIFECYCLES = (
    LIFECYCLE_DRAFT,
    LIFECYCLE_SHADOW,
    LIFECYCLE_LIVE,
    LIFECYCLE_DEPRECATED,
)
_LIFECYCLE_ALIASES = {
    "active": LIFECYCLE_LIVE,
    "inactive": LIFECYCLE_DEPRECATED,
    "disabled": LIFECYCLE_DEPRECATED,
}

# Per source (depending) lifecycle -> the target lifecycles it may depend on.
# A ``live`` feature must be production-safe: it may depend only on
# ``live`` inputs so shadow/draft code never leaks into online serving. ``shadow`` may lean on
# stable ``live`` inputs; ``deprecated`` is historical-only and unrestricted. The net effect is
# that no non-``deprecated`` feature may depend on a ``deprecated`` one (no new dependents).
ALLOWED_DEPENDENCY_LIFECYCLES = {
    LIFECYCLE_LIVE: frozenset({LIFECYCLE_LIVE}),
    LIFECYCLE_SHADOW: frozenset({LIFECYCLE_LIVE, LIFECYCLE_SHADOW}),
    LIFECYCLE_DRAFT: frozenset({LIFECYCLE_LIVE, LIFECYCLE_SHADOW, LIFECYCLE_DRAFT}),
    LIFECYCLE_DEPRECATED: frozenset(FEATURE_LIFECYCLES),
}


def normalize_lifecycle(status: str) -> str | None:
    """Map a raw ``status`` to a canonical lifecycle, or ``None`` if unknown."""
    token = status.strip().lower()
    if token in FEATURE_LIFECYCLES:
        return token
    return _LIFECYCLE_ALIASES.get(token)


def _check_no_empty_or_duplicate(names: Sequence[str], ctx: str) -> None:
    seen: set[str] = set()
    for name in names:
        if not name or not name.strip():
            raise ValueError(f"{ctx} must not contain empty names")
        if name in seen:
            raise ValueError(f"{ctx} has duplicate name {name!r}")
        seen.add(name)


@dataclass(frozen=True)
class EntityDef:
    """A business entity and the names of its key fields."""

    name: str
    key_fields: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError("entity name must be non-empty")
        if not self.key_fields:
            raise ValueError(f"entity {self.name!r} must have at least one key field")
        _check_no_empty_or_duplicate(self.key_fields, f"entity {self.name!r} key_fields")


@dataclass(frozen=True)
class SourceDef:
    """An input source. The MVP supports only raw-report sources."""

    name: str
    type: str
    report_type: str
    ts_field: str

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError("source name must be non-empty")
        if self.type != "raw_report":
            raise ValueError(
                f"source {self.name!r} has unsupported type {self.type!r}; "
                "only 'raw_report' is supported in the MVP"
            )
        if not self.report_type or not self.report_type.strip():
            raise ValueError(f"source {self.name!r} must have a report_type")
        if not self.ts_field or not self.ts_field.strip():
            raise ValueError(f"source {self.name!r} must have a ts_field")


@dataclass(frozen=True)
class FeatureDependency:
    """A declared F2 input edge: one feature depending on another.

    ``feature`` names another feature in the same view; ``version`` is the explicit pin
    (``None`` means "the version of that feature in this view" — still an exact pin, since
    each name has exactly one version per view; never an implicit *latest*). ``required``
    (default true) drives OK/SKIPPED vs DEGRADED when the input is stale/missing;
    ``max_input_age_seconds`` is the per-edge staleness bound (``None`` = never stale).
    ``propagation`` is the per-edge policy : ``lazy`` (default),
    ``reactive``, ``scheduled``, or ``none``.
    """

    feature: str
    version: int | None = None
    required: bool = True
    max_input_age_seconds: int | None = None
    propagation: str = DEFAULT_PROPAGATION

    def __post_init__(self) -> None:
        if not self.feature or not self.feature.strip():
            raise ValueError("feature dependency must name a feature")
        if self.version is not None and self.version < 1:
            raise ValueError(
                f"dependency on {self.feature!r} must pin an explicit version >= 1"
            )
        if self.max_input_age_seconds is not None and self.max_input_age_seconds < 0:
            raise ValueError(
                f"dependency on {self.feature!r} max_input_age_seconds must be >= 0"
            )
        if self.propagation not in PROPAGATION_POLICIES:
            raise ValueError(
                f"dependency on {self.feature!r} has unknown propagation policy "
                f"{self.propagation!r}; must be one of {list(PROPAGATION_POLICIES)}"
            )


@dataclass(frozen=True)
class ModelSpec:
    """A pinned model behind an F3 model-as-feature.

    ``uri`` + ``digest`` pin the exact model artifact (e.g. ``mlflow://pd_model/17`` +
    ``sha256:...``); ``output_name`` names the prediction column the runner returns.
    ``batch_only`` must be True — online F3 is not part of beta, so an
    online-capable model feature is rejected here at registry build.
    """

    uri: str
    digest: str
    output_name: str
    runner: str | None = None
    batch_only: bool = True

    def __post_init__(self) -> None:
        if not self.uri or not self.uri.strip():
            raise ValueError("model feature must declare a model uri")
        if not self.digest or not self.digest.strip():
            raise ValueError("model feature must declare a model digest")
        if not self.output_name or not self.output_name.strip():
            raise ValueError("model feature must declare a model output_name")
        if not self.batch_only:
            raise ValueError(
                "model feature must be batch_only=True; online F3 is not supported in beta"
            )


@dataclass(frozen=True)
class FeatureDef:
    """One feature in a view.

    ``kind="udf"`` is computed by ComputeCore (requires a ``udf`` + at least one input
    or dependency). ``kind="model_score"`` is an externally supplied feature written via
    ``/v1/model-scores`` — it is never computed, so it requires no udf/inputs/deps.
    ``kind="model"`` is an F3 batch-only model-as-feature : its inputs are
    version-pinned feature refs (``deps``), it carries a pinned ``model`` spec, and it is
    computed by the batch model path (never by ComputeCore, never online).

    ``deps`` are F2 dependency edges. Bare-string entries (legacy) are normalized to
    ``FeatureDependency`` with an implicit version pin and no staleness bound.
    """

    name: str
    kind: str
    feature_version: int
    udf: str
    dtype: str
    status: str
    inputs: tuple[str, ...] = ()
    deps: tuple[FeatureDependency, ...] = ()
    model: ModelSpec | None = None
    description: str | None = None
    # Canonical lifecycle derived from ``status``; set in __post_init__.
    lifecycle: str = field(init=False, default="")

    def __post_init__(self) -> None:
        # Normalize bare-string deps to FeatureDependency (backward compatible).
        normalized = tuple(
            d if isinstance(d, FeatureDependency) else FeatureDependency(feature=d)
            for d in self.deps
        )
        object.__setattr__(self, "deps", normalized)
        if not self.name or not self.name.strip():
            raise ValueError("feature name must be non-empty")
        if self.kind not in ("udf", "model_score", "model"):
            raise ValueError(
                f"feature {self.name!r} has unsupported kind {self.kind!r}; "
                "only 'udf', 'model_score', and 'model' are supported"
            )
        if self.feature_version < 1:
            raise ValueError(f"feature {self.name!r} version must be >= 1")
        if not self.dtype or not self.dtype.strip():
            raise ValueError(f"feature {self.name!r} must have a dtype")
        if not self.status or not self.status.strip():
            raise ValueError(f"feature {self.name!r} must have a status")
        lifecycle = normalize_lifecycle(self.status)
        if lifecycle is None:
            raise ValueError(
                f"feature {self.name!r} has unknown lifecycle/status {self.status!r}; "
                f"expected one of {list(FEATURE_LIFECYCLES)} (legacy 'active'/'inactive' ok)"
            )
        object.__setattr__(self, "lifecycle", lifecycle)
        if self.kind != "model" and self.model is not None:
            raise ValueError(
                f"feature {self.name!r} of kind {self.kind!r} must not declare a model spec"
            )
        if self.kind == "udf":
            if not self.udf or not self.udf.strip():
                raise ValueError(f"feature {self.name!r} must reference a udf")
            if not self.inputs and not self.deps:
                raise ValueError(
                    f"feature {self.name!r} must have at least one input or dependency"
                )
        elif self.kind == "model":  # F3: model computed from feature-ref inputs (deps)
            if self.model is None:
                raise ValueError(f"model feature {self.name!r} must declare a model spec")
            if self.udf and self.udf.strip():
                raise ValueError(f"model feature {self.name!r} must not reference a udf")
            if self.inputs:
                raise ValueError(
                    f"model feature {self.name!r} must not read raw sources; declare its "
                    "input feature refs as deps"
                )
            if not self.deps:
                raise ValueError(
                    f"model feature {self.name!r} must declare at least one input "
                    "feature (deps)"
                )
        else:  # model_score: externally written, not computed
            if self.inputs or self.deps:
                raise ValueError(
                    f"model_score feature {self.name!r} must not have inputs or deps"
                )

    def dep_names(self) -> tuple[str, ...]:
        return tuple(d.feature for d in self.deps)

    def ref(self) -> FeatureRef:
        return FeatureRef(self.name, self.feature_version)


@dataclass(frozen=True)
class FeatureViewDef:
    """A versioned serving contract grouping features for one entity."""

    name: str
    entity: str
    key_fields: tuple[str, ...]
    view_version: int
    owner: str
    status: str
    features: tuple[FeatureDef, ...]
    # Per-view logical groups: group name -> explicit output feature names. Membership
    # is explicit (no tag-based groups). Cross-reference checks (features exist in the
    # view) live in validator.validate_registry.
    feature_groups: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    # F2 dependency depth cap : None -> DEFAULT_DEPENDENCY_DEPTH_CAP (3); an
    # explicit override must be 1..MAX_DEPENDENCY_DEPTH_CAP (5). The measured longest
    # dependency chain per feature must not exceed the effective cap (validator).
    max_dependency_depth: int | None = None

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError("feature view name must be non-empty")
        if not self.entity or not self.entity.strip():
            raise ValueError(f"feature view {self.name!r} must reference an entity")
        if not self.key_fields:
            raise ValueError(f"feature view {self.name!r} must have key_fields")
        _check_no_empty_or_duplicate(self.key_fields, f"view {self.name!r} key_fields")
        if self.view_version < 1:
            raise ValueError(f"feature view {self.name!r} version must be >= 1")
        if not self.owner or not self.owner.strip():
            raise ValueError(f"feature view {self.name!r} must have an owner")
        if not self.status or not self.status.strip():
            raise ValueError(f"feature view {self.name!r} must have a status")
        if not self.features:
            raise ValueError(f"feature view {self.name!r} must have at least one feature")
        if self.max_dependency_depth is not None and not (
            1 <= self.max_dependency_depth <= MAX_DEPENDENCY_DEPTH_CAP
        ):
            raise ValueError(
                f"feature view {self.name!r} max_dependency_depth "
                f"{self.max_dependency_depth} must be 1..{MAX_DEPENDENCY_DEPTH_CAP}"
            )
        for group_name, group_features in self.feature_groups.items():
            if not group_name or not group_name.strip():
                raise ValueError(
                    f"feature view {self.name!r} has an empty feature group name"
                )
            if not group_features:
                raise ValueError(
                    f"feature group {group_name!r} in view {self.name!r} must be non-empty"
                )
            _check_no_empty_or_duplicate(
                group_features, f"feature group {group_name!r} in view {self.name!r}"
            )

    def ref(self) -> FeatureViewRef:
        return FeatureViewRef(self.name, self.view_version)


@dataclass(frozen=True)
class Registry:
    """The whole registry contract."""

    registry_version: str
    entities: tuple[EntityDef, ...]
    sources: tuple[SourceDef, ...]
    feature_views: tuple[FeatureViewDef, ...]

    def __post_init__(self) -> None:
        if not self.registry_version or not self.registry_version.strip():
            raise ValueError("registry_version must be non-empty")
        if not self.entities:
            raise ValueError("registry must define at least one entity")
        if not self.sources:
            raise ValueError("registry must define at least one source")
        if not self.feature_views:
            raise ValueError("registry must define at least one feature view")

    def find_view(self, name: str, version: int) -> FeatureViewDef:
        """The ONE authoritative (name, version) -> FeatureViewDef lookup.

        Replaces five byte-identical copies across workers/app/feature_store. Exact
        legacy behavior preserved: linear scan, ``ValueError`` with the established
        message on no match. (``ComputeCore._find_view`` keeps its intentionally
        richer version-mismatch diagnostic — view names are validator-unique, so the
        found/not-found sets are identical.)
        """
        for view in self.feature_views:
            if view.name == name and view.view_version == version:
                return view
        raise ValueError(f"unknown view {name!r} version {version}")
