"""Local compute + golden harness for ``fsctl``.

Pure, Docker-free helpers the CLI wraps: resolve a feature's view, compute one feature value
through the real ``ComputeCore`` (or a fake F3 model runner), and run YAML golden cases with a
real pass/fail comparison. UDFs are Python code referenced by name, so they load from a
provider (``module:callable``), defaulting to the demo registry's UDF map.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from fintech_feature_platform.fs_core.compute.context import (
    STATUS_OK,
    FeatureComputation,
    RequestContext,
)
from fintech_feature_platform.fs_core.compute.engine import ComputeCore
from fintech_feature_platform.fs_core.compute.udf_provider import (
    UdfProviderLoadError,
    load_udf_provider,
)
from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry
from fintech_feature_platform.fs_core.hashing import value_hash
from fintech_feature_platform.fs_core.model_runner import FakeModelRunner, ModelRef
from fintech_feature_platform.fs_core.models import EntityKey, SourceStamp
from fintech_feature_platform.fs_core.planner import plan_features
from fintech_feature_platform.fs_core.registry.models import FeatureViewDef, Registry

# Fixed local clock: local smoke/goldens are freshness-agnostic (no staleness policy needed).
_LOCAL_TS = datetime(2000, 1, 1, tzinfo=UTC)
_SUPPORTED_MODEL_URI_PREFIX = "mlflow://"


class UdfProviderError(UdfProviderLoadError):
    """A UDF provider (``module:callable``) could not be loaded — a user/config error.

    The CLI-facing name for the shared ``UdfProviderLoadError`` : the
    loader mechanics live in ``fs_core.compute.udf_provider``; ``fsctl`` maps this
    type into the machine-readable JSON error contract. Platform bugs
    are never wrapped into this type.
    """


def load_udfs(spec: str | None) -> UdfRegistry:
    """Load a ``UdfRegistry`` from ``module:callable`` (or the demo default when ``None``).

    Thin adapter over the ONE shared loader (``fs_core.compute.udf_provider``):
    expected load failures surface as ``UdfProviderError`` with an actionable message
    and no traceback at the CLI surface.
    """
    if spec is None:
        from fintech_feature_platform.api.backend import build_registry_and_udfs

        return build_registry_and_udfs()[1]
    try:
        return load_udf_provider(spec, spec_label="--udfs")
    except UdfProviderError:
        raise  # already the CLI type (subclass)
    except UdfProviderLoadError as exc:
        raise UdfProviderError(str(exc)) from exc


def resolve_view_for_feature(
    registry: Registry,
    feature_name: str,
    *,
    view: str | None = None,
    view_version: int | None = None,
) -> FeatureViewDef | None:
    """Find the view that declares ``feature_name`` (optionally pinned by name/version)."""
    for candidate in registry.feature_views:
        if view is not None and candidate.name != view:
            continue
        if view_version is not None and candidate.view_version != view_version:
            continue
        if any(f.name == feature_name for f in candidate.features):
            return candidate
    return None


def compute_feature_value(
    registry: Registry,
    udfs: UdfRegistry | None,
    view_def: FeatureViewDef,
    feature_name: str,
    *,
    deps: Mapping[str, Any] | None = None,
    sources: Mapping[str, Any] | None = None,
    allow_shadow: bool = False,
    model_runner: str | None = None,
) -> Any:
    """Compute one feature locally: real UDF via ComputeCore, or F3 via a fake model runner.

    ``deps`` supplies dependency (feature-ref) input values; ``sources`` supplies raw-source
    payloads. Lifecycle/kind gating is enforced through ``plan_features`` for UDF features.
    """
    features_by_name = {f.name: f for f in view_def.features}
    feature = features_by_name.get(feature_name)
    if feature is None:
        raise ValueError(f"unknown feature {feature_name!r} in view {view_def.name!r}")
    if feature.kind == "model":
        return _compute_model_feature(feature, deps or {}, model_runner)
    if feature.kind == "model_score":
        raise ValueError(
            f"feature {feature_name!r} is an externally-written model_score and is not "
            "computable locally"
        )

    # Lifecycle + kind gate (raises PlannerError on draft/shadow-without-allow/deprecated).
    plan_features(view_def, [feature_name], [], allow_shadow=allow_shadow)
    if udfs is None:
        raise ValueError("no UDF provider available to compute this feature")
    core = ComputeCore(registry, udfs)
    sources = dict(sources or {})

    def _loader(name: str) -> Any:
        if name not in sources:
            raise ValueError(f"missing source {name!r} for feature {feature_name!r}")
        return sources[name]

    context = RequestContext(_loader)
    for dep_name, dep_value in (deps or {}).items():
        context.cache_feature(
            dep_name,
            FeatureComputation(
                value=dep_value, data_ts=_LOCAL_TS, max_input_data_ts=_LOCAL_TS,
                value_hash=value_hash(dep_value), input_fingerprint=value_hash(dep_value),
                status=STATUS_OK,
            ),
        )
    source_stamps = {
        name: SourceStamp(report_ts=_LOCAL_TS, content_hash=value_hash(payload))
        for name, payload in sources.items()
    }
    entity_key = EntityKey.from_mapping(
        {field_name: "local" for field_name in view_def.key_fields},
        key_order=view_def.key_fields,
    )
    results = core.compute(
        view=view_def.name, view_version=view_def.view_version, entity_key=entity_key,
        requested_features=[feature_name], context=context, source_stamps=source_stamps,
        calc_ts=_LOCAL_TS,
    )
    if feature_name not in results:
        raise ValueError(
            f"feature {feature_name!r} was skipped (a required input was missing/stale)"
        )
    return results[feature_name].value


def _compute_model_feature(feature, deps: Mapping[str, Any], model_runner: str | None) -> Any:
    if model_runner != "fake":
        raise ValueError(
            f"feature {feature.name!r} is a batch-only model feature (F3); pass "
            "--model-runner fake to run it locally"
        )
    model = feature.model
    if not model.uri.startswith(_SUPPORTED_MODEL_URI_PREFIX):
        raise ValueError(f"unsupported model uri {model.uri!r}")
    runner = FakeModelRunner()
    ref = ModelRef(uri=model.uri, digest=model.digest, output_name=model.output_name)
    return runner.predict(ref, [dict(deps)])[0]


# --- golden cases ------------------------------------------------------------


@dataclass(frozen=True)
class GoldenCase:
    name: str
    feature: str
    expected_value: Any
    view: str | None = None
    view_version: int | None = None
    deps: dict[str, Any] = field(default_factory=dict)
    sources: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CaseResult:
    name: str
    feature: str
    status: str  # passed | failed | error
    expected: Any = None
    actual: Any = None
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "name": self.name, "feature": self.feature, "status": self.status,
            "expected": self.expected, "actual": self.actual, "error": self.error,
        }


def load_cases(path: str | Path) -> list[GoldenCase]:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    cases: list[GoldenCase] = []
    for raw in data.get("cases", []):
        cases.append(
            GoldenCase(
                name=raw["name"],
                feature=raw["feature"],
                expected_value=raw["expected"]["value"],
                view=raw.get("view"),
                view_version=raw.get("view_version"),
                deps=dict(raw.get("deps", {})),
                sources=dict(raw.get("sources", {})),
            )
        )
    return cases


def run_golden_cases(
    registry: Registry,
    udfs: UdfRegistry | None,
    cases: list[GoldenCase],
    *,
    allow_shadow: bool = False,
    model_runner: str | None = None,
) -> list[CaseResult]:
    """Compute each case's feature and compare to ``expected.value`` (real pass/fail)."""
    results: list[CaseResult] = []
    for case in cases:
        view_def = resolve_view_for_feature(
            registry, case.feature, view=case.view, view_version=case.view_version
        )
        if view_def is None:
            results.append(
                CaseResult(case.name, case.feature, "error",
                           expected=case.expected_value, error="feature/view not found")
            )
            continue
        try:
            actual = compute_feature_value(
                registry, udfs, view_def, case.feature, deps=case.deps,
                sources=case.sources, allow_shadow=allow_shadow, model_runner=model_runner,
            )
        except Exception as exc:  # noqa: BLE001 - report any compute failure as a case error
            results.append(
                CaseResult(case.name, case.feature, "error",
                           expected=case.expected_value, error=str(exc))
            )
            continue
        status = "passed" if actual == case.expected_value else "failed"
        results.append(
            CaseResult(case.name, case.feature, status,
                       expected=case.expected_value, actual=actual)
        )
    return results
