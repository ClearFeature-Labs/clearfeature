"""The ONE UDF provider loader (``module:callable`` -> ``UdfRegistry``).

Consolidates the previously duplicated import/validation mechanics of
``cli/harness.load_udfs`` and ``api/backend._load_udf_provider`` into a neutral core
module (no dependency on ``api``; no sys.path manipulation; no installation). The
provider target may return a ``UdfRegistry``, a ``{name: fn}`` mapping, or a
``(registry, udfs)`` tuple.

``spec_label`` parameterizes the user-facing name of the spec in error messages so the
CLI ("--udfs") and the runtime ("FSP_UDF_PROVIDER") keep their established wording;
presentation stays with the callers (the CLI maps ``UdfProviderLoadError`` into its
JSON error contract, the runtime lets it fail worker startup loudly).

Failures rooted in the USER's spec or the user's own module raise
``UdfProviderLoadError`` (a ``ValueError``) with the original exception preserved as
``__cause__`` — platform bugs are never wrapped.
"""

from __future__ import annotations

import importlib
from collections.abc import Mapping

from fintech_feature_platform.fs_core.compute.udf_registry import UdfRegistry


class UdfProviderLoadError(ValueError):
    """A UDF provider spec/module could not be loaded — a user/configuration error."""


def load_udf_provider(spec: str, *, spec_label: str = "udf provider") -> UdfRegistry:
    """Load a ``UdfRegistry`` from a ``module.path:callable_or_attr`` spec."""
    module_path, sep, attr = spec.partition(":")
    if not sep or not attr or not module_path:
        raise UdfProviderLoadError(
            f"{spec_label} must be 'module.path:callable_or_attr'"
        )
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise UdfProviderLoadError(
            f"provider module {module_path!r} cannot be imported: {exc}; is the "
            "Feature Project installed in this environment (uv pip install -e .)?"
        ) from exc
    except Exception as exc:  # noqa: BLE001 - the USER's module raised at import time
        raise UdfProviderLoadError(
            f"provider module {module_path!r} raised while importing: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    try:
        obj = getattr(module, attr)
    except AttributeError as exc:
        raise UdfProviderLoadError(
            f"provider module {module_path!r} has no attribute {attr!r}"
        ) from exc
    try:
        result = obj() if callable(obj) else obj
    except Exception as exc:  # noqa: BLE001 - the USER's provider callable raised
        raise UdfProviderLoadError(
            f"provider {spec!r} raised while building the UDF registry: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    if isinstance(result, tuple):
        result = result[1]
    if isinstance(result, UdfRegistry):
        return result
    if isinstance(result, Mapping):
        return UdfRegistry(dict(result))
    raise UdfProviderLoadError(
        f"{spec_label} target {spec!r} did not yield a UdfRegistry or mapping"
    )
