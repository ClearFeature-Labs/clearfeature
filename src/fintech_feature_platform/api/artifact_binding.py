"""Runtime artifact-binding gate.

Called from ``build_registry_and_udfs`` — the single seam every feature-computation role
(online/batch/propagation/model-score/offline/metadata workers + REST API) passes through —
so verification is enforced once per process for all of them. Resolves the promoted bundle
via the pointer + bundle stores, applies the binding policy, and fail-closed-verifies any
bound ``feature_artifact`` against the runtime evidence and the installed feature code.

No runtime installation, no wheel download at request time — the wheel is evidence already
present in the worker image.
"""

from __future__ import annotations

import importlib.metadata

from fintech_feature_platform.api.settings import (
    BINDING_REQUIRED,
    ENV_DEVELOPMENT,
    Settings,
)
from fintech_feature_platform.fs_core.registry.bundle import (
    FileBundleStore,
    RegistryBundle,
    compute_bundle_digest,
)
from fintech_feature_platform.fs_core.registry.models import Registry
from fintech_feature_platform.fs_core.registry.promotion import FilePointerStore
from fintech_feature_platform.fs_core.runtime.artifact_verifier import (
    CAT_PROVIDER_MISMATCH,
    CAT_REGISTRY_MISMATCH,
    CAT_REQUIRED,
    ArtifactVerificationError,
    load_runtime_evidence,
    verify_feature_artifact,
)

_CORE_DISTRIBUTION = "fintech-feature-platform"


def installed_core_version() -> str:
    """The installed ClearFeature core version (never hard-coded)."""
    return importlib.metadata.version(_CORE_DISTRIBUTION)


def resolve_promoted_bundle(settings: Settings) -> RegistryBundle | None:
    """Resolve the active/shadow promoted bundle, or None if not configured / absent."""
    if not (settings.bundle_store and settings.pointer_store and settings.bundle_env):
        return None
    pointer = FilePointerStore(settings.pointer_store).get_pointer(
        settings.bundle_env, settings.bundle_stage
    )
    if pointer is None:
        return None
    return FileBundleStore(settings.bundle_store).get(pointer.bundle_digest)


def enforce_artifact_binding(
    registry: Registry, settings: Settings, metrics=None
) -> dict | None:
    """Fail-closed artifact-binding gate. Returns a safe descriptor if a bound bundle was
    verified, or None when the legacy (unbound) path is permitted. Raises
    ``ArtifactVerificationError`` to fail closed.

    ``metrics`` : records verification
    outcomes — success for a verified bound bundle, failure + stable category on any
    rejection. Failures normally terminate startup, so the durable operational signal
    remains scrape-target-down; the counters cover any pre-crash scrape and tests.
    """
    try:
        result = _enforce(registry, settings)
    except ArtifactVerificationError as exc:
        if metrics is not None:
            metrics.incr("fsp_artifact_verification_total", {"result": "failure"})
            metrics.incr(
                "fsp_artifact_verification_failure_total", {"category": exc.category}
            )
        # Structured governance event : stable category only — the
        # raw message may reference local paths and belongs to the raised exception.
        import logging

        from fintech_feature_platform.fs_core.observability.logs import (
            get_logger,
            log_event,
        )

        log_event(
            get_logger("artifact"), logging.ERROR, "artifact_verification_failed",
            operation="artifact_binding", result="failure", category=exc.category,
        )
        raise
    if result is not None and metrics is not None:
        metrics.incr("fsp_artifact_verification_total", {"result": "success"})
    return result


def _enforce(registry: Registry, settings: Settings) -> dict | None:
    production_like = settings.environment != ENV_DEVELOPMENT

    # Startup guard: a production-like process must not weaken artifact binding.
    if production_like and settings.artifact_binding != BINDING_REQUIRED:
        raise ArtifactVerificationError(
            CAT_REQUIRED,
            f"environment {settings.environment!r} must set "
            f"FSP_ARTIFACT_BINDING={BINDING_REQUIRED} (artifact binding cannot be weakened "
            "in a production-like environment)",
            {"category": CAT_REQUIRED, "environment": settings.environment},
        )

    bundle = resolve_promoted_bundle(settings)
    bound = bundle is not None and bundle.feature_artifact is not None

    if not bound:
        if settings.artifact_binding == BINDING_REQUIRED:
            raise ArtifactVerificationError(
                CAT_REQUIRED,
                "artifact binding is required but no artifact-bound promoted bundle is "
                "configured (set FSP_BUNDLE_STORE/FSP_POINTER_STORE/FSP_BUNDLE_ENV)",
                {"category": CAT_REQUIRED},
            )
        return None  # legacy-compatible: permit the trusted-author unbound path

    # A bound bundle is ALWAYS verified — even in development / compatibility mode.
    feature_artifact = dict(bundle.feature_artifact)

    # The served definitions must equal the promoted bundle's definitions.
    if compute_bundle_digest(registry) != bundle.registry_definition_digest:
        raise ArtifactVerificationError(
            CAT_REGISTRY_MISMATCH,
            "served registry does not match the promoted bundle registry_definition_digest",
            {"category": CAT_REGISTRY_MISMATCH, "final_bundle_digest": bundle.bundle_digest},
        )

    # The served UDF provider must equal the verified provider (else served != verified code).
    if settings.udf_provider and settings.udf_provider != feature_artifact.get("provider"):
        raise ArtifactVerificationError(
            CAT_PROVIDER_MISMATCH,
            "configured FSP_UDF_PROVIDER does not match the promoted feature_artifact provider",
            {"category": CAT_PROVIDER_MISMATCH, "final_bundle_digest": bundle.bundle_digest},
        )

    evidence = load_runtime_evidence(settings.feature_artifact_manifest)
    descriptor = verify_feature_artifact(
        feature_artifact=feature_artifact,
        evidence=evidence,
        installed_core_version=installed_core_version(),
    )
    descriptor["final_bundle_digest"] = bundle.bundle_digest
    return descriptor
