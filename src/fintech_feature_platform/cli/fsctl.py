"""``fsctl`` — the feature-as-code CLI.

The safe workflow, one thin command each:

    validate  -> load + validate a registry, print its bundle_digest
    run-local -> plan/compute a feature locally (no publish, no network)
    test      -> run YAML golden cases through real ComputeCore
    publish   -> write an immutable bundle to a local bundle store
    promote   -> repoint an (env, stage) pointer under approval rules
    rollback  -> repoint to the previous/known digest (never a rebuild)

Each subcommand prints a bounded JSON summary and returns a process exit code (0 ok, non-zero
on failure). CI templates are thin wrappers over these commands — no second control plane.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fintech_feature_platform.cli.artifact import (
    ArtifactStoreError,
    WheelBuildError,
    WheelIdentityError,
    WheelStore,
    WheelTestError,
    build_wheel,
    install_and_test_wheel,
    sha256_file,
)
from fintech_feature_platform.cli.harness import (
    UdfProviderError,
    load_cases,
    load_udfs,
    resolve_view_for_feature,
    run_golden_cases,
)
from fintech_feature_platform.cli.manifest import (
    MANIFEST_FILENAME,
    ManifestError,
    load_manifest,
    resolve_cli_defaults,
)
from fintech_feature_platform.cli.scaffold import package_name_from, write_scaffold
from fintech_feature_platform.fs_core.planner import PlannerError, plan_features
from fintech_feature_platform.fs_core.registry.bundle import (
    FileBundleStore,
    build_registry_bundle,
    compute_bundle_digest,
)
from fintech_feature_platform.fs_core.registry.loader import load_registry_file
from fintech_feature_platform.fs_core.registry.promotion import (
    FilePointerStore,
    PromotionError,
    promote,
    rollback,
)


def _print(obj: dict) -> None:
    print(json.dumps(obj, sort_keys=True))


def _load_registry(path: str):
    """Load a registry; on any load/validation error return (None, error-message)."""
    try:
        return load_registry_file(path), None
    except (FileNotFoundError, ValueError) as exc:
        return None, str(exc)


def _require(args: argparse.Namespace, names: list[tuple[str, str]]) -> str | None:
    """Return a clear error if any (attr, flag) is unresolved after manifest/env filling."""
    missing = [flag for attr, flag in names if getattr(args, attr, None) is None]
    if missing:
        return (
            f"missing required input(s): {', '.join(missing)} — pass the flag or set it "
            f"in feature_project.yaml"
        )
    return None


# --- init --------------------------------------------------------------------

def _cmd_init(args: argparse.Namespace) -> int:
    name = args.name
    package = args.package or package_name_from(name)
    target = Path(args.dir) if args.dir else Path.cwd() / name
    try:
        written = write_scaffold(target, name, package)
    except FileExistsError as exc:
        _print({"ok": False, "errors": [str(exc)]})
        return 1
    _print({"ok": True, "project": name, "package": package,
            "path": str(target), "files": written})
    return 0


# --- validate ----------------------------------------------------------------

def _cmd_validate(args: argparse.Namespace) -> int:
    error = _require(args, [("registry", "--registry")])
    if error is None:
        registry, error = _load_registry(args.registry)
    else:
        registry = None
    if registry is None:
        _print({"valid": False, "bundle_digest": None, "views_count": 0,
                "features_count": 0, "errors": [error]})
        return 1
    _print({
        "valid": True,
        "bundle_digest": compute_bundle_digest(registry),
        "views_count": len(registry.feature_views),
        "features_count": sum(len(v.features) for v in registry.feature_views),
        "errors": [],
    })
    return 0


# --- run-local ---------------------------------------------------------------

def _cmd_run_local(args: argparse.Namespace) -> int:
    error = _require(args, [("registry", "--registry")])
    if error:
        _print({"ok": False, "errors": [error]})
        return 1
    registry, error = _load_registry(args.registry)
    if registry is None:
        _print({"ok": False, "errors": [error]})
        return 1
    view_def = resolve_view_for_feature(registry, args.feature)
    if view_def is None:
        _print({"ok": False, "errors": [f"unknown feature {args.feature!r}"]})
        return 1
    feature = next(f for f in view_def.features if f.name == args.feature)
    out: dict[str, Any] = {
        "ok": True, "feature": args.feature, "kind": feature.kind,
        "lifecycle": feature.lifecycle,
    }
    deps: dict[str, Any] = {}
    sources: dict[str, Any] = {}
    if args.entity_json:
        fixture = json.loads(Path(args.entity_json).read_text(encoding="utf-8"))
        deps = dict(fixture.get("deps", {}))
        sources = dict(fixture.get("sources", {}))

    try:
        if feature.kind == "udf":
            # Lifecycle gate always runs (rejects shadow without --allow-shadow, etc.).
            plan_features(view_def, [args.feature], [], allow_shadow=args.allow_shadow)
            if args.entity_json:
                out["value"] = _run_local_compute(registry, args, view_def, deps, sources)
        elif feature.kind == "model":
            # F3 is only runnable locally with an explicit fake runner (else rejected).
            out["value"] = _run_local_compute(registry, args, view_def, deps, sources)
        else:
            _print({"ok": False, "feature": args.feature,
                    "errors": [f"feature kind {feature.kind!r} is not runnable locally"]})
            return 1
    except (PlannerError, ValueError) as exc:
        _print({"ok": False, "feature": args.feature, "errors": [str(exc)]})
        return 1
    _print(out)
    return 0


def _run_local_compute(registry, args, view_def, deps, sources):
    from fintech_feature_platform.cli.harness import compute_feature_value

    udfs = load_udfs(args.udfs) if not args.no_udfs else None
    return compute_feature_value(
        registry, udfs, view_def, args.feature, deps=deps, sources=sources,
        allow_shadow=args.allow_shadow, model_runner=args.model_runner,
    )


# --- test --------------------------------------------------------------------

def _cmd_test(args: argparse.Namespace) -> int:
    error = _require(args, [("registry", "--registry"), ("tests", "--tests")])
    if error:
        _print({"ok": False, "errors": [error]})
        return 1
    registry, error = _load_registry(args.registry)
    if registry is None:
        _print({"ok": False, "errors": [error]})
        return 1
    cases = load_cases(args.tests)
    udfs = load_udfs(args.udfs)
    results = run_golden_cases(
        registry, udfs, cases, allow_shadow=args.allow_shadow, model_runner=args.model_runner
    )
    failed = [r for r in results if r.status != "passed"]
    _print({
        "ok": not failed,
        "total": len(results),
        "passed": sum(1 for r in results if r.status == "passed"),
        "failed": len(failed),
        "cases": [r.to_dict() for r in results],
    })
    return 0 if not failed else 1


# --- publish -----------------------------------------------------------------

def _cmd_publish(args: argparse.Namespace) -> int:
    if args.project is not None:
        return _cmd_publish_wheel(args)
    # Legacy unbound publish (unchanged): definitional bundle, no feature artifact.
    error = _require(args, [("registry", "--registry"), ("bundle_store", "--bundle-store")])
    if error:
        _print({"ok": False, "errors": [error]})
        return 1
    registry, error = _load_registry(args.registry)
    if registry is None:
        _print({"ok": False, "errors": [error]})
        return 1
    bundle = build_registry_bundle(
        registry, created_at=datetime.now(UTC), source_ref=args.source_ref
    )
    try:
        path = FileBundleStore(args.bundle_store).put(bundle)
    except ValueError as exc:
        _print({"ok": False, "bundle_digest": bundle.bundle_digest, "errors": [str(exc)]})
        return 1
    _print({"ok": True, "bundle_digest": bundle.bundle_digest, "bundle_path": str(path)})
    return 0


def _cmd_publish_wheel(args: argparse.Namespace) -> int:
    """Artifact-bound publish, in strict order:

    build -> hash -> install exact wheel -> golden-test exact wheel -> upload immutable
    wheel -> read back and verify -> persist bound bundle. The bound bundle is never
    persisted unless its wheel has been stored and its bytes verified on read-back.
    """
    if args.bundle_store is None:
        _print({"ok": False, "errors": ["--bundle-store is required"]})
        return 1
    project = Path(args.project)
    manifest_path = project / MANIFEST_FILENAME
    if not manifest_path.is_file():
        _print({"ok": False, "errors": [f"no {MANIFEST_FILENAME} in {project}"]})
        return 1
    try:
        manifest = load_manifest(manifest_path)
    except ManifestError as exc:
        _print({"ok": False, "errors": [str(exc)]})
        return 1

    # An artifact-bound publish requires a resolved wheel store (flag > env > manifest).
    wheel_store_uri = (
        args.wheel_store
        or os.environ.get("FSP_WHEEL_STORE")
        or manifest.resolved_path(manifest.wheel_store)
    )
    project_name = manifest.project or manifest.package
    missing = [
        name
        for name, value in (
            ("registry", manifest.registry),
            ("tests", manifest.tests),
            ("udf_provider", manifest.udf_provider),
            ("project", project_name),
            ("wheel_store", wheel_store_uri),
        )
        if value is None
    ]
    if missing:
        _print({"ok": False, "errors": [f"artifact-bound publish requires: {missing}"]})
        return 1

    registry, error = _load_registry(manifest.resolved_path(manifest.registry))
    if registry is None:
        _print({"ok": False, "errors": [error]})
        return 1

    with tempfile.TemporaryDirectory() as work:
        try:
            # build -> hash -> install + identity + golden-test the EXACT wheel
            wheel = Path(args.wheel) if args.wheel else build_wheel(project, Path(work) / "dist")
            sha256 = sha256_file(wheel)
            result = install_and_test_wheel(
                wheel,
                provider=manifest.udf_provider,
                registry_rel=manifest.registry,
                tests_rel=manifest.tests,
                manifest_project=project_name,
            )
            # upload immutable wheel + release record -> read back and verify BOTH before
            # persisting the bundle
            identity = result["identity"]
            store = WheelStore(wheel_store_uri)
            release = store.put(
                wheel, name=identity["name"], version=identity["version"], sha256=sha256
            )
            store.read_back_verify(
                name=identity["name"], version=identity["version"], sha256=sha256
            )
            wheel_location = store.location(release["storage_key"])
        except (WheelBuildError, WheelTestError, WheelIdentityError, ArtifactStoreError) as exc:
            _print({"ok": False, "errors": [str(exc)]})
            return 1

        feature_artifact = {
            "name": identity["name"],
            "version": identity["version"],
            "provider": manifest.udf_provider,
            "sha256": sha256,
            "filename": release["filename"],
            "storage_key": release["storage_key"],
        }
        # requires_core (manifest) travels with the bundle so the runtime can enforce it
        # against the installed core version. Part of final_bundle_digest.
        if manifest.requires_core:
            feature_artifact["requires_core"] = manifest.requires_core
        bundle = build_registry_bundle(
            registry,
            created_at=datetime.now(UTC),
            source_ref=args.source_ref,
            feature_artifact=feature_artifact,
        )
        try:
            path = FileBundleStore(args.bundle_store).put(bundle)
        except ValueError as exc:
            _print({"ok": False, "errors": [str(exc)]})
            return 1

    _print({
        "ok": True,
        "registry_definition_digest": bundle.registry_definition_digest,
        "feature_artifact_sha256": sha256,
        "final_bundle_digest": bundle.bundle_digest,
        "bundle_path": str(path),
        "wheel_location": wheel_location,
    })
    return 0


# --- image-context -----------------------------------------------------------

def _cmd_image_context(args: argparse.Namespace) -> int:
    """Generate a customer feature-worker image build context."""
    from fintech_feature_platform.cli.image_context import (
        ImageContextError,
        build_image_context,
    )

    try:
        out = build_image_context(
            args.project,
            args.dir,
            wheel=args.wheel,
            base_image=args.base_image,
        )
    except (ImageContextError, ManifestError, WheelBuildError, WheelTestError,
            WheelIdentityError) as exc:
        _print({"ok": False, "errors": [str(exc)]})
        return 1
    _print(out)
    return 0


# --- promote -----------------------------------------------------------------

def _cmd_promote(args: argparse.Namespace) -> int:
    error = _require(
        args, [("bundle_store", "--bundle-store"), ("pointer_store", "--pointer-store")]
    )
    if error:
        _print({"ok": False, "errors": [error]})
        return 1
    bundle_exists = FileBundleStore(args.bundle_store).get(args.bundle_digest) is not None
    try:
        record = promote(
            bundle_exists=bundle_exists,
            pointer_store=FilePointerStore(args.pointer_store),
            bundle_digest=args.bundle_digest,
            env=args.env,
            stage=args.to,
            actor=args.actor,
            reason=args.reason,
            now=datetime.now(UTC),
            profile=args.profile,
            approved_by=args.approved_by or [],
            shadow_age_override=args.allow_shadow_age_override,
            override_reason=args.override_reason,
        )
    except PromotionError as exc:
        _print({"ok": False, "errors": [str(exc)]})
        return 1
    _print({"ok": True, "record": record.to_dict()})
    return 0


# --- rollback ----------------------------------------------------------------

def _cmd_rollback(args: argparse.Namespace) -> int:
    error = _require(args, [("pointer_store", "--pointer-store")])
    if error:
        _print({"ok": False, "errors": [error]})
        return 1
    bundle_exists = None
    if args.bundle_digest and args.bundle_store:
        bundle_exists = (
            FileBundleStore(args.bundle_store).get(args.bundle_digest) is not None
        )
    try:
        record = rollback(
            pointer_store=FilePointerStore(args.pointer_store),
            env=args.env,
            actor=args.actor,
            reason=args.reason,
            now=datetime.now(UTC),
            stage=args.stage,
            to_previous=args.to_previous,
            bundle_digest=args.bundle_digest,
            bundle_exists=bundle_exists,
        )
    except PromotionError as exc:
        _print({"ok": False, "errors": [str(exc)]})
        return 1
    _print({"ok": True, "record": record.to_dict()})
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fsctl", description="Feature-as-code CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init", help="scaffold a domain-neutral external Feature Project")
    p_init.add_argument("--name", required=True, help="project name (e.g. customer-features)")
    p_init.add_argument("--dir", default=None, help="target directory (default: ./<name>)")
    p_init.add_argument("--package", default=None, help="Python package name (default from --name)")
    p_init.set_defaults(func=_cmd_init)

    # --registry / --tests / --bundle-store / --pointer-store are resolved from
    # feature_project.yaml (or FSP_REGISTRY_PATH / FSP_UDF_PROVIDER) when the flag is
    # omitted; the command handlers report a clear error if still unresolved.
    p_validate = sub.add_parser("validate", help="validate a registry + print bundle_digest")
    p_validate.add_argument("--registry", default=None)
    p_validate.set_defaults(func=_cmd_validate)

    p_run = sub.add_parser("run-local", help="plan/compute a feature locally")
    p_run.add_argument("--registry", default=None)
    p_run.add_argument("--feature", required=True)
    p_run.add_argument("--entity-json", default=None, help="fixture JSON: {deps, sources}")
    p_run.add_argument("--allow-shadow", action="store_true")
    p_run.add_argument("--model-runner", choices=["fake"], default=None)
    p_run.add_argument("--udfs", default=None, help="module.path:callable UDF provider")
    p_run.add_argument("--no-udfs", action="store_true", help="skip UDF loading")
    p_run.set_defaults(func=_cmd_run_local)

    p_test = sub.add_parser("test", help="run YAML golden cases through ComputeCore")
    p_test.add_argument("--registry", default=None)
    p_test.add_argument("--tests", default=None)
    p_test.add_argument("--allow-shadow", action="store_true")
    p_test.add_argument("--model-runner", choices=["fake"], default=None)
    p_test.add_argument("--udfs", default=None)
    p_test.set_defaults(func=_cmd_test)

    p_pub = sub.add_parser("publish", help="write an immutable bundle to a local store")
    p_pub.add_argument("--registry", default=None)
    p_pub.add_argument("--bundle-store", default=None)
    p_pub.add_argument("--source-ref", default=None)
    # Artifact-bound publish : build/hash/isolated-test a feature wheel and bind
    # it to the bundle. With --project the registry/tests/provider come from
    # feature_project.yaml; --wheel reuses a prebuilt wheel instead of building.
    p_pub.add_argument("--project", default=None, help="Feature Project dir (artifact-bound)")
    p_pub.add_argument("--wheel", default=None, help="prebuilt wheel (default: build it)")
    p_pub.add_argument("--wheel-store", default=None, help="dir or s3:// for the wheel")
    p_pub.set_defaults(func=_cmd_publish)

    p_prom = sub.add_parser("promote", help="repoint an (env, stage) pointer under approvals")
    p_prom.add_argument("--bundle-store", default=None)
    p_prom.add_argument("--pointer-store", default=None)
    p_prom.add_argument("--bundle-digest", required=True)
    p_prom.add_argument("--env", required=True)
    p_prom.add_argument("--to", required=True, choices=["shadow", "live"])
    p_prom.add_argument("--actor", required=True)
    p_prom.add_argument("--reason", required=True)
    p_prom.add_argument("--profile", default=None, choices=["bank", "energy"])
    p_prom.add_argument("--approved-by", action="append", dest="approved_by", default=None)
    p_prom.add_argument("--allow-shadow-age-override", action="store_true")
    p_prom.add_argument("--override-reason", default=None)
    p_prom.set_defaults(func=_cmd_promote)

    p_rb = sub.add_parser("rollback", help="repoint to the previous/known digest")
    p_rb.add_argument("--pointer-store", default=None)
    p_rb.add_argument("--bundle-store", default=None)
    p_rb.add_argument("--env", required=True)
    p_rb.add_argument("--stage", default="live", choices=["shadow", "live"])
    p_rb.add_argument("--to-previous", action="store_true")
    p_rb.add_argument("--bundle-digest", default=None)
    p_rb.add_argument("--actor", required=True)
    p_rb.add_argument("--reason", required=True)
    p_rb.set_defaults(func=_cmd_rollback)

    p_img = sub.add_parser(
        "image-context",
        help="generate a customer feature-worker image build context from an exact wheel",
    )
    p_img.add_argument("--project", required=True, help="Feature Project directory")
    p_img.add_argument("--dir", required=True, help="output build-context directory")
    p_img.add_argument("--wheel", default=None, help="prebuilt wheel (default: build it)")
    p_img.add_argument("--base-image", default="fsp-app:latest",
                       help="ClearFeature base worker image reference")
    p_img.set_defaults(func=_cmd_image_context)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        resolve_cli_defaults(args)  # fill --registry/--udfs/... from env then manifest
    except ManifestError as exc:
        _print({"ok": False, "errors": [str(exc)]})
        return 1
    try:
        return args.func(args)
    except (ManifestError, UdfProviderError) as exc:
        # Expected user/configuration failures follow the JSON error contract; anything
        # else (a platform bug) still surfaces as a traceback on purpose.
        _print({"ok": False, "errors": [str(exc)]})
        return 1


if __name__ == "__main__":  # pragma: no cover - manual entrypoint
    sys.exit(main())
