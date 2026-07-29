"""``feature_project.yaml`` — the external Feature Project manifest.

The manifest supplies **defaults only** for the ``fsctl`` commands so a Data Scientist
stops retyping ``--registry``/``--udfs``/``--tests``/``--bundle-store``/``--pointer-store``
on every call. Resolution precedence is: explicit flag > environment variable > manifest >
built-in default. The manifest is committed to the customer repo, so it must never carry
secrets (DSN, keys) or promotion approvers — those are per-action or per-environment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

MANIFEST_FILENAME = "feature_project.yaml"

# The strict allowlist of manifest fields. Anything else fails explicitly (never ignored).
_KNOWN_KEYS = frozenset(
    {
        "project",
        "package",
        "requires_core",
        "registry",
        "udf_provider",
        "tests",
        "bundle_store",
        "pointer_store",
        "wheel_store",
    }
)

# Keys that must never live in a committed manifest, matched case-insensitively at ANY
# nesting depth. Secrets come from the environment / secret store; promotion identity
# (actor/approvers) is supplied per action for a truthful audit trail.
_FORBIDDEN_KEYS = frozenset(
    {
        "dsn",
        "password",
        "secret",
        "token",
        "api_key",
        "api_keys",
        "fsp_api_keys",
        "credentials",
        "aws_secret_access_key",
        "aws_access_key_id",
        "actor",
        "approved_by",
    }
)


class ManifestError(ValueError):
    """Raised when a ``feature_project.yaml`` is malformed or carries forbidden keys."""


@dataclass(frozen=True)
class FeatureProjectManifest:
    """Parsed ``feature_project.yaml``. ``root`` is the manifest's directory (project root)."""

    root: Path
    project: str | None = None
    package: str | None = None
    requires_core: str | None = None
    registry: str | None = None
    udf_provider: str | None = None
    tests: str | None = None
    bundle_store: str | None = None
    pointer_store: str | None = None
    wheel_store: str | None = None

    def resolved_path(self, value: str | None) -> str | None:
        """Resolve a manifest-relative path against the project root; absolute stays as-is."""
        if value is None:
            return None
        candidate = Path(value)
        return str(candidate if candidate.is_absolute() else (self.root / candidate))


def discover_manifest(start: str | Path | None = None) -> Path | None:
    """Walk up from ``start`` (default: cwd) looking for ``feature_project.yaml``."""
    origin = Path(start or Path.cwd()).resolve()
    for directory in (origin, *origin.parents):
        candidate = directory / MANIFEST_FILENAME
        if candidate.is_file():
            return candidate
    return None


def _reject_forbidden_keys(node: object, where: str = "") -> None:
    """Recursively reject secret/approver keys at any depth, matched case-insensitively."""
    if isinstance(node, dict):
        for key, value in node.items():
            label = f"{where}.{key}" if where else str(key)
            if str(key).lower() in _FORBIDDEN_KEYS:
                raise ManifestError(
                    f"{MANIFEST_FILENAME} must not contain secret or approver keys: "
                    f"found {label!r}"
                )
            _reject_forbidden_keys(value, label)
    elif isinstance(node, list):
        for index, item in enumerate(node):
            _reject_forbidden_keys(item, f"{where}[{index}]")


def load_manifest(path: str | Path) -> FeatureProjectManifest:
    """Parse a manifest under a strict schema.

    Fails explicitly on: a non-mapping document, any secret/approver key at any depth or
    casing, any unknown top-level field (never silently ignored), and any non-scalar value
    for a known field (which is how a nested credential mapping is caught).
    """
    path = Path(path)
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ManifestError(
            f"{MANIFEST_FILENAME} cannot be read: {exc.strerror or exc}"
        ) from exc
    try:
        data = yaml.safe_load(raw) or {}
    except yaml.YAMLError as exc:
        # Concise location hint only — never the parser traceback.
        mark = getattr(exc, "problem_mark", None)
        location = f" (line {mark.line + 1}, column {mark.column + 1})" if mark else ""
        problem = getattr(exc, "problem", None) or "invalid YAML"
        raise ManifestError(
            f"{MANIFEST_FILENAME} is not valid YAML{location}: {problem}"
        ) from exc
    if not isinstance(data, dict):
        raise ManifestError(f"{MANIFEST_FILENAME} must be a mapping")

    _reject_forbidden_keys(data)

    normalized: dict[str, object] = {}
    for key, value in data.items():
        lowered = str(key).lower()
        if lowered not in _KNOWN_KEYS:
            raise ManifestError(
                f"{MANIFEST_FILENAME} has unknown field {str(key)!r}; "
                f"allowed fields: {sorted(_KNOWN_KEYS)}"
            )
        if lowered in normalized:
            raise ManifestError(
                f"{MANIFEST_FILENAME} has a duplicate field {lowered!r} (differing only by case)"
            )
        if value is not None and not isinstance(value, str):
            raise ManifestError(
                f"{MANIFEST_FILENAME} field {lowered!r} must be a string, not "
                f"{type(value).__name__}"
            )
        normalized[lowered] = value

    return FeatureProjectManifest(
        root=path.parent,
        project=normalized.get("project"),
        package=normalized.get("package"),
        requires_core=normalized.get("requires_core"),
        registry=normalized.get("registry"),
        udf_provider=normalized.get("udf_provider"),
        tests=normalized.get("tests"),
        bundle_store=normalized.get("bundle_store"),
        pointer_store=normalized.get("pointer_store"),
        wheel_store=normalized.get("wheel_store"),
    )


# Attributes fsctl can fill from env/manifest, with their env var (None = no env fallback)
# and whether the value is a filesystem path (resolved relative to the project root).
_FILLABLE = (
    # (args attr, manifest attr, env var, is_path)
    ("registry", "registry", "FSP_REGISTRY_PATH", True),
    ("udfs", "udf_provider", "FSP_UDF_PROVIDER", False),
    ("tests", "tests", None, True),
    ("bundle_store", "bundle_store", None, True),
    ("pointer_store", "pointer_store", None, True),
)


def resolve_cli_defaults(args) -> None:
    """Fill ``None`` CLI args from env then the discovered manifest, in place.

    Precedence: an explicit flag (already set) is never overwritten; otherwise the
    environment variable wins over the manifest. Only runs when the command actually has
    fillable attributes, so ``init`` (which has none) never triggers manifest discovery.
    """
    fillable = [f for f in _FILLABLE if hasattr(args, f[0])]
    if not fillable:
        return
    manifest: FeatureProjectManifest | None = None
    manifest_path = discover_manifest()
    if manifest_path is not None:
        manifest = load_manifest(manifest_path)  # may raise ManifestError

    for attr, manifest_attr, env_var, is_path in fillable:
        if getattr(args, attr) is not None:
            continue  # explicit flag wins
        env_val = os.environ.get(env_var) if env_var else None
        if env_val:
            setattr(args, attr, env_val)
            continue
        if manifest is not None:
            manifest_val = getattr(manifest, manifest_attr)
            if manifest_val is not None:
                setattr(
                    args,
                    attr,
                    manifest.resolved_path(manifest_val) if is_path else manifest_val,
                )
