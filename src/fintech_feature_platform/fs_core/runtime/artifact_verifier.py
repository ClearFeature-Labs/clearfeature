"""Fail-closed runtime verification of the artifact-bound feature wheel.

Proves the invariant chain for an artifact-bound promoted bundle:

    tested wheel == published wheel == wheel in the worker runtime evidence
    == feature_artifact referenced by the promoted final bundle
    == Python feature code actually imported by the worker

A matching package name and version are NOT sufficient. This module hashes the actual
wheel bytes, reads the wheel METADATA + RECORD, resolves the installed distribution, hashes
the installed files and proves byte-level correspondence to the wheel RECORD, proves the
configured provider imports from that verified distribution, and enforces ``requires_core``
against the installed core version. It is pure over its inputs (no settings/env reads, no
network, no installation) so it can be unit-tested exhaustively.
"""

from __future__ import annotations

import base64
import hashlib
import importlib
import importlib.metadata
import json
import re
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from fintech_feature_platform.fs_core.registry.artifact import (
    artifact_storage_key,
    canonicalize_name,
    sha256_file,
)

# Small self-contained helpers so this runtime path needs no third-party import (the
# ``packaging`` library is not a dependency anywhere in the platform).


# requires_core is a deliberately limited MVP grammar (NOT full PEP 440): comma-separated
# AND constraints, operators ``== >= > <= <`` only, over normalized numeric release versions
# (e.g. 0.1, 0.1.4, 1.0.0). Anything else — other operators (``!= ~= ===``), wildcards
# (``1.*``), prereleases/dev/local/epoch (``1.0a1``, ``1.0.dev1``, ``1+x``, ``1!2``), empty
# clauses — raises ``ValueError`` (mapped to feature_artifact_metadata_invalid). Never
# partially interpreted or silently accepted.
_RELEASE_RE = re.compile(r"\d+(?:\.\d+)*")
_CLAUSE_RE = re.compile(r"\s*(==|>=|<=|>|<)\s*(\d+(?:\.\d+)*)\s*")


def _version_key(version: str) -> tuple[int, ...]:
    # ``version`` is already validated as a normalized numeric release by the callers below.
    return tuple(int(component) for component in str(version).split("."))


def _compare_versions(left: tuple[int, ...], right: tuple[int, ...]) -> int:
    width = max(len(left), len(right))
    left = left + (0,) * (width - len(left))
    right = right + (0,) * (width - len(right))
    return (left > right) - (left < right)


def core_version_satisfies(installed: str, requires_core: str) -> bool:
    """Evaluate the limited MVP ``requires_core`` grammar against ``installed`` (fail closed).

    Supported: comma-separated AND constraints with operators ``== >= > <= <`` over
    normalized numeric release versions. Raises ``ValueError`` on any unsupported operator,
    wildcard, prerelease/dev/local/epoch version, malformed or empty clause.
    """
    if not _RELEASE_RE.fullmatch(str(installed).strip()):
        raise ValueError(f"non-normalized installed version {installed!r}")
    installed_key = _version_key(str(installed).strip())

    clauses = str(requires_core).split(",")
    if not any(clause.strip() for clause in clauses):
        raise ValueError("empty version specifier")
    for raw in clauses:
        if not raw.strip():
            raise ValueError("empty constraint clause (a stray comma is not allowed)")
        match = _CLAUSE_RE.fullmatch(raw)
        if not match:
            raise ValueError(f"unsupported or malformed constraint {raw.strip()!r}")
        op = match.group(1)
        cmp = _compare_versions(installed_key, _version_key(match.group(2)))
        ok = {"==": cmp == 0, ">=": cmp >= 0, "<=": cmp <= 0, ">": cmp > 0, "<": cmp < 0}[op]
        if not ok:
            return False
    return True

# --- stable internal error categories (never contain secrets/values) ---------
CAT_REQUIRED = "feature_artifact_required"
CAT_MANIFEST_MISSING = "feature_artifact_manifest_missing"
CAT_WHEEL_MISSING = "feature_artifact_wheel_missing"
CAT_SHA_MISMATCH = "feature_artifact_sha_mismatch"
CAT_DISTRIBUTION_MISMATCH = "feature_artifact_distribution_mismatch"
CAT_INSTALLATION_MISMATCH = "feature_artifact_installation_mismatch"
CAT_PROVIDER_MISMATCH = "feature_artifact_provider_mismatch"
CAT_CORE_INCOMPATIBLE = "feature_artifact_core_incompatible"
CAT_METADATA_INVALID = "feature_artifact_metadata_invalid"
CAT_REGISTRY_MISMATCH = "feature_artifact_registry_mismatch"

_MANIFEST_FIELDS = (
    "format_version", "name", "version", "provider", "filename", "sha256", "wheel_path",
)


class ArtifactVerificationError(RuntimeError):
    """Fail-closed verification failure carrying a stable ``category`` and safe ``evidence``."""

    def __init__(self, category: str, message: str, evidence: Mapping[str, object] | None = None):
        super().__init__(message)
        self.category = category
        self.evidence = dict(evidence or {})


@dataclass(frozen=True)
class RuntimeArtifactEvidence:
    """Build-generated evidence for the installed feature artifact (parsed manifest)."""

    format_version: int
    name: str
    version: str
    provider: str
    filename: str
    sha256: str
    wheel_path: str


def load_runtime_evidence(path: str | Path) -> RuntimeArtifactEvidence:
    """Load + shape-validate the runtime artifact manifest. Missing file → manifest_missing."""
    p = Path(path)
    if not p.is_file():
        raise ArtifactVerificationError(
            CAT_MANIFEST_MISSING, f"runtime artifact manifest not found at {p}"
        )
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise ArtifactVerificationError(
            CAT_METADATA_INVALID, f"runtime artifact manifest is not valid JSON: {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise ArtifactVerificationError(CAT_METADATA_INVALID, "runtime manifest must be an object")
    missing = [f for f in _MANIFEST_FIELDS if f not in data]
    if missing:
        raise ArtifactVerificationError(
            CAT_METADATA_INVALID, f"runtime manifest missing fields: {sorted(missing)}"
        )
    return RuntimeArtifactEvidence(
        format_version=data["format_version"],
        name=str(data["name"]),
        version=str(data["version"]),
        provider=str(data["provider"]),
        filename=str(data["filename"]),
        sha256=str(data["sha256"]),
        wheel_path=str(data["wheel_path"]),
    )


def _record_hash(data: bytes) -> str:
    """The wheel-RECORD hash form: ``sha256=<urlsafe-b64-no-padding>``."""
    return "sha256=" + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=")


def _read_wheel_metadata_and_record(wheel: Path) -> tuple[str, str, dict[str, str]]:
    """Return (METADATA Name, METADATA Version, {relpath: record_hash}) from a wheel zip."""
    try:
        with zipfile.ZipFile(wheel) as zf:
            names = zf.namelist()
            meta_names = [n for n in names if n.endswith(".dist-info/METADATA")]
            record_names = [n for n in names if n.endswith(".dist-info/RECORD")]
            if not meta_names or not record_names:
                raise ArtifactVerificationError(CAT_METADATA_INVALID, "wheel lacks METADATA/RECORD")
            name = version = None
            for line in zf.read(meta_names[0]).decode("utf-8", "replace").splitlines():
                if line.startswith("Name: "):
                    name = line[len("Name: "):].strip()
                elif line.startswith("Version: "):
                    version = line[len("Version: "):].strip()
                if name and version:
                    break
            record: dict[str, str] = {}
            for line in zf.read(record_names[0]).decode("utf-8", "replace").splitlines():
                parts = line.split(",")
                if len(parts) >= 2 and parts[1]:
                    record[parts[0]] = parts[1]
    except zipfile.BadZipFile as exc:
        raise ArtifactVerificationError(
            CAT_METADATA_INVALID, f"wheel is not a valid zip: {exc}"
        ) from exc
    if not name or not version:
        raise ArtifactVerificationError(CAT_METADATA_INVALID, "wheel METADATA missing Name/Version")
    return name, version, record


def _verify_installed_matches_wheel(
    dist, record: dict[str, str], provider_file: str | None
) -> None:
    """Hash installed distribution files and require byte-level match to the wheel RECORD.

    Also require the imported provider module file to belong to this verified distribution.
    """
    provider_resolved = str(Path(provider_file).resolve()) if provider_file else None
    provider_in_dist = False
    checked = 0
    for entry in dist.files or []:
        rel = str(entry)
        if rel.endswith(".dist-info/RECORD") or rel not in record:
            continue
        located = Path(dist.locate_file(entry)).resolve()
        if not located.is_file():
            raise ArtifactVerificationError(
                CAT_INSTALLATION_MISMATCH, f"installed file missing: {rel}"
            )
        if _record_hash(located.read_bytes()) != record[rel]:
            raise ArtifactVerificationError(
                CAT_INSTALLATION_MISMATCH, f"installed file differs from the wheel: {rel}"
            )
        checked += 1
        if provider_resolved is not None and str(located) == provider_resolved:
            provider_in_dist = True
    if checked == 0:
        raise ArtifactVerificationError(
            CAT_INSTALLATION_MISMATCH, "no installed files corresponded to the wheel RECORD"
        )
    if provider_resolved is not None and not provider_in_dist:
        raise ArtifactVerificationError(
            CAT_PROVIDER_MISMATCH,
            "provider module does not belong to the verified distribution — a local "
            "module is likely shadowing the installed package (e.g. the process was "
            "started inside the Feature Project source directory); run it from a "
            "directory where the verified installed package is the one imported",
        )


def verify_feature_artifact(
    *,
    feature_artifact: Mapping[str, object],
    evidence: RuntimeArtifactEvidence,
    installed_core_version: str,
) -> dict:
    """Verify the full invariant chain; raise ``ArtifactVerificationError`` (fail closed).

    ``feature_artifact`` is the promoted bundle's metadata; ``evidence`` is the parsed
    runtime manifest; ``installed_core_version`` comes from ``importlib.metadata`` at the
    call site (never hard-coded). Returns a safe descriptor on success.
    """
    fa = feature_artifact
    for field in ("name", "version", "provider", "sha256", "filename"):
        if not fa.get(field):
            raise ArtifactVerificationError(
                CAT_METADATA_INVALID, f"promoted feature_artifact missing {field!r}"
            )
    exp_name = canonicalize_name(str(fa["name"]))
    exp_version = str(fa["version"])
    exp_sha = str(fa["sha256"])
    exp_filename = str(fa["filename"])
    exp_provider = str(fa["provider"])
    safe = {
        "expected_name": exp_name,
        "expected_version": exp_version,
        "expected_sha256": exp_sha,
        "expected_provider": exp_provider,
    }

    def fail(category: str, message: str):
        return ArtifactVerificationError(category, message, {**safe, "category": category})

    # 1. Runtime manifest evidence must agree with the promoted bundle (not proof alone).
    if canonicalize_name(evidence.name) != exp_name or evidence.version != exp_version:
        raise fail(CAT_DISTRIBUTION_MISMATCH, "runtime manifest name/version != promoted bundle")
    if evidence.filename != exp_filename:
        raise fail(CAT_DISTRIBUTION_MISMATCH, "runtime manifest filename != promoted bundle")
    if evidence.sha256 != exp_sha:
        raise fail(CAT_SHA_MISMATCH, "runtime manifest sha256 != promoted bundle")
    if evidence.provider != exp_provider:
        raise fail(CAT_PROVIDER_MISMATCH, "runtime manifest provider != promoted bundle")

    # 2. storage_key is deterministic and matches the bundle metadata.
    expected_key = artifact_storage_key(exp_name, exp_version, exp_sha, exp_filename)
    if fa.get("storage_key") != expected_key:
        raise fail(CAT_METADATA_INVALID, "feature_artifact.storage_key is not deterministic")

    # 3. The actual wheel file: exists, hashes to the bundle SHA, filename matches.
    wheel = Path(evidence.wheel_path)
    if not wheel.is_file():
        raise fail(CAT_WHEEL_MISSING, "runtime wheel file is missing")
    if sha256_file(wheel) != exp_sha:
        raise fail(CAT_SHA_MISMATCH, "actual wheel bytes do not match the promoted SHA-256")
    if wheel.name != exp_filename:
        raise fail(CAT_METADATA_INVALID, "wheel filename != promoted filename")

    # 4. Wheel METADATA identity.
    meta_name, meta_version, record = _read_wheel_metadata_and_record(wheel)
    if canonicalize_name(meta_name) != exp_name:
        raise fail(CAT_DISTRIBUTION_MISMATCH, "wheel METADATA Name != promoted name")
    if meta_version != exp_version:
        raise fail(CAT_DISTRIBUTION_MISMATCH, "wheel METADATA Version != promoted version")

    # 5. The installed distribution behind the provider.
    try:
        dist = importlib.metadata.distribution(exp_name)
    except importlib.metadata.PackageNotFoundError as exc:
        raise fail(CAT_INSTALLATION_MISMATCH, "feature distribution is not installed") from exc
    if canonicalize_name(dist.metadata["Name"]) != exp_name or dist.version != exp_version:
        raise fail(CAT_DISTRIBUTION_MISMATCH, "installed distribution name/version mismatch")

    # 6. Provider imports and belongs to the verified distribution.
    module_path, sep, attr = exp_provider.partition(":")
    if not sep or not attr:
        raise fail(CAT_METADATA_INVALID, "provider must be 'module:callable'")
    try:
        module = importlib.import_module(module_path)
        obj = getattr(module, attr)
    except (ImportError, AttributeError) as exc:
        raise fail(CAT_PROVIDER_MISMATCH, "provider is not importable") from exc
    if not callable(obj):
        raise fail(CAT_PROVIDER_MISMATCH, "provider target is not callable")

    # 7. Installed files match the wheel RECORD byte-for-byte + provider ownership.
    try:
        _verify_installed_matches_wheel(dist, record, getattr(module, "__file__", None))
    except ArtifactVerificationError as exc:
        raise ArtifactVerificationError(
            exc.category, str(exc), {**safe, "category": exc.category}
        ) from exc

    # 8. Core compatibility (requires_core when present; installed version is passed in).
    requires_core = fa.get("requires_core")
    if requires_core:
        try:
            satisfied = core_version_satisfies(installed_core_version, str(requires_core))
        except ValueError as exc:
            raise fail(CAT_METADATA_INVALID, f"invalid requires_core: {exc}") from exc
        if not satisfied:
            raise fail(
                CAT_CORE_INCOMPATIBLE,
                f"installed core {installed_core_version} does not satisfy {requires_core}",
            )

    return {**safe, "verified": True}
