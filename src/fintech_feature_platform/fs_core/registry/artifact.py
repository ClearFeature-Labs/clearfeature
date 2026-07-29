"""Feature-artifact hashing and the content-addressed storage-key contract.

Single source of truth shared by the publish CLI (``cli/artifact.py``) and the runtime
verifier (``fs_core/runtime/artifact_verifier.py``): the SHA helpers and the deterministic
``storage_key`` layout ``<normalized-name>/<version>/<sha256>/<filename>``. Keeping one
definition here means publish and runtime cannot drift.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path


def canonicalize_name(name: str) -> str:
    """Normalize a distribution name (matches ``packaging.utils.canonicalize_name`` for
    release names): lowercase, collapse runs of ``-_.`` to a single ``-``.

    Self-contained so neither the runtime image nor a bare CLI install needs the
    third-party ``packaging`` library.
    """
    return re.sub(r"[-_.]+", "-", str(name)).strip().lower()


def parse_wheel_filename(filename: str) -> tuple[str, str]:
    """Return ``(canonical_name, version)`` from a wheel filename — strict, fail-closed.

    Expects the standard ``{distribution}-{version}(-{build})?-{python}-{abi}-{platform}.whl``
    form with a normalized numeric-or-PEP440 version field. Raises ``ValueError`` on
    anything else rather than guessing.
    """
    if not filename.endswith(".whl"):
        raise ValueError(f"not a wheel filename: {filename!r}")
    parts = filename[: -len(".whl")].split("-")
    if len(parts) not in (5, 6):
        raise ValueError(f"unrecognized wheel filename structure: {filename!r}")
    distribution, version = parts[0], parts[1]
    if not distribution or not version or not version[0].isdigit():
        raise ValueError(f"unrecognized wheel name/version in: {filename!r}")
    return canonicalize_name(distribution), version


def sha256_file(path: str | Path) -> str:
    """Return ``sha256:<hex>`` of a file's bytes."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def sha256_bytes(data: bytes) -> str:
    """Return ``sha256:<hex>`` of a bytes object."""
    return "sha256:" + hashlib.sha256(data).hexdigest()


def artifact_storage_key(name: str, version: str, sha256: str, filename: str) -> str:
    """Deterministic, content-addressed relative key ``<name>/<version>/<sha256>/<filename>``.

    ``name`` is the canonical (normalized) distribution name; ``sha256`` is the hex digest
    (no ``sha256:`` prefix, no bucket/URL/credential/absolute component).
    """
    sha_hex = sha256.split(":", 1)[-1]
    return f"{name}/{version}/{sha_hex}/{filename}"
