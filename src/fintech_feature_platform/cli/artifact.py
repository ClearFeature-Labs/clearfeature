"""Feature-wheel build, exact-wheel install/test and immutable artifact storage.
 (hardened). The publish pipeline binds a bundle to the *exact* built
wheel: build → hash → **install** the wheel into an isolated target → verify its identity
against standard wheel metadata and feature_project.yaml → golden-test the installed wheel
→ upload the immutable wheel → read back and verify → persist the bound bundle. The source
checkout, an editable install, and inherited ``PYTHONPATH`` cannot shadow the installed
wheel. Runtime verification / binding policy lives in the runtime verifier (not here).
"""

from __future__ import annotations

import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

# Single source of truth for hashing, name canonicalization, wheel-filename parsing and
# the content-addressed storage-key contract. Deliberately self-contained (no third-party
# ``packaging`` import) so a bare `pip install fintech-feature-platform` gives a working
# publish/image CLI — the same lesson as the runtime path.
from fintech_feature_platform.fs_core.registry.artifact import (
    artifact_storage_key,
    canonicalize_name,
    parse_wheel_filename,
    sha256_bytes,
    sha256_file,
)

__all__ = ["artifact_storage_key", "sha256_file", "sha256_bytes"]  # re-exported


class WheelBuildError(RuntimeError):
    """Raised when the feature wheel cannot be built or installed."""


class WheelTestError(RuntimeError):
    """Raised when the exact-wheel golden test does not pass."""


class WheelIdentityError(RuntimeError):
    """Raised when wheel metadata/identity does not match the feature project."""


class ArtifactStoreError(RuntimeError):
    """Raised on immutable-store violations (same key, different bytes) or store failures."""


def _uv() -> str:
    uv = shutil.which("uv")
    if uv is None:  # pragma: no cover - environment-dependent
        raise WheelBuildError("uv is required for feature wheels but was not found on PATH")
    return uv


def build_wheel(project_dir: str | Path, out_dir: str | Path) -> Path:
    """Build a single wheel from ``project_dir`` into ``out_dir`` (offline). Returns its path."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [_uv(), "build", "--wheel", "--offline", "--out-dir", str(out_dir), str(project_dir)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise WheelBuildError(f"wheel build failed: {detail}")
    wheels = sorted(out_dir.glob("*.whl"))
    if not wheels:
        raise WheelBuildError("wheel build produced no .whl file")
    return wheels[-1]


def install_wheel(wheel: str | Path, target: str | Path) -> None:
    """Install the exact wheel into an isolated ``--target`` directory (offline, no deps)."""
    result = subprocess.run(
        [_uv(), "pip", "install", "--target", str(target), "--no-deps", "--offline", str(wheel)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise WheelBuildError(f"wheel install failed: {detail}")


def _isolated_env(target: Path) -> dict[str, str]:
    """Env whose ``PYTHONPATH`` is REPLACED by the install target only.

    Replacing (not extending) the inherited ``PYTHONPATH`` means the source checkout and any
    inherited path cannot shadow the installed wheel; the platform itself remains importable
    from the interpreter's own site-packages. ``PYTHONPATH`` entries precede site-packages,
    so the installed wheel also wins over any editable install of the same package.
    """
    env = dict(os.environ)
    env["PYTHONPATH"] = str(target)
    env["PYTHONSAFEPATH"] = "1"  # do not add the (neutral) cwd to sys.path
    return env


def installed_identity(target: str | Path) -> tuple[str, str]:
    """Return ``(Name, Version)`` from the installed distribution's METADATA under ``target``."""
    import importlib.metadata as importlib_metadata

    dists = list(importlib_metadata.distributions(path=[str(target)]))
    if not dists:
        raise WheelIdentityError(f"no installed distribution found under {target}")
    if len(dists) > 1:
        names = sorted(d.metadata["Name"] for d in dists)
        raise WheelIdentityError(f"expected exactly one installed distribution, found {names}")
    dist = dists[0]
    return dist.metadata["Name"], dist.version


def verify_wheel_identity(
    wheel: str | Path, target: str | Path, *, manifest_project: str
) -> dict[str, str]:
    """Validate wheel identity via standard metadata; return ``{name, version}`` (canonical).

    Checks: the filename (parsed with ``packaging``) agrees with the installed METADATA
    Name/Version, and that identity matches the feature_project.yaml project name.
    """
    fname_name, fname_version = parse_wheel_filename(Path(wheel).name)
    meta_name, meta_version = installed_identity(target)
    if canonicalize_name(fname_name) != canonicalize_name(meta_name):
        raise WheelIdentityError(
            f"wheel filename name {fname_name!r} != METADATA Name {meta_name!r}"
        )
    if str(fname_version) != str(meta_version):
        raise WheelIdentityError(
            f"wheel filename version {fname_version} != METADATA Version {meta_version}"
        )
    if canonicalize_name(meta_name) != canonicalize_name(manifest_project):
        raise WheelIdentityError(
            f"wheel identity {meta_name!r} != feature_project.yaml project {manifest_project!r}"
        )
    return {"name": str(canonicalize_name(meta_name)), "version": str(meta_version)}


def verify_provider_imports_from_wheel(
    target: Path, provider: str, *, cwd: Path
) -> None:
    """Verify ``module:callable`` imports from the installed wheel (not source/editable)."""
    code = (
        "import importlib, json, sys;"
        "spec = sys.argv[1];"
        "mod_name, _, attr = spec.partition(':');"
        "mod = importlib.import_module(mod_name);"
        "obj = getattr(mod, attr);"
        "assert callable(obj);"
        "print(json.dumps({'file': getattr(mod, '__file__', '')}))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code, provider],
        capture_output=True,
        text=True,
        env=_isolated_env(target),
        cwd=str(cwd),
    )
    if proc.returncode != 0:
        raise WheelIdentityError(
            f"provider {provider!r} does not import from the installed wheel: "
            f"{proc.stderr.strip() or proc.stdout.strip()}"
        )
    try:
        module_file = json.loads(proc.stdout)["file"]
    except (json.JSONDecodeError, KeyError) as exc:  # pragma: no cover - defensive
        raise WheelIdentityError(f"provider check produced no result: {proc.stdout!r}") from exc
    resolved_target = Path(target).resolve()
    if not module_file or not Path(module_file).resolve().is_relative_to(resolved_target):
        raise WheelIdentityError(
            f"provider {provider!r} imported from {module_file!r}, not the installed wheel"
        )


def install_and_test_wheel(
    wheel: str | Path,
    *,
    provider: str,
    registry_rel: str,
    tests_rel: str,
    manifest_project: str,
) -> dict:
    """Install the exact wheel, verify its identity + provider, and golden-test it in isolation.

    Returns ``{"identity": {name, version}, "report": <golden report>}``; raises on any
    identity or test failure. The wheel is installed and the golden test runs from a neutral
    cwd with ``PYTHONPATH`` pointing only at the install target, so the source checkout
    cannot shadow it.
    """
    with tempfile.TemporaryDirectory() as work:
        target = Path(work) / "site"
        neutral = Path(work) / "cwd"
        neutral.mkdir()
        install_wheel(wheel, target)
        identity = verify_wheel_identity(wheel, target, manifest_project=manifest_project)
        verify_provider_imports_from_wheel(target, provider, cwd=neutral)

        registry = target / registry_rel
        tests = target / tests_rel
        if not registry.is_file() or not tests.is_file():
            raise WheelTestError(
                f"installed wheel lacks expected registry/tests: {registry_rel}, {tests_rel}"
            )
        proc = subprocess.run(
            [
                sys.executable, "-m", "fintech_feature_platform.cli.fsctl", "test",
                "--registry", str(registry), "--tests", str(tests), "--udfs", provider,
            ],
            capture_output=True,
            text=True,
            env=_isolated_env(target),
            cwd=str(neutral),
        )
        try:
            report = json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            raise WheelTestError(
                f"isolated wheel test produced no JSON (exit {proc.returncode}): "
                f"{proc.stderr.strip() or proc.stdout.strip()}"
            ) from exc
        if not report.get("ok"):
            passed, total = report.get("passed"), report.get("total")
            raise WheelTestError(f"exact-wheel golden test failed: {passed}/{total} passed")
        return {"identity": identity, "report": report}


def _release_key(name: str, version: str) -> str:
    return f"{name}/{version}/release.json"


class WheelStore:
    """Immutable, content-addressed wheel store: a local dir (dev/tests) or ``s3://`` (MinIO).

    Layout: ``<normalized-name>/<version>/<sha256>/<filename>`` for the wheel plus an
    immutable ``<normalized-name>/<version>/release.json`` release record. Version
    immutability is enforced via the release record: the first name+version publication
    records the SHA; the same name+version+SHA is idempotent success; the same name+version
    with a different SHA fails closed. The release-record write is an atomic compare-and-set
    on both backends — ``os.link`` locally, a conditional ``If-None-Match: *`` PUT on S3 —
    so concurrent publishers cannot overwrite it.
    """

    def __init__(self, uri: str, *, client=None, bucket_secure: bool = False) -> None:
        self.uri = uri
        self._client = client
        self._bucket_secure = bucket_secure

    def put(self, wheel: str | Path, *, name: str, version: str, sha256: str) -> dict:
        """Store the wheel + release record immutably. Returns the release record dict.

        Idempotent for an identical name+version+SHA; fails closed on a same name+version
        with a different SHA (``ArtifactStoreError``).
        """
        wheel = Path(wheel)
        filename = wheel.name
        storage_key = artifact_storage_key(name, version, sha256, filename)
        release_key = _release_key(name, version)
        release = {
            "name": name,
            "version": version,
            "sha256": sha256,
            "filename": filename,
            "storage_key": storage_key,
        }
        release_bytes = json.dumps(release, sort_keys=True).encode("utf-8")

        # The wheel is content-addressed (its SHA is in the key), so identical bytes map to
        # one key: writing is idempotent, concurrent same-SHA writes converge, and different
        # SHAs never collide.
        self._write(storage_key, wheel.read_bytes())

        # Atomically claim the (name, version) release record — first writer wins. A second
        # concurrent publisher never overwrites it: an identical SHA is idempotent success,
        # and a different SHA fails closed.
        if self._write_if_absent(release_key, release_bytes):
            return release
        existing = json.loads(self._read(release_key).decode("utf-8"))
        if existing.get("sha256") != sha256:
            raise ArtifactStoreError(
                f"immutable store: {name} {version} already released with a different SHA"
            )
        return existing

    def read_back_verify(self, *, name: str, version: str, sha256: str) -> dict:
        """Re-read the release record and the stored wheel; confirm both match ``sha256``.

        Returns the verified release record. Raises ``ArtifactStoreError`` on any mismatch or
        missing object, so an artifact-bound bundle is only ever persisted after this passes.
        """
        release_bytes = self._read(_release_key(name, version))
        if release_bytes is None:
            raise ArtifactStoreError(f"release record missing for {name} {version}")
        record = json.loads(release_bytes.decode("utf-8"))
        if record.get("sha256") != sha256:
            raise ArtifactStoreError(f"release record SHA mismatch for {name} {version}")
        storage_key = record.get("storage_key")
        if storage_key != artifact_storage_key(name, version, sha256, record.get("filename", "")):
            raise ArtifactStoreError(f"release record storage_key mismatch for {name} {version}")
        stored = self._read(storage_key)
        if stored is None:
            raise ArtifactStoreError(f"stored wheel missing at {storage_key}")
        if sha256_bytes(stored) != sha256:
            raise ArtifactStoreError(f"stored wheel SHA mismatch at {storage_key}")
        return record

    def location(self, storage_key: str) -> str:
        """A human-facing location for a stored key (info only; not part of bundle identity)."""
        if self.uri.startswith("s3://"):
            return f"{self.uri.rstrip('/')}/{storage_key}"
        return str(self._local_base() / storage_key)

    # -- backends ------------------------------------------------------------
    def _local_base(self) -> Path:
        raw = self.uri[len("file://"):] if self.uri.startswith("file://") else self.uri
        return Path(raw)

    def _read(self, key: str) -> bytes | None:
        if self.uri.startswith("s3://"):
            return self._read_s3(key)
        path = self._local_base() / key
        return path.read_bytes() if path.is_file() else None

    def _write(self, key: str, data: bytes) -> None:
        """Write (overwrite) a key. Local writes are atomic (temp + ``os.replace``)."""
        if self.uri.startswith("s3://"):
            self._write_s3(key, data)
            return
        path = self._local_base() / key
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        tmp.write_bytes(data)
        os.replace(tmp, path)

    def _write_if_absent(self, key: str, data: bytes) -> bool:
        """Create ``key`` atomically only if absent; return True if this call created it.

        Local: write a temp file then ``os.link`` it into place — the link is atomic and
        raises ``FileExistsError`` if the record already exists, so exactly one concurrent
        publisher wins and the file is always fully-formed when a loser reads it back. S3:
        a conditional ``If-None-Match: *`` PUT (compare-and-set), see ``_write_if_absent_s3``.
        """
        if self.uri.startswith("s3://"):
            return self._write_if_absent_s3(key, data)
        path = self._local_base() / key
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        tmp.write_bytes(data)
        try:
            os.link(tmp, path)
            return True
        except FileExistsError:
            return False
        finally:
            tmp.unlink(missing_ok=True)

    # -- s3 / MinIO ----------------------------------------------------------
    def _s3_client(self):
        if self._client is not None:
            return self._client
        # pragma: no cover - needs a live endpoint; gated integration test injects a client
        from fintech_feature_platform.fs_core.raw.minio_payload_store import connect_minio

        endpoint = os.environ.get("FSP_MINIO_ENDPOINT", "localhost:9000")
        access = os.environ.get("FSP_MINIO_ACCESS_KEY", "minioadmin")
        secret = os.environ.get("FSP_MINIO_SECRET_KEY", "minioadmin")
        return connect_minio(endpoint, access, secret, secure=self._bucket_secure)

    def _bucket_prefix(self) -> tuple[str, str]:
        without_scheme = self.uri[len("s3://"):]
        bucket, _, prefix = without_scheme.partition("/")
        return bucket, prefix.rstrip("/")

    def _object_key(self, key: str) -> str:
        bucket, prefix = self._bucket_prefix()
        return f"{prefix}/{key}" if prefix else key

    @staticmethod
    def _ensure_bucket(client, bucket: str) -> None:
        """Create the bucket, tolerating a concurrent creator."""
        from minio.error import S3Error

        if client.bucket_exists(bucket):
            return
        try:
            client.make_bucket(bucket)
        except S3Error as exc:  # pragma: no cover - concurrent-create race
            if exc.code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise

    def _read_s3(self, key: str) -> bytes | None:
        client = self._s3_client()
        bucket, _ = self._bucket_prefix()
        try:
            response = client.get_object(bucket, self._object_key(key))
        except Exception:  # noqa: BLE001 - any "not found" means no existing object
            return None
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def _write_s3(self, key: str, data: bytes) -> None:
        client = self._s3_client()
        bucket, _ = self._bucket_prefix()
        self._ensure_bucket(client, bucket)
        client.put_object(
            bucket, self._object_key(key), io.BytesIO(data), length=len(data),
            content_type="application/octet-stream",
        )

    def _write_if_absent_s3(self, key: str, data: bytes) -> bool:  # pragma: no cover - gated
        """Atomic first-writer-wins on S3 via a conditional PUT (``If-None-Match: *``).

        A successful conditional create returns True; a ``412 PreconditionFailed`` means the
        object already exists (another publisher won) and returns False. This is a genuine
        compare-and-set — it never overwrites an existing release record — and does not rely
        on bucket versioning or Object Lock. Uses the MinIO client's own signed-request
        transport (``_execute``) because ``put_object`` does not expose conditional headers.
        """
        from minio.error import S3Error

        client = self._s3_client()
        bucket, _ = self._bucket_prefix()
        self._ensure_bucket(client, bucket)
        try:
            client._execute(
                "PUT",
                bucket,
                self._object_key(key),
                body=data,
                headers={"If-None-Match": "*", "Content-Type": "application/octet-stream"},
            )
            return True
        except S3Error as exc:
            # 412: another publisher already created the release record (never overwrite).
            conflict_codes = (
                "PreconditionFailed",
                "AtLeastOneOfTheHeadersConditionEvaluatedToFalse",
            )
            if exc.code in conflict_codes:
                return False
            raise
