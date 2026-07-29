"""MinIO / S3-backed implementation of the raw payload store.

Implements the ``RawPayloadStore`` contract (``get_payload(storage_uri)`` /
``put(storage_uri, payload)``) against MinIO (or any S3-compatible store). The
internal ``storage_uri`` has the form ``s3://bucket/key`` and serialization is
inferred from the key extension:

- ``.json``    -> plain JSON;
- ``.json.gz`` -> gzip-compressed JSON.

The ``minio`` SDK is imported lazily (only in ``connect_minio``), so this module
can be imported — and its unit tests run — without the optional ``storage`` extra
installed. A missing object is raised as ``KeyError`` so ``ReportResolver`` treats
missing payloads identically across the in-memory and MinIO stores.
"""

from __future__ import annotations

import gzip
import json
from io import BytesIO
from typing import Any
from urllib.parse import urlparse

_JSON_CONTENT_TYPE = "application/json"
_GZIP_CONTENT_TYPE = "application/gzip"
_NOT_FOUND_CODES = {"NoSuchKey", "NoSuchBucket"}


def connect_minio(
    endpoint: str,
    access_key: str,
    secret_key: str,
    *,
    secure: bool = False,
):
    """Create a MinIO client. Imports the ``minio`` SDK lazily (``storage`` extra)."""
    from minio import Minio

    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def connect_minio_probe(
    endpoint: str,
    access_key: str,
    secret_key: str,
    *,
    secure: bool = False,
    timeout_s: float = 2.0,
):
    """Readiness-probe client : SHORT deadline, NO retries.

    Built ONCE at backend construction, used only by the readiness ``bucket_exists``
    probe. The SDK's default data-plane policy (300 s connect/read x 5 urllib3
    retries) is correct for real payload transfers but is NOT an acceptable readiness
    recovery bound — this client bounds the probe at ``timeout_s`` per phase with a
    single attempt. Data-plane clients are deliberately untouched.
    """
    import urllib3
    from minio import Minio

    return Minio(
        endpoint, access_key=access_key, secret_key=secret_key, secure=secure,
        http_client=urllib3.PoolManager(
            timeout=urllib3.Timeout(connect=timeout_s, read=timeout_s),
            retries=False,  # urllib3: no retries, raise on first failure
        ),
    )


class MinIOPayloadStore:
    """Stores/loads raw payloads in MinIO, addressed by ``s3://bucket/key``."""

    def __init__(self, client, *, create_bucket: bool = False) -> None:
        self._client = client
        self._create_bucket = create_bucket

    def get_payload(self, storage_uri: str) -> Any:
        bucket, key = _parse_s3_uri(storage_uri)
        compress = _compress_for(key)
        try:
            response = self._client.get_object(bucket, key)
        except KeyError as exc:
            raise KeyError(storage_uri) from exc
        except Exception as exc:
            if _is_not_found(exc):
                raise KeyError(storage_uri) from exc
            raise
        try:
            data = response.read()
        finally:
            _release(response)
        return _deserialize(data, compress=compress)

    def put(self, storage_uri: str, payload: Any) -> None:
        bucket, key = _parse_s3_uri(storage_uri)
        compress = _compress_for(key)
        body = _serialize(payload, compress=compress)
        self._ensure_bucket(bucket)
        self._client.put_object(
            bucket,
            key,
            BytesIO(body),
            len(body),
            content_type=_GZIP_CONTENT_TYPE if compress else _JSON_CONTENT_TYPE,
        )

    def _ensure_bucket(self, bucket: str) -> None:
        if self._client.bucket_exists(bucket):
            return
        if not self._create_bucket:
            raise ValueError(
                f"bucket {bucket!r} does not exist (pass create_bucket=True to create it)"
            )
        self._client.make_bucket(bucket)


def _parse_s3_uri(storage_uri: str) -> tuple[str, str]:
    parsed = urlparse(storage_uri)
    if parsed.scheme != "s3":
        raise ValueError(
            f"unsupported storage_uri scheme {parsed.scheme!r}; expected 's3'"
        )
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        raise ValueError(f"storage_uri {storage_uri!r} must be 's3://bucket/key'")
    return bucket, key


def _compress_for(key: str) -> bool:
    if key.endswith(".json.gz"):
        return True
    if key.endswith(".json"):
        return False
    raise ValueError(
        f"unsupported payload extension for {key!r}; expected '.json' or '.json.gz'"
    )


def _serialize(payload: Any, *, compress: bool) -> bytes:
    raw = json.dumps(payload, sort_keys=True, default=str).encode()
    return gzip.compress(raw) if compress else raw


def _deserialize(data: bytes, *, compress: bool) -> Any:
    if compress:
        try:
            data = gzip.decompress(data)
        except (OSError, EOFError) as exc:
            raise ValueError(f"invalid gzip payload: {exc}") from exc
    try:
        return json.loads(data)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON payload: {exc}") from exc


def _is_not_found(exc: Exception) -> bool:
    return getattr(exc, "code", None) in _NOT_FOUND_CODES


def _release(response: Any) -> None:
    # Real MinIO responses must be closed/released; fake clients may not have these.
    for name in ("close", "release_conn"):
        method = getattr(response, name, None)
        if callable(method):
            method()
