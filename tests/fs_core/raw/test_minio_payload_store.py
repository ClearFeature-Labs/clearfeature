import os
import sys

import pytest

from fintech_feature_platform.fs_core.raw.minio_payload_store import MinIOPayloadStore


class _FakeResponse:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self.closed = False
        self.released = False

    def read(self) -> bytes:
        return self._data

    def close(self) -> None:
        self.closed = True

    def release_conn(self) -> None:
        self.released = True


class _FakeS3Error(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class _FakeMinioClient:
    def __init__(self, buckets=()) -> None:
        self._objects: dict[str, dict[str, bytes]] = {b: {} for b in buckets}

    def bucket_exists(self, bucket: str) -> bool:
        return bucket in self._objects

    def make_bucket(self, bucket: str) -> None:
        self._objects.setdefault(bucket, {})

    def put_object(self, bucket, key, data, length, content_type=None) -> None:
        if bucket not in self._objects:
            raise _FakeS3Error("NoSuchBucket")
        self._objects[bucket][key] = data.read()

    def get_object(self, bucket, key):
        if bucket not in self._objects or key not in self._objects[bucket]:
            raise _FakeS3Error("NoSuchKey")
        return _FakeResponse(self._objects[bucket][key])


def _store(buckets=("raw-reports",), **kwargs) -> MinIOPayloadStore:
    return MinIOPayloadStore(_FakeMinioClient(buckets), **kwargs)


def test_module_imports_without_minio_package():
    # Importing the store (above) must not pull in the optional `minio` SDK.
    assert "minio" not in sys.modules


def test_json_round_trip():
    store = _store()
    payload = {"declared_income": 3_500_000, "monthly_obligations": 700_000}
    store.put("s3://raw-reports/credit/2026/06/21/rep_abc.json", payload)
    assert store.get_payload("s3://raw-reports/credit/2026/06/21/rep_abc.json") == payload


def test_gzip_round_trip():
    store = _store()
    payload = {"income": 3_000_000}
    store.put("s3://raw-reports/tax/rep_tax.json.gz", payload)
    assert store.get_payload("s3://raw-reports/tax/rep_tax.json.gz") == payload


def test_gzip_payload_is_actually_compressed():
    client = _FakeMinioClient(buckets=("raw-reports",))
    store = MinIOPayloadStore(client)
    store.put("s3://raw-reports/tax/rep_tax.json.gz", {"income": 3_000_000})
    stored = client._objects["raw-reports"]["tax/rep_tax.json.gz"]
    assert stored[:2] == b"\x1f\x8b"  # gzip magic bytes


def test_parses_bucket_and_key():
    client = _FakeMinioClient(buckets=("raw-reports",))
    store = MinIOPayloadStore(client)
    store.put("s3://raw-reports/path/to/object.json.gz", {"x": 1})
    assert "path/to/object.json.gz" in client._objects["raw-reports"]


def test_unsupported_scheme_raises_value_error():
    with pytest.raises(ValueError):
        _store().get_payload("mem://raw-reports/rep.json")


def test_missing_bucket_or_key_in_uri_raises_value_error():
    store = _store()
    with pytest.raises(ValueError):
        store.get_payload("s3://raw-reports")  # no key
    with pytest.raises(ValueError):
        store.get_payload("s3:///rep.json")  # no bucket


def test_unsupported_extension_raises_value_error():
    with pytest.raises(ValueError):
        _store().get_payload("s3://raw-reports/rep.txt")


def test_missing_object_raises_key_error():
    with pytest.raises(KeyError):
        _store().get_payload("s3://raw-reports/never-written.json")


def test_invalid_json_raises_value_error():
    client = _FakeMinioClient(buckets=("raw-reports",))
    client._objects["raw-reports"]["bad.json"] = b"not json{"
    store = MinIOPayloadStore(client)
    with pytest.raises(ValueError):
        store.get_payload("s3://raw-reports/bad.json")


def test_invalid_gzip_raises_value_error():
    client = _FakeMinioClient(buckets=("raw-reports",))
    client._objects["raw-reports"]["bad.json.gz"] = b"not gzip data"
    store = MinIOPayloadStore(client)
    with pytest.raises(ValueError):
        store.get_payload("s3://raw-reports/bad.json.gz")


def test_put_missing_bucket_without_create_raises_value_error():
    store = _store(buckets=())  # no buckets
    with pytest.raises(ValueError):
        store.put("s3://raw-reports/rep.json", {"x": 1})


def test_put_missing_bucket_with_create_succeeds():
    store = _store(buckets=(), create_bucket=True)
    store.put("s3://raw-reports/rep.json", {"x": 1})
    assert store.get_payload("s3://raw-reports/rep.json") == {"x": 1}


@pytest.mark.skipif(
    os.getenv("FSP_MINIO_INTEGRATION") != "1",
    reason="set FSP_MINIO_INTEGRATION=1 (and run local MinIO) to enable",
)
def test_live_minio_round_trip():
    pytest.importorskip("minio")
    from fintech_feature_platform.fs_core.raw.minio_payload_store import connect_minio

    client = connect_minio("localhost:9000", "minioadmin", "minioadmin", secure=False)
    store = MinIOPayloadStore(client, create_bucket=True)
    uri = "s3://fsp-it/test/rep_it.json.gz"
    store.put(uri, {"income": 1})
    assert store.get_payload(uri) == {"income": 1}
