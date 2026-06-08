"""S3-compatible object storage helpers (MinIO in local dev)."""

from __future__ import annotations

import hashlib
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, BinaryIO

from .logger import get_logger

_logger = get_logger(__name__)

DEFAULT_ENDPOINT = "http://localhost:9000"
DEFAULT_ACCESS_KEY = "minioadmin"
DEFAULT_SECRET_KEY = "minioadmin"
DEFAULT_RAW_BUCKET = "lichess-raw"
DEFAULT_PROCESSED_BUCKET = "lichess-processed"


def s3_endpoint() -> str:
    return os.getenv("AWS_ENDPOINT_URL", DEFAULT_ENDPOINT)


def s3_access_key() -> str:
    return os.getenv("AWS_ACCESS_KEY_ID", DEFAULT_ACCESS_KEY)


def s3_secret_key() -> str:
    return os.getenv("AWS_SECRET_ACCESS_KEY", DEFAULT_SECRET_KEY)


def raw_bucket_name(config: dict[str, Any] | None = None) -> str:
    if config:
        storage = config.get("storage") or {}
        if storage.get("raw_bucket"):
            return str(storage["raw_bucket"])
    return os.getenv("LICHESS_RAW_BUCKET", DEFAULT_RAW_BUCKET)


def processed_bucket_name(config: dict[str, Any] | None = None) -> str:
    if config:
        storage = config.get("storage") or {}
        if storage.get("processed_bucket"):
            return str(storage["processed_bucket"])
    return os.getenv("LICHESS_PROCESSED_BUCKET", DEFAULT_PROCESSED_BUCKET)


def s3_uri(bucket: str, key: str) -> str:
    key = key.lstrip("/")
    return f"s3://{bucket}/{key}"


def raw_object_key(prefix: str, filename: str) -> str:
    prefix = prefix.strip("/")
    return f"{prefix}/{filename}" if prefix else filename


def fact_games_prefix(year: int, month: int) -> str:
    return f"fact_games/year={year}/month={month:02d}"


def wide_games_prefix(year: int, month: int) -> str:
    return f"wide_games/year={year}/month={month:02d}"


@lru_cache(maxsize=1)
def s3_client():
    """Return a cached boto3 S3 client configured for MinIO."""
    import boto3

    kwargs: dict[str, Any] = {
        "service_name": "s3",
        "aws_access_key_id": s3_access_key(),
        "aws_secret_access_key": s3_secret_key(),
        "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    }
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client(**kwargs")


def object_exists(bucket: str, key: str) -> bool:
    client = s3_client()
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as exc:
        from botocore.exceptions import ClientError

        if isinstance(exc, ClientError):
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
        raise


class ChecksumMismatchError(Exception):
    """Computed SHA256 does not match the expected checksum."""

    def __init__(self, key: str, expected: str, actual: str) -> None:
        self.key = key
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"SHA256 mismatch for {key!r}: expected {expected}, got {actual}"
        )


class _HashingReader:
    """File-like wrapper that accumulates SHA256 while reading from *source*."""

    def __init__(self, source: BinaryIO) -> None:
        self._source = source
        self._hasher = hashlib.sha256()

    def read(self, amt: int = -1) -> bytes:
        data = self._source.read(amt)
        if data:
            self._hasher.update(data)
        return data

    def hexdigest(self) -> str:
        return self._hasher.hexdigest()


def object_sha256(bucket: str, key: str) -> str | None:
    """Return the ``sha256`` user metadata on an object, or ``None`` if absent."""
    if not object_exists(bucket, key):
        return None
    client = s3_client()
    resp = client.head_object(Bucket=bucket, Key=key)
    return resp.get("Metadata", {}).get("sha256")


def skip_if_verified(bucket: str, key: str, expected_sha256: str) -> str | None:
    """Return ``s3://`` URI when remote object SHA256 metadata matches *expected_sha256*."""
    actual = object_sha256(bucket, key)
    if actual and actual.lower() == expected_sha256.lower():
        uri = s3_uri(bucket, key)
        _logger.info("Skipping (checksum metadata match): %s", uri)
        return uri
    return None


def upload_stream(
    body: BinaryIO,
    bucket: str,
    key: str,
    *,
    expected_sha256: str | None = None,
    metadata: dict[str, str] | None = None,
    multipart_threshold: int = 8 * 1024 * 1024,
) -> str:
    """Upload from a readable stream via S3 multipart upload. Returns ``s3://`` URI."""
    from boto3.s3.transfer import TransferConfig

    client = s3_client()
    uri = s3_uri(bucket, key)
    extra_args: dict[str, Any] = {}
    if metadata:
        extra_args["Metadata"] = metadata

    reader: BinaryIO = body
    hasher: _HashingReader | None = None
    if expected_sha256 is not None:
        hasher = _HashingReader(body)
        reader = hasher  # type: ignore[assignment]

    _logger.info("Streaming upload -> %s", uri)
    try:
        client.upload_fileobj(
            reader,
            bucket,
            key,
            ExtraArgs=extra_args or None,
            Config=TransferConfig(multipart_threshold=multipart_threshold),
        )
    except Exception:
        _logger.exception("Stream upload failed for %s", uri)
        raise

    if expected_sha256 is not None and hasher is not None:
        actual = hasher.hexdigest()
        if actual.lower() != expected_sha256.lower():
            _logger.warning(
                "Checksum mismatch after upload; deleting %s", uri
            )
            client.delete_object(Bucket=bucket, Key=key)
            raise ChecksumMismatchError(key, expected_sha256, actual)

    return uri


def upload_file(
    local_path: str | Path,
    bucket: str,
    key: str,
    *,
    skip_if_unchanged: bool = True,
) -> str:
    """Upload a local file to S3. Returns the ``s3://`` URI."""
    local_path = Path(local_path).resolve()
    if not local_path.is_file():
        raise FileNotFoundError(f"Local file not found: {local_path}")

    client = s3_client()
    uri = s3_uri(bucket, key)

    if skip_if_unchanged and object_exists(bucket, key):
        remote = client.head_object(Bucket=bucket, Key=key)
        remote_size = remote.get("ContentLength")
        if remote_size == local_path.stat().st_size:
            _logger.info("Skipping upload (size match): %s", uri)
            return uri

    _logger.info("Uploading %s -> %s", local_path, uri)
    client.upload_file(str(local_path), bucket, key)
    return uri


def download_file(bucket: str, key: str, local_path: str | Path) -> Path:
    """Download an S3 object to a local path."""
    local_path = Path(local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    client = s3_client()
    uri = s3_uri(bucket, key)
    _logger.info("Downloading %s -> %s", uri, local_path)
    client.download_file(bucket, key, str(local_path))
    return local_path.resolve()


def storage_backend(config: dict[str, Any] | None = None) -> str:
    env = os.getenv("LICHESS_STORAGE_BACKEND")
    if env:
        return env.strip().lower()
    if config:
        storage = config.get("storage") or {}
        backend = storage.get("backend")
        if backend:
            return str(backend).strip().lower()
    return "local"


def is_minio_backend(config: dict[str, Any] | None = None) -> bool:
    return storage_backend(config) == "minio"


def list_s3_keys(bucket: str, prefix: str, *, suffix: str = "") -> list[str]:
    """List object keys under a bucket prefix."""
    client = s3_client()
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/")):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if suffix and not key.endswith(suffix):
                continue
            keys.append(key)
    return keys


def read_parquet_prefix(bucket: str, prefix: str) -> "pd.DataFrame":
    """Download and concatenate Parquet objects under an S3 prefix."""
    import tempfile

    import pandas as pd

    keys = list_s3_keys(bucket, prefix, suffix=".parquet")
    if not keys:
        raise FileNotFoundError(f"No parquet objects under s3://{bucket}/{prefix}")
    frames = []
    with tempfile.TemporaryDirectory(prefix="lichess_parquet_") as tmp_dir:
        tmp = Path(tmp_dir)
        for index, key in enumerate(keys):
            local = download_file(bucket, key, tmp / f"part-{index:05d}.parquet")
            frames.append(pd.read_parquet(local))
    return pd.concat(frames, ignore_index=True)
