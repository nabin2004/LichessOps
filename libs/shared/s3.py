"""S3-compatible object storage helpers (MinIO in local dev)."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

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
    return boto3.client(**kwargs)


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
