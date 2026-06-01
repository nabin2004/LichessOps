"""Upload local artifacts to MinIO."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from libs.shared import get_artifact_path, get_logger, is_minio_backend, load_config, upload_file
from libs.shared.s3 import raw_bucket_name, raw_object_key
from libs.shared.storage_config import raw_prefix

from lichess_data.extract import lichess_downloader as ld

_logger = get_logger(__name__)


def upload_raw_shard(
    month: str,
    *,
    local_path: Path | None = None,
    config: dict[str, Any] | None = None,
) -> str | None:
    """Upload a monthly raw ``.pgn.zst`` shard to MinIO.

    Returns the ``s3://`` URI, or ``None`` when storage backend is ``local``.
    """
    cfg = config or load_config("lichess_data")

    if not is_minio_backend(cfg):
        _logger.info("Storage backend is local; skipping upload for %s", month)
        return None

    if local_path is None:
        dl_cfg = cfg.get("download") or {}
        subpath = dl_cfg.get("output_subpath", "raw/pgn")
        base = get_artifact_path("lichess_data", subpath, create=False)
        local_path = base / ld.shard_filename(month)

    local_path = Path(local_path).resolve()
    if not local_path.is_file():
        raise FileNotFoundError(f"Raw shard not found: {local_path}")

    bucket = raw_bucket_name(cfg)
    key = raw_object_key(raw_prefix(cfg), local_path.name)
    uri = upload_file(local_path, bucket, key, skip_if_unchanged=True)
    _logger.info("Uploaded raw shard: %s", uri)
    return uri
