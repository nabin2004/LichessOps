"""Orchestrate shard transform from local or MinIO input."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from lichess_libs.shared import get_artifact_path, get_logger, is_minio_backend, load_config
from lichess_libs.shared.s3 import download_file, object_exists, raw_bucket_name
from lichess_libs.shared.storage_config import raw_prefix, raw_s3_uri

from lichess_data.extract import lichess_downloader as ld
from lichess_data.spark.submit import submit_transform
from lichess_data.spark.transform import run_local_transform

_logger = get_logger(__name__)


def resolve_raw_input(month: str, *, config: dict[str, Any]) -> Path:
    """Return a local path to the raw shard, downloading from MinIO if needed."""
    dl_cfg = config.get("download") or {}
    subpath = dl_cfg.get("output_subpath", "raw/pgn")
    local_path = get_artifact_path("lichess_data", subpath, create=False)
    shard = local_path / ld.shard_filename(month)

    if shard.is_file():
        return shard

    if not is_minio_backend(config):
        raise FileNotFoundError(f"Raw shard not found locally: {shard}")

    filename = ld.shard_filename(month)
    bucket = raw_bucket_name(config)
    key = f"{raw_prefix(config).strip('/')}/{filename}"
    if not object_exists(bucket, key):
        raise FileNotFoundError(
            f"Raw shard not found locally ({shard}) or in {raw_s3_uri(config, filename)}"
        )

    tmp_dir = Path(tempfile.mkdtemp(prefix="lichess_raw_"))
    return download_file(bucket, key, tmp_dir / filename)


def run_transform(
    month: str,
    *,
    config: dict[str, Any] | None = None,
    local: bool = False,
    use_spark_cluster: bool = False,
) -> dict[str, str | Path]:
    """Run star-schema transform for a monthly shard."""
    cfg = config or load_config("lichess_data")
    input_path = resolve_raw_input(month, config=cfg)
    _logger.info("Transform input: %s", input_path)

    if use_spark_cluster and is_minio_backend(cfg) and not local:
        submit_transform(month, input_path=input_path, local=False)

    return run_local_transform(input_path, month, config=cfg, local_only=local)
