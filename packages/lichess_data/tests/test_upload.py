"""Tests for MinIO upload helpers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest


def test_upload_skips_local_backend():
    from lichess_data.load.upload import upload_raw_shard

    cfg = {"storage": {"backend": "local"}}
    assert upload_raw_shard("2024-01", config=cfg) is None


def test_upload_raises_when_shard_missing(tmp_path: Path):
    from lichess_data.load.upload import upload_raw_shard

    cfg = {"storage": {"backend": "minio", "raw_bucket": "lichess-raw", "raw_prefix": "pgn"}}
    with pytest.raises(FileNotFoundError):
        upload_raw_shard("2024-01", local_path=tmp_path / "missing.pgn.zst", config=cfg)


def test_upload_calls_s3(tmp_path: Path):
    from lichess_data.load.upload import upload_raw_shard

    shard = tmp_path / "lichess_db_standard_rated_2024-01.pgn.zst"
    shard.write_bytes(b"zst")
    cfg = {"storage": {"backend": "minio", "raw_bucket": "lichess-raw", "raw_prefix": "pgn"}}

    with patch("lichess_data.load.upload.upload_file", return_value="s3://lichess-raw/pgn/x") as mock:
        uri = upload_raw_shard("2024-01", local_path=shard, config=cfg)
        assert uri == "s3://lichess-raw/pgn/x"
        mock.assert_called_once()
