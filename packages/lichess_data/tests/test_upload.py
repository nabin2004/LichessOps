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
    with patch("lichess_data.load.upload.ld.fetch_sha256_map", return_value={}):
        with pytest.raises(FileNotFoundError):
            upload_raw_shard("2024-01", local_path=tmp_path / "missing.pgn.zst", config=cfg)


def test_upload_calls_s3(tmp_path: Path):
    from lichess_data.load.upload import upload_raw_shard

    shard = tmp_path / "lichess_db_standard_rated_2024-01.pgn.zst"
    shard.write_bytes(b"zst")
    cfg = {"storage": {"backend": "minio", "raw_bucket": "lichess-raw", "raw_prefix": "pgn"}}

    with patch("lichess_data.load.upload.ld.fetch_sha256_map", return_value={}), patch(
        "lichess_data.load.upload.upload_file", return_value="s3://lichess-raw/pgn/x"
    ) as mock:
        uri = upload_raw_shard("2024-01", local_path=shard, config=cfg)
        assert uri == "s3://lichess-raw/pgn/x"
        mock.assert_called_once()


def test_upload_skips_when_sha256_metadata_matches(tmp_path: Path):
    from lichess_data.load.upload import upload_raw_shard

    fn = "lichess_db_standard_rated_2024-01.pgn.zst"
    digest = "a" * 64
    cfg = {"storage": {"backend": "minio", "raw_bucket": "lichess-raw", "raw_prefix": "pgn"}}

    with patch(
        "lichess_data.load.upload.ld.fetch_sha256_map",
        return_value={fn: digest},
    ), patch(
        "lichess_data.load.upload.skip_if_verified",
        return_value="s3://lichess-raw/pgn/x",
    ) as mock_skip, patch("lichess_data.load.upload.upload_file") as mock_upload:
        uri = upload_raw_shard("2024-01", config=cfg)
        assert uri == "s3://lichess-raw/pgn/x"
        mock_skip.assert_called_once()
        mock_upload.assert_not_called()
