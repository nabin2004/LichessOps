"""Tests for direct-to-MinIO download."""

from __future__ import annotations

import hashlib
import io
from unittest.mock import patch

import pytest

import lichess_data.extract.lichess_downloader as ld

MINIO_CFG = {
    "storage": {
        "backend": "minio",
        "raw_bucket": "lichess-raw",
        "raw_prefix": "pgn",
    }
}


def test_download_month_to_minio_streams_and_returns_uri(monkeypatch):
    fn = "lichess_db_standard_rated_2013-01.pgn.zst"
    body = b"direct-minio-shard"
    digest = hashlib.sha256(body).hexdigest()

    monkeypatch.setattr(
        ld,
        "fetch_sha256_map",
        lambda *_args, **_kw: {fn: digest},
    )
    monkeypatch.setattr(
        ld,
        "_iter_download_chunks",
        lambda *_args, **_kw: iter([body]),
    )
    monkeypatch.setattr(ld, "skip_if_verified", lambda *_args, **_kw: None)

    with patch("lichess_data.extract.lichess_downloader.upload_stream") as mock_upload:
        mock_upload.return_value = f"s3://lichess-raw/pgn/{fn}"
        uri = ld.download_month_to_minio("2013-01", config=MINIO_CFG, progress=False)

    assert uri == f"s3://lichess-raw/pgn/{fn}"
    mock_upload.assert_called_once()
    call_kwargs = mock_upload.call_args.kwargs
    assert call_kwargs["expected_sha256"] == digest
    assert call_kwargs["metadata"] == {"sha256": digest}


def test_download_month_to_minio_skips_when_verified(monkeypatch):
    fn = "lichess_db_standard_rated_2013-01.pgn.zst"
    digest = "a" * 64

    monkeypatch.setattr(
        ld,
        "fetch_sha256_map",
        lambda *_args, **_kw: {fn: digest},
    )
    monkeypatch.setattr(
        ld,
        "skip_if_verified",
        lambda *_args, **_kw: f"s3://lichess-raw/pgn/{fn}",
    )

    with patch("lichess_data.extract.lichess_downloader.upload_stream") as mock_upload:
        uri = ld.download_month_to_minio("2013-01", config=MINIO_CFG, progress=False)

    assert uri == f"s3://lichess-raw/pgn/{fn}"
    mock_upload.assert_not_called()


def test_download_month_to_minio_checksum_failure(monkeypatch):
    fn = "lichess_db_standard_rated_2013-01.pgn.zst"
    digest = "b" * 64

    monkeypatch.setattr(
        ld,
        "fetch_sha256_map",
        lambda *_args, **_kw: {fn: digest},
    )
    monkeypatch.setattr(
        ld,
        "_iter_download_chunks",
        lambda *_args, **_kw: iter([b"wrong"]),
    )
    monkeypatch.setattr(ld, "skip_if_verified", lambda *_args, **_kw: None)

    from lichess_libs.shared.s3 import ChecksumMismatchError

    with patch(
        "lichess_data.extract.lichess_downloader.upload_stream",
        side_effect=ChecksumMismatchError("pgn/x", digest, "c" * 64),
    ):
        with pytest.raises(ChecksumMismatchError):
            ld.download_month_to_minio("2013-01", config=MINIO_CFG, progress=False)


def test_chunk_iter_reader_adapts_iterator():
    reader = ld._ChunkIterReader(iter([b"abc", b"def"]))
    assert reader.read(2) == b"ab"
    assert reader.read(4) == b"cdef"
    assert reader.read() == b""
