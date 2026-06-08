"""Tests for S3 streaming upload helpers."""

from __future__ import annotations

import hashlib
import io
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def moto_s3(monkeypatch):
    pytest.importorskip("moto")
    import boto3
    from moto import mock_aws

    with mock_aws():
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
        from lichess_libs.shared import s3 as s3mod

        s3mod.s3_client.cache_clear()
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="lichess-raw")
        yield client
        s3mod.s3_client.cache_clear()


def test_upload_stream_happy_path(moto_s3):
    from lichess_libs.shared.s3 import object_sha256, upload_stream

    body = b"streamed-shard-data"
    expected = hashlib.sha256(body).hexdigest()
    reader = io.BytesIO(body)

    uri = upload_stream(
        reader,
        "lichess-raw",
        "pgn/test.pgn.zst",
        expected_sha256=expected,
        metadata={"sha256": expected},
    )
    assert uri == "s3://lichess-raw/pgn/test.pgn.zst"
    assert object_sha256("lichess-raw", "pgn/test.pgn.zst") == expected


def test_upload_stream_checksum_mismatch_deletes_object(moto_s3):
    from lichess_libs.shared.s3 import ChecksumMismatchError, object_exists, upload_stream

    body = b"bad-data"
    reader = io.BytesIO(body)

    with pytest.raises(ChecksumMismatchError):
        upload_stream(
            reader,
            "lichess-raw",
            "pgn/bad.pgn.zst",
            expected_sha256="a" * 64,
        )

    assert not object_exists("lichess-raw", "pgn/bad.pgn.zst")


def test_skip_if_verified_with_metadata(moto_s3):
    from lichess_libs.shared.s3 import skip_if_verified, upload_stream

    body = b"verified"
    expected = hashlib.sha256(body).hexdigest()
    upload_stream(
        io.BytesIO(body),
        "lichess-raw",
        "pgn/verified.pgn.zst",
        expected_sha256=expected,
        metadata={"sha256": expected},
    )

    uri = skip_if_verified("lichess-raw", "pgn/verified.pgn.zst", expected)
    assert uri == "s3://lichess-raw/pgn/verified.pgn.zst"


def test_skip_if_verified_without_metadata(moto_s3):
    from lichess_libs.shared.s3 import skip_if_verified, upload_file
    from pathlib import Path

    tmp = Path("/tmp")  # moto only; use BytesIO path via upload_file
    import tempfile

    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(b"legacy")
        path = Path(f.name)

    upload_file(path, "lichess-raw", "pgn/legacy.pgn.zst", skip_if_unchanged=False)
    path.unlink(missing_ok=True)

    assert skip_if_verified("lichess-raw", "pgn/legacy.pgn.zst", "b" * 64) is None


def test_upload_stream_failure_does_not_leave_partial(moto_s3):
    from lichess_libs.shared.s3 import object_exists

    reader = io.BytesIO(b"partial")
    with patch("lichess_libs.shared.s3.s3_client") as mock_client_factory:
        client = MagicMock()
        client.upload_fileobj.side_effect = RuntimeError("network blip")
        mock_client_factory.return_value = client

        from lichess_libs.shared.s3 import upload_stream

        with pytest.raises(RuntimeError):
            upload_stream(reader, "lichess-raw", "pgn/fail.pgn.zst")

    assert not object_exists("lichess-raw", "pgn/fail.pgn.zst")
