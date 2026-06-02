"""Tests for shared storage helpers."""

from __future__ import annotations

from pathlib import Path

import pytest


def test_s3_uri():
    from lichess_libs.shared.s3 import s3_uri

    assert s3_uri("lichess-raw", "pgn/file.pgn.zst") == "s3://lichess-raw/pgn/file.pgn.zst"


def test_raw_object_key():
    from lichess_libs.shared.s3 import raw_object_key

    assert raw_object_key("pgn", "shard.pgn.zst") == "pgn/shard.pgn.zst"


def test_fact_games_prefix():
    from lichess_libs.shared.s3 import fact_games_prefix

    assert fact_games_prefix(2024, 6) == "fact_games/year=2024/month=06"


@pytest.fixture
def moto_s3(monkeypatch):
    moto = pytest.importorskip("moto")
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


def test_upload_and_object_exists(tmp_path: Path, moto_s3, monkeypatch):
    from lichess_libs.shared.s3 import object_exists, upload_file

    local = tmp_path / "test.bin"
    local.write_bytes(b"hello")
    uri = upload_file(local, "lichess-raw", "pgn/test.bin", skip_if_unchanged=False)
    assert uri == "s3://lichess-raw/pgn/test.bin"
    assert object_exists("lichess-raw", "pgn/test.bin")

    uri2 = upload_file(local, "lichess-raw", "pgn/test.bin", skip_if_unchanged=True)
    assert uri2 == uri
