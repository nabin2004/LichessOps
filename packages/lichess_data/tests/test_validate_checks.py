"""Tests for checksum validation in lichess_data.validate."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from lichess_data.validate import validate_checksum


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def sample_shard(tmp_path: Path) -> Path:
    path = tmp_path / "lichess_db_standard_rated_2013-01.pgn.zst"
    path.write_bytes(b"hello-lichess")
    return path


def test_validate_checksum_ok(sample_shard: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    expected = _digest(sample_shard.read_bytes())
    monkeypatch.setattr(
        "lichess_data.extract.lichess_downloader.fetch_sha256_map",
        lambda *_args, **_kw: {sample_shard.name: expected},
    )

    result = validate_checksum(sample_shard, config={"download": {"chunk_size_bytes": 4}})

    assert result.ok is True
    assert result.expected == expected
    assert result.actual == expected
    assert result.reason is None


def test_validate_checksum_mismatch(sample_shard: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "lichess_data.extract.lichess_downloader.fetch_sha256_map",
        lambda *_args, **_kw: {sample_shard.name: "0" * 64},
    )

    result = validate_checksum(sample_shard, config={"download": {"chunk_size_bytes": 8}})

    assert result.ok is False
    assert result.reason == "checksum-mismatch"
    assert result.expected == "0" * 64
    assert result.actual is not None


def test_validate_checksum_missing_entry(sample_shard: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "lichess_data.extract.lichess_downloader.fetch_sha256_map",
        lambda *_args, **_kw: {},
    )

    result = validate_checksum(sample_shard)

    assert result.ok is False
    assert result.reason == "missing-checksum"
    assert result.expected is None
    assert result.actual is None


def test_validate_checksum_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "lichess_db_standard_rated_2013-02.pgn.zst"

    result = validate_checksum(missing)

    assert result.ok is False
    assert result.reason == "missing-file"
    assert result.expected is None
    assert result.actual is None
