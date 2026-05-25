"""Unit tests for :mod:`lichess_data.extract.lichess_downloader`."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

import lichess_data.extract.lichess_downloader as ld

FIXTURE_INDEX = Path(__file__).parent / "fixtures" / "standard_games_index.html"


def test_parse_monthly_index_html_from_fixture():
    html_text = FIXTURE_INDEX.read_text(encoding="utf-8", errors="replace")
    shards = ld.parse_monthly_index_html(
        html_text,
        base_url="https://database.lichess.org",
        category="standard",
    )
    assert len(shards) >= 1
    assert shards[0].year_month == "2026-04"
    assert shards[0].filename == "lichess_db_standard_rated_2026-04.pgn.zst"
    assert shards[0].download_url.endswith(
        "standard/lichess_db_standard_rated_2026-04.pgn.zst"
    )


def test_parse_sha256sums_text():
    text = """
# comment
aa40b3671fa3cf1072eb182892cd90b0e1e003a4a5943492f64b77e7f3fd1635 lichess_db_standard_rated_2013-01.pgn.zst
"""
    m = ld.parse_sha256sums_text(text)
    assert (
        m["lichess_db_standard_rated_2013-01.pgn.zst"]
        == "aa40b3671fa3cf1072eb182892cd90b0e1e003a4a5943492f64b77e7f3fd1635"
    )


def test_shard_filename():
    assert (
        ld.shard_filename("2026-04")
        == "lichess_db_standard_rated_2026-04.pgn.zst"
    )


def test_shard_filename_invalid():
    with pytest.raises(ValueError):
        ld.shard_filename("13-01")
    with pytest.raises(ValueError):
        ld.shard_filename("2026-13")


def test_resolve_previous_month():
    assert ld.resolve_previous_month(date(2026, 5, 15)) == "2026-04"
    assert ld.resolve_previous_month(date(2026, 1, 1)) == "2025-12"


def test_skip_existing_when_checksum_matches(tmp_path, monkeypatch):
    fn = "lichess_db_standard_rated_2013-01.pgn.zst"
    body = b"hello-download-test"
    dest = tmp_path / fn
    dest.write_bytes(body)
    import hashlib

    digest = hashlib.sha256(body).hexdigest()
    monkeypatch.setattr(
        ld,
        "fetch_sha256_map",
        lambda *_args, **_kw: {fn: digest},
    )

    path = ld.download_month("2013-01", dest_dir=tmp_path, config={})
    assert path == dest
    assert path.read_bytes() == body


def test_checksum_mismatch_deletes_partial(tmp_path, monkeypatch):
    fn = "lichess_db_standard_rated_2013-01.pgn.zst"
    fake_digest = "a" * 64
    monkeypatch.setattr(
        ld,
        "fetch_sha256_map",
        lambda *_args, **_kw: {fn: fake_digest},
    )

    def fake_stream(_url, part_path, **kwargs):
        part_path.parent.mkdir(parents=True, exist_ok=True)
        part_path.write_bytes(b"wrong-bytes")

    monkeypatch.setattr(ld, "_stream_download_url", fake_stream)

    with pytest.raises(ld.ChecksumMismatchError):
        ld.download_month("2013-01", dest_dir=tmp_path, config={})

    assert not (tmp_path / f"{fn}.part").exists()
    assert not (tmp_path / fn).exists()
