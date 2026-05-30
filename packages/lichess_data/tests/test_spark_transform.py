"""Tests for star-schema transform (local mode)."""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import zstandard as zstd

from lichess_data.spark.transform import run_local_transform


def _write_mini_shard(path: Path, games: list[str]) -> None:
    pgn_text = "\n\n".join(games) + "\n"
    cctx = zstd.ZstdCompressor()
    path.write_bytes(cctx.compress(pgn_text.encode("utf-8")))


MINI_GAME = """[Event "Rated Blitz"]
[Site "https://lichess.org/abc123"]
[Date "2024.01.01"]
[White "Alice"]
[Black "Bob"]
[Result "1-0"]
[UTCDate "2024.01.01"]
[UTCTime "12:00:00"]
[WhiteElo "1500"]
[BlackElo "1400"]
[ECO "C00"]
[Opening "French Defense"]
[TimeControl "180+2"]

1. e4 e6 2. d4 d5 1-0"""


def test_local_transform_writes_star_schema(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("ARTIFACT_DIR", str(tmp_path / "artifacts"))
    shard = tmp_path / "mini.pgn.zst"
    _write_mini_shard(shard, [MINI_GAME])

    cfg = {"storage": {"backend": "local"}, "extract": {"output_subpath": "processed"}}
    outputs = run_local_transform(shard, "2024-01", config=cfg, local_only=True)

    fact_path = Path(outputs["fact_games"])
    wide_path = Path(outputs["wide_games"])
    assert fact_path.is_file()
    assert wide_path.is_file()

    fact = pq.read_table(fact_path)
    assert fact.num_rows == 1
    assert "game_id" in fact.column_names

    wide = pq.read_table(wide_path)
    assert wide.num_rows == 1
    assert "white" in wide.column_names


def test_build_star_records():
    from lichess_data.spark.schema import build_star_records

    wide = {
        "white": "Alice",
        "black": "Bob",
        "eco": "C00",
        "opening": "French Defense",
        "utc_date": "2024.01.01",
        "utc_time": "12:00:00",
        "result": "1-0",
        "moves": ["e2e4"],
    }
    tables = build_star_records(wide, year=2024, month=1)
    assert len(tables["fact_games"]) == 1
    assert len(tables["dim_player"]) == 2
    assert tables["fact_games"][0]["move_count"] == 1
