"""Tests for DuckDB sync and wide export."""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import zstandard as zstd

from lichess_data.load.duckdb_sync import sync_month
from lichess_data.spark.transform import run_local_transform

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


def _write_mini_shard(path: Path, games: list[str]) -> None:
    pgn_text = "\n\n".join(games) + "\n"
    cctx = zstd.ZstdCompressor()
    path.write_bytes(cctx.compress(pgn_text.encode("utf-8")))


def test_duckdb_sync_exports_wide_parquet(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("ARTIFACT_DIR", str(tmp_path / "artifacts"))
    shard = tmp_path / "mini.pgn.zst"
    _write_mini_shard(shard, [MINI_GAME])

    cfg = {
        "storage": {"backend": "local", "duckdb_path": "duckdb/lichess.duckdb"},
        "extract": {"output_subpath": "processed"},
    }
    run_local_transform(shard, "2024-01", config=cfg, local_only=True)

    out = sync_month("2024-01", config=cfg)
    assert out.is_file()
    table = pq.read_table(out)
    assert table.num_rows == 1
    assert "white" in table.column_names
    assert "result" in table.column_names
