"""Tests for ColumnStore sync and wide export."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import zstandard as zstd

from lichess_data.load.columnstore_sync import load_month_tables, sync_month
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


def test_load_month_tables_from_local_star_schema(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("ARTIFACT_DIR", str(tmp_path / "artifacts"))
    shard = tmp_path / "mini.pgn.zst"
    _write_mini_shard(shard, [MINI_GAME])

    cfg = {
        "storage": {"backend": "local"},
        "extract": {"output_subpath": "processed"},
    }
    run_local_transform(shard, "2024-01", config=cfg, local_only=True)

    tables = load_month_tables(cfg, 2024, 1)
    assert len(tables["fact_games"]) == 1
    assert "white" not in tables["fact_games"].columns
    assert len(tables["wide_games"]) == 1
    assert tables["wide_games"].iloc[0]["white"] == "Alice"


def test_columnstore_sync_exports_wide_parquet(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("ARTIFACT_DIR", str(tmp_path / "artifacts"))
    shard = tmp_path / "mini.pgn.zst"
    _write_mini_shard(shard, [MINI_GAME])

    cfg = {
        "storage": {"backend": "local"},
        "extract": {"output_subpath": "processed"},
    }
    run_local_transform(shard, "2024-01", config=cfg, local_only=True)

    wide_path = tmp_path / "artifacts" / "lichess_data" / "processed" / "2024-01.parquet"

    def _fake_export(month, out_path, **kwargs):
        tables = load_month_tables(cfg, 2024, 1)
        tables["wide_games"].to_parquet(out_path, index=False)
        return out_path

    with (
        patch("lichess_data.load.columnstore_sync.ensure_schema"),
        patch("lichess_data.load.columnstore_sync.bulk_upsert_month", return_value=1),
        patch("lichess_data.load.columnstore_sync.bulk_replace_dimension", return_value=1),
        patch("lichess_data.load.columnstore_sync.export_wide_parquet", side_effect=_fake_export),
    ):
        out = sync_month("2024-01", config=cfg)

    assert out == wide_path
    assert out.is_file()
    table = pq.read_table(out)
    assert table.num_rows == 1
    assert "white" in table.column_names
    assert "result" in table.column_names
