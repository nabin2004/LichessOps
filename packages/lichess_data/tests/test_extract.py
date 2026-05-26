"""Tests for PGN extraction into Parquet."""

from __future__ import annotations

import io
from pathlib import Path

import chess.pgn
import pyarrow.parquet as pq
import pytest
from compression import zstd

from lichess_data.cli import run_extraction
from lichess_data.extract.parquet_stream_writer import ParquetStreamWriter
from lichess_data.extract.pgn_parser import PGNParser

SAMPLE_PGN = """\
[Event "Rated Blitz game"]
[Site "https://lichess.org/abc123"]
[Date "2013.01.01"]
[White "player1"]
[Black "player2"]
[Result "1-0"]
[UTCDate "2013.01.01"]
[UTCTime "08:30:00"]
[WhiteElo "1500"]
[BlackElo "1450"]
[WhiteRatingDiff "10"]
[BlackRatingDiff "-10"]
[ECO "B20"]
[Opening "Sicilian Defense"]
[TimeControl "180+0"]
[Termination "Normal"]

1. e4 c5 2. Nf3 d6 1-0

[Event "Rated Bullet game"]
[Site "https://lichess.org/def456"]
[Date "2013.01.02"]
[White "player3"]
[Black "player4"]
[Result "1/2-1/2"]
[UTCDate "2013.01.02"]
[UTCTime "20:15:00"]
[WhiteElo "?"]
[BlackElo "1250"]
[ECO "A00"]
[Opening "Van't Kruijs Opening"]
[TimeControl "60+0"]
[Termination "Normal"]

1. e3 e5 2. d4 exd4 1/2-1/2
"""


@pytest.fixture
def sample_pgn_zst(tmp_path: Path) -> Path:
    zst_path = tmp_path / "lichess_db_standard_rated_2013-01.pgn.zst"
    with zstd.open(zst_path, "wt", encoding="utf-8") as f:
        f.write(SAMPLE_PGN)
    return zst_path


def test_pgn_parser_parses_headers_and_moves() -> None:
    game = chess.pgn.read_game(io.StringIO(SAMPLE_PGN))
    assert game is not None

    record = PGNParser().parse(game)

    assert record["event"] == "Rated Blitz game"
    assert record["white"] == "player1"
    assert record["black"] == "player2"
    assert record["white_elo"] == 1500
    assert record["moves"] == ["e2e4", "c7c5", "g1f3", "d7d6"]


def test_pgn_parser_safe_int_handles_missing_elo() -> None:
    games: list[chess.pgn.Game] = []
    stream = io.StringIO(SAMPLE_PGN)
    while True:
        game = chess.pgn.read_game(stream)
        if game is None:
            break
        games.append(game)

    record = PGNParser().parse(games[1])
    assert record["white_elo"] is None
    assert record["black_elo"] == 1250


def test_parquet_stream_writer_batches_records(tmp_path: Path) -> None:
    output = tmp_path / "games.parquet"
    records = [
        {"event": "game-a", "moves": ["e2e4"]},
        {"event": "game-b", "moves": ["d2d4", "d7d5"]},
        {"event": "game-c", "moves": ["g1f3"]},
    ]

    with ParquetStreamWriter(str(output), batch_size=2) as writer:
        for record in records:
            writer.add(record)

    assert writer.total == 3
    table = pq.read_table(output)
    assert table.num_rows == 3
    assert table.column("event").to_pylist() == ["game-a", "game-b", "game-c"]


def test_run_extraction_writes_parquet(tmp_path: Path, sample_pgn_zst: Path) -> None:
    output = tmp_path / "2013-01.parquet"

    run_extraction(str(sample_pgn_zst), str(output), batch_size=1)

    table = pq.read_table(output)
    assert table.num_rows == 2
    assert table.column("result").to_pylist() == ["1-0", "1/2-1/2"]
