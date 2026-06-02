"""Parse a monthly PGN shard into wide and star-schema records."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import chess.pgn
# from compression import zstd
import zstandard as zstd

from lichess_data.extract.pgn_parser import PGNParser
from lichess_data.spark.schema import build_star_records


def iter_shard_records(
    input_path: str | Path,
    *,
    year: int,
    month: int,
) -> Iterator[dict[str, list[dict[str, Any]]]]:
    """Yield star-schema record batches for each game in a shard."""
    parser = PGNParser()
    with zstd.open(str(input_path), "rt", encoding="utf-8") as handle:
        while True:
            game = chess.pgn.read_game(handle)
            if game is None:
                break
            wide = parser.parse(game)
            yield build_star_records(wide, year=year, month=month)


def collect_shard_tables(
    input_path: str | Path,
    *,
    year: int,
    month: int,
) -> dict[str, list[dict[str, Any]]]:
    """Collect all table rows from a shard into lists keyed by table name."""
    tables: dict[str, list[dict[str, Any]]] = {
        "fact_games": [],
        "dim_player": [],
        "dim_opening": [],
        "dim_date": [],
        "wide_games": [],
    }
    seen_players: set[str] = set()
    seen_openings: set[str] = set()
    seen_dates: set[int] = set()

    for batch in iter_shard_records(input_path, year=year, month=month):
        tables["fact_games"].extend(batch["fact_games"])
        tables["wide_games"].extend(batch["wide_games"])

        for row in batch["dim_player"]:
            pid = row["player_id"]
            if pid not in seen_players:
                seen_players.add(pid)
                tables["dim_player"].append(row)
            else:
                for existing in tables["dim_player"]:
                    if existing["player_id"] == pid:
                        if row.get("last_known_elo") is not None:
                            existing["last_known_elo"] = row["last_known_elo"]
                        if row.get("title"):
                            existing["title"] = row["title"]
                        break

        for row in batch["dim_opening"]:
            oid = row["opening_id"]
            if oid not in seen_openings:
                seen_openings.add(oid)
                tables["dim_opening"].append(row)

        for row in batch["dim_date"]:
            did = row["date_id"]
            if did not in seen_dates:
                seen_dates.add(did)
                tables["dim_date"].append(row)

    return tables
