"""Star-schema table schemas and record builders."""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any

WIDE_COLUMNS = [
    "event",
    "site",
    "date",
    "round",
    "white",
    "black",
    "white_title",
    "black_title",
    "result",
    "utc_date",
    "utc_time",
    "white_elo",
    "black_elo",
    "white_rating_diff",
    "black_rating_diff",
    "eco",
    "opening",
    "time_control",
    "termination",
    "moves",
]


def stable_id(*parts: str | None) -> str:
    payload = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def game_id_from_record(record: dict[str, Any]) -> str:
    site = record.get("site")
    if site:
        return stable_id("site", site)
    return stable_id(
        "game",
        record.get("white"),
        record.get("black"),
        record.get("utc_date"),
        record.get("utc_time"),
        record.get("result"),
    )


def player_id(username: str | None) -> str | None:
    if not username:
        return None
    return stable_id("player", username)


def opening_id(eco: str | None, opening_name: str | None) -> str | None:
    if not eco and not opening_name:
        return None
    return stable_id("opening", eco, opening_name)


def date_id_from_utc(utc_date: str | None) -> int | None:
    if not utc_date:
        return None
    try:
        dt = datetime.strptime(utc_date, "%Y.%m.%d")
    except ValueError:
        return None
    return int(dt.strftime("%Y%m%d"))


def calendar_date_from_utc(utc_date: str | None) -> str | None:
    if not utc_date:
        return None
    try:
        dt = datetime.strptime(utc_date, "%Y.%m.%d")
    except ValueError:
        return None
    return dt.date().isoformat()


def day_of_week(utc_date: str | None) -> int | None:
    if not utc_date:
        return None
    try:
        dt = datetime.strptime(utc_date, "%Y.%m.%d")
    except ValueError:
        return None
    return dt.weekday()


def utc_datetime(utc_date: str | None, utc_time: str | None) -> str | None:
    if not utc_date or not utc_time:
        return None
    return f"{utc_date.replace('.', '-')}T{utc_time}Z"


def build_star_records(wide: dict[str, Any], *, year: int, month: int) -> dict[str, list[dict[str, Any]]]:
    """Build fact/dimension rows from a wide game record."""
    white = wide.get("white")
    black = wide.get("black")
    eco = wide.get("eco")
    opening_name = wide.get("opening")
    utc_date = wide.get("utc_date")

    white_pid = player_id(white)
    black_pid = player_id(black)
    oid = opening_id(eco, opening_name)
    did = date_id_from_utc(utc_date)

    players: list[dict[str, Any]] = []
    if white_pid:
        players.append(
            {
                "player_id": white_pid,
                "username": white,
                "title": wide.get("white_title"),
                "last_known_elo": wide.get("white_elo"),
            }
        )
    if black_pid:
        players.append(
            {
                "player_id": black_pid,
                "username": black,
                "title": wide.get("black_title"),
                "last_known_elo": wide.get("black_elo"),
            }
        )

    openings: list[dict[str, Any]] = []
    if oid:
        openings.append(
            {
                "opening_id": oid,
                "eco": eco,
                "opening_name": opening_name,
            }
        )

    dates: list[dict[str, Any]] = []
    if did is not None:
        dates.append(
            {
                "date_id": did,
                "calendar_date": calendar_date_from_utc(utc_date),
                "year": year,
                "month": month,
                "day_of_week": day_of_week(utc_date),
            }
        )

    moves = wide.get("moves") or []
    fact = {
        "game_id": game_id_from_record(wide),
        "white_player_id": white_pid,
        "black_player_id": black_pid,
        "opening_id": oid,
        "date_id": did,
        "result": wide.get("result"),
        "white_elo": wide.get("white_elo"),
        "black_elo": wide.get("black_elo"),
        "white_rating_diff": wide.get("white_rating_diff"),
        "black_rating_diff": wide.get("black_rating_diff"),
        "time_control": wide.get("time_control"),
        "termination": wide.get("termination"),
        "event": wide.get("event"),
        "utc_datetime": utc_datetime(wide.get("utc_date"), wide.get("utc_time")),
        "move_count": len(moves),
        "year": year,
        "month": month,
    }

    return {
        "fact_games": [fact],
        "dim_player": players,
        "dim_opening": openings,
        "dim_date": dates,
        "wide_games": [wide],
    }
