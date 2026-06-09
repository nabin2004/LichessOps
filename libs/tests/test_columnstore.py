"""Tests for ColumnStore insert helpers."""

from __future__ import annotations

import math
from unittest.mock import MagicMock

import pandas as pd


def test_insert_dataframe_converts_nan_to_none():
    from lichess_libs.shared.columnstore import _insert_dataframe

    df = pd.DataFrame(
        {
            "game_id": ["abc123"],
            "white_elo": [1500.0],
            "white_rating_diff": [float("nan")],
        }
    )
    cursor = MagicMock()

    count = _insert_dataframe(cursor, "fact_games", df)

    assert count == 1
    cursor.executemany.assert_called_once()
    _sql, rows = cursor.executemany.call_args[0]
    assert len(rows) == 1
    game_id, white_elo, white_rating_diff = rows[0]
    assert game_id == "abc123"
    assert white_elo == 1500.0
    assert white_rating_diff is None
    assert not (isinstance(white_rating_diff, float) and math.isnan(white_rating_diff))


def test_wide_export_sql_quotes_reserved_aliases():
    from lichess_libs.shared.columnstore import WIDE_EXPORT_SQL

    assert "AS `date`" in WIDE_EXPORT_SQL
    assert "AS `round`" in WIDE_EXPORT_SQL
    assert "AS `result`" in WIDE_EXPORT_SQL
    assert "DATE_FORMAT(dd.calendar_date, '%Y.%m.%d') AS utc_date" in WIDE_EXPORT_SQL
