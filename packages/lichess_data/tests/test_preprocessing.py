"""Tests for the preprocessing pipeline."""

from __future__ import annotations

import pandas as pd
import pytest

from lichess_data.preprocessing import (
    parse_event,
    run_pipeline,
)


@pytest.fixture
def sample_games() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "event": "Rated Blitz game",
                "site": "https://lichess.org/game1",
                "result": "1-0",
                "utc_date": "2013.01.01",
                "utc_time": "08:30:00",
                "white": "player1",
                "black": "player2",
                "white_title": "FM",
                "black_title": "IM",
                "white_elo": 1500,
                "black_elo": None,
                "white_rating_diff": 10,
                "black_rating_diff": None,
                "eco": "B20",
                "opening": "Sicilian Defense: Smith-Morra Gambit",
                "time_control": "180+0",
                "moves": ["e2e4", "c7c5"],
            },
            {
                "event": "Rated Bullet tournament https://lichess.org/tournament/abc",
                "site": "https://lichess.org/game2",
                "result": "1/2-1/2",
                "utc_date": "2013.01.02",
                "utc_time": "20:15:00",
                "white": "player3",
                "black": "player4",
                "white_title": None,
                "black_title": "CM",
                "white_elo": 1200,
                "black_elo": 1250,
                "white_rating_diff": 0,
                "black_rating_diff": 0,
                "eco": "A00",
                "opening": "Van't Kruijs Opening",
                "time_control": "60+0",
                "moves": ["e2e3", "e7e5", "d2d4"],
            },
            {
                "event": "Rated Blitz game",
                "site": "https://lichess.org/game3",
                "result": "0-1",
                "utc_date": "2013.01.03",
                "utc_time": "17:00:00",
                "white": "player1",
                "black": "player2",
                "white_title": "FM",
                "black_title": "IM",
                "white_elo": 1400,
                "black_elo": 1450,
                "white_rating_diff": -5,
                "black_rating_diff": 5,
                "eco": "C00",
                "opening": "French Defense: Knight Variation",
                "time_control": "180+2",
                "moves": ["e2e4", "e7e6"],
            },
        ]
    )


def test_parse_event(sample_games: pd.DataFrame) -> None:
    df = parse_event(sample_games.copy())
    assert df.loc[0, "time_control"] == "Blitz"
    assert df.loc[1, "time_control"] == "Bullet"
    assert df.loc[1, "is_tournament"] == 1
    assert "lichess.org/tournament/abc" in df.loc[1, "tournament_url"]
    assert df.loc[1, "tournament_type"] == "Arena"
    assert df.loc[0, "tournament_type"] == "Game"
    assert df.loc[0, "time_control_raw"] == "180+0"


def test_run_pipeline(tmp_path, sample_games: pd.DataFrame) -> None:
    raw = tmp_path / "raw.parquet"
    out = tmp_path / "processed"
    sample_games.to_parquet(raw, index=False)

    features = run_pipeline(raw, save_dir=out)

    assert len(features) == 3
    assert "result_label" in features.columns
    assert "elo_diff" in features.columns
    assert "move_count" in features.columns
    assert "tc_seconds" in features.columns
    assert "expected_white" in features.columns
    assert "base_seconds" in features.columns
    assert "increment_seconds" in features.columns
    assert "title_diff" in features.columns
    assert "opening_frequency" in features.columns
    assert "white_eco_score" in features.columns
    assert "h2h_total" in features.columns
    assert (out / "features.parquet").exists()
