"""Put the repo root on ``sys.path`` so local imports work in tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def board_row(result_label: int) -> dict:
    return {
        "result_label": result_label,
        "white_elo": 1800.0,
        "black_elo": 1700.0,
        "white_rating_diff": 5.0,
        "black_rating_diff": -5.0,
        "white_rating_bucket": "2000-2200",
        "black_rating_bucket": "<2000",
        "white_rating_bucket_id": 1,
        "black_rating_bucket_id": 0,
        "white_title_rank": 0,
        "black_title_rank": 0,
        "white_eco_prior_count": 10,
        "black_eco_prior_count": 8,
        "white_eco_score": 0.55,
        "black_eco_score": 0.45,
        "white_color_perf": 0.52,
        "black_color_perf": 0.48,
        "expected_white": 0.6,
        "avg_elo": 1750.0,
        "time_control": "Blitz",
        "time_control_raw": "180+0",
        "base_seconds": 180.0,
        "increment_seconds": 0.0,
        "estimated_40move_time": 180.0,
        "base_minutes": 3.0,
        "base_x_increment": 180.0,
        "is_blitz": 1,
        "is_rapid": 0,
        "is_classical": 0,
        "tournament_type": "Game",
        "is_tournament": 0,
        "eco": "B20",
        "eco_area": "B",
        "opening_family": "Sicilian Defense",
        "opening_type": "Semi-Open",
        "is_gambit": 0,
        "opening_frequency": 0.05,
        "opening_white_win_rate": 0.52,
        "eco_prior_count": 100,
        "h2h_total": 3,
        "h2h_white_win_rate": 0.33,
        "h2h_draw_rate": 0.33,
        "h2h_black_win_rate": 0.34,
        "rating_bucket_pair": "2000-2200|<2000",
        "day_of_week": 2,
        "hour": 14,
        "time_of_day": "Afternoon",
        "session_bucket": "Afternoon",
        "is_weekend": 0,
        "is_night": 0,
        "is_morning": 0,
        "is_afternoon": 1,
        "is_evening": 0,
        "is_peak_gaming": 0,
        "utc_datetime": pd.Timestamp("2013-01-02 14:00:00"),
        "site": "https://lichess.org/abc123",
    }
