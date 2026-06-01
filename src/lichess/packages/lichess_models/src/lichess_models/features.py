"""Sklearn preprocessing pipeline for player-centric features."""

from __future__ import annotations

from typing import Any

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from libs.shared import load_config

from lichess_models.dataset import feature_columns


def build_preprocessor(config: dict | None = None) -> ColumnTransformer:
    numeric, categorical = feature_columns(config)

    numeric_pipe = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    categorical_pipe = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="most_frequent")),
            (
                "onehot",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
            ),
        ]
    )

    return ColumnTransformer(
        transformers=[
            ("num", numeric_pipe, numeric),
            ("cat", categorical_pipe, categorical),
        ],
        remainder="drop",
    )


def build_inference_row(request: dict[str, Any], config: dict | None = None) -> pd.DataFrame:
    """Build a single-row dataframe from an inference request dict."""
    cfg = config or load_config("lichess_models")
    numeric, categorical = feature_columns(cfg)
    all_cols = numeric + categorical

    player_color = 0 if request.get("player_color", "white") == "white" else 1
    player_elo = float(request["player_elo"])
    opponent_elo = float(request["opponent_elo"])
    elo_diff = player_elo - opponent_elo

    defaults: dict[str, Any] = {
        "player_elo": player_elo,
        "opponent_elo": opponent_elo,
        "player_rating_diff": float(request.get("player_rating_diff", 0)),
        "opponent_rating_diff": float(request.get("opponent_rating_diff", 0)),
        "elo_diff": elo_diff,
        "elo_diff_abs": abs(elo_diff),
        "avg_elo": (player_elo + opponent_elo) / 2.0,
        "rating_diff_net": float(request.get("rating_diff_net", 0)),
        "expected_player": 1.0 / (1.0 + 10.0 ** (-elo_diff / 400.0)),
        "player_rating_bucket_id": int(request.get("player_rating_bucket_id", 0)),
        "opponent_rating_bucket_id": int(request.get("opponent_rating_bucket_id", 0)),
        "rating_bucket_diff": int(request.get("rating_bucket_diff", 0)),
        "player_title_rank": int(request.get("player_title_rank", 0)),
        "opponent_title_rank": int(request.get("opponent_title_rank", 0)),
        "title_diff": int(request.get("title_diff", 0)),
        "base_seconds": float(request.get("base_seconds", 180)),
        "increment_seconds": float(request.get("increment_seconds", 0)),
        "estimated_40move_time": float(request.get("estimated_40move_time", 180)),
        "base_minutes": float(request.get("base_minutes", 3)),
        "base_x_increment": float(request.get("base_x_increment", 180)),
        "is_blitz": int(request.get("is_blitz", 1)),
        "is_rapid": int(request.get("is_rapid", 0)),
        "is_classical": int(request.get("is_classical", 0)),
        "is_tournament": int(request.get("is_tournament", 0)),
        "is_gambit": int(request.get("is_gambit", 0)),
        "opening_frequency": float(request.get("opening_frequency", 0.01)),
        "opening_population_win_rate": float(
            request.get("opening_population_win_rate", 0.5)
        ),
        "eco_prior_count": int(request.get("eco_prior_count", 0)),
        "player_eco_prior_count": int(request.get("player_eco_prior_count", 0)),
        "player_eco_score": float(request.get("player_eco_score", 0.5)),
        "h2h_total": int(request.get("h2h_total", 0)),
        "player_h2h_win_rate": float(request.get("player_h2h_win_rate", 0.5)),
        "player_h2h_draw_rate": float(request.get("player_h2h_draw_rate", 0.1)),
        "player_h2h_loss_rate": float(request.get("player_h2h_loss_rate", 0.4)),
        "player_color_perf": float(request.get("player_color_perf", 0.5)),
        "player_color": player_color,
        "day_of_week": int(request.get("day_of_week", 3)),
        "hour": int(request.get("hour", 12)),
        "is_weekend": int(request.get("is_weekend", 0)),
        "is_night": int(request.get("is_night", 0)),
        "is_morning": int(request.get("is_morning", 0)),
        "is_afternoon": int(request.get("is_afternoon", 1)),
        "is_evening": int(request.get("is_evening", 0)),
        "is_peak_gaming": int(request.get("is_peak_gaming", 0)),
        "time_control": request.get("time_control", "Blitz"),
        "time_control_raw": request.get("time_control_raw", "180+0"),
        "tournament_type": request.get("tournament_type", "Game"),
        "eco": request["eco"],
        "eco_area": request.get("eco_area", str(request["eco"])[0]),
        "opening_family": request.get("opening_family") or "Unknown",
        "opening_type": request.get("opening_type", "Semi-Open"),
        "player_rating_bucket": request.get("player_rating_bucket", "<2000"),
        "rating_bucket_pair": request.get("rating_bucket_pair", "<2000|<2000"),
        "time_of_day": request.get("time_of_day", "Afternoon"),
        "session_bucket": request.get("session_bucket", "Afternoon"),
    }

    for key, value in request.items():
        if key in all_cols:
            defaults[key] = value

    row = {col: defaults.get(col) for col in all_cols}
    return pd.DataFrame([row])
