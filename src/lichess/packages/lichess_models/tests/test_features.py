"""Tests for sklearn preprocessing pipeline."""

from __future__ import annotations

import pandas as pd

from lichess_models.dataset import feature_columns, split_features_labels, to_player_perspective
from lichess_models.features import build_inference_row, build_preprocessor
from conftest import board_row


def test_preprocessor_fit_transform() -> None:
    df = pd.DataFrame([board_row(0), board_row(1), board_row(2)])
    player_df = to_player_perspective(df)
    X, y, _ = split_features_labels(player_df)

    preprocessor = build_preprocessor()
    transformed = preprocessor.fit_transform(X)

    assert transformed.shape[0] == len(X)
    assert transformed.shape[1] > 0
    assert len(y) == 6


def test_inference_row_has_all_feature_columns() -> None:
    row = build_inference_row(
        {
            "player_elo": 1800,
            "opponent_elo": 1700,
            "player_color": "white",
            "eco": "B20",
            "opening_family": "Sicilian Defense",
            "time_control": "Blitz",
        }
    )
    numeric, categorical = feature_columns()
    for col in numeric + categorical:
        assert col in row.columns
