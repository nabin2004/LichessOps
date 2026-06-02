"""Tests for player-centric dataset expansion."""

from __future__ import annotations

import pandas as pd
import pytest

from lichess_models.dataset import (
    BLACK_OUTCOME_MAP,
    WHITE_OUTCOME_MAP,
    to_player_perspective,
)

from .conftest import board_row


@pytest.mark.parametrize(
    ("result_label", "white_outcome", "black_outcome"),
    [
        (0, WHITE_OUTCOME_MAP[0], BLACK_OUTCOME_MAP[0]),
        (1, WHITE_OUTCOME_MAP[1], BLACK_OUTCOME_MAP[1]),
        (2, WHITE_OUTCOME_MAP[2], BLACK_OUTCOME_MAP[2]),
    ],
)
def test_player_outcome_mapping(result_label, white_outcome, black_outcome) -> None:
    df = pd.DataFrame([board_row(result_label)])
    expanded = to_player_perspective(df)
    assert len(expanded) == 2

    white_row = expanded[expanded["player_color"] == 0].iloc[0]
    black_row = expanded[expanded["player_color"] == 1].iloc[0]

    assert white_row["player_outcome"] == white_outcome
    assert black_row["player_outcome"] == black_outcome
    assert white_row["player_elo"] == 1800.0
    assert black_row["player_elo"] == 1700.0
    assert white_row["expected_player"] == pytest.approx(0.6)
    assert black_row["expected_player"] == pytest.approx(0.4)


def test_three_games_expand_to_six_rows() -> None:
    df = pd.DataFrame([board_row(0), board_row(1), board_row(2)])
    expanded = to_player_perspective(df)
    assert len(expanded) == 6
