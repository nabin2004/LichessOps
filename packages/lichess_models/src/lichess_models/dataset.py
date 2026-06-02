"""Load split parquet and expand games to player-centric rows."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from lichess_libs.shared import get_artifact_path, get_logger, load_config

log = get_logger("lichess_models.dataset")

# Board result_label: 0=white win, 1=black win, 2=draw
# Player outcome: 0=lose, 1=win, 2=draw
WHITE_OUTCOME_MAP = {0: 1, 1: 0, 2: 2}
BLACK_OUTCOME_MAP = {0: 0, 1: 1, 2: 2}

OUTCOME_DISPLAY = {0: "0", 1: "1", 2: "½"}
OUTCOME_PROB_KEYS = {0: "lose", 1: "win", 2: "draw"}

METADATA_COLUMNS = ("utc_datetime", "site", "result_label")
LABEL_COLUMN = "player_outcome"


def split_paths(month: str, data_config: dict | None = None) -> tuple[Path, Path]:
    data_cfg = data_config or load_config("lichess_data")
    subpath = (data_cfg.get("preprocessing") or {}).get("output_subpath", "preprocessed")
    base = get_artifact_path("lichess_data", f"{subpath}/{month}", create=False)
    return base / "train.parquet", base / "test.parquet"


def load_split(month: str, *, split: str = "train") -> pd.DataFrame:
    train_path, test_path = split_paths(month)
    path = train_path if split == "train" else test_path
    if not path.is_file():
        raise FileNotFoundError(f"Split parquet not found: {path}")
    df = pd.read_parquet(path)
    log.info("Loaded %s split for %s: %d rows", split, month, len(df))
    return df


def _player_row(df: pd.DataFrame, *, color: str) -> pd.DataFrame:
    """Build player-centric rows for white or black."""
    out = pd.DataFrame(index=df.index)

    if color == "white":
        out["player_elo"] = df["white_elo"]
        out["opponent_elo"] = df["black_elo"]
        out["player_rating_diff"] = df["white_rating_diff"]
        out["opponent_rating_diff"] = df["black_rating_diff"]
        out["player_rating_bucket"] = df["white_rating_bucket"]
        out["opponent_rating_bucket"] = df["black_rating_bucket"]
        out["player_rating_bucket_id"] = df["white_rating_bucket_id"]
        out["opponent_rating_bucket_id"] = df["black_rating_bucket_id"]
        out["player_title_rank"] = df["white_title_rank"]
        out["opponent_title_rank"] = df["black_title_rank"]
        out["player_eco_prior_count"] = df["white_eco_prior_count"]
        out["player_eco_score"] = df["white_eco_score"]
        out["player_color_perf"] = df["white_color_perf"]
        out["player_h2h_win_rate"] = df["h2h_white_win_rate"]
        out["player_h2h_draw_rate"] = df["h2h_draw_rate"]
        out["player_h2h_loss_rate"] = df["h2h_black_win_rate"]
        out["expected_player"] = df["expected_white"]
        out["player_color"] = 0
        out[LABEL_COLUMN] = df["result_label"].map(WHITE_OUTCOME_MAP)
    else:
        out["player_elo"] = df["black_elo"]
        out["opponent_elo"] = df["white_elo"]
        out["player_rating_diff"] = df["black_rating_diff"]
        out["opponent_rating_diff"] = df["white_rating_diff"]
        out["player_rating_bucket"] = df["black_rating_bucket"]
        out["opponent_rating_bucket"] = df["white_rating_bucket"]
        out["player_rating_bucket_id"] = df["black_rating_bucket_id"]
        out["opponent_rating_bucket_id"] = df["white_rating_bucket_id"]
        out["player_title_rank"] = df["black_title_rank"]
        out["opponent_title_rank"] = df["white_title_rank"]
        out["player_eco_prior_count"] = df["black_eco_prior_count"]
        out["player_eco_score"] = df["black_eco_score"]
        out["player_color_perf"] = df["black_color_perf"]
        out["player_h2h_win_rate"] = df["h2h_black_win_rate"]
        out["player_h2h_draw_rate"] = df["h2h_draw_rate"]
        out["player_h2h_loss_rate"] = df["h2h_white_win_rate"]
        out["expected_player"] = 1.0 - df["expected_white"]
        out["player_color"] = 1
        out[LABEL_COLUMN] = df["result_label"].map(BLACK_OUTCOME_MAP)

    out["elo_diff"] = out["player_elo"] - out["opponent_elo"]
    out["elo_diff_abs"] = out["elo_diff"].abs()
    out["avg_elo"] = df["avg_elo"]
    out["rating_diff_net"] = out["player_rating_diff"] - out["opponent_rating_diff"]
    out["rating_bucket_diff"] = (
        out["player_rating_bucket_id"] - out["opponent_rating_bucket_id"]
    )
    out["title_diff"] = out["player_title_rank"] - out["opponent_title_rank"]

    shared = [
        "time_control",
        "time_control_raw",
        "base_seconds",
        "increment_seconds",
        "estimated_40move_time",
        "base_minutes",
        "base_x_increment",
        "is_blitz",
        "is_rapid",
        "is_classical",
        "tournament_type",
        "is_tournament",
        "eco",
        "eco_area",
        "opening_family",
        "opening_type",
        "is_gambit",
        "opening_frequency",
        "eco_prior_count",
        "h2h_total",
        "rating_bucket_pair",
        "day_of_week",
        "hour",
        "time_of_day",
        "session_bucket",
        "is_weekend",
        "is_night",
        "is_morning",
        "is_afternoon",
        "is_evening",
        "is_peak_gaming",
    ]
    for col in shared:
        out[col] = df[col]

    out["opening_population_win_rate"] = np.where(
        out["player_color"] == 0,
        df["opening_white_win_rate"],
        1.0 - df["opening_white_win_rate"],
    )

    for col in METADATA_COLUMNS:
        if col in df.columns:
            out[col] = df[col]

    return out


def to_player_perspective(df: pd.DataFrame) -> pd.DataFrame:
    """Expand one row per game into two player-centric rows."""
    if "result_label" not in df.columns:
        raise ValueError("Input dataframe must contain result_label")

    white_rows = _player_row(df, color="white")
    black_rows = _player_row(df, color="black")
    combined = pd.concat([white_rows, black_rows], ignore_index=True)

    if "utc_datetime" in combined.columns:
        combined = combined.sort_values(["utc_datetime", "player_color"]).reset_index(
            drop=True
        )

    log.info(
        "Expanded %d games → %d player rows (win=%d lose=%d draw=%d)",
        len(df),
        len(combined),
        (combined[LABEL_COLUMN] == 1).sum(),
        (combined[LABEL_COLUMN] == 0).sum(),
        (combined[LABEL_COLUMN] == 2).sum(),
    )
    return combined


def feature_columns(config: dict | None = None) -> tuple[list[str], list[str]]:
    cfg = config or load_config("lichess_models")
    feat_cfg = cfg.get("features") or {}
    numeric = list(feat_cfg.get("numeric") or [])
    categorical = list(feat_cfg.get("categorical") or [])
    return numeric, categorical


def split_features_labels(
    df: pd.DataFrame, config: dict | None = None
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    """Return X, y, and metadata (utc_datetime) for CV ordering."""
    cfg = config or load_config("lichess_models")
    label_col = (cfg.get("training") or {}).get("label_column", LABEL_COLUMN)
    numeric, categorical = feature_columns(cfg)
    feature_cols = numeric + categorical

    missing = [c for c in feature_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing feature columns: {missing}")

    X = df[feature_cols].copy()
    y = df[label_col].astype(int)
    meta = df[[c for c in METADATA_COLUMNS if c in df.columns]].copy()
    return X, y, meta
