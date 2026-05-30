"""Opening weakness analysis by player rating bucket."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline

from libs.shared import get_logger, load_config

from lichess_models.dataset import load_split, split_features_labels, to_player_perspective
from lichess_models.train import load_pipeline

log = get_logger("lichess_models.analyze")


def run_analyze(
    month: str,
    run_dir: Path,
    *,
    config: dict | None = None,
    split: str = "test",
) -> Path:
    cfg = config or load_config("lichess_models")
    analyze_cfg = cfg.get("analyze") or {}
    min_games = int(analyze_cfg.get("min_games", 20))
    group_cols = list(analyze_cfg.get("group_by") or ["player_rating_bucket", "eco"])

    pipeline: Pipeline = load_pipeline(run_dir)
    df = to_player_perspective(load_split(month, split=split))
    X, y, _meta = split_features_labels(df, cfg)

    df = df.copy()
    df["y_true"] = y.values
    y_proba = pipeline.predict_proba(X)
    df["predicted_win_rate"] = y_proba[:, 1]

    grouped = (
        df.groupby(group_cols, dropna=False)
        .agg(
            n_games=("player_outcome", "size"),
            empirical_win_rate=("player_outcome", lambda s: (s == 1).mean()),
            predicted_win_rate=("predicted_win_rate", "mean"),
        )
        .reset_index()
    )
    grouped = grouped[grouped["n_games"] >= min_games].copy()

    grouped["weakness_rank"] = grouped.groupby("player_rating_bucket")[
        "predicted_win_rate"
    ].rank(method="dense", ascending=True)

    grouped = grouped.sort_values(
        ["player_rating_bucket", "weakness_rank", "n_games"],
        ascending=[True, True, False],
    )

    out_path = run_dir / "opening_weakness.csv"
    grouped.to_csv(out_path, index=False)
    log.info("Opening weakness report → %s (%d rows)", out_path, len(grouped))
    return out_path
