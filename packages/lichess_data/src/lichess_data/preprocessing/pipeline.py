"""Orchestrates preprocessing transforms into model-ready features."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from libs.shared import get_logger

from lichess_data.preprocessing.transforms import (
    add_historical_pregame_features,
    encode_result,
    extract_date_features,
    extract_move_features,
    extract_opening_features,
    extract_time_features,
    extract_time_control_features,
    extract_title_features,
    impute_ratings,
    parse_event,
)

log = get_logger("lichess_data.preprocessing")

PIPELINE_STAGES = [
    parse_event,
    encode_result,
    extract_date_features,
    extract_time_features,
    extract_time_control_features,
    impute_ratings,
    extract_title_features,
    extract_opening_features,
    add_historical_pregame_features,
    extract_move_features,
]


def run_pipeline(
    path: str | Path,
    save_dir: str | Path | None = None,
) -> pd.DataFrame:
    """
    Load raw parquet, run all preprocessing stages, return feature matrix.

    Parameters
    ----------
    path:
        Path to the raw ``.parquet`` file.
    save_dir:
        If provided, write ``features.parquet`` here.

    Returns
    -------
    Feature dataframe (full history; split happens in ``lichess_features``).
    """
    path = Path(path)
    log.info("═" * 60)
    log.info("Loading data from %s", path)
    df = pd.read_parquet(path)
    log.info("Loaded %d rows × %d columns", *df.shape)
    log.info("═" * 60)

    for stage_fn in PIPELINE_STAGES:
        df = stage_fn(df)
        log.info("  → shape after %-30s: %s", stage_fn.__name__, df.shape)
        log.info("─" * 60)

    if save_dir is not None:
        out = Path(save_dir)
        out.mkdir(parents=True, exist_ok=True)
        features_path = out / "features.parquet"
        df.to_parquet(features_path, index=False)
        log.info("Saved → %s", features_path)

    log.info("Pipeline complete. Features: %s", df.shape)
    return df
