"""Orchestrates preprocessing transforms and temporal train/test split."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from libs.shared import get_logger

from lichess_data.preprocessing.transforms import (
    encode_result,
    extract_date_features,
    extract_move_features,
    extract_opening_features,
    extract_time_features,
    impute_ratings,
    parse_event,
)

log = get_logger("lichess_data.preprocessing")

PIPELINE_STAGES = [
    parse_event,
    encode_result,
    extract_date_features,
    extract_time_features,
    impute_ratings,
    extract_opening_features,
    extract_move_features,
]


def temporal_split(
    df: pd.DataFrame, test_size: float = 0.2
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Sort by ``utc_datetime`` and split chronologically."""
    log.info("Stage 8 — temporal train/test split (test_size=%.2f)", test_size)

    df = df.sort_values("utc_datetime").reset_index(drop=True)
    split_idx = int(len(df) * (1 - test_size))
    train, test = df.iloc[:split_idx], df.iloc[split_idx:]

    log.info(
        "  train: %d rows (%s → %s)",
        len(train),
        train["utc_datetime"].min(),
        train["utc_datetime"].max(),
    )
    log.info(
        "  test:  %d rows (%s → %s)",
        len(test),
        test["utc_datetime"].min(),
        test["utc_datetime"].max(),
    )
    return train, test


def run_pipeline(
    path: str | Path,
    test_size: float = 0.2,
    save_dir: str | Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load raw parquet, run all preprocessing stages, return (train, test).

    Parameters
    ----------
    path:
        Path to the raw ``.parquet`` file.
    test_size:
        Fraction of data reserved for test (chronological tail).
    save_dir:
        If provided, write ``train.parquet`` and ``test.parquet`` here.

    Returns
    -------
    (df_train, df_test)
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

    df_train, df_test = temporal_split(df, test_size=test_size)

    if save_dir is not None:
        out = Path(save_dir)
        out.mkdir(parents=True, exist_ok=True)
        train_path = out / "train.parquet"
        test_path = out / "test.parquet"
        df_train.to_parquet(train_path, index=False)
        df_test.to_parquet(test_path, index=False)
        log.info("Saved → %s", train_path)
        log.info("Saved → %s", test_path)

    log.info("Pipeline complete. Train: %s | Test: %s", df_train.shape, df_test.shape)
    return df_train, df_test
