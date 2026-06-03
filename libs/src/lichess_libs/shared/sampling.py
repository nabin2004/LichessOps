"""Shared helpers for OOM-safe game-level row caps and temporal splits."""

from __future__ import annotations

import pandas as pd

from lichess_libs.shared import get_logger

log = get_logger("lichess_libs.sampling")


def limit_games(
    df: pd.DataFrame,
    *,
    use_sample: bool,
    max_rows: int | None,
) -> pd.DataFrame:
    """Keep the first ``max_rows`` games by ``utc_datetime`` when sampling is enabled."""
    if not use_sample:
        return df
    if max_rows is None or max_rows <= 0:
        raise ValueError("max_rows must be a positive integer when use_sample is true")

    before = len(df)
    if "utc_datetime" not in df.columns:
        raise ValueError("Cannot sample games without utc_datetime column")

    limited = df.sort_values("utc_datetime").reset_index(drop=True).iloc[:max_rows]
    log.info(
        "Sampled games: %d → %d rows (max_rows=%d)",
        before,
        len(limited),
        max_rows,
    )
    return limited


def temporal_split(
    df: pd.DataFrame, test_size: float = 0.2
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Sort by ``utc_datetime`` and split chronologically."""
    log.info("Temporal train/test split (test_size=%.2f)", test_size)

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
