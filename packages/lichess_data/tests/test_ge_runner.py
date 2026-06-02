"""Tests for Great Expectations validation in lichess_data.validate.ge_runner."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from lichess_data.validate.ge_runner import (
    validate_ge_preprocessed_dir,
    validate_ge_processed_parquet,
)


def _processed_row() -> dict[str, str]:
    return {
        "result": "1-0",
        "white": "player_a",
        "black": "player_b",
        "utc_date": "2013-03-01",
        "utc_time": "12:00:00",
    }


def _preprocessed_row() -> dict[str, object]:
    return {
        "result_label": 1,
        "white_elo": 1500,
        "black_elo": 1600,
        "utc_datetime": "2013-03-01T12:00:00",
    }


def test_validate_ge_processed_parquet_ok(tmp_path: Path) -> None:
    row = _processed_row()
    row["extra_col"] = "ok"
    path = tmp_path / "2013-03.parquet"
    pd.DataFrame([row]).to_parquet(path)

    result = validate_ge_processed_parquet(path)

    assert result.ok is True


def test_validate_ge_processed_parquet_missing_column(tmp_path: Path) -> None:
    row = _processed_row()
    del row["utc_time"]
    path = tmp_path / "2013-03.parquet"
    pd.DataFrame([row]).to_parquet(path)

    result = validate_ge_processed_parquet(path)

    assert result.ok is False


def test_validate_ge_preprocessed_dir_ok(tmp_path: Path) -> None:
    month_dir = tmp_path / "2013-03"
    month_dir.mkdir()
    row = _preprocessed_row()
    row["extra_col"] = "ok"
    df = pd.DataFrame([row])
    df.to_parquet(month_dir / "train.parquet")
    df.to_parquet(month_dir / "test.parquet")

    result = validate_ge_preprocessed_dir(month_dir)

    assert result.ok is True


def test_validate_ge_preprocessed_dir_missing_column(tmp_path: Path) -> None:
    month_dir = tmp_path / "2013-03"
    month_dir.mkdir()
    row = _preprocessed_row()
    del row["utc_datetime"]
    df = pd.DataFrame([row])
    df.to_parquet(month_dir / "train.parquet")
    df.to_parquet(month_dir / "test.parquet")

    result = validate_ge_preprocessed_dir(month_dir)

    assert result.ok is False
