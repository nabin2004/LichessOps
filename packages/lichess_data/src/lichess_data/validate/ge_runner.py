"""Great Expectations validation runner for Lichess datasets."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import great_expectations as gx
import pandas as pd

from lichess_libs.shared import get_logger, load_config

_logger = get_logger(__name__)

PROCESSED_REQUIRED_COLUMNS = [
    "result",
    "white",
    "black",
    "utc_date",
    "utc_time",
]

PREPROCESSED_REQUIRED_COLUMNS = [
    "result_label",
    "white_elo",
    "black_elo",
    "utc_datetime",
]


@dataclass(frozen=True)
class GEValidationResult:
    """Summary for a Great Expectations validation run."""

    ok: bool
    details: dict[str, Any]


def _validate_dataframe(
    df: pd.DataFrame,
    apply_expectations: Callable[[Any, pd.DataFrame], None],
) -> GEValidationResult:
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas("pandas")
    asset = datasource.add_dataframe_asset("validation_asset")
    batch_definition = asset.add_batch_definition_whole_dataframe("validation_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    validator = context.get_validator(batch=batch)
    apply_expectations(validator, df)
    result = validator.validate()
    return GEValidationResult(ok=result.success, details=result.to_json_dict())


def _apply_processed_expectations(validator, df: pd.DataFrame) -> None:
    validator.expect_table_row_count_to_be_between(min_value=1)
    validator.expect_table_columns_to_match_set(
        column_set=PROCESSED_REQUIRED_COLUMNS,
        exact_match=False,
    )
    for col in PROCESSED_REQUIRED_COLUMNS:
        if col in df.columns:
            validator.expect_column_values_to_not_be_null(col)


def _apply_preprocessed_expectations(validator, df: pd.DataFrame) -> None:
    validator.expect_table_row_count_to_be_between(min_value=1)
    validator.expect_table_columns_to_match_set(
        column_set=PREPROCESSED_REQUIRED_COLUMNS,
        exact_match=False,
    )
    for col in PREPROCESSED_REQUIRED_COLUMNS:
        if col in df.columns:
            validator.expect_column_values_to_not_be_null(col)


def validate_ge_processed_parquet(
    path: str | Path,
    *,
    config: dict[str, Any] | None = None,
) -> GEValidationResult:
    """Validate a processed parquet file using Great Expectations.

    Accepts both legacy extract output and ``wide_games`` exports from DuckDB sync.
    """
    del config
    parquet_path = Path(path).expanduser().resolve()
    if not parquet_path.exists():
        msg = f"Processed parquet not found: {parquet_path}"
        _logger.warning(msg)
        return GEValidationResult(ok=False, details={"error": msg})

    df = pd.read_parquet(parquet_path)
    return _validate_dataframe(df, _apply_processed_expectations)


def validate_ge_features_parquet(
    path: str | Path,
    *,
    config: dict[str, Any] | None = None,
) -> GEValidationResult:
    """Validate full pre-split features parquet using Great Expectations."""
    del config
    parquet_path = Path(path).expanduser().resolve()
    if not parquet_path.exists():
        msg = f"Features parquet not found: {parquet_path}"
        _logger.warning(msg)
        return GEValidationResult(ok=False, details={"error": msg})

    df = pd.read_parquet(parquet_path)
    return _validate_dataframe(df, _apply_preprocessed_expectations)


def validate_ge_preprocessed_dir(
    dir_path: str | Path,
    *,
    config: dict[str, Any] | None = None,
) -> GEValidationResult:
    """Validate train/test parquet outputs in a preprocessed directory."""
    del config
    base = Path(dir_path).expanduser().resolve()
    train_path = base / "train.parquet"
    test_path = base / "test.parquet"
    if not train_path.exists() or not test_path.exists():
        msg = f"Missing train/test parquet in {base}"
        _logger.warning(msg)
        return GEValidationResult(ok=False, details={"error": msg})

    df_train = pd.read_parquet(train_path)
    df_test = pd.read_parquet(test_path)
    df = pd.concat([df_train, df_test], ignore_index=True)
    return _validate_dataframe(df, _apply_preprocessed_expectations)
