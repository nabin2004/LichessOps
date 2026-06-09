"""Sync MinIO/local Parquet into MariaDB ColumnStore and export wide games for ML."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from lichess_libs.shared import get_artifact_path, get_logger, is_minio_backend, load_config
from lichess_libs.shared.columnstore import (
    bulk_replace_dimension,
    bulk_upsert_month,
    ensure_schema,
    export_wide_parquet,
)
from lichess_libs.shared.s3 import (
    download_file,
    fact_games_prefix,
    list_s3_keys,
    processed_bucket_name,
    read_parquet_prefix,
)
from lichess_libs.shared.storage_config import parse_month

_logger = get_logger(__name__)

DIMENSION_TABLES = ("dim_player", "dim_opening", "dim_date")

def _read_local_parquet(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Parquet not found: {path}")
    return pd.read_parquet(path)


def _load_month_from_s3(config: dict[str, Any], year: int, month: int) -> dict[str, pd.DataFrame]:
    bucket = processed_bucket_name(config)
    fact_prefix = fact_games_prefix(year, month)
    wide_prefix = f"wide_games/year={year}/month={month:02d}"

    tables: dict[str, pd.DataFrame] = {
        "fact_games": read_parquet_prefix(bucket, fact_prefix),
        "wide_games": read_parquet_prefix(bucket, wide_prefix),
    }
    for dim in DIMENSION_TABLES:
        keys = list_s3_keys(bucket, dim, suffix=".parquet")
        if not keys:
            tables[dim] = pd.DataFrame()
            continue
        import tempfile

        with tempfile.TemporaryDirectory(prefix="lichess_dim_") as tmp_dir:
            tmp = Path(tmp_dir)
            frames = [
                pd.read_parquet(download_file(bucket, key, tmp / f"{dim}-{index}.parquet"))
                for index, key in enumerate(keys)
            ]
        tables[dim] = pd.concat(frames, ignore_index=True)
    return tables


def _load_month_from_local(year: int, month: int) -> dict[str, pd.DataFrame]:
    base = get_artifact_path("lichess_data", "processed/star_schema", create=False)
    fact_path = base / f"fact_games_year={year}_month={month:02d}_part-00000.parquet"
    wide_path = base / f"wide_games_year={year}_month={month:02d}_part-00000.parquet"

    tables: dict[str, pd.DataFrame] = {
        "fact_games": _read_local_parquet(fact_path),
    }
    if wide_path.is_file():
        tables["wide_games"] = _read_local_parquet(wide_path)
    else:
        tables["wide_games"] = pd.DataFrame()

    for dim in DIMENSION_TABLES:
        dim_path = base / f"{dim}.parquet"
        tables[dim] = _read_local_parquet(dim_path) if dim_path.is_file() else pd.DataFrame()
    return tables


def _ensure_partition_columns(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    frame = df.copy()
    if "year" not in frame.columns:
        frame["year"] = year
    if "month" not in frame.columns:
        frame["month"] = month
    return frame


def load_month_tables(
    config: dict[str, Any],
    year: int,
    month: int,
) -> dict[str, pd.DataFrame]:
    if is_minio_backend(config):
        return _load_month_from_s3(config, year, month)
    return _load_month_from_local(year, month)


def sync_tables_to_columnstore(
    tables: dict[str, pd.DataFrame],
    year: int,
    month: int,
    *,
    config: dict[str, Any] | None = None,
) -> None:
    ensure_schema(config)

    fact = _ensure_partition_columns(tables["fact_games"], year, month)
    count = bulk_upsert_month("fact_games", fact, year, month, config=config)
    _logger.info("Loaded %s fact_games rows for %04d-%02d", count, year, month)

    for dim in DIMENSION_TABLES:
        dim_df = tables.get(dim, pd.DataFrame())
        if dim_df.empty:
            continue
        dim_count = bulk_replace_dimension(dim, dim_df, config=config)
        _logger.info("Refreshed %s with %s rows", dim, dim_count)


def sync_month(month: str, *, config: dict[str, Any] | None = None) -> Path:
    """Load processed Parquet into ColumnStore and export ML-ready wide parquet."""
    cfg = config or load_config("lichess_data")
    year, mon = parse_month(month)
    tables = load_month_tables(cfg, year, mon)
    sync_tables_to_columnstore(tables, year, mon, config=cfg)

    extract_cfg = cfg.get("extract") or {}
    subpath = extract_cfg.get("output_subpath", "processed")
    out_dir = get_artifact_path("lichess_data", subpath, create=True)
    out_path = out_dir / f"{month}.parquet"

    wide = tables.get("wide_games", pd.DataFrame())
    if not wide.empty:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        wide.to_parquet(out_path, index=False)
        _logger.info("Exported wide_games parquet to %s", out_path)
        return out_path

    return export_wide_parquet(month, out_path, config=cfg, prefer_wide_table=False)
