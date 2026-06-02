"""Sync MinIO Parquet into DuckDB and export wide games for ML."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb

from lichess_libs.shared import get_artifact_path, get_logger, is_minio_backend, load_config
from lichess_libs.shared.s3 import processed_bucket_name, s3_endpoint
from lichess_libs.shared.storage_config import (
    duckdb_path,
    fact_games_s3_glob,
    parse_month,
    wide_games_s3_glob,
)

_logger = get_logger(__name__)

WIDE_EXPORT_SQL = """
SELECT
    fg.event,
    site.game_id AS site,
    dd.calendar_date AS date,
    NULL AS round,
    wp.username AS white,
    bp.username AS black,
    wp.title AS white_title,
    bp.title AS black_title,
    fg.result,
    dd.calendar_date AS utc_date,
    substr(fg.utc_datetime, 12, 8) AS utc_time,
    fg.white_elo,
    fg.black_elo,
    fg.white_rating_diff,
    fg.black_rating_diff,
    op.eco,
    op.opening_name AS opening,
    fg.time_control,
    fg.termination,
    CAST([] AS VARCHAR[]) AS moves
FROM fact_games fg
LEFT JOIN dim_player wp ON fg.white_player_id = wp.player_id
LEFT JOIN dim_player bp ON fg.black_player_id = bp.player_id
LEFT JOIN dim_opening op ON fg.opening_id = op.opening_id
LEFT JOIN dim_date dd ON fg.date_id = dd.date_id
LEFT JOIN (
    SELECT game_id FROM fact_games
) site ON fg.game_id = site.game_id
WHERE fg.year = ? AND fg.month = ?
"""


def _configure_httpfs(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("INSTALL httpfs; LOAD httpfs;")
    endpoint = s3_endpoint().replace("http://", "").replace("https://", "")
    use_ssl = s3_endpoint().startswith("https://")
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'};")
    con.execute(f"SET s3_access_key_id='{os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin')}';")
    con.execute(
        f"SET s3_secret_access_key='{os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')}';"
    )
    con.execute("SET s3_url_style='path';")


def _connect(config: dict[str, Any]) -> duckdb.DuckDBPyConnection:
    db_path = duckdb_path(config)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def _ensure_table_like(con: duckdb.DuckDBPyConnection, name: str, staging: str) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {name} AS
        SELECT * FROM {staging} WHERE 1=0
        """
    )


def _register_month_from_s3(
    con: duckdb.DuckDBPyConnection,
    config: dict[str, Any],
    year: int,
    month: int,
) -> None:
    _configure_httpfs(con)
    fact_glob = fact_games_s3_glob(config, year, month)
    wide_glob = wide_games_s3_glob(config, year, month)
    bucket = processed_bucket_name(config)

    con.execute(
        f"""
        CREATE OR REPLACE TABLE fact_games_staging AS
        SELECT * FROM read_parquet('{fact_glob}', hive_partitioning=true)
        """
    )
    _ensure_table_like(con, "fact_games", "fact_games_staging")
    con.execute(f"DELETE FROM fact_games WHERE year = {year} AND month = {month}")
    con.execute("INSERT INTO fact_games SELECT * FROM fact_games_staging")

    for dim in ("dim_player", "dim_opening", "dim_date"):
        glob = f"s3://{bucket}/{dim}/*.parquet"
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {dim}_staging AS
            SELECT * FROM read_parquet('{glob}')
            """
        )
        _ensure_table_like(con, dim, f"{dim}_staging")
        con.execute(f"DELETE FROM {dim}")
        con.execute(f"INSERT INTO {dim} SELECT * FROM {dim}_staging")

    con.execute(
        f"""
        CREATE OR REPLACE TABLE wide_games_staging AS
        SELECT * FROM read_parquet('{wide_glob}', hive_partitioning=true)
        """
    )
    _logger.info("Registered month %04d-%02d from %s", year, month, fact_glob)


def _register_month_from_local(
    con: duckdb.DuckDBPyConnection,
    year: int,
    month: int,
) -> None:
    base = get_artifact_path("lichess_data", "processed/star_schema", create=False)
    fact_path = base / f"fact_games_year={year}_month={month:02d}_part-00000.parquet"
    wide_path = base / f"wide_games_year={year}_month={month:02d}_part-00000.parquet"

    if not fact_path.is_file():
        raise FileNotFoundError(f"Local fact parquet not found: {fact_path}")

    con.execute(
        f"""
        CREATE OR REPLACE TABLE fact_games_staging AS
        SELECT * FROM read_parquet('{fact_path.as_posix()}')
        """
    )
    _ensure_table_like(con, "fact_games", "fact_games_staging")
    con.execute(f"DELETE FROM fact_games WHERE year = {year} AND month = {month}")
    con.execute("INSERT INTO fact_games SELECT * FROM fact_games_staging")

    for dim in ("dim_player", "dim_opening", "dim_date"):
        dim_path = base / f"{dim}.parquet"
        if dim_path.is_file():
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {dim}_staging AS
                SELECT * FROM read_parquet('{dim_path.as_posix()}')
                """
            )
            _ensure_table_like(con, dim, f"{dim}_staging")
            con.execute(f"DELETE FROM {dim}")
            con.execute(f"INSERT INTO {dim} SELECT * FROM {dim}_staging")

    if wide_path.is_file():
        con.execute(
            f"""
            CREATE OR REPLACE TABLE wide_games_staging AS
            SELECT * FROM read_parquet('{wide_path.as_posix()}')
            """
        )


def export_wide_games(
    con: duckdb.DuckDBPyConnection,
    month: str,
    *,
    config: dict[str, Any],
) -> Path:
    """Export denormalized wide games parquet for downstream preprocess."""
    year, mon = parse_month(month)
    extract_cfg = config.get("extract") or {}
    subpath = extract_cfg.get("output_subpath", "processed")
    out_dir = get_artifact_path("lichess_data", subpath, create=True)
    out_path = out_dir / f"{month}.parquet"

    tables = {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }

    if "wide_games_staging" in tables:
        con.execute(
            f"""
            COPY (SELECT * FROM wide_games_staging)
            TO '{out_path.as_posix()}'
            (FORMAT PARQUET)
            """
        )
    else:
        con.execute(
            f"""
            COPY ({WIDE_EXPORT_SQL})
            TO '{out_path.as_posix()}'
            (FORMAT PARQUET)
            """,
            [year, mon],
        )

    _logger.info("Exported wide games: %s", out_path)
    return out_path


def sync_month(month: str, *, config: dict[str, Any] | None = None) -> Path:
    """Load processed Parquet into DuckDB and export ML-ready wide parquet."""
    cfg = config or load_config("lichess_data")
    year, mon = parse_month(month)
    con = _connect(cfg)

    if is_minio_backend(cfg):
        _register_month_from_s3(con, cfg, year, mon)
    else:
        _register_month_from_local(con, year, mon)

    return export_wide_games(con, month, config=cfg)
