"""Write star-schema and wide Parquet to local paths or MinIO."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from libs.shared import get_logger, is_minio_backend, upload_file
from libs.shared.s3 import (
    fact_games_prefix,
    processed_bucket_name,
    s3_uri,
    wide_games_prefix,
)
from libs.shared.storage_config import parse_month

from lichess_data.spark.transform_core import collect_shard_tables

_logger = get_logger(__name__)

_TABLE_SCHEMAS: dict[str, list[tuple[str, pa.DataType]]] = {
    "fact_games": [
        ("game_id", pa.string()),
        ("white_player_id", pa.string()),
        ("black_player_id", pa.string()),
        ("opening_id", pa.string()),
        ("date_id", pa.int64()),
        ("result", pa.string()),
        ("white_elo", pa.int64()),
        ("black_elo", pa.int64()),
        ("white_rating_diff", pa.int64()),
        ("black_rating_diff", pa.int64()),
        ("time_control", pa.string()),
        ("termination", pa.string()),
        ("event", pa.string()),
        ("utc_datetime", pa.string()),
        ("move_count", pa.int64()),
        ("year", pa.int64()),
        ("month", pa.int64()),
    ],
    "dim_player": [
        ("player_id", pa.string()),
        ("username", pa.string()),
        ("title", pa.string()),
        ("last_known_elo", pa.int64()),
    ],
    "dim_opening": [
        ("opening_id", pa.string()),
        ("eco", pa.string()),
        ("opening_name", pa.string()),
    ],
    "dim_date": [
        ("date_id", pa.int64()),
        ("calendar_date", pa.string()),
        ("year", pa.int64()),
        ("month", pa.int64()),
        ("day_of_week", pa.int64()),
    ],
    "wide_games": [
        ("event", pa.string()),
        ("site", pa.string()),
        ("date", pa.string()),
        ("round", pa.string()),
        ("white", pa.string()),
        ("black", pa.string()),
        ("white_title", pa.string()),
        ("black_title", pa.string()),
        ("result", pa.string()),
        ("utc_date", pa.string()),
        ("utc_time", pa.string()),
        ("white_elo", pa.int64()),
        ("black_elo", pa.int64()),
        ("white_rating_diff", pa.int64()),
        ("black_rating_diff", pa.int64()),
        ("eco", pa.string()),
        ("opening", pa.string()),
        ("time_control", pa.string()),
        ("termination", pa.string()),
        ("moves", pa.list_(pa.string())),
    ],
}


def _rows_to_table(table_name: str, rows: list[dict[str, Any]]) -> pa.Table:
    schema = pa.schema([pa.field(name, dtype) for name, dtype in _TABLE_SCHEMAS[table_name]])
    if not rows:
        return pa.Table.from_pylist([], schema=schema)
    return pa.Table.from_pylist(rows, schema=schema)


def _write_table_local(table: pa.Table, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return path


def _upload_table(table: pa.Table, bucket: str, key: str) -> str:
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        pq.write_table(table, tmp_path)
        return upload_file(tmp_path, bucket, key, skip_if_unchanged=False)
    finally:
        tmp_path.unlink(missing_ok=True)


def _write_dim_table(
    table_name: str,
    rows: list[dict[str, Any]],
    *,
    config: dict[str, Any],
    local_base: Path | None,
) -> str | Path:
    table = _rows_to_table(table_name, rows)
    if is_minio_backend(config):
        bucket = processed_bucket_name(config)
        key = f"{table_name}/data.parquet"
        return _upload_table(table, bucket, key)
    assert local_base is not None
    out = local_base / f"{table_name}.parquet"
    return _write_table_local(table, out)


def run_local_transform(
    input_path: str | Path,
    month: str,
    *,
    config: dict[str, Any],
    local_output_base: Path | None = None,
    local_only: bool = False,
) -> dict[str, str | Path]:
    """Parse a shard and write star-schema + wide Parquet (local or MinIO)."""
    year, mon = parse_month(month)
    tables = collect_shard_tables(input_path, year=year, month=mon)
    outputs: dict[str, str | Path] = {}

    if local_output_base is None:
        from libs.shared import get_artifact_path

        local_output_base = get_artifact_path(
            "lichess_data", "processed/star_schema", create=True
        )

    use_minio = is_minio_backend(config) and not local_only
    bucket = processed_bucket_name(config)
    fact_key = f"{fact_games_prefix(year, mon)}/part-00000.parquet"
    wide_key = f"{wide_games_prefix(year, mon)}/part-00000.parquet"

    fact_table = _rows_to_table("fact_games", tables["fact_games"])
    wide_table = _rows_to_table("wide_games", tables["wide_games"])

    if use_minio:
        outputs["fact_games"] = _upload_table(fact_table, bucket, fact_key)
        outputs["wide_games"] = _upload_table(wide_table, bucket, wide_key)
    else:
        fact_path = local_output_base / fact_key.replace("/", "_")
        wide_path = local_output_base / wide_key.replace("/", "_")
        outputs["fact_games"] = _write_table_local(fact_table, fact_path)
        outputs["wide_games"] = _write_table_local(wide_table, wide_path)

    for dim in ("dim_player", "dim_opening", "dim_date"):
        if use_minio:
            outputs[dim] = _write_dim_table(
                dim,
                tables[dim],
                config=config,
                local_base=local_output_base,
            )
        else:
            outputs[dim] = _write_dim_table(
                dim,
                tables[dim],
                config={**config, "storage": {**(config.get("storage") or {}), "backend": "local"}},
                local_base=local_output_base,
            )

    _logger.info("Transform complete for %s: %s games", month, len(tables["fact_games"]))
    return outputs
