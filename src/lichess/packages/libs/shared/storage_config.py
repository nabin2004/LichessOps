"""Resolve storage-related paths from lichess_data config."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .artifact_manager import get_artifact_path
from .s3 import (
    fact_games_prefix,
    processed_bucket_name,
    raw_bucket_name,
    raw_object_key,
    s3_uri,
    wide_games_prefix,
)


def duckdb_path(config: dict[str, Any]) -> Path:
    storage = config.get("storage") or {}
    rel = Path(str(storage.get("duckdb_path", "duckdb/lichess.duckdb")))
    base = get_artifact_path("lichess_data", rel.parent.as_posix(), create=True)
    return base / rel.name


def raw_prefix(config: dict[str, Any]) -> str:
    storage = config.get("storage") or {}
    return str(storage.get("raw_prefix", "pgn"))


def raw_s3_uri(config: dict[str, Any], filename: str) -> str:
    bucket = raw_bucket_name(config)
    key = raw_object_key(raw_prefix(config), filename)
    return s3_uri(bucket, key)


def fact_games_s3_glob(config: dict[str, Any], year: int, month: int) -> str:
    bucket = processed_bucket_name(config)
    prefix = fact_games_prefix(year, month)
    return s3_uri(bucket, f"{prefix}/*.parquet")


def wide_games_s3_glob(config: dict[str, Any], year: int, month: int) -> str:
    bucket = processed_bucket_name(config)
    prefix = wide_games_prefix(year, month)
    return s3_uri(bucket, f"{prefix}/*.parquet")


def parse_month(month: str) -> tuple[int, int]:
    parts = month.strip().split("-")
    if len(parts) != 2:
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    return int(parts[0]), int(parts[1])
