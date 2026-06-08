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


def columnstore_settings(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Resolve MariaDB ColumnStore connection settings from config and env."""
    import os

    storage = (config or {}).get("storage") or {}
    cs = storage.get("columnstore") or {}
    return {
        "host": os.getenv("MARIADB_COLUMNSTORE_HOST", str(cs.get("host", "localhost"))),
        "port": int(os.getenv("MARIADB_COLUMNSTORE_PORT", str(cs.get("port", 3307)))),
        "user": os.getenv("MARIADB_COLUMNSTORE_USER", str(cs.get("user", "lichess"))),
        "password": os.getenv(
            "MARIADB_COLUMNSTORE_PASSWORD",
            str(cs.get("password", "Lichess_Analytics1!")),
        ),
        "database": os.getenv(
            "MARIADB_COLUMNSTORE_DATABASE",
            str(cs.get("database", "lichess_analytics")),
        ),
    }


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
