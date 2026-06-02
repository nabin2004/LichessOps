"""Load raw shards to object storage and sync DuckDB."""

from lichess_data.load.duckdb_sync import sync_month
from lichess_data.load.upload import upload_raw_shard

__all__ = ["sync_month", "upload_raw_shard"]
