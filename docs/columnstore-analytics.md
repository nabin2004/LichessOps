# Object storage (MinIO) and MariaDB ColumnStore analytics

## What changed

| Before | After |
|--------|-------|
| DuckDB embedded file (`artifacts/.../lichess.duckdb`) | **MariaDB ColumnStore** Docker service (`mcs1`, port `3307`) |
| CLI `lichess-data duckdb-sync` | CLI `lichess-data columnstore-sync` |
| Airflow task `duckdb_sync` | Airflow task `columnstore_sync` |
| `services/duckdb/` interactive CLI | `services/columnstore/` ColumnStore stack |
| Inference ephemeral (API response only) | **Full persistence**: `prediction_logs`, `batch_predictions`, `inference_runs` |
| Evidently reads Parquet only | Evidently reads ColumnStore by default (`EVIDENTLY_DATA_SOURCE=columnstore`) |

Downstream ML steps are **unchanged**: `preprocess` → Feast split → validate → train still consume `artifacts/lichess_data/processed/YYYY-MM.parquet`.

## Architecture

```mermaid
flowchart LR
    download["download (streams to MinIO)"] --> upload["upload (no-op if verified)"]
    upload --> spark[spark-transform]
    spark --> minio[(MinIO Parquet)]
    minio --> sync[columnstore-sync]
    sync --> cs[(MariaDB ColumnStore)]
    sync --> wide[YYYY-MM.parquet]
    wide --> ml[preprocess / Feast / train]
    serving[/predict] --> cs
    evaluate[evaluate] --> cs
    evidently[Evidently API] --> cs
```

**ELT flow:** stream raw shard to MinIO → star-schema transform → sync into ColumnStore → export wide Parquet for ML.

## Docker startup

```bash
# Object store + ColumnStore + ML + orchestration
docker compose --profile core --profile ml --profile orchestration up -d
```

ColumnStore (`mcs1`) starts with profile `core`. On first boot the entrypoint wrapper:

1. Starts ColumnStore services
2. Runs `provision mcs1` (single-node cluster)
3. Applies `services/columnstore/init/01_schema.sql`

Verify:

```bash
mysql -h 127.0.0.1 -P 3307 -u lichess -p'Lichess_Analytics1!' lichess_analytics -e "SHOW TABLES;"
```

## Bucket layout (unchanged)

| Bucket | Prefix | Content |
|--------|--------|---------|
| `lichess-raw` | `pgn/lichess_db_standard_rated_YYYY-MM.pgn.zst` | Raw monthly shard |
| `lichess-processed` | `fact_games/year=YYYY/month=MM/` | Fact table Parquet |
| `lichess-processed` | `dim_player/`, `dim_opening/`, `dim_date/` | Dimensions |
| `lichess-processed` | `wide_games/year=YYYY/month=MM/` | Denormalized wide table |

Local export cache: `artifacts/lichess_data/processed/YYYY-MM.parquet` (from `columnstore-sync`).

## ColumnStore schema

### Star schema (analytics)

| Table | Engine | Description |
|-------|--------|-------------|
| `fact_games` | Columnstore | Game facts, partitioned by `year`/`month` |
| `dim_player` | Columnstore | Player dimension |
| `dim_opening` | Columnstore | Opening dimension |
| `dim_date` | Columnstore | Date dimension |
| Wide ML export | SQL join over star schema | Exported to `YYYY-MM.parquet` (not stored as a separate wide table) |

### Inference tables

| Table | Purpose |
|-------|---------|
| `prediction_logs` | Online `/predict` and Evidently POST logs |
| `batch_predictions` | Offline evaluation test-set predictions |
| `inference_runs` | Run metadata (model URI, metrics, row counts) |

## CLI commands

```bash
# Full ELT path for one month (download streams directly to MinIO by default)
uv run lichess-data download --month 2013-01
uv run lichess-data upload --month 2013-01
uv run lichess-data spark-transform --month 2013-01
uv run lichess-data columnstore-sync --month 2013-01
uv run lichess-data preprocess --month 2013-01

# SQL inspection
mysql -h 127.0.0.1 -P 3307 -u lichess -plichess lichess_analytics \
  -e "SELECT COUNT(*) FROM fact_games WHERE year=2013 AND month=1;"
```

## Environment variables

| Variable | Default | Used by |
|----------|---------|---------|
| `MARIADB_COLUMNSTORE_HOST` | `localhost` / `mcs1` in Compose | All ColumnStore clients |
| `MARIADB_COLUMNSTORE_PORT` | `3307` (host) / `3306` (in-network) | Connection |
| `MARIADB_COLUMNSTORE_USER` | `lichess` | Connection |
| `MARIADB_COLUMNSTORE_PASSWORD` | `Lichess_Analytics1!` | Connection |
| `MARIADB_COLUMNSTORE_DATABASE` | `lichess_analytics` | Connection |
| `MARIADB_COLUMNSTORE_ROOT_PASSWORD` | `C0lumnStore!` | Docker admin / provision |
| `MARIADB_COLUMNSTORE_DISABLED` | unset | Set to `1` to skip inference writes |
| `EVIDENTLY_DATA_SOURCE` | `columnstore` | Evidently API (`columnstore` or `parquet`) |

## Inference persistence

### Online serving

`lichess_serving` `POST /predict` returns the prediction immediately and writes to `prediction_logs` in a background task. Failures are logged but do not affect the API response.

### Batch evaluation

`lichess-models train` persists test predictions by default. Disable with `--no-persist-columnstore`.

```bash
uv run lichess-models evaluate --month 2024-01 --run-dir artifacts/lichess_models/run-xxx --persist-columnstore
```

### Evidently

- Default data source: ColumnStore (`batch_predictions` for drift/performance reports)
- `POST /monitor/prediction-logs` dual-writes JSONL and ColumnStore
- `GET /monitor/prediction-logs?data_source=columnstore` reads from ColumnStore
- Pass `"data_source": "parquet"` in report requests to use local Parquet files

## Airflow DAG

```
download → upload → spark-transform → columnstore-sync → preprocess → feast split → validate → validate-ge → train
```

Training persists evaluation predictions to ColumnStore unless `lichess-models train --no-persist-columnstore` is used (not the DAG default).

## Troubleshooting

| Issue | Fix |
|-------|-----|
| ColumnStore healthcheck failing | Allow 3+ minutes on first boot for `provision mcs1`; check `docker logs mcs1` |
| `shm_size` errors | Ensure `shm_size: 512m` on the `columnstore` service |
| Airflow worker cannot connect | Use `MARIADB_COLUMNSTORE_HOST=mcs1` and port `3306` inside Compose network |
| Sync fails on empty MinIO prefix | Re-run `spark-transform`; confirm objects under `lichess-processed/` |
| Evidently 404 on ColumnStore reports | Run training for the month first so `batch_predictions` has rows |
| Disable ColumnStore writes | `MARIADB_COLUMNSTORE_DISABLED=1` |
