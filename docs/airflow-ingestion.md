# Airflow ingestion DAGs

This document describes the Airflow DAG that automates the `lichess_data` ingestion pipeline using local Compose services and optional MinIO/DuckDB ELT.

See [Object storage and DuckDB](./object-storage-and-duckdb.md) for bucket layout and CLI details.

## DAG: `lichess_monthly_ingestion`

Location: [services/airflow/dags/lichess_monthly_ingestion.py](../services/airflow/dags/lichess_monthly_ingestion.py)

Schedule: monthly on the 1st at 03:00 UTC (`0 3 1 * *`), `catchup=False`.

### Parameters

| Param | Default | Description |
| --- | --- | --- |
| `month` | `""` | Use a specific shard like `2024-06`; empty uses `--previous-month`. |
| `verify_checksum` | `true` | Validate downloaded shard checksum during download. |
| `skip_existing` | `true` | Skip re-download if the shard already exists and is valid. |
| `test_size` | `0.2` | Fraction for temporal test split in Feast (`lichess-features split`). |
| `run_validation` | `true` | Run checksum validation and Great Expectations checks. |
| `run_training` | `true` | Run `lichess-models train` after split. |
| `use_elt` | `true` | When true, run MinIO upload → spark-transform → duckdb-sync instead of legacy `extract`. |

### Task graph (ELT path, `use_elt=true`)

```
download → upload → spark-transform → duckdb-sync → preprocess → feast split → validate → validate-ge → train
```

Legacy path (`use_elt=false`):

```
download → extract → preprocess → feast split → validate → validate-ge → train
```

## Local setup

1. Start MinIO when using ELT (recommended):

```bash
docker compose --profile core up -d
```

2. Copy env example (optional but recommended):

```bash
touch services/airflow/.env
```

Set these keys if you want explicit paths:

```bash
AIRFLOW_PROJ_DIR=.
AIRFLOW_PROJECT_DIR=../..
LICHESS_STORAGE_BACKEND=minio
```

Paths are relative to `services/airflow/` (where the Airflow compose file lives). `AIRFLOW_PROJECT_DIR=../..` mounts the repo root at `/opt/airflow/project`, which editable package installs expect.

3. Install Python deps into the Airflow image. For local dev, set `_PIP_ADDITIONAL_REQUIREMENTS` in `services/airflow/.env` or a root `.env`:

```bash
_PIP_ADDITIONAL_REQUIREMENTS=chess==1.10.0 zstandard==0.23.0 pandas==2.2.2 pyarrow==16.1.0 feast==0.46.0 boto3 duckdb
```

For a longer-lived setup, build a custom Airflow image that bakes these dependencies in.

4. Start Airflow:

```bash
docker compose --profile orchestration up -d
```

Airflow workers receive `AWS_ENDPOINT_URL=http://minio:9000` and MinIO credentials from the compose file.

## Triggering a run

- Open Airflow UI at `http://localhost:8080`
- Unpause `lichess_monthly_ingestion`
- Use the Trigger button to provide params (for example `{"month": "2013-01", "use_elt": true}`)

If `month` is blank, the DAG uses `--previous-month` for all steps.

Validation includes:

1. Checksum validation on the raw shard.
2. Great Expectations checks on processed (wide export), features, and train/test data.

See [Great Expectations validation](./great-expectations.md) for details.

## Output locations

| Stage | Path |
|-------|------|
| Raw shard (local) | `artifacts/lichess_data/raw/pgn/lichess_db_standard_rated_YYYY-MM.pgn.zst` |
| Raw shard (MinIO) | `s3://lichess-raw/pgn/lichess_db_standard_rated_YYYY-MM.pgn.zst` |
| Star schema (MinIO) | `s3://lichess-processed/fact_games/year=YYYY/month=MM/` etc. |
| Processed wide parquet | `artifacts/lichess_data/processed/YYYY-MM.parquet` (from `duckdb-sync` or legacy `extract`) |
| DuckDB catalog | `artifacts/lichess_data/duckdb/lichess.duckdb` |
| Preprocessed features | `artifacts/lichess_data/preprocessed/YYYY-MM/features.parquet` |
| Train/test | `artifacts/lichess_data/preprocessed/YYYY-MM/train.parquet`, `test.parquet` |

## Troubleshooting

- If Airflow does not see DAGs, confirm `AIRFLOW_PROJ_DIR=./services/airflow` and restart the stack.
- If imports fail, confirm `AIRFLOW_PROJECT_DIR=../..` in `services/airflow/.env` so `/opt/airflow/project/packages/` exists in workers, then restart the stack.
- If ELT upload fails, ensure MinIO (`--profile core`) is running and reachable from workers at `http://minio:9000`.
- If dependencies are missing, use `_PIP_ADDITIONAL_REQUIREMENTS` or a custom Airflow image.
