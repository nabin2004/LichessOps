# Airflow ingestion DAGs

This document describes the Airflow DAG that automates the `lichess_data` ingestion steps (download -> extract -> preprocess -> validate) using the local Compose services.

## DAG: `lichess_monthly_ingestion`

Location: [services/airflow/dags/lichess_monthly_ingestion.py](../services/airflow/dags/lichess_monthly_ingestion.py)

Schedule: monthly on the 1st at 03:00 UTC (`0 3 1 * *`), `catchup=False`.

### Parameters

| Param | Default | Description |
| --- | --- | --- |
| `month` | `""` | Use a specific shard like `2024-06`; empty uses `--previous-month`. |
| `verify_checksum` | `true` | Validate downloaded shard checksum during download. |
| `skip_existing` | `true` | Skip re-download if the shard already exists and is valid. |
| `test_size` | `0.2` | Fraction for temporal test split in preprocessing. |
| `run_validation` | `true` | Run checksum validation after download. |

## Local setup

1. Copy env example (optional but recommended):

```bash
touch services/airflow/.env
```

Set these keys if you want explicit paths:

```bash
AIRFLOW_PROJ_DIR=./services/airflow
AIRFLOW_PROJECT_DIR=.
```

2. Install Python deps into the Airflow image. For local dev, set `_PIP_ADDITIONAL_REQUIREMENTS` in `services/airflow/.env` or a root `.env`:

```bash
_PIP_ADDITIONAL_REQUIREMENTS=chess==1.10.0 zstandard==0.23.0 pandas==2.2.2 pyarrow==16.1.0
```

For a longer-lived setup, build a custom Airflow image that bakes these dependencies in.

3. Start Airflow:

```bash
docker compose --profile orchestration up -d
```

## Triggering a run

- Open Airflow UI at `http://localhost:8080`
- Unpause `lichess_monthly_ingestion`
- Use the Trigger button to provide params (for example `{"month": "2024-06"}`)

If `month` is blank, the DAG uses `--previous-month` for download, extract, preprocess, and validate.

## Output locations

Defaults resolve to the artifact layout described in [Artifact management](./artifact-management.md):

- Raw shard: `artifacts/lichess_data/raw/pgn/lichess_db_standard_rated_YYYY-MM.pgn.zst`
- Extracted parquet: `artifacts/lichess_data/processed/YYYY-MM.parquet`
- Preprocessed train/test: `artifacts/lichess_data/preprocessed/YYYY-MM/train.parquet` and `test.parquet`

## Troubleshooting

- If Airflow does not see DAGs, confirm `AIRFLOW_PROJ_DIR=./services/airflow` and restart the stack.
- If imports fail, confirm `AIRFLOW_PROJECT_DIR=.` and the `PYTHONPATH` setting in the compose file.
- If dependencies are missing, use `_PIP_ADDITIONAL_REQUIREMENTS` or a custom Airflow image.
