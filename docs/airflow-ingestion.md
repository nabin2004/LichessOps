# Airflow ingestion DAGs

This document describes the Airflow DAG that automates the `lichess_data` ingestion pipeline using local Compose services and optional MinIO/ColumnStore ELT.

See [ColumnStore analytics](./columnstore-analytics.md) for bucket layout and CLI details.

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
| `use_sample` | `false` | When true, cap combined **game** rows before split/train (OOM-safe dev runs). |
| `max_rows` | `1000` | Max games to keep when `use_sample` is true (applied at Feast split and training). |

Feast `FileSource` in `lichess_features/feast_repo` must use `timestamp_field="utc_datetime"` (Feast 0.46 ignores `event_timestamp_column`).
| `run_validation` | `true` | Run checksum validation and Great Expectations checks. |
| `run_training` | `true` | Run `lichess-models train` after split (logs to MLflow). |
| `use_cv` | `false` | When true, pass `--cv` for cross-validation and hyperparameter search (slow on large shards). |
| `use_elt` | `true` | When true, run MinIO upload → spark-transform → columnstore-sync instead of legacy `extract`. |

### Task graph (ELT path, `use_elt=true`)

```
download → upload → spark-transform → columnstore-sync → preprocess → feast split → validate → validate-ge → train
```

Legacy path (`use_elt=false`):

```
download → extract → preprocess → feast split → validate → validate-ge → train
```

## Local setup

1. Start MinIO, ColumnStore, and MLflow when using ELT and scheduled training (recommended):

```bash
docker compose --profile core --profile ml up -d
```

ColumnStore (`mcs1`) is included in profile `core`. Allow up to 3 minutes on first boot for provisioning.

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
_PIP_ADDITIONAL_REQUIREMENTS=chess==1.10.0 zstandard==0.23.0 pandas==2.2.2 pyarrow==16.1.0 feast==0.46.0 boto3 pymysql sqlalchemy
```

For a longer-lived setup, build a custom Airflow image that bakes these dependencies in.

4. Start Airflow (rebuild the image after Dockerfile changes):

```bash
docker compose --profile orchestration up -d --build
```

Airflow workers receive `AWS_ENDPOINT_URL=http://minio:9000`, MinIO credentials, and `MLFLOW_TRACKING_URI=http://mlflow:5000` from the compose file. Start **`core` + `ml` + `orchestration`** before triggering a run that includes training, or MLflow logging will fail.

## Triggering a run

- Open Airflow UI at `http://localhost:8080`
- Unpause `lichess_monthly_ingestion`
- Use the Trigger button to provide params (for example `{"month": "2013-01", "use_elt": true}`)

For memory-constrained training on a full shard:

```json
{"month": "2013-01", "use_sample": true, "max_rows": 1000, "test_size": 0.2, "use_cv": false}
```

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
| Processed wide parquet | `artifacts/lichess_data/processed/YYYY-MM.parquet` (from `columnstore-sync` or legacy `extract`) |
| ColumnStore catalog | MariaDB `lichess_analytics` database on `mcs1` (host port `3307`) |
| Inference predictions | `batch_predictions`, `prediction_logs`, `inference_runs` tables in ColumnStore |
| Preprocessed features | `artifacts/lichess_data/preprocessed/YYYY-MM/features.parquet` |
| Train/test | `artifacts/lichess_data/preprocessed/YYYY-MM/train.parquet`, `test.parquet` |
| Model runs | `artifacts/lichess_models/{run_id}/` (also logged to MLflow when tracking is enabled) |

## Slack alerts

Airflow sends failure notifications to Slack via an Incoming Webhook when `SLACK_WEBHOOK_TOKEN` is set.

1. Copy the token (path after `https://hooks.slack.com/services/`) into `.env` at the repo root or `services/airflow/.env`:

```bash
SLACK_WEBHOOK_TOKEN=your-team-id/your-channel-id/your-secret-token
```

2. Rebuild and restart Airflow so the Slack provider and connection are loaded:

```bash
docker compose --profile orchestration up -d --build
```

3. Verify the connection inside the scheduler:

```bash
docker compose exec airflow-scheduler airflow connections get slackwebhook
```

4. Confirm delivery with a quick webhook test:

```bash
curl -X POST -H 'Content-type: application/json' \
  --data '{"text":"lichess airflow test"}' \
  "https://hooks.slack.com/services/${SLACK_WEBHOOK_TOKEN}"
```

`lichess_monthly_ingestion` uses both DAG-level and task-level `on_failure_callback` hooks (see `services/airflow/plugins/slack_callbacks.py`). Other DAGs can import the same callbacks.

For non-Airflow components, use the shared helper:

```python
from lichess_libs.shared.slack import send_slack_alert

send_slack_alert("lichess_data", "MinIO upload failed: connection refused")
```

Optional host-side health probes (MinIO, MLflow, serving) with Slack alerts on failure:

```bash
uv run python scripts/slack_health_check.py
```

Override probe URLs with `MINIO_HEALTH_URL`, `MLFLOW_HEALTH_URL`, and `SERVING_HEALTH_URL`.

## Troubleshooting

- If Airflow does not see DAGs, confirm `AIRFLOW_PROJ_DIR=./services/airflow` and restart the stack.
- If imports fail, confirm `AIRFLOW_PROJECT_DIR=../..` in `services/airflow/.env` so `/opt/airflow/project/packages/` exists in workers, then restart the stack.
- If ELT upload fails, ensure MinIO (`--profile core`) is running and reachable from workers at `http://minio:9000`.
- If dependencies are missing, use `_PIP_ADDITIONAL_REQUIREMENTS` or a custom Airflow image.
- If training fails on MLflow, ensure `core` and `ml` profiles are running and MLflow is healthy at `http://localhost:5000/health`.
- If the Airflow log shows `MLflow logging skipped` with `403` and `Invalid Host header - possible DNS rebinding attack detected`, MLflow 3.5+ rejected the worker's `Host: mlflow:5000` header. Recreate the tracking server with allowed hosts configured in [`services/mlflow/docker-compose.yml`](../services/mlflow/docker-compose.yml) (`MLFLOW_SERVER_ALLOWED_HOSTS=mlflow:5000,localhost:*,127.0.0.1:*`), then backfill without retraining: `lichess-models register --month YYYY-MM --run-dir artifacts/lichess_models/{run_id}`.
