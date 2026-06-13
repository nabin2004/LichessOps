# Documentation

- [Lichess database downloader](./lichess-database-downloader.md) — monthly `.pgn.zst` HTTPS download, SHA256 verification, artifacts layout, CLI and Python API.
- [Package organization](./package-organization.md) — notebooks to packages mapping, repo layout conventions, phased Docker rollout, and how services orchestrate runs.
- [Airflow ingestion](./airflow-ingestion.md) — monthly `lichess_data` ingestion DAG, parameters, and local setup.
- [Great Expectations validation](./great-expectations.md) — GE checks for processed and preprocessed datasets, CLI usage, and configuration.
- [Logging and exceptions](./logging-and-exceptions.md) — `setup_logging`, `get_logger`, `LichessException`, environment variables, and usage patterns for developers.
- [Config loading](./config-loading.md) — `load_config`, YAML paths, deep merge behavior, and examples.
- [Artifact management](./artifact-management.md) — `get_artifact_path`, `get_run_dir`, `ARTIFACT_DIR`, path safety, and layout.
- [ColumnStore analytics](./columnstore-analytics.md) — MinIO buckets, star-schema Parquet, MariaDB ColumnStore, inference storage, and ELT CLI commands.
- [Pipeline evaluation and monitoring](./pipeline-evaluation-and-monitoring.md) — model performance metrics, confusion matrices, test set evaluation, model drift analysis, Prometheus/Grafana monitoring strategy, and Airflow retraining policies.

## Model training and serving

After monthly preprocessing and Feast split:

```bash
# Default: single fit per candidate, no cross-validation (fast on large shards)
uv run lichess-models train --month YYYY-MM

# Optional: cross-validation + hyperparameter search (slow; for dev/small data)
uv run lichess-models train --month YYYY-MM --cv

uv run lichess-serving --port 8082
```

Prometheus and Grafana (profile `monitoring`) scrape `http://host.docker.internal:8082/metrics`. Evidently drift reports: `docker compose --profile evidently up -d --build` with `reference.parquet` and `current.parquet` in `services/evidently/data/`.

Training logs to MLflow by default (test metrics, model artifact, registry). The MLflow Python client is not installed by `uv sync` (version pins conflict with `pandas>=3` / `pyarrow>=24`); use the Airflow image or the install pattern in `services/airflow/Dockerfile`. Set `MLFLOW_TRACKING_URI` (default `http://localhost:5000`), and start the tracking server with `docker compose --profile core --profile ml up -d`. Use `--no-mlflow` to skip tracking on local runs. Hold-out test metrics (`test_*` in MLflow) are the authoritative evaluation signal when CV is disabled.

Set `MODEL_URI` for serving from the registry. See [Package organization](./package-organization.md) for the full pipeline map.
