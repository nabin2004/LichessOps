# Documentation

- [Lichess database downloader](./lichess-database-downloader.md) — monthly `.pgn.zst` HTTPS download, SHA256 verification, artifacts layout, CLI and Python API.
- [Package organization](./package-organization.md) — notebooks to packages mapping, repo layout conventions, phased Docker rollout, and how services orchestrate runs.
- [Airflow ingestion](./airflow-ingestion.md) — monthly `lichess_data` ingestion DAG, parameters, and local setup.
- [Great Expectations validation](./great-expectations.md) — GE checks for processed and preprocessed datasets, CLI usage, and configuration.
- [Logging and exceptions](./logging-and-exceptions.md) — `setup_logging`, `get_logger`, `LichessException`, environment variables, and usage patterns for developers.
- [Config loading](./config-loading.md) — `load_config`, YAML paths, deep merge behavior, and examples.
- [Artifact management](./artifact-management.md) — `get_artifact_path`, `get_run_dir`, `ARTIFACT_DIR`, path safety, and layout.

## Model training and serving

After monthly preprocessing and Feast split:

```bash
uv run lichess-models train --month YYYY-MM
uv run lichess-serving --port 8082
```

Prometheus and Grafana (profile `monitoring`) scrape `http://host.docker.internal:8082/metrics`. Evidently drift reports: `docker compose --profile evidently up -d --build` with `reference.parquet` and `current.parquet` in `services/evidently/data/`.

Set `MLFLOW_TRACKING_URI` and `MODEL_URI` for experiment tracking and model loading. MLflow is optional (install separately when compatible with your pandas/pyarrow versions); training works without it via `--no-mlflow`. See [Package organization](./package-organization.md) for the full pipeline map.
