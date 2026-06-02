# Lichess Compose services

All stacks are merged from the repository root via [`docker-compose.yml`](../docker-compose.yml) (`name: lichess`). Shared network **`lichess-net`** and the **`minio_data`** volume are declared in [`compose/networks.yml`](compose/networks.yml).

## Prerequisites

- Docker Engine >= 24 and Docker Compose v2 (supports `include`).
- From the **repo root**, run every `docker compose` command so paths and `include:` resolve correctly.

## Profiles

| Profile           | Stack |
| ----------------- | ------ |
| **`core`**        | MinIO (S3-compatible) + one-shot bucket bootstrap (`lichess-raw`, `lichess-processed`, `mlflow-artifacts`) |
| **`ml`**          | MLflow + dedicated Postgres (artifacts on shared MinIO — **enable `core` too**) |
| **`monitoring`**  | Prometheus + node-exporter + Grafana (datasource auto-provisioned) |
| **`orchestration`** | Apache Airflow (CeleryExecutor) + Postgres + Redis |
| **`flower`**      | Celery Flower (**use with** `orchestration`, e.g. `docker compose --profile orchestration --profile flower up -d flower`) |
| **`pipeline`**    | Spark master + worker (master UI on host **8081**; S3 env wired to MinIO) |
| **`feast`**       | Feast OSS feature server (Redis online store, optional Jupyter, CLI helper) |
| **`evidently`**   | Evidently drift API (**5001**) + Streamlit (**8501**) + Postgres |
| **`ge`**          | Postgres for Great Expectations **metadata** only (run validations from `lichess-data` on the host or another image) |
| **`tools`**       | Interactive DuckDB CLI (`docker compose --profile tools run --rm duckdb`) |
| **`debug`**       | Airflow `airflow-cli` service |

Compose **does not start any service until you pass at least one profile** that matches. `docker compose config` with no profiles expands to `services: {}` by design.

## Common commands

```bash
# Inspect merged file (always pass the profiles you plan to run)
docker compose --profile core --profile ml config --quiet

# Object store + experiment tracking
docker compose --profile core --profile ml up -d

# Add metrics / dashboards (start serving on host port 8082 first)
docker compose --profile monitoring up -d

# Drift reports (place reference/current parquet under services/evidently/data/)
docker compose --profile evidently up -d --build

# Airflow (first run may take several minutes for migrations)
docker compose --profile orchestration up -d

# Spark (MinIO creds default to minioadmin; start `core` if jobs need S3)
docker compose --profile core --profile pipeline up -d
```

## ELT quickstart (MinIO + DuckDB)

Full object-store pipeline from the host (see [docs/object-storage-and-duckdb.md](../docs/object-storage-and-duckdb.md)):

```bash
# Object store + optional Spark cluster
docker compose --profile core --profile pipeline up -d

# Monthly shard through ELT (example month)
export AWS_ENDPOINT_URL=http://localhost:9000
export LICHESS_STORAGE_BACKEND=minio
uv run lichess-data download --month 2013-01
uv run lichess-data upload --month 2013-01
uv run lichess-data spark-transform --month 2013-01
uv run lichess-data duckdb-sync --month 2013-01

# Inspect DuckDB catalog
docker compose --profile tools run --rm duckdb \
  artifacts/lichess_data/duckdb/lichess.duckdb
```

Spark submit example (cluster mode):

```bash
docker compose --profile core --profile pipeline run --rm spark-submit \
  --master spark://spark:7077 \
  /workspace/packages/lichess_data/src/lichess_data/spark/job.py \
  --month 2013-01 --input /workspace/artifacts/lichess_data/raw/pgn/lichess_db_standard_rated_2013-01.pgn.zst --local
```

## Airflow ingestion DAGs

DAGs live under [services/airflow/dags](airflow/dags). The `lichess_monthly_ingestion` DAG runs download → ELT (upload, spark-transform, duckdb-sync) or legacy extract → preprocess → Feast split → validate → train.

Recommended environment variables (set in `services/airflow/.env` or a root `.env`):

```bash
AIRFLOW_PROJ_DIR=.
AIRFLOW_PROJECT_DIR=../..
_PIP_ADDITIONAL_REQUIREMENTS=chess==1.10.0 zstandard==0.23.0 pandas==2.2.2 pyarrow==16.1.0
```

See [docs/airflow-ingestion.md](../docs/airflow-ingestion.md) for DAG parameters and troubleshooting.

## Host ports (defaults)

| Port  | Service |
| ----- | ------- |
| 9000 / 9001 | MinIO API / console |
| 5000 | MLflow UI |
| 8082 | Lichess serving API (host; scraped by Prometheus) |
| 5001 | Evidently API |
| 3000 | Grafana |
| 9090 | Prometheus |
| 9100 | node-exporter |
| 8080 | Airflow API server |
| 8081 | Spark master UI |
| 7077 | Spark master RPC |
| 5555 | Flower (when `flower` profile is used) |
| 8501 | Evidently Streamlit |
| 6566 / 8888 | Feast feature server / Jupyter |

Postgres and Redis for Airflow, MLflow, Evidently, GE, and Feast are **not published** to the host.

## Environment

- [`services/.env.example`](.env.example) — shared variables (MinIO, Grafana admin, GX DB, Evidently DB, `AIRFLOW_UID`).
- [`services/feast/.env.example`](feast/.env.example) — Feast version, repo path, and Jupyter token.
- **`services/airflow/.env`** — optional; Airflow image reads `AIRFLOW_UID` here (do not commit secrets).

## Implementation notes

- **MinIO** uses a pinned `minio/minio` image and a named volume (no bind-mounted `./data`). The `minio_setup` job uses `minio/mc` to create buckets.
- **MLflow** no longer embeds MinIO; it depends on the shared `minio` service.
- **Grafana** ships only inside **`services/prometheus/`**; the old standalone `services/grafana/` stack was removed.
- **Feast** uses the Python feature server image; run `feast apply` via the `feast-cli` service before serving features.
- **DuckDB** uses `network_mode: host` so `CALL start_ui()` matches native DuckDB UI expectations on Linux.
