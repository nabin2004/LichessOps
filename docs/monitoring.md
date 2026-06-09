# Monitoring (Prometheus + Grafana)

Basic metrics stack for the Lichess MLOps pipeline. Prometheus scrapes serving and host metrics; Grafana dashboards are auto-provisioned on startup.

## Quick start

From the repo root:

```bash
./scripts/setup_monitoring.sh
```

Or manually:

```bash
docker compose --profile monitoring up -d
```

The full pipeline (`./scripts/run_pipeline_2013_01.sh`) starts the monitoring profile automatically and prints all observability URLs when it finishes.

## URLs

| Service | URL | Notes |
| --- | --- | --- |
| Prometheus | http://localhost:9090 | Scrape config in `services/prometheus/prometheus.yml` |
| Grafana | http://localhost:3000 | Default credentials: `admin` / `changeme` (override via `GF_SECURITY_ADMIN_*` in `.env`) |
| node-exporter | http://localhost:9100/metrics | Host CPU, memory, disk |

## Dashboards

Grafana loads dashboards from `services/prometheus/grafana/provisioning/dashboards/json/`:

- **Lichess Serving** — model loaded, request rate, predictions, HTTP latency (requires serving on port 8082)
- **Lichess System** — CPU, memory, and root disk usage from node-exporter

Open Grafana → **Dashboards** → browse the provisioned panels.

## Metrics flow

```
lichess-serving container (:8082/metrics)
        │
        ▼
Prometheus (job: lichess-serving)
        │
        ▼
Grafana (Lichess Serving dashboard)

node-exporter (:9100/metrics)
        │
        ▼
Prometheus (job: node-exporter)
        │
        ▼
Grafana (Lichess System dashboard)
```

Start serving via the pipeline (container) or manually:

```bash
# Container (recommended — scraped in-network by Prometheus)
export MODEL_URI=/opt/lichess/project/artifacts/lichess_models/<run_id>/model.joblib
docker compose --profile serving up -d lichess-serving

# Host-only dev fallback
export MODEL_URI=artifacts/lichess_models/<run_id>/model.joblib
uv run lichess-serving --port 8082
```

For host-only serving, change the Prometheus target to `host.docker.internal:8082` in `services/prometheus/prometheus.yml`.

## Verify

```bash
curl -sf http://localhost:9090/-/healthy && echo "Prometheus OK"
curl -sf http://localhost:3000/api/health && echo "Grafana OK"
```

In Prometheus → **Status → Targets**, confirm `lichess-serving`, `node-exporter`, and `prometheus` are UP.

## Full pipeline with monitoring

```bash
# Optional: Slack alerts per pipeline phase
export SLACK_WEBHOOK_TOKEN=your-team-id/your-channel-id/your-secret

# Run end-to-end pipeline for 2013-01 (Airflow + monitoring + serving container)
./scripts/run_pipeline_2013_01.sh

# Dev smoke test
./scripts/run_pipeline_2013_01.sh --use-sample --max-rows 1000
```

The orchestrator prints an observability summary at the end with links to Airflow, MLflow, Grafana, Prometheus, Spark, MinIO, and the serving API.

See [columnstore-analytics.md](./columnstore-analytics.md) for ELT prerequisites and [services/README.md](../services/README.md) for Compose profiles.
