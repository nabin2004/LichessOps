## `lichess-serving`

FastAPI inference service for game outcome prediction.

### CLI

From the repo root:

```bash
uv run lichess-serving --help
uv run lichess-serving --port 8082
```

Swagger UI: `http://127.0.0.1:8082/docs` — OpenAPI JSON: `http://127.0.0.1:8082/openapi.json`

### Loading from MLflow registry

`lichess-serving` does not install MLflow via `uv sync` (pandas/pyarrow pin conflicts). Install it once with the same overrides as the Airflow image:

```bash
printf 'pandas>=3.0.3\npyarrow>=24.0.0\n' > /tmp/uv-overrides.txt
uv pip install --overrides /tmp/uv-overrides.txt mlflow==3.12.0
```

Start MinIO and MLflow, then export env vars in the shell that runs serving:

```bash
docker compose --profile core --profile ml up -d

export MLFLOW_TRACKING_URI=http://localhost:5000
export MLFLOW_S3_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export MODEL_URI=models:/lichess-outcome-predictor/4

uv run lichess-serving --port 8082
```

`MODEL_URI` overrides [`configs/default.yaml`](configs/default.yaml). Use a `.joblib` path instead to skip MLflow entirely.

### Endpoint examples

**Health**

```bash
curl -s http://127.0.0.1:8082/health
# {"status":"ok","model_loaded":true}
```

**Predict (full payload)**

```bash
curl -s -X POST http://127.0.0.1:8082/predict \
  -H 'Content-Type: application/json' \
  -d '{
    "player_elo": 1350,
    "opponent_elo": 1420,
    "player_color": "white",
    "eco": "C50",
    "opening_family": "Italian Game",
    "time_control": "Blitz",
    "time_control_raw": "300+0",
    "player_eco_score": 0.62,
    "player_h2h_win_rate": 0.48,
    "opening_population_win_rate": 0.53
  }'
```

**Predict (minimal — only required fields)**

```bash
curl -s -X POST http://127.0.0.1:8082/predict \
  -H 'Content-Type: application/json' \
  -d '{"player_elo": 1800, "opponent_elo": 1700, "player_color": "black", "eco": "B20"}'
```

**Validation error (422)**

```bash
curl -s -X POST http://127.0.0.1:8082/predict \
  -H 'Content-Type: application/json' \
  -d '{"player_elo": 100, "opponent_elo": 1500, "player_color": "white", "eco": "C50"}'
```

**Prometheus metrics**

```bash
curl -s http://127.0.0.1:8082/metrics | grep lichess_
```

Responses use `predicted_outcome` (`"1"` win, `"0"` loss, `"½"` draw) and `probabilities` keys `lose`, `win`, `draw`.
