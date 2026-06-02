## `lichess-serving`

FastAPI inference service for game outcome prediction.

### CLI

From the repo root:

```bash
uv run lichess-serving --help
uv run lichess-serving --port 8082
```

Set `MODEL_URI` (and optionally `MLFLOW_TRACKING_URI`) to control how the model is loaded.
