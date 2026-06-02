# LichessOps

End-to-end MLOps pipeline for learning from the public Lichess game database: ingest monthly `.pgn.zst` shards, validate and preprocess them, build features, train models, and serve predictions.

## Monorepo layout

- `packages/`: uv workspace packages
  - `packages/lichess_data/`: ingestion, validation, preprocessing, Spark transforms
  - `packages/lichess_features/`: feature materialization + chronological split (Feast-oriented)
  - `packages/lichess_models/`: training, evaluation, MLflow registration helpers
  - `packages/lichess_serving/`: FastAPI inference service
- `libs/`: shared utilities (distributed as `lichess-libs`, imported as `lichess_libs.*`)
- `services/`: Docker Compose stacks (Airflow, MinIO, Spark, MLflow, monitoring, etc.)
- `docs/`: documentation hub (start at `docs/index.md`)
- `docs/notes/`: project notes (CS329 writeups and milestones)
- `notebook/`: exploratory notebooks (prototype logic and EDA)
- `config/`: global YAML defaults merged into component configs
- `artifacts/`: local pipeline outputs (override via `ARTIFACT_DIR`)

## Quick start

From the repo root:

```bash
uv lock
uv sync --all-packages
```

Run package CLIs:

```bash
uv run lichess-data --help
uv run lichess-features --help
uv run lichess-models --help
uv run lichess-serving --help
```

For Docker services and profiles, see `services/README.md`.