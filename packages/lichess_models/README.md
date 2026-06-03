## `lichess-models`

Workspace package for training and evaluating Lichess outcome prediction models.

### CLI

From the repo root:

```bash
uv run lichess-models --help
uv run lichess-models train --help
uv run lichess-models evaluate --help
```

### Training modes

By default (`training.use_cv: false` in `configs/default.yaml`), training fits each candidate estimator once on the full train split with sklearn defaults and picks the best by the configured scorer. No cross-validation or hyperparameter search — suitable for large monthly shards.

Enable search with `--cv` (CLI) or `use_cv: true` in config. That runs `GridSearchCV` / `RandomizedSearchCV` with `TimeSeriesSplit` over `PARAM_GRIDS`.

Hold-out test metrics from `run_evaluate` are always written to the run directory and logged to MLflow as `test_*` metrics.

### MLflow

Install the tracking client:

```bash
uv sync --package lichess-models --extra ml
```

Set `MLFLOW_TRACKING_URI` (default `http://localhost:5000`). The `train` command logs parameters, train/cv score, test metrics, artifacts, and registers the model unless `--no-mlflow` is passed.

### Configuration

Key keys in `configs/default.yaml`:

| Key | Default | Description |
| --- | --- | --- |
| `training.use_cv` | `false` | Cross-validation + hyperparameter search |
| `training.cv_folds` | `3` | TimeSeriesSplit folds (only when `use_cv` is true) |
| `training.scoring` | `balanced_accuracy` | Scorer for candidate selection |
| `model.candidates` | logistic, RF, HGB | Estimators to compare |

Models and metrics are written under `artifacts/lichess_models/` by default (override with `ARTIFACT_DIR`).
