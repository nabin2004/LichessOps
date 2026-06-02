## `lichess-models`

Workspace package for training and evaluating Lichess outcome prediction models.

### CLI

From the repo root:

```bash
uv run lichess-models --help
uv run lichess-models train --help
uv run lichess-models evaluate --help
```

Models and metrics are written under `artifacts/lichess_models/` by default (override with `ARTIFACT_DIR`).
