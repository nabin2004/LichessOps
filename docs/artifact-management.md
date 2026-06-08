# Artifact management

This project stores generated outputs (exports, checkpoints, run logs on disk, etc.) under a shared **artifact root**. The helpers in [`libs.shared.artifact_manager`](../libs/shared/artifact_manager.py) resolve safe, per-component paths and optionally create directories, consistent with [config loading](./config-loading.md) (repo root from `pyproject.toml`).

## Quick start

```python
from libs.shared import get_run_dir

run_dir = get_run_dir("lichess_data")
(run_dir / "metrics.json").write_text("{}", encoding="utf-8")
```

`component` should match the package folder under `packages/` (for example `lichess_data`, `lichess_serving`, `lichess_models`, `lichess_features`).

## Directory layout

| Level | Path pattern |
|-------|----------------|
| Artifact root | `{repo}/artifacts` by default, or `ARTIFACT_DIR` |
| Component | `{artifact_root}/{component}/` |
| Subpath / run | `{artifact_root}/{component}/{subpath…}/` |

## `get_artifact_path(component, subpath="", *, create=True)`

- **`component`:** Single path segment (no `/` or `\`). Typically the `packages/<name>/` directory name.
- **`subpath`:** Optional relative path (string or `pathlib.Path`) under that component, e.g. `checkpoints/epoch_1`. Each segment is validated; `..`, `.`, and absolute paths are rejected.
- **`create`:** If `True` (default), runs `mkdir(parents=True, exist_ok=True)` on the resolved path. If `False`, returns the path without creating directories (useful for read-only checks).
- **Returns:** A `pathlib.Path` under the artifact root.

After building the path, the implementation resolves both the component directory and the target and ensures the target **stays inside** the component directory (blocks path traversal).

## `get_run_dir(component, run_id=None, *, create=True)`

- **`run_id`:** If omitted, a timestamp `YYYYMMDD_HHMMSS` is used (same idea as before this module was hardened).
- If provided, **`run_id` must be a single segment** (no path separators); it is validated like `component`.
- Delegates to `get_artifact_path(component, run_id, create=create)`.

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ARTIFACT_DIR` | *(unset)* | If set, absolute or relative path (after `~` expansion) used as the artifact root instead of `{repo}/artifacts`. |

Example:

```bash
ARTIFACT_DIR=/var/lib/lichess/artifacts uv run python main.py
```

At **`LOG_LEVEL=DEBUG`**, ensured directories are logged (see [Logging and exceptions](./logging-and-exceptions.md)).

## Path rules and errors

Invalid input raises **`ValueError`** with a short explanation, for example:

- Empty component or `run_id` segment, or `.` / `..`
- Multiple path segments passed where only one is allowed (`component`, `run_id`)
- Absolute paths for `component`, `run_id`, or `subpath`
- A segment containing `/` or `\\`
- A resolved path that would escape the component directory (defense in depth)

## Git and cleanup

Contents under `artifacts/` are **gitignored** except `artifacts/.gitkeep`, which keeps the directory in the repo. Do not commit large binaries there; use your storage or pipeline conventions for long-term retention.

When `LICHESS_STORAGE_BACKEND=minio`, MinIO holds authoritative raw and star-schema Parquet; local paths under `artifacts/lichess_data/` act as an export cache (for example `processed/YYYY-MM.parquet` from ColumnStore sync). See [ColumnStore analytics](./columnstore-analytics.md).

## See also

- [Config loading](./config-loading.md)
- [Logging and exceptions](./logging-and-exceptions.md)
