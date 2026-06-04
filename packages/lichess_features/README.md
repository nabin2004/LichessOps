## `lichess-features`

Workspace package for feature materialization and chronological splits (Feast-oriented).

### Dependencies and Feast runtime

`uv sync` does **not** install Feast. Feast 0.46 pins `numpy<2` and `pandas<3`, which conflict with the workspace (`numpy>=2`, `pandas>=3`). Production installs Feast out-of-band (see `services/airflow/Dockerfile` and the Docker `feast` profile).

Commands that call Feast (`apply`, `split`, etc.) need Feast in the **runtime** environment:

- Docker `feast-cli` or Airflow worker image
- Or a manual install matching the Airflow pattern (`feast==0.46.0` with `--no-deps` plus its transitive deps)

### CLI

From the repo root:

```bash
uv run lichess-features --help
uv run lichess-features split --help
```

### Feast repo

`feast_repo/feature_defs.py` defines the offline `FileSource`. On Feast 0.46, set
`timestamp_field` (not the deprecated `event_timestamp_column`) so apply does not
infer between `utc_date` and `utc_datetime`.

### Related

- `feast_repo/`: Feast repo files used by the Docker `feast` profile
- `docs/index.md`: docs hub
