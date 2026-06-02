## `lichess-features`

Workspace package for feature materialization and chronological splits (Feast-oriented).

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
