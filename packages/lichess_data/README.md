# `lichess-data`

Workspace package for Lichess open-database ingestion, validation, and downstream transforms.

## Monthly PGN downloader

The module **`lichess_data.extract.lichess_downloader`** downloads standard-rated monthly `.pgn.zst` files from [database.lichess.org](https://database.lichess.org/), verifies **SHA256** checksums, and writes under `artifacts/lichess_data/raw/pgn/` by default (override with `download.output_subpath` or `--dest`).

Full documentation (CLI, YAML, Python API, resume semantics, orchestration hints):

[**docs/lichess-database-downloader.md**](../../docs/lichess-database-downloader.md)

From the repo root (so `libs.shared` resolves):

```bash
PYTHONPATH=. uv run --package lichess-data lichess-data download --help
```
