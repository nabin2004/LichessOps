# Lichess monthly database downloader

This document describes [`lichess_data.extract.lichess_downloader`](../packages/lichess_data/src/lichess_data/extract/lichess_downloader.py)—how it fetches monthly **standard-rated** game archives from [database.lichess.org](https://database.lichess.org/), verifies them, and where files land locally.

Related concepts:

- [Config loading](./config-loading.md) — how `lichess_data` YAML is merged from the repo
- [Artifact management](./artifact-management.md) — `get_artifact_path`, `ARTIFACT_DIR`
- [Package organization](./package-organization.md) — broader pipeline phases

---

## Purpose

Each month Lichess publishes a single **non-cumulative** shard: a Zstandard-compressed PGN file named like `lichess_db_standard_rated_YYYY-MM.pgn.zst`, plus aggregated checksums at `{category}/sha256sums.txt` (typically `standard/sha256sums.txt`). The downloader:

1. Optionally discovers available months by parsing the public index HTML.
2. Streams the shard over HTTPS into a `{filename}.part` file (resumable).
3. Verifies **SHA256** against the official list.
4. Renames the `.part` file atomically to the final name.

Orchestration (for example monthly Airflow) should call this step before any decompression / PGN parsing / Parquet transform.

---

## CLI

Install workspace dependencies, then run the `lichess-data` console script **from the repository root** with the repo root on `PYTHONPATH` so `libs.shared` resolves (`load_config`, logging, artifact helpers):

```bash
cd /path/to/lichess  # workspace root containing pyproject.toml and libs/

PYTHONPATH=. uv run --package lichess-data lichess-data download --month 2024-06
PYTHONPATH=. uv run --package lichess-data lichess-data download --previous-month
PYTHONPATH=. uv run --package lichess-data lichess-data download --list
```

Common flags:

| Flag | Meaning |
| ---- | ------- |
| `--month YYYY-MM` | Which shard to download |
| `--previous-month` | Prior calendar month (useful after a shard is fully published) |
| `--list` | Print shards from the live index (tabular on stdout) |
| `--no-verify` | Skip checksum verification (avoid in production) |
| `--no-skip-existing` | Force re-download even if the destination file exists |
| `--dest PATH` | Override output directory (default: artifact layout; see below) |
| `--no-progress` | Disable `tqdm` progress bar |
| `--base-url URL` | Override `https://database.lichess.org` |

---

## Configuration

Defaults live in [`packages/lichess_data/configs/default.yaml`](../packages/lichess_data/configs/default.yaml):

```yaml
download:
  base_url: https://database.lichess.org
  category: standard
  output_subpath: raw/pgn
  chunk_size_bytes: 8388608
  verify_checksum: true
  skip_existing: true
```

When `--dest` is omitted, files are written under:

`get_artifact_path("lichess_data", <output_subpath>, create=True)`

which resolves to **`{ARTIFACT_ROOT}/lichess_data/raw/pgn/`** with `ARTIFACT_ROOT` normally the repo root’s `artifacts/` directory unless [artifact management](./artifact-management.md)’s `ARTIFACT_DIR` overrides it.

Final object path shape:

```
{artifact_root}/lichess_data/raw/pgn/lichess_db_standard_rated_YYYY-MM.pgn.zst
```

---

## Python API

Import from `lichess_data.extract.lichess_downloader` or the package re-exports under `lichess_data.extract`.

| Symbol | Role |
| ------ | ---- |
| `MonthShard` | Dataclass from one index row: label, `year_month`, `filename`, absolute `download_url`, size, game count strings |
| `shard_filename(month)` | `"2024-06"` → expected filename string |
| `resolve_previous_month(today)` | Prior calendar month as `YYYY-MM` |
| `parse_monthly_index_html(html_text, base_url=..., category="standard")` | Parse HTML offline (tests / mirrors) |
| `fetch_monthly_index(...)` | HTTP GET index + parse (`category` defaults from config) |
| `parse_sha256sums_text(text)` | Parse checksum file body → `dict[filename, hex]` |
| `fetch_sha256_map(...)` | HTTP GET `standard/sha256sums.txt`-style URL + parse |
| `download_month(month, *, dest_dir=..., verify=..., ...)` | End-to-end download + verify → `Path` |
| `download_previous_month(**kwargs)` | Convenience for scheduled runs |

**Errors:** mismatched SHA256 raises `ChecksumMismatchError` (includes `filename`, `expected`, `actual`). Network or missing index structure surface as `RuntimeError` / `ValueError` with short messages suitable for logs.

---

## Download behavior details

### Streaming and resume

- Data is read in chunks (default **8 MiB**) via `urllib.request`; the full file is never held in memory.
- Active download path: `{destination}/{basename}.part`.
- If a `.part` file already exists with size `N > 0`, the client sends `Range: bytes=N-`. A **206** response continues appending. If the server returns **200** instead, partial data is discarded and a full restart is attempted.
- **HTTP 416** while resuming is treated as “likely already complete” (no extra bytes fetched); checksum verification decides success.

### Idempotency and verification

- With `skip_existing` / `skip_existing=True` and verification enabled: an existing destination file whose SHA256 matches the official list is **skipped** immediately.
- If the file exists but the hash differs, it is deleted and re-downloaded.
- After download, the `.part` file is hashed once; mismatch deletes the `.part` and raises `ChecksumMismatchError` before renaming.

### Index parsing

The live site wraps the chess-games panel in markup that includes `#standard_games`. The parser locates `id="standard_games"` and reads the first enclosing `</section>` block, then parses table rows into `MonthShard` entries using the `.pgn.zst` hyperlink (torrent links are skipped by filename filter). If Lichess changes page structure drastically, parsing may raise `ValueError`; fix by updating the regex scope in-module.

---

## Orchestration notes (Airflow, CI, Docker)

Airflow typically runs something equivalent to:

```bash
PYTHONPATH=<repo_root> uv run lichess-data download --previous-month
```

Bake the same **workspace root** and **Python environment** into the worker image, or install `lichess-data` and ensure `libs` is importable (today the repo assumes `PYTHONPATH` includes the workspace root). After download, the ELT pipeline uploads the shard with `lichess-data upload` — see [ColumnStore analytics](./columnstore-analytics.md).

---

## Tests and fixtures

Unit tests use a saved HTML snippet under [`packages/lichess_data/tests/fixtures/standard_games_index.html`](../packages/lichess_data/tests/fixtures/standard_games_index.html) and mock network for checksum/download behavior. Heavy end-to-end tests against multi-gigabyte files are intentionally omitted.
