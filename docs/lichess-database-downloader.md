# Lichess monthly database downloader

This document describes [`lichess_data.extract.lichess_downloader`](../packages/lichess_data/src/lichess_data/extract/lichess_downloader.py)—how it fetches monthly **standard-rated** game archives from [database.lichess.org](https://database.lichess.org/), verifies them, and lands them in MinIO or local artifacts.

Related concepts:

- [Config loading](./config-loading.md) — how `lichess_data` YAML is merged from the repo
- [Artifact management](./artifact-management.md) — `get_artifact_path`, `ARTIFACT_DIR`
- [Package organization](./package-organization.md) — broader pipeline phases

---

## Purpose

Each month Lichess publishes a single **non-cumulative** shard: a Zstandard-compressed PGN file named like `lichess_db_standard_rated_YYYY-MM.pgn.zst`, plus aggregated checksums at `{category}/sha256sums.txt` (typically `standard/sha256sums.txt`). The downloader:

1. Optionally discovers available months by parsing the public index HTML.
2. Streams the shard over HTTPS (default **8 MiB** chunks; the full file is never held in memory).
3. Verifies **SHA256** against the official list during or after the transfer.
4. Writes to **MinIO** directly (default when `storage.backend: minio`) or to a local `.part` file with resume support (when `--local` or `--dest` is used).

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
| `--dest PATH` | Override output directory (forces local download; see below) |
| `--local` | Download to local `artifacts/` even when storage backend is MinIO |
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
  direct_to_minio: true
```

When `storage.backend` is `minio` and `direct_to_minio: true` (the default), `lichess-data download` streams the shard directly to MinIO via the S3 API and prints an `s3://` URI — no local copy is written under `artifacts/`. The separate `lichess-data upload` step becomes an idempotent no-op when the object is already present with matching SHA256 metadata.

Use `--local` or `--dest PATH` to force the legacy local download path (resumable via `.part` files).

When `--dest` is omitted and local download is used, files are written under:

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
| `download_month(month, *, dest_dir=..., verify=..., ...)` | End-to-end local download + verify → `Path` |
| `download_month_to_minio(month, *, verify=..., ...)` | Stream directly to MinIO + verify → `s3://` URI |
| `download_previous_month(**kwargs)` | Convenience for scheduled local runs |
| `download_previous_month_to_minio(**kwargs)` | Convenience for scheduled MinIO runs |

**Errors:** mismatched SHA256 raises `ChecksumMismatchError` (includes `filename`, `expected`, `actual`). Network or missing index structure surface as `RuntimeError` / `ValueError` with short messages suitable for logs.

---

## Download behavior details

### Direct-to-MinIO (default)

- When `storage.backend: minio` and `direct_to_minio: true`, bytes stream from Lichess HTTPS straight into MinIO via S3 multipart upload.
- SHA256 is verified during the upload stream; mismatch deletes the partial object and raises `ChecksumMismatchError`.
- Verified objects store `sha256` in S3 user metadata; `skip_existing` checks that metadata before re-downloading.
- **No HTTP Range resume** for direct-to-MinIO transfers — use `--local` if you need resumable downloads on disk.

### Local streaming and resume (`--local` or `--dest`)

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

Bake the same **workspace root** and **Python environment** into the worker image, or install `lichess-data` and ensure `libs` is importable (today the repo assumes `PYTHONPATH` includes the workspace root). With `direct_to_minio: true`, the download task lands the shard in MinIO; the subsequent `upload` task is a fast idempotent skip. See [ColumnStore analytics](./columnstore-analytics.md).

---

## Tests and fixtures

Unit tests use a saved HTML snippet under [`packages/lichess_data/tests/fixtures/standard_games_index.html`](../packages/lichess_data/tests/fixtures/standard_games_index.html) and mock network for checksum/download behavior. Heavy end-to-end tests against multi-gigabyte files are intentionally omitted.
