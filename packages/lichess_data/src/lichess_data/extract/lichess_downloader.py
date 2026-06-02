"""Download monthly Lichess standard-rated database shards (.pgn.zst).

Fetches from https://database.lichess.org/, verifies SHA256 against the
published ``sha256sums.txt``, and writes under the ``lichess_data`` artifact
tree (default: ``artifacts/lichess_data/raw/pgn/``).

**User-facing documentation:** see ``docs/lichess-database-downloader.md`` at the
workspace root for CLI usage, YAML keys, streaming/resume semantics, and
orchestration (Airflow) notes.

Orchestration (example)::

    PYTHONPATH=. uv run lichess-data download --previous-month

"""

from __future__ import annotations

import hashlib
import html
import re
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from lichess_libs.shared import load_config
from lichess_libs.shared.artifact_manager import get_artifact_path
from lichess_libs.shared.logger import get_logger

_logger = get_logger(__name__)

DEFAULT_BASE_URL = "https://database.lichess.org"
DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024

_SECTION_IDS: dict[str, str] = {
    "standard": "standard_games",
}


class ChecksumMismatchError(Exception):
    """Computed SHA256 does not match the published checksum."""

    def __init__(self, filename: str, expected: str, actual: str) -> None:
        self.filename = filename
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"SHA256 mismatch for {filename!r}: expected {expected}, got {actual}"
        )


@dataclass(frozen=True)
class MonthShard:
    """One row from the Lichess database index (standard rated games)."""

    month_label: str
    year_month: str
    filename: str
    download_url: str
    size: str
    game_count: str


def _download_options(config: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = config if config is not None else load_config("lichess_data")
    dl = cfg.get("download") or {}
    return {
        "base_url": str(dl.get("base_url", DEFAULT_BASE_URL)).rstrip("/"),
        "category": str(dl.get("category", "standard")),
        "output_subpath": str(dl.get("output_subpath", "raw/pgn")),
        "chunk_size_bytes": int(dl.get("chunk_size_bytes", DEFAULT_CHUNK_SIZE)),
        "verify_checksum": bool(dl.get("verify_checksum", True)),
        "skip_existing": bool(dl.get("skip_existing", True)),
    }


def shard_filename(month: str) -> str:
    """Return the Lichess filename for ``month`` (``YYYY-MM``)."""
    m = re.fullmatch(r"(\d{4})-(\d{2})", month.strip())
    if not m:
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    y, mm = int(m.group(1)), int(m.group(2))
    if mm < 1 or mm > 12:
        raise ValueError(f"invalid month in {month!r}")
    return f"lichess_db_standard_rated_{m.group(1)}-{m.group(2)}.pgn.zst"


def resolve_previous_month(today: date | None = None) -> str:
    """Return ``YYYY-MM`` for the calendar month before ``today`` (default: UTC date)."""
    d = today or date.today()
    first = d.replace(day=1)
    prev_last = first - timedelta(days=1)
    return f"{prev_last.year:04d}-{prev_last.month:02d}"


def _strip_tags(s: str) -> str:
    s = re.sub(r"<[^>]+>", "", s)
    s = html.unescape(s)
    return re.sub(r"\s+", " ", s).strip()


def _Ym_from_shard_filename(fname: str) -> str | None:
    found = re.search(
        r"lichess_db_standard_rated_(\d{4}-\d{2})\.pgn\.zst", fname)
    return found.group(1) if found else None


def parse_monthly_index_html(
    html_text: str,
    *,
    base_url: str,
    category: str = "standard",
) -> list[MonthShard]:
    """Parse the index page HTML into :class:`MonthShard` rows (offline-testable)."""
    sec_id = _SECTION_IDS.get(category)
    if not sec_id:
        raise ValueError(f"Unsupported category: {category!r}")

    base = base_url.rstrip("/")
    m = re.search(
        rf'id="{re.escape(sec_id)}"[^>]*>(.*?)</section>',
        html_text,
        re.S | re.I,
    )
    if not m:
        raise ValueError(
            f"Could not find #{sec_id} section; page structure may have changed."
        )
    sec = m.group(1)
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", sec, re.S)

    shards: list[MonthShard] = []
    for r in rows:
        if "<th" in r.lower():
            continue
        cols = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", r, re.S)
        if len(cols) != 4:
            continue
        month_cell, size_cell, games_cell, dl_cell = cols

        link = re.search(r'href="([^"]+)"', dl_cell, re.I)
        if not link:
            continue
        href = link.group(1).strip()
        if not href:
            continue
        fname = Path(href).name
        ym = _Ym_from_shard_filename(fname)
        if not ym:
            continue

        if href.startswith("http://") or href.startswith("https://"):
            full_url = href
        else:
            full_url = f"{base}/{href.lstrip('/')}"

        shards.append(
            MonthShard(
                month_label=_strip_tags(month_cell),
                year_month=ym,
                filename=fname,
                download_url=full_url,
                size=_strip_tags(size_cell),
                game_count=_strip_tags(games_cell),
            )
        )
    return shards


def fetch_monthly_index(
    category: str | None = None,
    *,
    base_url: str | None = None,
    config: dict[str, Any] | None = None,
) -> list[MonthShard]:
    """GET the database homepage and parse the monthly standard-games table."""
    opts = _download_options(config)
    cat = category or opts["category"]
    bu = (base_url or opts["base_url"]).rstrip("/")
    idx_url = f"{bu}/"
    _logger.info("Fetching index %s", idx_url)
    try:
        with urlopen(idx_url, timeout=120) as resp:
            html_text = resp.read().decode("utf-8", "replace")
    except (URLError, HTTPError) as e:
        raise RuntimeError(f"Failed to fetch index {idx_url!r}") from e
    return parse_monthly_index_html(html_text, base_url=bu, category=cat)


def parse_sha256sums_text(text: str) -> dict[str, str]:
    """Parse ``sha256sums.txt`` body into ``{filename: lowercase_hex}``."""
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        digest, fname = parts[0].lower(), parts[1].strip()
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            continue
        out[fname] = digest
    return out


def fetch_sha256_map(
    category: str | None = None,
    *,
    base_url: str | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, str]:
    """GET ``{category}/sha256sums.txt`` and parse filename → digest."""
    opts = _download_options(config)
    cat = category or opts["category"]
    bu = (base_url or opts["base_url"]).rstrip("/")
    sums_url = f"{bu}/{cat}/sha256sums.txt"
    _logger.debug("Fetching checksums %s", sums_url)
    try:
        with urlopen(sums_url, timeout=120) as resp:
            text = resp.read().decode("utf-8", "replace")
    except (URLError, HTTPError) as e:
        raise RuntimeError(f"Failed to fetch checksums {sums_url!r}") from e
    return parse_sha256sums_text(text)


def _file_sha256(path: Path, chunk_size: int) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _stream_download_url(
    url: str,
    part_path: Path,
    *,
    chunk_size: int,
    progress: bool = True,
) -> None:
    """Append-bytes semantics: resume with ``Range`` when ``.part`` exists."""
    part_path.parent.mkdir(parents=True, exist_ok=True)
    start = part_path.stat().st_size if part_path.exists() else 0

    if start > 0:
        req = Request(url, headers={"Range": f"bytes={start}-"})
    else:
        if part_path.exists():
            part_path.unlink()
        req = Request(url)

    try:
        resp = urlopen(req, timeout=600)
    except (URLError, HTTPError) as e:
        raise RuntimeError(f"HTTP request failed for {url!r}") from e

    try:
        status = resp.status
        # Range not satisfiable — often means we already have the full object.
        if start > 0 and status == 416:
            _logger.info(
                "HTTP 416 for resume of %s; assuming download already complete",
                part_path.name,
            )
            return

        if start > 0 and status == 200:
            _logger.info(
                "Server ignored Range; restarting full download for %s",
                part_path.name,
            )
            resp.close()
            part_path.unlink(missing_ok=True)
            return _stream_download_url(
                url, part_path, chunk_size=chunk_size, progress=progress
            )

        if start > 0 and status != 206:
            raise RuntimeError(
                f"Expected 206 Partial Content when resuming, got HTTP {status}"
            )
        if start == 0 and status not in (200, 206):
            raise RuntimeError(f"Unexpected HTTP status {status} for {url!r}")

        mode = "ab" if start > 0 and status == 206 else "wb"
        if mode == "wb" and part_path.exists():
            part_path.unlink()

        total_hdr = resp.headers.get("Content-Length")
        total_i = int(total_hdr) if total_hdr and total_hdr.isdigit() else None

        pbar = None
        if progress:
            try:
                from tqdm import tqdm

                pbar = tqdm(
                    total=total_i + start if total_i is not None else None,
                    initial=start,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=part_path.name,
                )
            except ImportError:
                pbar = None

        with open(part_path, mode) as out:
            while True:
                chunk = resp.read(chunk_size)
                if not chunk:
                    break
                out.write(chunk)
                if pbar is not None:
                    pbar.update(len(chunk))

        if pbar is not None:
            pbar.close()
    finally:
        resp.close()


def download_month(
    month: str,
    *,
    dest_dir: Path | None = None,
    skip_existing: bool | None = None,
    verify: bool | None = None,
    base_url: str | None = None,
    category: str | None = None,
    chunk_size_bytes: int | None = None,
    config: dict[str, Any] | None = None,
    progress: bool = True,
) -> Path:
    """Download one monthly shard; verify SHA256; return final artifact path."""
    opts = _download_options(config)
    bu = (base_url or opts["base_url"]).rstrip("/")
    cat = category or opts["category"]
    chunk = chunk_size_bytes or opts["chunk_size_bytes"]
    skip = opts["skip_existing"] if skip_existing is None else skip_existing
    do_verify = opts["verify_checksum"] if verify is None else verify

    if dest_dir is None:
        dest_dir_path = get_artifact_path(
            "lichess_data",
            opts["output_subpath"],
            create=True,
        )
    else:
        dest_dir_path = Path(dest_dir).expanduser().resolve()
        dest_dir_path.mkdir(parents=True, exist_ok=True)

    filename = shard_filename(month)
    dest = dest_dir_path / filename
    part = dest_dir_path / f"{filename}.part"

    expected: str | None = None
    if do_verify:
        sha_map = fetch_sha256_map(cat, base_url=bu, config=config)
        expected = sha_map.get(filename)
        if expected is None:
            raise ValueError(
                f"No SHA256 entry for {filename!r} in published checksums"
            )

        if skip and dest.exists():
            got = _file_sha256(dest, chunk)
            if got == expected:
                _logger.info("Skipping existing verified file %s", dest)
                return dest
            _logger.warning(
                "Existing file checksum mismatch; re-downloading: %s", dest
            )
            dest.unlink()
    else:
        if skip and dest.exists():
            _logger.info("Skipping existing file (checksum verify off): %s", dest)
            return dest

    file_url = f"{bu}/{cat}/{filename}"
    _logger.info("Downloading %s -> %s", file_url, part)
    try:
        _stream_download_url(
            file_url, part, chunk_size=chunk, progress=progress
        )
    except Exception:
        _logger.exception("Download failed")
        raise

    if do_verify and expected is not None:
        got = _file_sha256(part, chunk)
        if got != expected:
            part.unlink(missing_ok=True)
            raise ChecksumMismatchError(filename, expected, got)
        _logger.info("Checksum OK for %s", filename)

    part.replace(dest)
    return dest


def download_previous_month(**kwargs: Any) -> Path:
    """Download the prior calendar month's shard (:func:`resolve_previous_month`)."""
    return download_month(resolve_previous_month(), **kwargs)
