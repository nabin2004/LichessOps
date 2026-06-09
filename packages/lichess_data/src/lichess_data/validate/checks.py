"""Validation checks for Lichess data artifacts."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lichess_libs.shared import is_minio_backend, load_config
from lichess_libs.shared.s3 import object_exists, object_sha256, raw_bucket_name, raw_object_key, skip_if_verified
from lichess_libs.shared.storage_config import raw_prefix

from lichess_data.extract import lichess_downloader as ld


@dataclass(frozen=True)
class ChecksumResult:
	"""Result of validating a shard checksum against published SHA256."""

	path: Path
	filename: str
	ok: bool
	expected: str | None
	actual: str | None
	reason: str | None


def _download_options(config: dict[str, Any] | None = None) -> dict[str, Any]:
	cfg = config if config is not None else load_config("lichess_data")
	dl = cfg.get("download") or {}
	return {
		"base_url": str(dl.get("base_url", ld.DEFAULT_BASE_URL)).rstrip("/"),
		"category": str(dl.get("category", "standard")),
		"chunk_size_bytes": int(dl.get("chunk_size_bytes", ld.DEFAULT_CHUNK_SIZE)),
	}


def _validate_minio_checksum(
	filename: str,
	*,
	config: dict[str, Any],
	base_url: str,
	category: str,
) -> ChecksumResult | None:
	"""Validate a raw shard stored in MinIO when no local copy exists."""
	bucket = raw_bucket_name(config)
	key = raw_object_key(raw_prefix(config), filename)
	path = Path(filename)

	try:
		if not object_exists(bucket, key):
			return None
	except Exception:
		return None

	try:
		sha_map = ld.fetch_sha256_map(category, base_url=base_url, config=config)
	except Exception:
		return ChecksumResult(
			path=path,
			filename=filename,
			ok=False,
			expected=None,
			actual=None,
			reason="checksum-fetch-failed",
		)

	expected = sha_map.get(filename)
	if expected is None:
		return ChecksumResult(
			path=path,
			filename=filename,
			ok=False,
			expected=None,
			actual=None,
			reason="missing-checksum",
		)

	if skip_if_verified(bucket, key, expected):
		return ChecksumResult(
			path=path,
			filename=filename,
			ok=True,
			expected=expected,
			actual=expected,
			reason=None,
		)

	actual = object_sha256(bucket, key)
	if actual and actual.lower() == expected.lower():
		return ChecksumResult(
			path=path,
			filename=filename,
			ok=True,
			expected=expected,
			actual=actual,
			reason=None,
		)

	return ChecksumResult(
		path=path,
		filename=filename,
		ok=False,
		expected=expected,
		actual=actual,
		reason="checksum-mismatch",
	)


def _file_sha256(path: Path, chunk_size: int) -> str:
	h = hashlib.sha256()
	with open(path, "rb") as f:
		while True:
			block = f.read(chunk_size)
			if not block:
				break
			h.update(block)
	return h.hexdigest()


def validate_checksum(
	file_path: str | Path,
	*,
	config: dict[str, Any] | None = None,
	base_url: str | None = None,
	category: str | None = None,
) -> ChecksumResult:
	"""Validate a downloaded shard against the published SHA256 list."""
	opts = _download_options(config)
	bu = (base_url or opts["base_url"]).rstrip("/")
	cat = category or opts["category"]
	chunk = opts["chunk_size_bytes"]

	path = Path(file_path).expanduser().resolve()
	if not path.exists() or not path.is_file():
		cfg = config if config is not None else load_config("lichess_data")
		if is_minio_backend(cfg):
			minio_result = _validate_minio_checksum(
				path.name,
				config=cfg,
				base_url=bu,
				category=cat,
			)
			if minio_result is not None:
				return minio_result
		return ChecksumResult(
			path=path,
			filename=path.name,
			ok=False,
			expected=None,
			actual=None,
			reason="missing-file",
		)

	try:
		sha_map = ld.fetch_sha256_map(cat, base_url=bu, config=config)
	except Exception:
		return ChecksumResult(
			path=path,
			filename=path.name,
			ok=False,
			expected=None,
			actual=None,
			reason="checksum-fetch-failed",
		)

	expected = sha_map.get(path.name)
	if expected is None:
		return ChecksumResult(
			path=path,
			filename=path.name,
			ok=False,
			expected=None,
			actual=None,
			reason="missing-checksum",
		)

	actual = _file_sha256(path, chunk)
	if actual != expected:
		return ChecksumResult(
			path=path,
			filename=path.name,
			ok=False,
			expected=expected,
			actual=actual,
			reason="checksum-mismatch",
		)

	return ChecksumResult(
		path=path,
		filename=path.name,
		ok=True,
		expected=expected,
		actual=actual,
		reason=None,
	)
