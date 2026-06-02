"""Validation entrypoints for downloaded Lichess shards."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from lichess_libs.shared.logger import get_logger

from .checks import ChecksumResult, validate_checksum

_logger = get_logger(__name__)


def validate_checksum_file(
	file_path: str | Path,
	*,
	config: dict[str, Any] | None = None,
	base_url: str | None = None,
	category: str | None = None,
) -> bool:
	"""Validate a shard checksum and log warnings on failure."""
	result = validate_checksum(
		file_path,
		config=config,
		base_url=base_url,
		category=category,
	)

	if result.ok:
		_logger.info("Checksum OK for %s", result.filename)
		return True

	if result.reason == "missing-file":
		_logger.warning("Checksum validation skipped; file missing: %s", result.path)
	elif result.reason == "missing-checksum":
		_logger.warning(
			"No published checksum for %s; cannot validate", result.filename
		)
	elif result.reason == "checksum-fetch-failed":
		_logger.warning("Failed to fetch checksum list for %s", result.filename)
	elif result.reason == "checksum-mismatch":
		_logger.warning(
			"Checksum mismatch for %s (expected=%s, actual=%s)",
			result.filename,
			result.expected,
			result.actual,
		)
	else:
		_logger.warning("Checksum validation failed for %s", result.filename)

	return False


def validate_checksum_result(
	file_path: str | Path,
	*,
	config: dict[str, Any] | None = None,
	base_url: str | None = None,
	category: str | None = None,
) -> ChecksumResult:
	"""Return the full checksum validation result for a shard."""
	return validate_checksum(
		file_path,
		config=config,
		base_url=base_url,
		category=category,
	)
