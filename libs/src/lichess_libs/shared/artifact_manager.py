"""Resolve and create per-component artifact directories under the repo or ``ARTIFACT_DIR``."""

from __future__ import annotations

import os
import time
from pathlib import Path

from .config_loader import _find_project_root
from .logger import get_logger

_logger = get_logger(__name__)

_BAD_SEGMENT_CHARS = ("/", "\\")


def _resolve_artifact_root() -> Path:
    env = os.getenv("ARTIFACT_DIR")
    if env:
        return Path(env).expanduser().resolve()
    return _find_project_root() / "artifacts"


def _validate_segment(segment: str, label: str) -> str:
    if not segment or segment in (".", ".."):
        raise ValueError(
            f"Invalid {label}: segment must be non-empty and not '.' or '..', got {segment!r}"
        )
    for ch in _BAD_SEGMENT_CHARS:
        if ch in segment:
            raise ValueError(f"Invalid {label}: segment {segment!r} must not contain {ch!r}")
    return segment


def _validate_single_segment_path(value: str, label: str) -> str:
    p = Path(value)
    if p.is_absolute():
        raise ValueError(f"Invalid {label}: must be a relative name, got {value!r}")
    parts = p.parts
    if len(parts) != 1:
        raise ValueError(
            f"Invalid {label}: must be a single path segment (no slashes), got {value!r}"
        )
    return _validate_segment(parts[0], label)


def _validated_subpath_parts(subpath: str | Path) -> tuple[str, ...]:
    if subpath == "" or subpath == Path():
        return ()
    p = Path(subpath)
    if p.is_absolute():
        raise ValueError(f"subpath must be relative, got {subpath!r}")
    return tuple(_validate_segment(part, "subpath segment") for part in p.parts)


def _component_root(component: str) -> Path:
    comp = _validate_single_segment_path(component, "component")
    return _resolve_artifact_root() / comp


def get_artifact_path(
    component: str,
    subpath: str | Path = "",
    *,
    create: bool = True,
) -> Path:
    """Return a path under ``{artifact_root}/{component}/`` for writable artifacts.

    When ``create`` is True, the path (and parents) are created if missing.
    """
    base = _component_root(component)
    parts = _validated_subpath_parts(subpath)
    target = base.joinpath(*parts)

    base_resolved = base.resolve()
    target_resolved = target.resolve()
    if not target_resolved.is_relative_to(base_resolved):
        raise ValueError(
            f"Resolved artifact path escapes component directory: {target_resolved}"
        )

    if create:
        target.mkdir(parents=True, exist_ok=True)
        _logger.debug("Ensured artifact directory: %s", target)

    return target


def get_run_dir(
    component: str,
    run_id: str | None = None,
    *,
    create: bool = True,
) -> Path:
    """Return a run-specific subdirectory (timestamped if ``run_id`` is omitted)."""
    if run_id is None:
        run_id = time.strftime("%Y%m%d_%H%M%S")
    else:
        run_id = _validate_single_segment_path(run_id, "run_id")
    return get_artifact_path(component, run_id, create=create)

