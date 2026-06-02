"""Load YAML config from repo-root ``config/`` and merge package overrides."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .logger import get_logger

_logger = get_logger(__name__)


def _find_project_root() -> Path:
    # In this monorepo we have multiple `pyproject.toml` files (root + workspace members).
    # We want the workspace root that owns `packages/` and `config/`.
    for parent in Path(__file__).resolve().parents:
        if (
            (parent / "pyproject.toml").exists()
            and (parent / "packages").is_dir()
            and (parent / "config").is_dir()
        ):
            return parent

    # Fallback for non-monorepo / ad-hoc usage.
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise FileNotFoundError("Could not locate project root (no pyproject.toml found)")


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _normalize_yaml_dict(data: Any) -> dict[str, Any]:
    if data is None:
        return {}
    if isinstance(data, dict):
        return data
    return {}


def _load_yaml_file(path: Path) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            raw = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in {path}: {e}") from e
    return _normalize_yaml_dict(raw)


def load_config(component_name: str, config_name: str = "default") -> dict[str, Any]:
    """Load global config, then deep-merge package-specific overrides.

    Merge order:
      1. ``{repo}/config/{config_name}.yaml`` (base)
      2. ``{repo}/packages/{component_name}/configs/{config_name}.yaml`` (overrides)

    Missing files are skipped. Empty or ``null`` YAML documents merge as ``{}``.
    """
    root = _find_project_root()
    global_cfg_path = root / "config" / f"{config_name}.yaml"
    component_cfg_path = (
        root / "packages" / component_name / "configs" / f"{config_name}.yaml"
    )

    config: dict[str, Any] = {}

    if global_cfg_path.exists():
        _logger.debug("Loading global config: %s", global_cfg_path)
        config = _load_yaml_file(global_cfg_path)
    else:
        _logger.debug("Global config not found, skipping: %s", global_cfg_path)

    if component_cfg_path.exists():
        _logger.debug("Loading component config: %s", component_cfg_path)
        component_cfg = _load_yaml_file(component_cfg_path)
        config = _deep_merge(config, component_cfg)
    else:
        _logger.debug("Component config not found, skipping: %s", component_cfg_path)

    return config

