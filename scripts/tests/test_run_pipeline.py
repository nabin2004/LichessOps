"""Tests for pipeline orchestrator command/phase construction."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.run_pipeline import (  # noqa: E402
    build_data_cmd,
    build_features_cmd,
    build_models_cmd,
    build_phases,
    phase_keys,
)


def test_build_data_cmd() -> None:
    cmd = build_data_cmd("download", "2013-01")
    assert cmd == ["uv", "run", "lichess-data", "download", "--month", "2013-01"]


def test_build_features_cmd_with_extra() -> None:
    cmd = build_features_cmd("split", "2013-01", ["--use-sample", "--max-rows", "1000"])
    assert "lichess-features" in cmd
    assert "--use-sample" in cmd


def test_build_models_cmd() -> None:
    cmd = build_models_cmd("train", "2013-01", ["--cv"])
    assert cmd[-1] == "--cv"


def test_build_phases_elt_default() -> None:
    phases = build_phases("2013-01", start_infra=False)
    keys = phase_keys(phases)
    assert keys[0] == "download"
    assert "upload" in keys
    assert "spark-transform" in keys
    assert "columnstore-sync" in keys
    assert "extract" not in keys
    assert keys[-1] == "serve"


def test_build_phases_legacy() -> None:
    phases = build_phases("2013-01", legacy=True, start_infra=False)
    keys = phase_keys(phases)
    assert "extract" in keys
    assert "upload" not in keys


def test_build_phases_skip_validation() -> None:
    phases = build_phases("2013-01", skip_validation=True, start_infra=False)
    keys = phase_keys(phases)
    assert "validate" not in keys
    assert "validate-ge" not in keys


def test_build_phases_with_infra_and_monitoring() -> None:
    phases = build_phases("2013-01", start_infra=True, with_monitoring=True)
    infra = phases[0]
    assert infra.key == "infra"
    assert "--profile" in infra.cmd
    assert "monitoring" in infra.cmd


def test_build_phases_no_serve() -> None:
    phases = build_phases("2013-01", start_infra=False, no_serve=True)
    assert phase_keys(phases)[-1] == "train"
