"""Smoke tests for model training."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from lichess_libs.shared import load_config

from lichess_models.analyze import run_analyze
from lichess_models.evaluate import run_evaluate
from lichess_models.train import run_train

from .conftest import board_row


@pytest.fixture
def synthetic_parquet(tmp_path: Path) -> tuple[Path, str]:
    month = "2013-01"
    rows = [board_row(i % 3) for i in range(30)]
    for i, row in enumerate(rows):
        row["utc_datetime"] = pd.Timestamp(f"2013-01-{(i % 28) + 1:02d} 12:00:00")
        row["site"] = f"https://lichess.org/game{i}"
    df = pd.DataFrame(rows)

    out_dir = tmp_path / "preprocessed" / month
    out_dir.mkdir(parents=True)
    train_path = out_dir / "train.parquet"
    test_path = out_dir / "test.parquet"
    df.iloc[:24].to_parquet(train_path, index=False)
    df.iloc[24:].to_parquet(test_path, index=False)
    return out_dir, month


def _patch_training_env(
    monkeypatch: pytest.MonkeyPatch,
    out_dir: Path,
    month: str,
    tiny_cfg: dict,
) -> None:
    def fake_artifact_path(component: str, subpath: str, *, create: bool = False) -> Path:
        if subpath.endswith(f"preprocessed/{month}"):
            return out_dir
        raise AssertionError(subpath)

    def fake_run_dir(*_args, **_kwargs) -> Path:
        run_path = out_dir / "run"
        run_path.mkdir(parents=True, exist_ok=True)
        return run_path

    monkeypatch.setattr("lichess_models.dataset.get_artifact_path", fake_artifact_path)
    monkeypatch.setattr("lichess_models.train.get_run_dir", fake_run_dir)


def _base_cfg(*, use_cv: bool) -> dict:
    return {
        "training": {
            "label_column": "player_outcome",
            "use_cv": use_cv,
            "cv_folds": 2,
            "scoring": "balanced_accuracy",
            "random_state": 0,
        },
        "model": {
            "search": "grid",
            "n_iter": 2,
            "candidates": ["logistic_regression"],
        },
        "features": load_config("lichess_models")["features"],
        "analyze": {"min_games": 1, "group_by": ["player_rating_bucket", "eco"]},
    }


def test_train_without_cv(synthetic_parquet, monkeypatch: pytest.MonkeyPatch) -> None:
    out_dir, month = synthetic_parquet
    tiny_cfg = _base_cfg(use_cv=False)
    _patch_training_env(monkeypatch, out_dir, month, tiny_cfg)

    with patch("lichess_models.train.load_config", return_value=tiny_cfg):
        with patch("lichess_models.evaluate.load_config", return_value=tiny_cfg):
            with patch("lichess_models.analyze.load_config", return_value=tiny_cfg):
                with patch("lichess_models.dataset.load_config", return_value=tiny_cfg):
                    result = run_train(month, config=tiny_cfg, run_id="run")

    metadata = json.loads((result.run_dir / "train_metadata.json").read_text())
    assert metadata["use_cv"] is False
    assert "best_train_score" in metadata
    assert "best_cv_score" not in metadata
    assert result.use_cv is False
    assert (result.run_dir / "model.joblib").exists()


def test_train_evaluate_analyze_smoke(synthetic_parquet, monkeypatch: pytest.MonkeyPatch) -> None:
    out_dir, month = synthetic_parquet
    tiny_cfg = _base_cfg(use_cv=True)
    _patch_training_env(monkeypatch, out_dir, month, tiny_cfg)

    with patch("lichess_models.train.load_config", return_value=tiny_cfg):
        with patch("lichess_models.evaluate.load_config", return_value=tiny_cfg):
            with patch("lichess_models.analyze.load_config", return_value=tiny_cfg):
                with patch("lichess_models.dataset.load_config", return_value=tiny_cfg):
                    result = run_train(month, config=tiny_cfg, run_id="run")
                    eval_result = run_evaluate(month, result.run_dir, config=tiny_cfg)
                    report_path = run_analyze(month, result.run_dir, config=tiny_cfg)

    metadata = json.loads((result.run_dir / "train_metadata.json").read_text())
    assert metadata["use_cv"] is True
    assert "best_cv_score" in metadata
    assert result.use_cv is True
    assert (result.run_dir / "model.joblib").exists()
    assert eval_result.metrics["accuracy"] >= 0.0
    assert report_path.exists()
