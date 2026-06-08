"""Evaluate trained outcome prediction models."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    log_loss,
)
from sklearn.pipeline import Pipeline

from lichess_libs.shared import get_logger, load_config

from lichess_models.dataset import (
    OUTCOME_DISPLAY,
    load_game_splits,
    split_features_labels,
    to_player_perspective,
)
from lichess_models.train import load_pipeline

log = get_logger("lichess_models.evaluate")


@dataclass
class EvalResult:
    metrics: dict[str, float]
    confusion: np.ndarray
    report: str
    predictions: pd.DataFrame


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray, y_proba: np.ndarray) -> dict[str, float]:
    metrics = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "balanced_accuracy": float(balanced_accuracy_score(y_true, y_pred)),
        "macro_f1": float(f1_score(y_true, y_pred, average="macro")),
    }
    try:
        metrics["log_loss"] = float(log_loss(y_true, y_proba, labels=[0, 1, 2]))
    except ValueError:
        metrics["log_loss"] = float("nan")
    return metrics


def persist_predictions_to_columnstore(
    predictions: pd.DataFrame,
    *,
    run_id: str,
    month: str,
    model_uri: str | None = None,
    metrics: dict[str, float] | None = None,
) -> int:
    """Write batch evaluation predictions and run metadata to ColumnStore."""
    from lichess_libs.shared.columnstore import (
        insert_batch_predictions,
        ping,
        record_inference_run,
    )

    if not ping():
        log.warning("ColumnStore unavailable; skipping prediction persistence")
        return 0

    count = insert_batch_predictions(
        predictions,
        run_id=run_id,
        month=month,
        model_uri=model_uri,
        source="evaluate",
    )
    record_inference_run(
        run_id=run_id,
        month=month,
        source="evaluate",
        row_count=count,
        model_uri=model_uri,
        metrics=metrics,
    )
    log.info("Persisted %s batch predictions to ColumnStore (run_id=%s)", count, run_id)
    return count


def run_evaluate(
    month: str,
    run_dir: Path,
    *,
    config: dict | None = None,
    split: str = "test",
    use_sample: bool | None = None,
    max_rows: int | None = None,
    persist_columnstore: bool = False,
    model_uri: str | None = None,
) -> EvalResult:
    cfg = config or load_config("lichess_models")
    training_cfg = cfg.get("training") or {}
    pipeline: Pipeline = load_pipeline(run_dir)

    if use_sample is None:
        use_sample = bool(training_cfg.get("use_sample", False))
    if max_rows is None and use_sample:
        max_rows = int(training_cfg.get("max_rows", 1000))
    test_size = float(training_cfg.get("test_size", 0.2))

    _train_games, test_games = load_game_splits(
        month,
        use_sample=use_sample,
        max_rows=max_rows,
        test_size=test_size,
    )
    df = to_player_perspective(test_games)
    X, y, meta = split_features_labels(df, cfg)

    y_pred = pipeline.predict(X)
    y_proba = pipeline.predict_proba(X)

    metrics = compute_metrics(y.to_numpy(), y_pred, y_proba)
    cm = confusion_matrix(y, y_pred, labels=[0, 1, 2])
    report = classification_report(y, y_pred, target_names=["lose", "win", "draw"])

    predictions = meta.copy()
    predictions["y_true"] = y.values
    predictions["y_pred"] = y_pred
    predictions["pred_display"] = [OUTCOME_DISPLAY[int(v)] for v in y_pred]
    predictions["prob_lose"] = y_proba[:, 0]
    predictions["prob_win"] = y_proba[:, 1]
    predictions["prob_draw"] = y_proba[:, 2]

    out_dir = run_dir
    (out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))
    (out_dir / "classification_report.txt").write_text(report)
    np.savetxt(out_dir / "confusion_matrix.csv", cm, fmt="%d", delimiter=",")

    log.info(
        "Evaluation on %s (%s): accuracy=%.4f balanced_accuracy=%.4f macro_f1=%.4f",
        split,
        month,
        metrics["accuracy"],
        metrics["balanced_accuracy"],
        metrics["macro_f1"],
    )

    if persist_columnstore:
        run_id = run_dir.name
        resolved_uri = model_uri
        if resolved_uri is None:
            metadata_path = run_dir / "train_metadata.json"
            if metadata_path.is_file():
                resolved_uri = json.loads(metadata_path.read_text()).get("model_uri")
        persist_predictions_to_columnstore(
            predictions,
            run_id=run_id,
            month=month,
            model_uri=resolved_uri,
            metrics=metrics,
        )

    return EvalResult(metrics=metrics, confusion=cm, report=report, predictions=predictions)


def log_metrics_to_mlflow(metrics: dict[str, float], run_id: str | None = None) -> None:
    try:
        import mlflow
    except ImportError:
        return
    if run_id:
        mlflow.start_run(run_id=run_id)
    for key, value in metrics.items():
        if not np.isnan(value):
            mlflow.log_metric(key, value)
