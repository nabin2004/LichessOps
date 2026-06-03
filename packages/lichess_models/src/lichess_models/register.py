"""MLflow model logging and registry helpers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from lichess_libs.shared import get_logger, load_config

from lichess_models.dataset import load_split, split_features_labels, to_player_perspective
from lichess_models.train import load_pipeline

log = get_logger("lichess_models.register")


def _tracking_uri() -> str:
    return os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")


def start_training_run(
    month: str,
    *,
    config: dict | None = None,
    run_name: str | None = None,
):
    try:
        import mlflow
    except ImportError as exc:
        raise RuntimeError(
            "MLflow is not installed. Install with: uv sync --package lichess-models --extra ml"
        ) from exc

    cfg = config or load_config("lichess_models")
    mlflow_cfg = cfg.get("mlflow") or {}
    mlflow.set_tracking_uri(_tracking_uri())
    experiment = mlflow_cfg.get("experiment_name", "lichess_outcome_prediction")
    mlflow.set_experiment(experiment)
    return mlflow.start_run(run_name=run_name or f"outcome-{month}")


def log_training_run(
    run_dir: Path,
    month: str,
    *,
    train_metadata: dict[str, Any],
    metrics: dict[str, float],
    config: dict | None = None,
    register: bool = True,
) -> str:
    """Log model, metrics, and artifacts to MLflow; optionally register."""
    try:
        import mlflow
        from mlflow.models import infer_signature
    except ImportError as exc:
        raise RuntimeError(
            "MLflow is not installed. Install with: uv sync --package lichess-models --extra ml"
        ) from exc

    cfg = config or load_config("lichess_models")
    mlflow_cfg = cfg.get("mlflow") or {}
    mlflow.set_tracking_uri(_tracking_uri())

    pipeline = load_pipeline(run_dir)
    train_df = to_player_perspective(load_split(month, split="train"))
    X_sample, _, _ = split_features_labels(train_df.head(100), cfg)
    signature = infer_signature(X_sample, pipeline.predict(X_sample))

    mlflow.log_params(
        {
            "month": month,
            "best_estimator": train_metadata.get("best_estimator", ""),
            "scoring": train_metadata.get("scoring", ""),
            "use_cv": train_metadata.get("use_cv", False),
            **{
                f"best_{k}": v
                for k, v in (train_metadata.get("best_params") or {}).items()
            },
        }
    )
    if train_metadata.get("use_cv"):
        mlflow.log_metric("cv_score", float(train_metadata.get("best_cv_score", 0)))
    else:
        mlflow.log_metric("train_score", float(train_metadata.get("best_train_score", 0)))
    for key, value in metrics.items():
        mlflow.log_metric(f"test_{key}", value)

    for artifact in (
        "train_metadata.json",
        "metrics.json",
        "classification_report.txt",
        "confusion_matrix.csv",
        "opening_weakness.csv",
    ):
        path = run_dir / artifact
        if path.is_file():
            mlflow.log_artifact(str(path))

    mlflow.sklearn.log_model(
        pipeline,
        artifact_path="model",
        signature=signature,
        input_example=X_sample.head(5),
    )

    run_id = mlflow.active_run().info.run_id if mlflow.active_run() else ""

    if register:
        model_name = mlflow_cfg.get("registered_model_name", "lichess-outcome-predictor")
        stage = mlflow_cfg.get("default_stage", "Staging")
        try:
            result = mlflow.register_model(f"runs:/{run_id}/model", model_name)
            client = mlflow.tracking.MlflowClient()
            client.transition_model_version_stage(
                name=model_name,
                version=result.version,
                stage=stage,
                archive_existing_versions=False,
            )
            log.info("Registered model %s version %s → %s", model_name, result.version, stage)
        except Exception as exc:
            log.warning("Model registry unavailable (%s); artifacts logged to run only", exc)

    return run_id


def run_register(run_dir: Path, month: str, *, config: dict | None = None) -> str:
    """Register an existing local training run with MLflow."""
    import json

    cfg = config or load_config("lichess_models")
    metadata_path = run_dir / "train_metadata.json"
    metrics_path = run_dir / "metrics.json"
    train_metadata = json.loads(metadata_path.read_text()) if metadata_path.is_file() else {}
    metrics = json.loads(metrics_path.read_text()) if metrics_path.is_file() else {}

    with start_training_run(month, config=cfg, run_name=f"register-{month}"):
        return log_training_run(
            run_dir,
            month,
            train_metadata=train_metadata,
            metrics=metrics,
            config=cfg,
            register=True,
        )
