"""Train outcome prediction models with sklearn hyperparameter search."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV, TimeSeriesSplit
from sklearn.pipeline import Pipeline

from lichess_libs.shared import get_logger, get_run_dir, load_config

from lichess_models.dataset import load_split, split_features_labels, to_player_perspective
from lichess_models.features import build_preprocessor

log = get_logger("lichess_models.train")

PARAM_GRIDS: dict[str, dict[str, list[Any]]] = {
    "logistic_regression": {
        "classifier__C": [0.01, 0.1, 1.0, 10.0],
        "classifier__class_weight": [None, "balanced"],
    },
    "random_forest": {
        "classifier__n_estimators": [100, 200, 300],
        "classifier__max_depth": [None, 10, 20],
        "classifier__min_samples_leaf": [1, 5, 10],
        "classifier__class_weight": [None, "balanced"],
    },
    "hist_gradient_boosting": {
        "classifier__max_depth": [None, 6, 10],
        "classifier__learning_rate": [0.05, 0.1, 0.2],
        "classifier__max_iter": [100, 200],
        "classifier__min_samples_leaf": [10, 20],
    },
}


@dataclass
class TrainResult:
    pipeline: Pipeline
    run_dir: Path
    best_estimator_name: str
    best_cv_score: float
    best_params: dict[str, Any]
    month: str


def _build_classifier(name: str, random_state: int):
    if name == "logistic_regression":
        return LogisticRegression(
            max_iter=1000,
            solver="lbfgs",
            random_state=random_state,
        )
    if name == "random_forest":
        return RandomForestClassifier(random_state=random_state, n_jobs=-1)
    if name == "hist_gradient_boosting":
        return HistGradientBoostingClassifier(random_state=random_state)
    raise ValueError(f"Unknown estimator: {name}")


def _build_pipeline(estimator_name: str, config: dict) -> Pipeline:
    training_cfg = config.get("training") or {}
    random_state = int(training_cfg.get("random_state", 42))
    return Pipeline(
        [
            ("preprocessor", build_preprocessor(config)),
            ("classifier", _build_classifier(estimator_name, random_state)),
        ]
    )


def _make_search(
    pipeline: Pipeline,
    param_grid: dict[str, list[Any]],
    cv,
    scoring: str,
    search_mode: str,
    n_iter: int,
    random_state: int,
):
    if search_mode == "grid":
        return GridSearchCV(
            pipeline,
            param_grid=param_grid,
            cv=cv,
            scoring=scoring,
            n_jobs=-1,
            refit=True,
        )
    return RandomizedSearchCV(
        pipeline,
        param_distributions=param_grid,
        n_iter=n_iter,
        cv=cv,
        scoring=scoring,
        n_jobs=-1,
        random_state=random_state,
        refit=True,
    )


def run_train(month: str, *, config: dict | None = None, run_id: str | None = None) -> TrainResult:
    cfg = config or load_config("lichess_models")
    training_cfg = cfg.get("training") or {}
    model_cfg = cfg.get("model") or {}

    random_state = int(training_cfg.get("random_state", 42))
    cv_folds = int(training_cfg.get("cv_folds", 3))
    scoring = training_cfg.get("scoring", "balanced_accuracy")
    search_mode = model_cfg.get("search", "randomized")
    n_iter = int(model_cfg.get("n_iter", 24))
    candidates = list(model_cfg.get("candidates") or ["random_forest"])

    train_df = to_player_perspective(load_split(month, split="train"))
    X, y, _meta = split_features_labels(train_df, cfg)

    cv = TimeSeriesSplit(n_splits=cv_folds)

    best_pipeline: Pipeline | None = None
    best_score = float("-inf")
    best_name = ""
    best_params: dict[str, Any] = {}

    for name in candidates:
        log.info("Hyperparameter search for %s", name)
        pipeline = _build_pipeline(name, cfg)
        param_grid = PARAM_GRIDS[name]
        search = _make_search(
            pipeline,
            param_grid,
            cv,
            scoring,
            search_mode,
            n_iter,
            random_state,
        )
        search.fit(X, y)
        log.info(
            "  %s best %s=%.4f params=%s",
            name,
            scoring,
            search.best_score_,
            search.best_params_,
        )
        if search.best_score_ > best_score:
            best_score = search.best_score_
            best_pipeline = search.best_estimator_
            best_name = name
            best_params = dict(search.best_params_)

    if best_pipeline is None:
        raise RuntimeError("No estimator was trained")

    run_dir = get_run_dir("lichess_models", run_id)
    model_path = run_dir / "model.joblib"
    joblib.dump(best_pipeline, model_path)

    metadata = {
        "month": month,
        "best_estimator": best_name,
        "best_cv_score": best_score,
        "best_params": best_params,
        "scoring": scoring,
        "n_train_rows": len(X),
    }
    (run_dir / "train_metadata.json").write_text(json.dumps(metadata, indent=2))

    log.info("Saved model → %s (estimator=%s, cv=%.4f)", model_path, best_name, best_score)
    return TrainResult(
        pipeline=best_pipeline,
        run_dir=run_dir,
        best_estimator_name=best_name,
        best_cv_score=best_score,
        best_params=best_params,
        month=month,
    )


def load_pipeline(run_dir: Path) -> Pipeline:
    path = run_dir / "model.joblib"
    if not path.is_file():
        raise FileNotFoundError(f"Model not found: {path}")
    return joblib.load(path)
