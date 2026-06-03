"""Train outcome prediction models with sklearn hyperparameter search."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import get_scorer
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV, TimeSeriesSplit
from sklearn.pipeline import Pipeline

from lichess_libs.shared import get_logger, get_run_dir, load_config

from lichess_models.dataset import (
    load_game_splits,
    split_features_labels,
    to_player_perspective,
)
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
class CandidateResult:
    pipeline: Pipeline
    score: float
    params: dict[str, Any]


@dataclass
class TrainResult:
    pipeline: Pipeline
    run_dir: Path
    best_estimator_name: str
    best_score: float
    best_params: dict[str, Any]
    month: str
    use_cv: bool
    candidates: dict[str, CandidateResult]


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


def _score_pipeline(pipeline: Pipeline, X, y, scoring: str) -> float:
    scorer = get_scorer(scoring)
    return float(scorer(pipeline, X, y))


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


def _train_with_cv(
    X,
    y,
    *,
    cfg: dict,
    candidates: list[str],
    scoring: str,
    cv_folds: int,
    search_mode: str,
    n_iter: int,
    random_state: int,
) -> tuple[Pipeline, str, float, dict[str, Any], dict[str, CandidateResult]]:
    cv = TimeSeriesSplit(n_splits=cv_folds)
    best_pipeline: Pipeline | None = None
    best_score = float("-inf")
    best_name = ""
    best_params: dict[str, Any] = {}
    candidate_results: dict[str, CandidateResult] = {}

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
        candidate_results[name] = CandidateResult(
            pipeline=search.best_estimator_,
            score=float(search.best_score_),
            params=dict(search.best_params_),
        )
        if search.best_score_ > best_score:
            best_score = search.best_score_
            best_pipeline = search.best_estimator_
            best_name = name
            best_params = dict(search.best_params_)

    if best_pipeline is None:
        raise RuntimeError("No estimator was trained")

    return best_pipeline, best_name, best_score, best_params, candidate_results


def _train_without_cv(
    X,
    y,
    *,
    cfg: dict,
    candidates: list[str],
    scoring: str,
) -> tuple[Pipeline, str, float, dict[str, Any], dict[str, CandidateResult]]:
    best_pipeline: Pipeline | None = None
    best_score = float("-inf")
    best_name = ""
    candidate_results: dict[str, CandidateResult] = {}

    for name in candidates:
        log.info("Training %s (no CV)", name)
        pipeline = _build_pipeline(name, cfg)
        pipeline.fit(X, y)
        score = _score_pipeline(pipeline, X, y, scoring)
        log.info("  %s %s=%.4f", name, scoring, score)
        candidate_results[name] = CandidateResult(
            pipeline=pipeline,
            score=score,
            params={},
        )
        if score > best_score:
            best_score = score
            best_pipeline = pipeline
            best_name = name

    if best_pipeline is None:
        raise RuntimeError("No estimator was trained")

    return best_pipeline, best_name, best_score, {}, candidate_results


def run_train(
    month: str,
    *,
    config: dict | None = None,
    run_id: str | None = None,
    use_cv: bool | None = None,
    use_sample: bool | None = None,
    max_rows: int | None = None,
) -> TrainResult:
    cfg = config or load_config("lichess_models")
    training_cfg = cfg.get("training") or {}
    model_cfg = cfg.get("model") or {}

    if use_cv is None:
        use_cv = bool(training_cfg.get("use_cv", False))
    if use_sample is None:
        use_sample = bool(training_cfg.get("use_sample", False))
    if max_rows is None and use_sample:
        max_rows = int(training_cfg.get("max_rows", 1000))

    random_state = int(training_cfg.get("random_state", 42))
    test_size = float(training_cfg.get("test_size", 0.2))
    cv_folds = int(training_cfg.get("cv_folds", 3))
    scoring = training_cfg.get("scoring", "balanced_accuracy")
    search_mode = model_cfg.get("search", "randomized")
    n_iter = int(model_cfg.get("n_iter", 24))
    candidates = list(model_cfg.get("candidates") or ["random_forest"])

    train_games, _test_games = load_game_splits(
        month,
        use_sample=use_sample,
        max_rows=max_rows,
        test_size=test_size,
    )
    train_df = to_player_perspective(train_games)
    X, y, _meta = split_features_labels(train_df, cfg)
    n_train_games = len(train_games)

    if use_cv:
        best_pipeline, best_name, best_score, best_params, candidate_results = _train_with_cv(
            X,
            y,
            cfg=cfg,
            candidates=candidates,
            scoring=scoring,
            cv_folds=cv_folds,
            search_mode=search_mode,
            n_iter=n_iter,
            random_state=random_state,
        )
    else:
        best_pipeline, best_name, best_score, best_params, candidate_results = _train_without_cv(
            X,
            y,
            cfg=cfg,
            candidates=candidates,
            scoring=scoring,
        )

    run_dir = get_run_dir("lichess_models", run_id)
    model_path = run_dir / "model.joblib"
    joblib.dump(best_pipeline, model_path)

    models_dir = run_dir / "models"
    models_dir.mkdir(exist_ok=True)
    candidate_metadata: dict[str, dict[str, Any]] = {}
    for name, result in candidate_results.items():
        joblib.dump(result.pipeline, models_dir / f"{name}.joblib")
        candidate_metadata[name] = {
            "score": result.score,
            "params": result.params,
        }

    metadata: dict[str, Any] = {
        "month": month,
        "best_estimator": best_name,
        "best_params": best_params,
        "scoring": scoring,
        "n_train_rows": len(X),
        "n_train_games": n_train_games,
        "n_train_player_rows": len(X),
        "use_cv": use_cv,
        "use_sample": use_sample,
        "max_rows": max_rows,
        "candidates": candidate_metadata,
    }
    if use_cv:
        metadata["best_cv_score"] = best_score
    else:
        metadata["best_train_score"] = best_score

    (run_dir / "train_metadata.json").write_text(json.dumps(metadata, indent=2))

    score_label = "cv" if use_cv else "train"
    log.info(
        "Saved model → %s (estimator=%s, %s=%.4f)",
        model_path,
        best_name,
        score_label,
        best_score,
    )
    return TrainResult(
        pipeline=best_pipeline,
        run_dir=run_dir,
        best_estimator_name=best_name,
        best_score=best_score,
        best_params=best_params,
        month=month,
        use_cv=use_cv,
        candidates=candidate_results,
    )


def load_pipeline(run_dir: Path) -> Pipeline:
    path = run_dir / "model.joblib"
    if not path.is_file():
        raise FileNotFoundError(f"Model not found: {path}")
    return joblib.load(path)
