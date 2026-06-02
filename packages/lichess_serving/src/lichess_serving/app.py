"""FastAPI application for game outcome prediction."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from prometheus_client import Counter, Gauge
from prometheus_fastapi_instrumentator import Instrumentator

from lichess_libs.shared import load_config

from lichess_models.dataset import OUTCOME_DISPLAY, OUTCOME_PROB_KEYS
from lichess_models.features import build_inference_row

from lichess_serving.schemas import HealthResponse, PredictRequest, PredictResponse

_pipeline: Any = None
MODEL_LOADED = Gauge(
    "lichess_model_loaded",
    "Whether the outcome prediction model is loaded (1=yes, 0=no)",
)
PREDICTIONS_TOTAL = Counter(
    "lichess_predictions_total",
    "Total successful /predict responses served since process start",
)


def _model_uri() -> str:
    cfg = load_config("lichess_serving")
    return os.environ.get(
        "MODEL_URI",
        (cfg.get("model") or {}).get("uri", "models:/lichess-outcome-predictor/Staging"),
    )


def _load_model() -> Any:
    global _pipeline
    if _pipeline is not None:
        return _pipeline

    uri = _model_uri()
    if uri.endswith(".joblib") or uri.startswith("/"):
        import joblib

        _pipeline = joblib.load(uri)
        return _pipeline

    try:
        import mlflow
    except ImportError as exc:
        raise RuntimeError(
            "MLflow is required for registry URIs. Install with: "
            "uv sync --package lichess-serving --extra ml"
        ) from exc

    tracking = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(tracking)
    _pipeline = mlflow.sklearn.load_model(uri)
    return _pipeline


def _sync_model_metric() -> bool:
    loaded = _pipeline is not None
    MODEL_LOADED.set(1 if loaded else 0)
    return loaded


@asynccontextmanager
async def lifespan(_app: FastAPI):
    try:
        _load_model()
    except Exception:
        pass
    _sync_model_metric()
    yield


app = FastAPI(title="Lichess Outcome Predictor", lifespan=lifespan)
Instrumentator().instrument(app).expose(app, include_in_schema=False)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    loaded = _pipeline is not None
    try:
        if not loaded:
            _load_model()
            loaded = True
    except Exception:
        loaded = False
    _sync_model_metric()
    return HealthResponse(status="ok" if loaded else "degraded", model_loaded=loaded)


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest) -> PredictResponse:
    try:
        pipeline = _load_model()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Model unavailable: {exc}") from exc

    payload = request.model_dump(exclude_none=True)
    row = build_inference_row(payload)
    pred = int(pipeline.predict(row)[0])
    proba = pipeline.predict_proba(row)[0]

    probabilities = {
        OUTCOME_PROB_KEYS[i]: float(proba[i]) for i in range(len(proba))
    }

    PREDICTIONS_TOTAL.inc()

    return PredictResponse(
        predicted_outcome=OUTCOME_DISPLAY[pred],
        probabilities=probabilities,
        recommended_opening_score=probabilities.get("win"),
    )
