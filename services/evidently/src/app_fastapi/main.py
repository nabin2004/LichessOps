"""Evidently drift reports for Lichess model features."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from evidently import DataDefinition, Dataset, Report
from evidently.core.datasets import MulticlassClassification
from evidently.presets import ClassificationPreset, DataDriftPreset, DataSummaryPreset
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATA_DIR = Path(os.environ.get("EVIDENTLY_DATA_DIR", "/app/data"))
REPORTS_DIR = Path(os.environ.get("EVIDENTLY_REPORTS_DIR", "/app/reports"))
LOGS_DIR = Path(os.environ.get("EVIDENTLY_LOGS_DIR", "/app/logs"))
DEFAULT_REFERENCE = os.environ.get(
    "EVIDENTLY_REFERENCE_PATH", str(DATA_DIR / "reference.parquet")
)
DEFAULT_CURRENT = os.environ.get(
    "EVIDENTLY_CURRENT_PATH", str(DATA_DIR / "current.parquet")
)
DATA_SOURCE = os.environ.get("EVIDENTLY_DATA_SOURCE", "columnstore").strip().lower()

# Alert thresholds (overridable via env)
DRIFT_THRESHOLD = float(os.environ.get("DRIFT_THRESHOLD", "0.5"))
ACCURACY_THRESHOLD = float(os.environ.get("ACCURACY_THRESHOLD", "0.65"))
MISSING_THRESHOLD = float(os.environ.get("MISSING_THRESHOLD", "0.05"))

app = FastAPI(title="lichess-evidently")

# ---------------------------------------------------------------------------
# Shared models
# ---------------------------------------------------------------------------


class DriftRequest(BaseModel):
    reference_path: str | None = None
    current_path: str | None = None
    report_name: str | None = None
    sample_size: int = Field(default=5000, ge=100, le=100_000)
    month: str | None = None
    reference_month: str | None = None
    current_month: str | None = None
    data_source: str | None = None


class ClassificationRequest(BaseModel):
    reference_path: str | None = None
    current_path: str | None = None
    target_col: str = "target"
    prediction_col: str = "prediction"
    report_name: str | None = None
    sample_size: int = Field(default=5000, ge=100, le=100_000)
    month: str | None = None
    reference_month: str | None = None
    current_month: str | None = None
    data_source: str | None = None


class SliceRequest(BaseModel):
    data_path: str | None = None
    target_col: str = "target"
    prediction_col: str = "prediction"
    slice_cols: list[str] = Field(default=["game_type"])
    report_name: str | None = None
    sample_size: int = Field(default=5000, ge=100, le=100_000)
    month: str | None = None
    reference_month: str | None = None
    current_month: str | None = None
    data_source: str | None = None


class PredictionLogEntry(BaseModel):
    player_elo: int
    opponent_elo: int
    game_type: str
    prediction: int
    probabilities: list[float] | None = None
    timestamp: str | None = None


class AlertRequest(BaseModel):
    drift_score: float | None = None
    accuracy: float | None = None
    missing_ratio: float | None = None
    drift_threshold: float = DRIFT_THRESHOLD
    accuracy_threshold: float = ACCURACY_THRESHOLD
    missing_threshold: float = MISSING_THRESHOLD


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_data_path(path: str) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = DATA_DIR / candidate
    if not candidate.is_file():
        raise HTTPException(status_code=404, detail=f"Data file not found: {candidate}")
    return candidate


def _optional_data_path(path: str | None, default: str, data_source: str | None) -> Path | None:
    if _resolve_data_source(data_source) == "columnstore":
        return None
    return _resolve_data_path(path or default)


def _resolve_data_source(data_source: str | None) -> str:
    return (data_source or DATA_SOURCE).strip().lower()


def _resolve_month(
    body: DriftRequest | ClassificationRequest | SliceRequest,
    *,
    reference: bool,
) -> str | None:
    if reference and body.reference_month:
        return body.reference_month
    if not reference and body.current_month:
        return body.current_month
    return body.month


def _load_frame_from_columnstore(
    *,
    month: str | None,
    sample_size: int,
    reference: bool = False,
) -> pd.DataFrame:
    try:
        from lichess_libs.shared.columnstore import (
            fetch_batch_predictions_as_monitoring_frame,
            query_dataframe,
        )
    except ImportError as exc:
        raise HTTPException(
            status_code=503,
            detail="ColumnStore client not available in evidently container",
        ) from exc

    if month:
        if reference:
            sql = """
                SELECT y_true AS target, y_pred AS prediction, player_elo, opponent_elo,
                       eco, game_type
                FROM batch_predictions
                WHERE month = %s
                ORDER BY id ASC
                LIMIT %s
            """
            frame = query_dataframe(sql, (month, sample_size))
        else:
            frame = fetch_batch_predictions_as_monitoring_frame(month, limit=sample_size)
    else:
        frame = query_dataframe(
            """
            SELECT y_true AS target, y_pred AS prediction, player_elo, opponent_elo,
                   eco, game_type
            FROM batch_predictions
            ORDER BY id DESC
            LIMIT %s
            """,
            (sample_size,),
        )
    if frame.empty:
        raise HTTPException(status_code=404, detail="No batch predictions found in ColumnStore")
    if len(frame) > sample_size:
        frame = frame.sample(n=sample_size, random_state=42)
    return frame


def _load_frame(
    path: Path | None,
    sample_size: int,
    *,
    month: str | None = None,
    data_source: str | None = None,
    reference: bool = False,
) -> pd.DataFrame:
    if _resolve_data_source(data_source) == "columnstore":
        return _load_frame_from_columnstore(month=month, sample_size=sample_size, reference=reference)

    if path is None:
        raise HTTPException(status_code=404, detail="Data path required for non-columnstore mode")

    if path.suffix == ".csv":
        frame = pd.read_csv(path)
    else:
        frame = pd.read_parquet(path)
    if len(frame) > sample_size:
        frame = frame.sample(n=sample_size, random_state=42)
    return frame


def _stamp() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")


def _as_classification_datasets(
    reference: pd.DataFrame,
    current: pd.DataFrame,
    target_col: str = "target",
    prediction_col: str = "prediction",
) -> tuple[Dataset, Dataset]:
    data_definition = DataDefinition(
        classification=[
            MulticlassClassification(
                target=target_col,
                prediction_labels=prediction_col,
            )
        ]
    )
    return (
        Dataset.from_pandas(reference, data_definition=data_definition),
        Dataset.from_pandas(current, data_definition=data_definition),
    )


def _write_html_report(
    reference: pd.DataFrame,
    current: pd.DataFrame,
    presets: list,
    report_name: str,
    *,
    classification: bool = False,
    target_col: str = "target",
    prediction_col: str = "prediction",
) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report = Report(presets)
    if classification:
        reference, current = _as_classification_datasets(
            reference, current, target_col, prediction_col
        )
    snapshot = report.run(reference_data=reference, current_data=current)
    html_path = REPORTS_DIR / f"{report_name}.html"
    snapshot.save_html(str(html_path))
    return html_path


def _read_report_html(report_name: str) -> Path:
    html_path = REPORTS_DIR / report_name
    if html_path.suffix != ".html":
        html_path = REPORTS_DIR / f"{report_name}.html"
    if not html_path.is_file():
        raise HTTPException(status_code=404, detail=f"Report not found: {report_name}")
    return html_path


# ---------------------------------------------------------------------------
# Health / root
# ---------------------------------------------------------------------------


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "evidently-api", "reports_dir": str(REPORTS_DIR)}


# ---------------------------------------------------------------------------
# 1. Feature Drift  (original endpoint, kept intact)
# ---------------------------------------------------------------------------


@app.post("/reports/drift")
async def generate_drift_report(body: DriftRequest) -> dict[str, str]:
    reference = _load_frame(
        _optional_data_path(body.reference_path, DEFAULT_REFERENCE, body.data_source),
        body.sample_size,
        month=_resolve_month(body, reference=True),
        data_source=body.data_source,
        reference=True,
    )
    current = _load_frame(
        _optional_data_path(body.current_path, DEFAULT_CURRENT, body.data_source),
        body.sample_size,
        month=_resolve_month(body, reference=False),
        data_source=body.data_source,
    )

    report_name = body.report_name or f"drift-{_stamp()}"
    html_path = _write_html_report(reference, current, [DataDriftPreset()], report_name)

    return {
        "report_name": report_name,
        "html_path": str(html_path),
        "reference_rows": str(len(reference)),
        "current_rows": str(len(current)),
    }


# ---------------------------------------------------------------------------
# 2. Data Quality
# ---------------------------------------------------------------------------


@app.post("/reports/data-quality")
async def generate_data_quality_report(body: DriftRequest) -> dict[str, Any]:
    reference = _load_frame(
        _optional_data_path(body.reference_path, DEFAULT_REFERENCE, body.data_source),
        body.sample_size,
        month=_resolve_month(body, reference=True),
        data_source=body.data_source,
        reference=True,
    )
    current = _load_frame(
        _optional_data_path(body.current_path, DEFAULT_CURRENT, body.data_source),
        body.sample_size,
        month=_resolve_month(body, reference=False),
        data_source=body.data_source,
    )

    report_name = body.report_name or f"data-quality-{_stamp()}"
    html_path = _write_html_report(reference, current, [DataSummaryPreset()], report_name)

    # Quick summary stats on the current frame
    missing_ratio = float(current.isnull().mean().mean())
    duplicate_rows = int(current.duplicated().sum())
    constant_cols = [c for c in current.columns if current[c].nunique() <= 1]
    schema_mismatches = [
        c for c in reference.columns
        if c in current.columns and reference[c].dtype != current[c].dtype
    ]

    return {
        "report_name": report_name,
        "html_path": str(html_path),
        "missing_ratio": round(missing_ratio, 4),
        "duplicate_rows": duplicate_rows,
        "constant_columns": constant_cols,
        "schema_mismatches": schema_mismatches,
        "current_rows": len(current),
        "reference_rows": len(reference),
    }


# ---------------------------------------------------------------------------
# 3. Target Drift
# ---------------------------------------------------------------------------


@app.post("/reports/target-drift")
async def generate_target_drift_report(body: ClassificationRequest) -> dict[str, Any]:
    reference = _load_frame(
        _optional_data_path(body.reference_path, DEFAULT_REFERENCE, body.data_source),
        body.sample_size,
        month=_resolve_month(body, reference=True),
        data_source=body.data_source,
        reference=True,
    )
    current = _load_frame(
        _optional_data_path(body.current_path, DEFAULT_CURRENT, body.data_source),
        body.sample_size,
        month=_resolve_month(body, reference=False),
        data_source=body.data_source,
    )

    for col in (body.target_col, body.prediction_col):
        for df, label in ((reference, "reference"), (current, "current")):
            if col not in df.columns:
                raise HTTPException(status_code=422, detail=f"Column '{col}' missing from {label} data")

    report_name = body.report_name or f"target-drift-{_stamp()}"
    html_path = _write_html_report(
        reference[[body.target_col, body.prediction_col]],
        current[[body.target_col, body.prediction_col]],
        [DataDriftPreset(columns=[body.target_col])],
        report_name,
    )

    # Distribution comparison
    ref_dist = reference[body.target_col].value_counts(normalize=True).to_dict()
    cur_dist = current[body.target_col].value_counts(normalize=True).to_dict()

    return {
        "report_name": report_name,
        "html_path": str(html_path),
        "reference_distribution": {str(k): round(v, 4) for k, v in ref_dist.items()},
        "current_distribution": {str(k): round(v, 4) for k, v in cur_dist.items()},
    }


# ---------------------------------------------------------------------------
# 4. Prediction Drift
# ---------------------------------------------------------------------------


@app.post("/reports/prediction-drift")
async def generate_prediction_drift_report(body: ClassificationRequest) -> dict[str, Any]:
    reference = _load_frame(
        _optional_data_path(body.reference_path, DEFAULT_REFERENCE, body.data_source),
        body.sample_size,
        month=_resolve_month(body, reference=True),
        data_source=body.data_source,
        reference=True,
    )
    current = _load_frame(
        _optional_data_path(body.current_path, DEFAULT_CURRENT, body.data_source),
        body.sample_size,
        month=_resolve_month(body, reference=False),
        data_source=body.data_source,
    )

    for col in (body.prediction_col,):
        for df, label in ((reference, "reference"), (current, "current")):
            if col not in df.columns:
                raise HTTPException(status_code=422, detail=f"Column '{col}' missing from {label} data")

    report_name = body.report_name or f"prediction-drift-{_stamp()}"
    # Reuse DataDriftPreset on the prediction column only
    html_path = _write_html_report(
        reference[[body.prediction_col]],
        current[[body.prediction_col]],
        [DataDriftPreset()],
        report_name,
    )

    ref_mean = float(reference[body.prediction_col].mean())
    cur_mean = float(current[body.prediction_col].mean())
    ref_dist = reference[body.prediction_col].value_counts(normalize=True).to_dict()
    cur_dist = current[body.prediction_col].value_counts(normalize=True).to_dict()

    return {
        "report_name": report_name,
        "html_path": str(html_path),
        "reference_mean_prediction": round(ref_mean, 4),
        "current_mean_prediction": round(cur_mean, 4),
        "reference_distribution": {str(k): round(v, 4) for k, v in ref_dist.items()},
        "current_distribution": {str(k): round(v, 4) for k, v in cur_dist.items()},
    }


# ---------------------------------------------------------------------------
# 5. Classification Performance
# ---------------------------------------------------------------------------


@app.post("/reports/classification-performance")
async def generate_classification_performance_report(body: ClassificationRequest) -> dict[str, Any]:
    reference = _load_frame(
        _optional_data_path(body.reference_path, DEFAULT_REFERENCE, body.data_source),
        body.sample_size,
        month=_resolve_month(body, reference=True),
        data_source=body.data_source,
        reference=True,
    )
    current = _load_frame(
        _optional_data_path(body.current_path, DEFAULT_CURRENT, body.data_source),
        body.sample_size,
        month=_resolve_month(body, reference=False),
        data_source=body.data_source,
    )

    for col in (body.target_col, body.prediction_col):
        for df, label in ((reference, "reference"), (current, "current")):
            if col not in df.columns:
                raise HTTPException(status_code=422, detail=f"Column '{col}' missing from {label} data")

    # Evidently expects columns named "target" and "prediction"
    ref_renamed = reference.rename(columns={body.target_col: "target", body.prediction_col: "prediction"})
    cur_renamed = current.rename(columns={body.target_col: "target", body.prediction_col: "prediction"})

    report_name = body.report_name or f"classification-{_stamp()}"
    html_path = _write_html_report(
        ref_renamed,
        cur_renamed,
        [ClassificationPreset()],
        report_name,
        classification=True,
    )

    # Quick sklearn metrics
    try:
        from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

        y_true = current[body.target_col]
        y_pred = current[body.prediction_col]
        avg = "weighted" if y_true.nunique() > 2 else "binary"

        metrics = {
            "accuracy": round(accuracy_score(y_true, y_pred), 4),
            "precision": round(precision_score(y_true, y_pred, average=avg, zero_division=0), 4),
            "recall": round(recall_score(y_true, y_pred, average=avg, zero_division=0), 4),
            "f1": round(f1_score(y_true, y_pred, average=avg, zero_division=0), 4),
        }
    except ImportError:
        metrics = {"note": "sklearn not installed; see HTML report for metrics"}

    return {"report_name": report_name, "html_path": str(html_path), **metrics}


# ---------------------------------------------------------------------------
# 6. Slice Performance
# ---------------------------------------------------------------------------


@app.post("/reports/performance-slices")
async def generate_slice_performance_report(body: SliceRequest) -> dict[str, Any]:
    data_path = _optional_data_path(body.data_path, DEFAULT_CURRENT, body.data_source)
    df = _load_frame(
        data_path,
        body.sample_size,
        month=_resolve_month(body, reference=False),
        data_source=body.data_source,
    )

    for col in [body.target_col, body.prediction_col, *body.slice_cols]:
        if col not in df.columns:
            raise HTTPException(status_code=422, detail=f"Column '{col}' not found in data")

    try:
        from sklearn.metrics import accuracy_score, f1_score

        slices: dict[str, Any] = {}
        for slice_col in body.slice_cols:
            slices[slice_col] = {}
            for val, group in df.groupby(slice_col):
                y_true = group[body.target_col]
                y_pred = group[body.prediction_col]
                avg = "weighted" if y_true.nunique() > 2 else "binary"
                slices[slice_col][str(val)] = {
                    "n": len(group),
                    "accuracy": round(accuracy_score(y_true, y_pred), 4),
                    "f1": round(f1_score(y_true, y_pred, average=avg, zero_division=0), 4),
                }
    except ImportError:
        raise HTTPException(status_code=501, detail="sklearn required for slice performance")

    report_name = body.report_name or f"slices-{_stamp()}"
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    json_path = REPORTS_DIR / f"{report_name}.json"
    json_path.write_text(json.dumps(slices, indent=2))

    return {"report_name": report_name, "json_path": str(json_path), "slices": slices}


# ---------------------------------------------------------------------------
# 7. Schema Validation
# ---------------------------------------------------------------------------


@app.post("/reports/schema-validation")
async def schema_validation(body: DriftRequest) -> dict[str, Any]:
    reference = _load_frame(
        _optional_data_path(body.reference_path, DEFAULT_REFERENCE, body.data_source),
        body.sample_size,
        month=_resolve_month(body, reference=True),
        data_source=body.data_source,
        reference=True,
    )
    current = _load_frame(
        _optional_data_path(body.current_path, DEFAULT_CURRENT, body.data_source),
        body.sample_size,
        month=_resolve_month(body, reference=False),
        data_source=body.data_source,
    )

    ref_cols = set(reference.columns)
    cur_cols = set(current.columns)

    missing_in_current = sorted(ref_cols - cur_cols)
    new_in_current = sorted(cur_cols - ref_cols)
    dtype_mismatches = {
        col: {"reference": str(reference[col].dtype), "current": str(current[col].dtype)}
        for col in ref_cols & cur_cols
        if reference[col].dtype != current[col].dtype
    }

    # Check for new unseen categories in object columns
    new_categories: dict[str, list] = {}
    for col in ref_cols & cur_cols:
        if reference[col].dtype == object:
            ref_cats = set(reference[col].dropna().unique())
            cur_cats = set(current[col].dropna().unique())
            new_cats = sorted(cur_cats - ref_cats)
            if new_cats:
                new_categories[col] = new_cats

    is_valid = not (missing_in_current or dtype_mismatches or new_categories)

    return {
        "valid": is_valid,
        "missing_columns_in_current": missing_in_current,
        "new_columns_in_current": new_in_current,
        "dtype_mismatches": dtype_mismatches,
        "new_unseen_categories": new_categories,
    }


# ---------------------------------------------------------------------------
# 8. Prediction Log
# ---------------------------------------------------------------------------


@app.post("/monitor/prediction-logs")
async def log_prediction(entry: PredictionLogEntry) -> dict[str, str]:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOGS_DIR / "prediction_logs.jsonl"

    record = entry.model_dump()
    record["timestamp"] = record["timestamp"] or datetime.now(tz=UTC).isoformat()

    with log_file.open("a") as f:
        f.write(json.dumps(record) + "\n")

    if _resolve_data_source(None) == "columnstore":
        try:
            from lichess_libs.shared.columnstore import insert_prediction_log

            outcome_map = {0: "0", 1: "1", 2: "½"}
            probs = entry.probabilities or []
            insert_prediction_log(
                player_elo=entry.player_elo,
                opponent_elo=entry.opponent_elo,
                predicted_outcome=outcome_map.get(entry.prediction, str(entry.prediction)),
                probabilities={
                    "lose": probs[0] if len(probs) > 0 else None,
                    "win": probs[1] if len(probs) > 1 else None,
                    "draw": probs[2] if len(probs) > 2 else None,
                },
                game_type=entry.game_type,
                source="evidently",
            )
        except Exception:
            pass

    return {"status": "logged", "log_file": str(log_file)}


@app.get("/monitor/prediction-logs")
async def get_prediction_logs(limit: int = 100, data_source: str | None = None) -> dict[str, Any]:
    if _resolve_data_source(data_source) == "columnstore":
        try:
            from lichess_libs.shared.columnstore import fetch_prediction_logs

            logs = fetch_prediction_logs(limit=limit)
            return {"logs": logs, "total": len(logs), "source": "columnstore"}
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"ColumnStore read failed: {exc}") from exc

    log_file = LOGS_DIR / "prediction_logs.jsonl"
    if not log_file.is_file():
        return {"logs": [], "total": 0, "source": "parquet"}

    lines = log_file.read_text().strip().splitlines()
    logs = [json.loads(line) for line in lines[-limit:]]
    return {"logs": logs, "total": len(lines), "source": "parquet"}


# ---------------------------------------------------------------------------
# 9. Model Health Summary
# ---------------------------------------------------------------------------


@app.get("/reports/summary")
async def get_summary_report() -> dict[str, Any]:
    """Aggregate last known metrics from saved report metadata."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    reports = sorted(REPORTS_DIR.glob("*.html"), key=lambda p: p.stat().st_mtime, reverse=True)
    last_report = reports[0].name if reports else None

    # Pull last prediction log stats if available
    log_file = LOGS_DIR / "prediction_logs.jsonl"
    prediction_summary: dict[str, Any] = {}
    if log_file.is_file():
        lines = log_file.read_text().strip().splitlines()
        if lines:
            logs = [json.loads(l) for l in lines[-1000:]]
            preds = [l["prediction"] for l in logs]
            prediction_summary = {
                "total_logged": len(lines),
                "last_1000_distribution": {
                    str(k): round(v / len(preds), 4)
                    for k, v in pd.Series(preds).value_counts().items()
                },
            }

    return {
        "status": "ok",
        "total_reports": len(reports),
        "latest_report": last_report,
        "prediction_log": prediction_summary,
        "thresholds": {
            "drift": DRIFT_THRESHOLD,
            "accuracy": ACCURACY_THRESHOLD,
            "missing": MISSING_THRESHOLD,
        },
    }


# ---------------------------------------------------------------------------
# 10. Alert Evaluation
# ---------------------------------------------------------------------------


@app.post("/alerts/evaluate")
async def evaluate_alerts(body: AlertRequest) -> dict[str, Any]:
    alerts: list[dict[str, Any]] = []

    if body.drift_score is not None and body.drift_score > body.drift_threshold:
        alerts.append({
            "type": "drift",
            "severity": "high" if body.drift_score > 0.75 else "medium",
            "message": f"Drift score {body.drift_score:.3f} exceeds threshold {body.drift_threshold}",
        })

    if body.accuracy is not None and body.accuracy < body.accuracy_threshold:
        alerts.append({
            "type": "accuracy",
            "severity": "high" if body.accuracy < 0.55 else "medium",
            "message": f"Accuracy {body.accuracy:.3f} below threshold {body.accuracy_threshold}",
        })

    if body.missing_ratio is not None and body.missing_ratio > body.missing_threshold:
        alerts.append({
            "type": "data_quality",
            "severity": "medium",
            "message": f"Missing value ratio {body.missing_ratio:.3f} exceeds threshold {body.missing_threshold}",
        })

    return {
        "alert_count": len(alerts),
        "alerts": alerts,
        "evaluated_at": datetime.now(tz=UTC).isoformat(),
    }




@app.get("/reports")
async def list_reports() -> dict[str, list[str]]:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    reports = sorted(p.name for p in REPORTS_DIR.glob("*.html") if p.is_file())
    return {"reports": reports}


@app.get("/reports/{report_name}")
async def get_report(report_name: str) -> HTMLResponse:
    html_path = _read_report_html(report_name)
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.get("/reports/{report_name}/download")
async def download_report(report_name: str) -> FileResponse:
    html_path = _read_report_html(report_name)
    return FileResponse(html_path, media_type="text/html", filename=html_path.name)