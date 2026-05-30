"""Evidently drift reports for Lichess model features."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from evidently import Report
from evidently.presets import DataDriftPreset
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, Field

DATA_DIR = Path(os.environ.get("EVIDENTLY_DATA_DIR", "/app/data"))
REPORTS_DIR = Path(os.environ.get("EVIDENTLY_REPORTS_DIR", "/app/reports"))
DEFAULT_REFERENCE = os.environ.get(
    "EVIDENTLY_REFERENCE_PATH", str(DATA_DIR / "reference.parquet")
)
DEFAULT_CURRENT = os.environ.get(
    "EVIDENTLY_CURRENT_PATH", str(DATA_DIR / "current.parquet")
)

app = FastAPI(title="lichess evidently")


class DriftRequest(BaseModel):
    reference_path: str | None = None
    current_path: str | None = None
    report_name: str | None = None
    sample_size: int = Field(default=5000, ge=100, le=100_000)


def _resolve_data_path(path: str) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = DATA_DIR / candidate
    if not candidate.is_file():
        raise HTTPException(status_code=404, detail=f"Data file not found: {candidate}")
    return candidate


def _load_frame(path: Path, sample_size: int) -> pd.DataFrame:
    if path.suffix == ".csv":
        frame = pd.read_csv(path)
    else:
        frame = pd.read_parquet(path)
    if len(frame) > sample_size:
        frame = frame.sample(n=sample_size, random_state=42)
    return frame


def _write_report(reference: pd.DataFrame, current: pd.DataFrame, report_name: str) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report = Report([DataDriftPreset()])
    snapshot = report.run(reference_data=reference, current_data=current)

    html_path = REPORTS_DIR / f"{report_name}.html"
    snapshot.save_html(str(html_path))
    return html_path


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "evidently-api", "reports_dir": str(REPORTS_DIR)}


@app.get("/reports")
async def list_reports() -> dict[str, list[str]]:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    reports = sorted(
        path.name for path in REPORTS_DIR.glob("*.html") if path.is_file()
    )
    return {"reports": reports}


@app.post("/reports/drift")
async def generate_drift_report(body: DriftRequest) -> dict[str, str]:
    reference_path = _resolve_data_path(body.reference_path or DEFAULT_REFERENCE)
    current_path = _resolve_data_path(body.current_path or DEFAULT_CURRENT)

    reference = _load_frame(reference_path, body.sample_size)
    current = _load_frame(current_path, body.sample_size)

    stamp = datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")
    report_name = body.report_name or f"drift-{stamp}"
    html_path = _write_report(reference, current, report_name)

    return {
        "report_name": report_name,
        "html_path": str(html_path),
        "reference_rows": str(len(reference)),
        "current_rows": str(len(current)),
    }


@app.get("/reports/{report_name}")
async def get_report(report_name: str) -> HTMLResponse:
    html_path = REPORTS_DIR / report_name
    if html_path.suffix != ".html":
        html_path = REPORTS_DIR / f"{report_name}.html"
    if not html_path.is_file():
        raise HTTPException(status_code=404, detail=f"Report not found: {report_name}")
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.get("/reports/{report_name}/download")
async def download_report(report_name: str) -> FileResponse:
    html_path = REPORTS_DIR / report_name
    if html_path.suffix != ".html":
        html_path = REPORTS_DIR / f"{report_name}.html"
    if not html_path.is_file():
        raise HTTPException(status_code=404, detail=f"Report not found: {report_name}")
    return FileResponse(html_path, media_type="text/html", filename=html_path.name)
