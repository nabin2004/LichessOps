"""Scheduled drift and data-quality monitoring via Evidently API."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from datetime import datetime

from airflow.sdk import dag, task
from slack_callbacks import (
    dag_failure_slack_webhook_notification,
    task_failure_slack_webhook_notification,
)

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "on_failure_callback": [task_failure_slack_webhook_notification],
}

EVIDENTLY_BASE_URL = os.environ.get("EVIDENTLY_API_URL", "http://evidently-api:5000")
DEFAULT_REFERENCE_MONTH = "2013-01"


def _get_params(context: dict | None) -> dict:
    if not context:
        return {}
    return context.get("params", {})


def _package_env() -> dict[str, str]:
    env = os.environ.copy()
    package_paths = os.pathsep.join(["/opt/airflow/project/libs/src"])
    existing_pythonpath = env.get("PYTHONPATH", "").strip()
    if existing_pythonpath:
        env["PYTHONPATH"] = f"{package_paths}{os.pathsep}{existing_pythonpath}"
    else:
        env["PYTHONPATH"] = package_paths
    return env


def _api_post(endpoint: str, payload: dict) -> dict:
    url = f"{EVIDENTLY_BASE_URL}{endpoint}"
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=600) as response:
        return json.loads(response.read().decode("utf-8"))


@dag(
    dag_id="lichess_monitoring",
    description="Daily drift, data quality, and classification monitoring on latest ColumnStore data.",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    on_failure_callback=[dag_failure_slack_webhook_notification],
    tags=["lichess", "monitoring"],
    params={
        "reference_month": DEFAULT_REFERENCE_MONTH,
        "current_month": "",
        "sample_size": 5000,
    },
)
def lichess_monitoring():
    @task.python
    def resolve_months(**context) -> dict[str, str]:
        params = _get_params(context)
        reference_month = (params.get("reference_month") or DEFAULT_REFERENCE_MONTH).strip()
        current_month = (params.get("current_month") or "").strip()

        if not current_month:
            from lichess_libs.shared.columnstore import query_dataframe

            frame = query_dataframe(
                "SELECT month FROM batch_predictions ORDER BY id DESC LIMIT 1",
                (),
                config=None,
            )
            if frame.empty:
                raise ValueError("No batch_predictions rows found in ColumnStore")
            current_month = str(frame.iloc[0]["month"])

        sample_size = int(params.get("sample_size", 5000))
        print(
            f"Monitoring reference={reference_month} current={current_month} sample_size={sample_size}",
            flush=True,
        )
        return {
            "reference_month": reference_month,
            "current_month": current_month,
            "sample_size": sample_size,
        }

    @task.python
    def run_drift_report(months: dict[str, str]) -> dict:
        payload = {
            "data_source": "columnstore",
            "reference_month": months["reference_month"],
            "current_month": months["current_month"],
            "sample_size": months["sample_size"],
        }
        result = _api_post("/reports/drift", payload)
        print(f"Drift report: {result.get('report_name')}", flush=True)
        return result

    @task.python
    def run_data_quality_report(months: dict[str, str]) -> dict:
        payload = {
            "data_source": "columnstore",
            "reference_month": months["reference_month"],
            "current_month": months["current_month"],
            "sample_size": months["sample_size"],
        }
        result = _api_post("/reports/data-quality", payload)
        print(
            f"Data quality report: {result.get('report_name')} "
            f"missing_ratio={result.get('missing_ratio')}",
            flush=True,
        )
        return result

    @task.python
    def run_classification_report(months: dict[str, str]) -> dict:
        payload = {
            "data_source": "columnstore",
            "reference_month": months["reference_month"],
            "current_month": months["current_month"],
            "sample_size": months["sample_size"],
        }
        result = _api_post("/reports/classification-performance", payload)
        print(f"Classification report: {result.get('report_name')}", flush=True)
        return result

    @task.python
    def evaluate_alerts(drift: dict, quality: dict, classification: dict) -> dict:
        alert_payload = {
            "missing_ratio": quality.get("missing_ratio"),
            "accuracy": classification.get("accuracy"),
        }
        result = _api_post("/alerts/evaluate", alert_payload)
        print(f"Alerts: {result.get('alert_count', 0)}", flush=True)
        if result.get("alert_count", 0) > 0:
            print(json.dumps(result.get("alerts", []), indent=2), flush=True)
        return result

    months = resolve_months()
    drift = run_drift_report(months)
    quality = run_data_quality_report(months)
    classification = run_classification_report(months)
    evaluate_alerts(drift, quality, classification)


lichess_monitoring()
