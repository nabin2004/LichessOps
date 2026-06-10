#!/usr/bin/env python3
"""Run the full Lichess MLOps pipeline end-to-end with Slack notifications."""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

from lichess_libs.shared.slack import (
    is_slack_configured,
    send_slack_failure,
    send_slack_pipeline_complete,
    send_slack_pipeline_start,
    send_slack_success,
)

COMPONENT = "pipeline"
DEFAULT_MONTH = "2013-01"
DAG_ID = "lichess_monthly_ingestion"
MONITORING_DAG_ID = "lichess_monitoring"
INFRA_WAIT_TIMEOUT_S = 600
DAG_WAIT_TIMEOUT_S = 7200
EVIDENTLY_WAIT_TIMEOUT_S = 300
SERVING_PORT = 8082
EVIDENTLY_PORT = 5001
PORTAL_PORT = 8502
SERVING_PID_FILE = Path("artifacts/.serving.pid")
SERVING_CONTAINER_MODEL_PATH = "/opt/lichess/project"
AIRFLOW_HEALTH_URL = "http://localhost:8080/api/v2/monitor/health"
EVIDENTLY_HEALTH_URL = f"http://localhost:{EVIDENTLY_PORT}/health"
PORTAL_HEALTH_URL = f"http://localhost:{PORTAL_PORT}/_stcore/health"
URL_MANIFEST_DIR = Path("artifacts/observability")
EVIDENTLY_REPORTS_DIR = Path("services/evidently/reports")

GRAFANA_SEED_PAYLOADS = [
    {"player_elo": 1800, "opponent_elo": 1700, "player_color": "black", "eco": "B20"},
    {"player_elo": 1350, "opponent_elo": 1420, "player_color": "white", "eco": "C50"},
    {"player_elo": 2100, "opponent_elo": 2050, "player_color": "white", "eco": "B90"},
    {"player_elo": 1500, "opponent_elo": 1600, "player_color": "black", "eco": "A40"},
    {"player_elo": 1900, "opponent_elo": 1850, "player_color": "white", "eco": "D30"},
]

ELT_ENV = {
    "AWS_ENDPOINT_URL": "http://localhost:9000",
    "LICHESS_STORAGE_BACKEND": "minio",
    "MARIADB_COLUMNSTORE_HOST": "127.0.0.1",
    "MARIADB_COLUMNSTORE_PORT": "3307",
}


@dataclass
class Phase:
    key: str
    cmd: list[str]
    detail: str = ""


@dataclass
class PipelineContext:
    month: str
    repo_root: Path
    use_sample: bool = False
    max_rows: int | None = None
    run_dir: Path | None = None
    mlflow_skipped: bool = False
    serving_pid: int | None = None
    dag_run_id: str | None = None
    full: bool = False
    monitor_reference_month: str | None = None
    skip_initial_monitoring: bool = False
    skip_grafana_seed: bool = False
    latest_drift_report: str | None = None
    extra_env: dict[str, str] = field(default_factory=dict)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def build_data_cmd(command: str, month: str, extra: list[str] | None = None) -> list[str]:
    cmd = ["uv", "run", "lichess-data", command, "--month", month]
    if extra:
        cmd.extend(extra)
    return cmd


def build_features_cmd(command: str, month: str, extra: list[str] | None = None) -> list[str]:
    cmd = ["uv", "run", "lichess-features", command, "--month", month]
    if extra:
        cmd.extend(extra)
    return cmd


def build_models_cmd(command: str, month: str, extra: list[str] | None = None) -> list[str]:
    cmd = ["uv", "run", "lichess-models", command, "--month", month]
    if extra:
        cmd.extend(extra)
    return cmd


def build_infra_profiles(
    *,
    with_monitoring: bool = True,
    with_flower: bool = False,
    full: bool = False,
) -> list[str]:
    profiles = ["core", "ml", "pipeline", "orchestration"]
    if with_monitoring:
        profiles.append("monitoring")
    if full:
        profiles.extend(["evidently", "portal"])
    if with_flower:
        profiles.append("flower")
    return profiles


def build_infra_cmd(
    *,
    with_monitoring: bool = True,
    with_flower: bool = False,
    full: bool = False,
    build: bool = True,
) -> list[str]:
    cmd = ["docker", "compose"]
    for profile in build_infra_profiles(
        with_monitoring=with_monitoring,
        with_flower=with_flower,
        full=full,
    ):
        cmd.extend(["--profile", profile])
    cmd.append("up")
    cmd.append("-d")
    if build:
        cmd.append("--build")
    return cmd


def build_airflow_conf(
    month: str,
    *,
    legacy: bool = False,
    skip_validation: bool = False,
    use_sample: bool = False,
    max_rows: int | None = None,
) -> dict[str, object]:
    conf: dict[str, object] = {
        "month": month,
        "use_elt": not legacy,
        "run_validation": not skip_validation,
        "run_training": True,
        "use_sample": use_sample,
    }
    if use_sample and max_rows is not None:
        conf["max_rows"] = max_rows
    return conf


def build_full_post_serve_phases(*, no_serve: bool, skip_initial_monitoring: bool) -> list[Phase]:
    if no_serve:
        return []
    phases: list[Phase] = [Phase("evidently-wait", [], detail="evidently + portal health")]
    if not skip_initial_monitoring:
        phases.append(Phase("monitor-initial", [], detail="drift + data-quality reports"))
    phases.extend(
        [
            Phase("grafana-seed", [], detail="sample /predict traffic"),
            Phase("grafana-verify", [], detail="prometheus targets"),
        ]
    )
    return phases


def build_airflow_phases(
    month: str,
    *,
    legacy: bool = False,
    skip_validation: bool = False,
    use_sample: bool = False,
    max_rows: int | None = None,
    start_infra: bool = True,
    with_monitoring: bool = True,
    with_flower: bool = False,
    no_serve: bool = False,
    full: bool = False,
    skip_initial_monitoring: bool = False,
) -> list[Phase]:
    phases: list[Phase] = []
    if start_infra:
        profiles = build_infra_profiles(
            with_monitoring=with_monitoring,
            with_flower=with_flower,
            full=full,
        )
        phases.append(
            Phase(
                "infra",
                build_infra_cmd(
                    with_monitoring=with_monitoring,
                    with_flower=with_flower,
                    full=full,
                ),
                detail=f"profiles={','.join(profiles)}",
            )
        )
    phases.append(Phase("wait", [], detail="health checks"))
    conf = build_airflow_conf(
        month,
        legacy=legacy,
        skip_validation=skip_validation,
        use_sample=use_sample,
        max_rows=max_rows,
    )
    phases.append(
        Phase(
            "airflow-trigger",
            [],
            detail=json.dumps(conf, separators=(",", ":")),
        )
    )
    phases.append(Phase("airflow-wait", [], detail=f"dag={DAG_ID}"))
    if not no_serve:
        phases.append(Phase("serve", [], detail="lichess-serving container"))
    if full:
        phases.extend(
            build_full_post_serve_phases(
                no_serve=no_serve,
                skip_initial_monitoring=skip_initial_monitoring,
            )
        )
    phases.append(Phase("observability", [], detail="print URLs"))
    return phases


def build_phases(
    month: str,
    *,
    legacy: bool = False,
    skip_validation: bool = False,
    use_sample: bool = False,
    max_rows: int | None = None,
    start_infra: bool = True,
    with_monitoring: bool = False,
    no_serve: bool = False,
    full: bool = False,
    skip_initial_monitoring: bool = False,
) -> list[Phase]:
    """Return ordered local-mode pipeline phases (excluding conditional register fallback)."""
    sample_extra: list[str] = []
    if use_sample:
        sample_extra.append("--use-sample")
        if max_rows is not None:
            sample_extra.extend(["--max-rows", str(max_rows)])

    phases: list[Phase] = []

    if start_infra:
        profiles = ["core", "ml", "pipeline"]
        if with_monitoring or full:
            profiles.append("monitoring")
        if full:
            profiles.extend(["evidently", "portal"])
        compose_cmd = ["docker", "compose"]
        for profile in profiles:
            compose_cmd.extend(["--profile", profile])
        compose_cmd.extend(["up", "-d"])
        phases.append(Phase("infra", compose_cmd, detail=f"profiles={','.join(profiles)}"))

    phases.append(Phase("download", build_data_cmd("download", month)))

    if legacy:
        phases.append(Phase("extract", build_data_cmd("extract", month)))
    else:
        phases.extend(
            [
                Phase("upload", build_data_cmd("upload", month)),
                Phase("spark-transform", build_data_cmd("spark-transform", month)),
                Phase("columnstore-sync", build_data_cmd("columnstore-sync", month)),
            ]
        )

    phases.append(Phase("preprocess", build_data_cmd("preprocess", month)))
    phases.append(Phase("feast-split", build_features_cmd("split", month, sample_extra or None)))

    if not skip_validation:
        phases.extend(
            [
                Phase("validate", build_data_cmd("validate", month, ["--strict"])),
                Phase("validate-ge", build_data_cmd("validate-ge", month, ["--stage", "all", "--strict"])),
            ]
        )

    train_extra = list(sample_extra)
    phases.append(Phase("train", build_models_cmd("train", month, train_extra or None)))

    if not no_serve:
        phases.append(Phase("serve", [], detail="background lichess-serving"))

    if full:
        phases.extend(
            build_full_post_serve_phases(
                no_serve=no_serve,
                skip_initial_monitoring=skip_initial_monitoring,
            )
        )
        phases.append(Phase("observability", [], detail="print URLs"))

    return phases


def phase_keys(phases: list[Phase]) -> list[str]:
    return [phase.key for phase in phases]


def _probe(url: str, timeout: float = 5.0) -> bool:
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, OSError):
        return False


def _probe_tcp(host: str, port: int, timeout: float = 5.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _probe_airflow(timeout: float = 5.0) -> bool:
    """Airflow 3 exposes readiness at /api/v2/monitor/health (not /health)."""
    request = urllib.request.Request(AIRFLOW_HEALTH_URL, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if response.status != 200:
                return False
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, json.JSONDecodeError, ValueError):
        return False

    for component in ("metadatabase", "scheduler", "dag_processor"):
        status = (payload.get(component) or {}).get("status", "")
        if str(status).lower() != "healthy":
            return False
    return True


def wait_for_services(
    timeout_s: float = float(INFRA_WAIT_TIMEOUT_S),
    *,
    airflow_mode: bool = False,
) -> None:
    """Wait until infrastructure services respond from the host."""
    http_checks = {
        "MinIO": "http://localhost:9000/minio/health/live",
        "MLflow": "http://localhost:5000/health",
        "Spark": "http://localhost:8081/",
    }
    if airflow_mode:
        http_checks.update(
            {
                "Prometheus": "http://localhost:9090/-/healthy",
                "Grafana": "http://localhost:3000/api/health",
            }
        )
    tcp_checks = {
        "ColumnStore": ("127.0.0.1", 3307),
    }
    deadline = time.monotonic() + timeout_s
    pending_http = dict(http_checks)
    pending_tcp = dict(tcp_checks)
    pending_airflow = airflow_mode
    last_status = 0.0

    while (pending_http or pending_tcp or pending_airflow) and time.monotonic() < deadline:
        if pending_airflow:
            try:
                if _probe_airflow():
                    print("Airflow is ready", flush=True)
                    pending_airflow = False
            except Exception:
                pass

        for name, url in list(pending_http.items()):
            try:
                if _probe(url):
                    print(f"{name} is ready", flush=True)
                    del pending_http[name]
            except Exception:
                pass

        for name, (host, port) in list(pending_tcp.items()):
            try:
                if _probe_tcp(host, port):
                    print(f"{name} is ready", flush=True)
                    del pending_tcp[name]
            except Exception:
                pass

        if pending_http or pending_tcp or pending_airflow:
            now = time.monotonic()
            if now - last_status >= 15:
                waiting = [*pending_http]
                if pending_airflow:
                    waiting.append("Airflow")
                waiting.extend(pending_tcp)
                print(f"Still waiting for: {', '.join(waiting)}", flush=True)
                last_status = now
            time.sleep(3)

    pending = [*pending_http, *pending_tcp]
    if pending_airflow:
        pending.append("Airflow")
    if pending:
        raise RuntimeError(f"Timed out waiting for: {', '.join(pending)}")


def _compose_exec(cmd: list[str], ctx: PipelineContext) -> subprocess.CompletedProcess[str]:
    full_cmd = ["docker", "compose", "exec", "-T", "airflow-scheduler", *cmd]
    return subprocess.run(
        full_cmd,
        cwd=ctx.repo_root,
        text=True,
        capture_output=True,
    )


def _parse_run_id(trigger_stdout: str) -> str | None:
    for line in trigger_stdout.splitlines():
        match = re.search(r"run_id[=:\s]+(\S+)", line, re.IGNORECASE)
        if match:
            return match.group(1).strip("|")
    json_match = re.search(r"\{.*\}", trigger_stdout, re.DOTALL)
    if json_match:
        try:
            payload = json.loads(json_match.group(0))
            if isinstance(payload, dict):
                for key in ("run_id", "dag_run_id"):
                    value = payload.get(key)
                    if isinstance(value, str) and value:
                        return value
        except json.JSONDecodeError:
            pass
    return None


def trigger_dag(ctx: PipelineContext, conf: dict[str, object]) -> str:
    unpause_dag(ctx, DAG_ID)
    if ctx.full:
        unpause_dag(ctx, MONITORING_DAG_ID)

    conf_json = json.dumps(conf, separators=(",", ":"))
    trigger = _compose_exec(
        ["airflow", "dags", "trigger", DAG_ID, "--conf", conf_json],
        ctx,
    )
    if trigger.returncode != 0:
        raise RuntimeError((trigger.stderr or trigger.stdout or "trigger failed").strip())

    if trigger.stdout:
        print(trigger.stdout, end="", flush=True)

    run_id = _parse_run_id(trigger.stdout or "")
    if not run_id:
        list_runs = _compose_exec(
            ["airflow", "dags", "list-runs", DAG_ID, "-o", "json"],
            ctx,
        )
        if list_runs.returncode == 0 and list_runs.stdout.strip():
            runs = json.loads(list_runs.stdout)
            if runs:
                run_id = runs[0].get("run_id")
    if not run_id:
        raise RuntimeError("Could not determine DAG run_id after trigger")
    return run_id


def _dag_run_state(ctx: PipelineContext, run_id: str) -> str | None:
    result = _compose_exec(
        ["airflow", "dags", "list-runs", DAG_ID, "-o", "json"],
        ctx,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return None
    runs = json.loads(result.stdout)
    for run in runs:
        if run.get("run_id") == run_id:
            return str(run.get("state", "")).lower()
    return None


def wait_for_dag_run(
    ctx: PipelineContext,
    run_id: str,
    timeout_s: float = float(DAG_WAIT_TIMEOUT_S),
) -> None:
    terminal = {"success", "failed"}
    deadline = time.monotonic() + timeout_s
    last_status = 0.0

    while time.monotonic() < deadline:
        state = _dag_run_state(ctx, run_id)
        if state in terminal:
            if state == "success":
                print(f"DAG run {run_id} succeeded", flush=True)
                return
            task_states = _compose_exec(
                ["airflow", "tasks", "states-for-dag-run", DAG_ID, run_id],
                ctx,
            )
            detail = (task_states.stdout or task_states.stderr or "").strip()
            ui = f"http://localhost:8080/dags/{DAG_ID}/grid?dag_run_id={urllib.parse.quote(run_id)}"
            raise RuntimeError(f"DAG run {run_id} failed. UI: {ui}\n{detail}")

        now = time.monotonic()
        if now - last_status >= 30:
            print(f"DAG run {run_id} state: {state or 'pending'}", flush=True)
            last_status = now
        time.sleep(15)

    ui = f"http://localhost:8080/dags/{DAG_ID}/grid?dag_run_id={urllib.parse.quote(run_id)}"
    raise RuntimeError(f"Timed out waiting for DAG run {run_id}. UI: {ui}")


def resolve_latest_model(repo_root_path: Path) -> Path:
    models_root = repo_root_path / "artifacts" / "lichess_models"
    if not models_root.is_dir():
        raise RuntimeError(f"No model artifacts directory: {models_root}")

    candidates: list[tuple[float, Path]] = []
    for child in models_root.iterdir():
        model_path = child / "model.joblib"
        if model_path.is_file():
            candidates.append((model_path.stat().st_mtime, model_path.parent))

    if not candidates:
        raise RuntimeError(f"No model.joblib found under {models_root}")

    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def _api_post_json(url: str, payload: dict, *, timeout: float = 300.0) -> dict:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _api_get_json(url: str, *, timeout: float = 10.0) -> dict:
    request = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def unpause_dag(ctx: PipelineContext, dag_id: str) -> None:
    result = _compose_exec(["airflow", "dags", "unpause", dag_id], ctx)
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or f"unpause {dag_id} failed").strip())


def wait_for_evidently_services(timeout_s: float = float(EVIDENTLY_WAIT_TIMEOUT_S)) -> None:
    checks = {
        "Evidently API": EVIDENTLY_HEALTH_URL,
        "Lichess Portal": PORTAL_HEALTH_URL,
    }
    deadline = time.monotonic() + timeout_s
    pending = dict(checks)
    while pending and time.monotonic() < deadline:
        for name, url in list(pending.items()):
            if _probe(url):
                print(f"{name} is ready", flush=True)
                del pending[name]
        if pending:
            time.sleep(3)
    if pending:
        raise RuntimeError(f"Timed out waiting for: {', '.join(pending)}")


def _monitoring_payload(ctx: PipelineContext) -> dict[str, object]:
    ref_month = ctx.monitor_reference_month or ctx.month
    return {
        "data_source": "columnstore",
        "reference_month": ref_month,
        "current_month": ctx.month,
        "sample_size": 5000,
    }


def run_initial_monitoring(ctx: PipelineContext) -> None:
    payload = _monitoring_payload(ctx)
    for endpoint, label in (
        ("/reports/drift", "drift"),
        ("/reports/data-quality", "data-quality"),
    ):
        result = _api_post_json(f"http://localhost:{EVIDENTLY_PORT}{endpoint}", payload)
        report_name = result.get("report_name", "")
        print(f"Generated {label} report: {report_name}", flush=True)
        if label == "drift" and report_name:
            ctx.latest_drift_report = report_name


def seed_grafana_metrics() -> int:
    success = 0
    for payload in GRAFANA_SEED_PAYLOADS:
        data = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            f"http://localhost:{SERVING_PORT}/predict",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                if 200 <= response.status < 300:
                    success += 1
        except (urllib.error.URLError, OSError) as exc:
            print(f"Grafana seed predict failed: {exc}", file=sys.stderr, flush=True)
    print(f"Grafana seed: {success}/{len(GRAFANA_SEED_PAYLOADS)} predict requests succeeded", flush=True)
    return success


def verify_grafana() -> None:
    grafana_serving_url = "http://localhost:3000/d/lichess-serving/lichess-serving"
    try:
        targets = _api_get_json("http://localhost:9090/api/v1/targets")
        active = targets.get("data", {}).get("activeTargets", [])
        serving_up = any(
            target.get("labels", {}).get("job") == "lichess-serving" and target.get("health") == "up"
            for target in active
        )
        if serving_up:
            print("Prometheus target lichess-serving is UP", flush=True)
        else:
            print("Warning: Prometheus target lichess-serving is not UP", file=sys.stderr, flush=True)

        model_query = _api_get_json(
            "http://localhost:9090/api/v1/query?query=lichess_model_loaded"
        )
        results = model_query.get("data", {}).get("result", [])
        if results:
            value = results[0].get("value", [None, "0"])[1]
            print(f"lichess_model_loaded = {value}", flush=True)
    except (urllib.error.URLError, OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"Grafana verify warning: {exc}", file=sys.stderr, flush=True)

    print(f"Grafana Lichess Serving dashboard: {grafana_serving_url}", flush=True)


def _latest_drift_report_path(repo_root: Path) -> str | None:
    reports_dir = repo_root / EVIDENTLY_REPORTS_DIR
    if not reports_dir.is_dir():
        return None
    reports = sorted(reports_dir.glob("drift-*.html"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not reports:
        return None
    return str(reports[0].relative_to(repo_root))


def build_url_manifest(ctx: PipelineContext) -> dict[str, object]:
    run_id = ctx.dag_run_id or ""
    model_rel = ""
    if ctx.run_dir:
        try:
            model_rel = ctx.run_dir.relative_to(ctx.repo_root).as_posix()
        except ValueError:
            model_rel = str(ctx.run_dir)

    latest_drift = ctx.latest_drift_report
    if latest_drift and not latest_drift.endswith(".html"):
        latest_drift = f"{latest_drift}.html"
    drift_path = _latest_drift_report_path(ctx.repo_root)
    drift_url = None
    if drift_path:
        drift_url = f"file://{ctx.repo_root / drift_path}"

    services = {
        "portal": f"http://localhost:{PORTAL_PORT}",
        "serving_health": f"http://localhost:{SERVING_PORT}/health",
        "serving_docs": f"http://localhost:{SERVING_PORT}/docs",
        "serving_metrics": f"http://localhost:{SERVING_PORT}/metrics",
        "grafana": "http://localhost:3000",
        "grafana_serving_dashboard": "http://localhost:3000/d/lichess-serving/lichess-serving",
        "grafana_system_dashboard": "http://localhost:3000/d/lichess-system/lichess-system",
        "prometheus": "http://localhost:9090",
        "prometheus_targets": "http://localhost:9090/targets",
        "evidently_api": f"http://localhost:{EVIDENTLY_PORT}",
        "evidently_streamlit": "http://localhost:8501",
        "airflow": "http://localhost:8080",
        "mlflow": "http://localhost:5000",
        "minio_console": "http://localhost:9001",
        "spark": "http://localhost:8081",
    }
    if run_id:
        services["airflow_dag_run"] = (
            f"http://localhost:8080/dags/{DAG_ID}/grid?dag_run_id={urllib.parse.quote(run_id)}"
        )
    if latest_drift:
        services["latest_drift_report_api"] = f"http://localhost:{EVIDENTLY_PORT}/reports/{latest_drift}"
    if drift_url:
        services["latest_drift_report_file"] = drift_url

    return {
        "month": ctx.month,
        "model_artifact": f"{model_rel}/model.joblib" if model_rel else "",
        "services": services,
    }


def write_url_manifest(ctx: PipelineContext) -> Path:
    manifest = build_url_manifest(ctx)
    out_dir = ctx.repo_root / URL_MANIFEST_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / "urls.json"
    json_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    html_lines = [
        "<!DOCTYPE html>",
        "<html><head><meta charset='utf-8'><title>Lichess MLOps URLs</title>",
        "<style>body{font-family:sans-serif;max-width:720px;margin:2rem auto}",
        "a{color:#1a73e8}li{margin:.4rem 0}</style></head><body>",
        "<h1>Lichess MLOps — Service URLs</h1>",
        f"<p>Month: <strong>{manifest['month']}</strong></p>",
        "<ul>",
    ]
    for name, url in manifest["services"].items():
        label = name.replace("_", " ").title()
        html_lines.append(f"<li><strong>{label}</strong>: <a href='{url}'>{url}</a></li>")
    if manifest.get("model_artifact"):
        html_lines.append(f"<li><strong>Model artifact</strong>: {manifest['model_artifact']}</li>")
    html_lines.extend(["</ul></body></html>"])

    html_path = out_dir / "urls.html"
    html_path.write_text("\n".join(html_lines), encoding="utf-8")
    return json_path


def deploy_serving_container(ctx: PipelineContext) -> None:
    if ctx.run_dir is None:
        ctx.run_dir = resolve_latest_model(ctx.repo_root)

    model_path = ctx.run_dir / "model.joblib"
    if not model_path.exists():
        raise RuntimeError(f"Model artifact not found: {model_path}")

    container_model_uri = f"{SERVING_CONTAINER_MODEL_PATH}/{model_path.relative_to(ctx.repo_root).as_posix()}"
    env = os.environ.copy()
    env["MODEL_URI"] = container_model_uri

    cmd = ["docker", "compose", "--profile", "serving", "up", "-d", "--build", "lichess-serving"]
    result = subprocess.run(cmd, cwd=ctx.repo_root, env=env, text=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or "serving container failed").strip())

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if _probe(f"http://localhost:{SERVING_PORT}/health"):
            print(f"Serving container ready (port={SERVING_PORT}, model={container_model_uri})", flush=True)
            return
        time.sleep(2)

    raise RuntimeError(f"Serving health check timed out on port {SERVING_PORT}")


def run_full_post_serve_phase(phase: Phase, ctx: PipelineContext) -> None:
    if phase.key == "evidently-wait":
        wait_for_evidently_services()
        return
    if phase.key == "monitor-initial":
        run_initial_monitoring(ctx)
        return
    if phase.key == "grafana-seed":
        if ctx.skip_grafana_seed:
            print("Skipping Grafana seed (--skip-grafana-seed)", flush=True)
            return
        seed_grafana_metrics()
        return
    if phase.key == "grafana-verify":
        verify_grafana()
        return
    if phase.key == "observability":
        print_observability_summary(ctx)
        return
    raise RuntimeError(f"Unknown full post-serve phase: {phase.key}")


def print_observability_summary(ctx: PipelineContext) -> None:
    manifest_path = write_url_manifest(ctx)
    manifest = build_url_manifest(ctx)
    services = manifest["services"]
    run_id = ctx.dag_run_id or "<run_id>"
    model_rel = manifest.get("model_artifact", "")

    lines = [
        "",
        "=== Lichess MLOps — Observability ===",
        f"URL manifest:    {manifest_path.relative_to(ctx.repo_root)}",
        f"                 {manifest_path.with_suffix('.html').relative_to(ctx.repo_root)}",
    ]
    if ctx.full:
        lines.append(f"Lichess Portal:  {services['portal']}")
        lines.append(f"Evidently API:   {services['evidently_api']}")
        lines.append(f"Evidently UI:    {services['evidently_streamlit']}")
    lines.extend(
        [
            "Airflow UI:      http://localhost:8080  (airflow / airflow)",
            "MLflow:          http://localhost:5000",
            "Grafana:         http://localhost:3000  (admin / changeme)",
            f"  └ Serving:    {services['grafana_serving_dashboard']}",
            f"  └ System:     {services['grafana_system_dashboard']}",
            f"Prometheus:      {services['prometheus']}  (Targets → lichess-serving, node-exporter)",
            f"Spark Master:    {services['spark']}",
            f"MinIO Console:   {services['minio_console']}",
            f"Serving API:     {services['serving_health']}",
            f"Serving Swagger: {services['serving_docs']}",
            f"Serving metrics: {services['serving_metrics']}",
            "ColumnStore:     mysql -h 127.0.0.1 -P 3307 lichess_analytics",
            f"DAG run:         http://localhost:8080/dags/{DAG_ID}/grid?dag_run_id={urllib.parse.quote(run_id)}",
        ]
    )
    if model_rel:
        lines.append(f"Model artifact:  {model_rel}")
    if ctx.latest_drift_report:
        lines.append(f"Latest drift:    {services.get('latest_drift_report_api', ctx.latest_drift_report)}")
    if ctx.full:
        lines.append(f"Monitoring DAG:  {MONITORING_DAG_ID} (daily schedule via Airflow)")
    print("\n".join(lines), flush=True)


def run_phase(phase: Phase, ctx: PipelineContext, *, notify: bool) -> subprocess.CompletedProcess[str]:
    print(f"\n=== Phase: {phase.key} ===", flush=True)
    env = os.environ.copy()
    env.update(ELT_ENV)
    env.update(ctx.extra_env)

    if phase.key == "infra":
        result = subprocess.run(
            phase.cmd,
            cwd=ctx.repo_root,
            env=env,
            text=True,
        )
        if result.returncode != 0:
            print("Docker compose failed. Is Docker running?", file=sys.stderr)
        else:
            try:
                wait_for_services(airflow_mode=False)
            except RuntimeError as exc:
                result = subprocess.CompletedProcess(
                    phase.cmd,
                    returncode=1,
                    stdout="",
                    stderr=str(exc),
                )
    elif phase.key in {
        "evidently-wait",
        "monitor-initial",
        "grafana-seed",
        "grafana-verify",
        "observability",
    }:
        try:
            run_full_post_serve_phase(phase, ctx)
            return subprocess.CompletedProcess(phase.cmd, returncode=0, stdout="", stderr="")
        except (RuntimeError, urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
            return subprocess.CompletedProcess(phase.cmd, returncode=1, stdout="", stderr=str(exc))
    elif phase.key == "serve":
        if ctx.run_dir is None:
            raise RuntimeError("Cannot start serving: run_dir not set after train")
        model_path = ctx.run_dir / "model.joblib"
        if not model_path.exists():
            raise RuntimeError(f"Model artifact not found: {model_path}")

        serve_env = env.copy()
        serve_env["MODEL_URI"] = str(model_path.resolve())
        serve_cmd = ["uv", "run", "lichess-serving", "--port", str(SERVING_PORT)]
        log_path = ctx.repo_root / "artifacts" / "serving.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_file = log_path.open("w", encoding="utf-8")
        proc = subprocess.Popen(
            serve_cmd,
            cwd=ctx.repo_root,
            env=serve_env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        ctx.serving_pid = proc.pid
        SERVING_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
        SERVING_PID_FILE.write_text(str(proc.pid), encoding="utf-8")

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if _probe(f"http://localhost:{SERVING_PORT}/health"):
                print(f"Serving started (pid={proc.pid}, port={SERVING_PORT})", flush=True)
                return subprocess.CompletedProcess(phase.cmd, returncode=0, stdout="", stderr="")
            if proc.poll() is not None:
                log_file.close()
                log_content = log_path.read_text(encoding="utf-8")[-2000:]
                return subprocess.CompletedProcess(
                    phase.cmd,
                    returncode=proc.returncode or 1,
                    stdout="",
                    stderr=f"Serving process exited early:\n{log_content}",
                )
            time.sleep(1)

        return subprocess.CompletedProcess(
            phase.cmd,
            returncode=1,
            stdout="",
            stderr=f"Serving health check timed out (pid={proc.pid})",
        )
    else:
        result = subprocess.run(
            phase.cmd,
            cwd=ctx.repo_root,
            env=env,
            text=True,
            capture_output=True,
        )

        if phase.key == "train" and result.returncode == 0:
            _capture_train_output(result.stdout, ctx)

    if result.returncode != 0:
        if notify:
            error = (result.stderr or result.stdout or "unknown error").strip()
            send_slack_failure(COMPONENT, phase.key, ctx.month, error)
        return result

    if notify:
        detail = phase.detail
        if phase.key == "train" and ctx.run_dir:
            detail = f"run_dir={ctx.run_dir}"
        elif phase.key == "serve" and ctx.serving_pid:
            detail = f"pid={ctx.serving_pid} port={SERVING_PORT}"
        send_slack_success(COMPONENT, phase.key, ctx.month, detail=detail)

    if result.stdout:
        print(result.stdout, end="", flush=True)
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr, flush=True)

    return result


def run_airflow_phase(
    phase: Phase,
    ctx: PipelineContext,
    *,
    notify: bool,
    airflow_conf: dict[str, object],
) -> subprocess.CompletedProcess[str]:
    print(f"\n=== Phase: {phase.key} ===", flush=True)
    cmd: list[str] = []

    try:
        if phase.key == "infra":
            result = subprocess.run(phase.cmd, cwd=ctx.repo_root, text=True)
            if result.returncode != 0:
                return subprocess.CompletedProcess(phase.cmd, returncode=result.returncode, stdout="", stderr="")
            wait_for_services(airflow_mode=True)
            return subprocess.CompletedProcess(phase.cmd, returncode=0, stdout="", stderr="")

        if phase.key == "wait":
            wait_for_services(airflow_mode=True)
            return subprocess.CompletedProcess(cmd, returncode=0, stdout="", stderr="")

        if phase.key == "airflow-trigger":
            ctx.dag_run_id = trigger_dag(ctx, airflow_conf)
            print(f"Triggered DAG run: {ctx.dag_run_id}", flush=True)
            return subprocess.CompletedProcess(cmd, returncode=0, stdout="", stderr="")

        if phase.key == "airflow-wait":
            if not ctx.dag_run_id:
                raise RuntimeError("dag_run_id not set")
            wait_for_dag_run(ctx, ctx.dag_run_id)
            ctx.run_dir = resolve_latest_model(ctx.repo_root)
            return subprocess.CompletedProcess(cmd, returncode=0, stdout="", stderr="")

        if phase.key == "serve":
            deploy_serving_container(ctx)
            return subprocess.CompletedProcess(cmd, returncode=0, stdout="", stderr="")

        if phase.key in {
            "evidently-wait",
            "monitor-initial",
            "grafana-seed",
            "grafana-verify",
            "observability",
        }:
            run_full_post_serve_phase(phase, ctx)
            return subprocess.CompletedProcess(cmd, returncode=0, stdout="", stderr="")

        raise RuntimeError(f"Unknown airflow phase: {phase.key}")
    except RuntimeError as exc:
        if notify:
            send_slack_failure(COMPONENT, phase.key, ctx.month, str(exc))
        return subprocess.CompletedProcess(cmd, returncode=1, stdout="", stderr=str(exc))


def _capture_train_output(stdout: str, ctx: PipelineContext) -> None:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if not lines:
        return
    run_dir_str = lines[0]
    ctx.run_dir = Path(run_dir_str)
    ctx.mlflow_skipped = any("MLflow logging skipped" in line for line in lines)


def run_register_fallback(ctx: PipelineContext, *, notify: bool) -> subprocess.CompletedProcess[str]:
    if ctx.run_dir is None:
        raise RuntimeError("Cannot register: run_dir not set")
    phase = Phase(
        "register",
        build_models_cmd("register", ctx.month, ["--run-dir", str(ctx.run_dir)]),
        detail="MLflow backfill",
    )
    return run_phase(phase, ctx, notify=notify)


def run_local_pipeline(args: argparse.Namespace) -> int:
    root = repo_root()
    start_infra = not args.no_infra
    full = args.full
    phases = build_phases(
        args.month,
        legacy=args.legacy,
        skip_validation=args.skip_validation,
        use_sample=args.use_sample,
        max_rows=args.max_rows,
        start_infra=start_infra,
        with_monitoring=args.with_monitoring or full,
        no_serve=args.no_serve,
        full=full,
        skip_initial_monitoring=args.skip_initial_monitoring,
    )
    notify = not args.no_slack

    if notify and not is_slack_configured():
        print("Slack is not configured; notifications will be skipped.", file=sys.stderr)

    ctx = PipelineContext(
        month=args.month,
        repo_root=root,
        full=full,
        monitor_reference_month=args.monitor_reference_month or args.month,
        skip_initial_monitoring=args.skip_initial_monitoring,
        skip_grafana_seed=args.skip_grafana_seed,
    )
    if notify:
        send_slack_pipeline_start(args.month, phase_keys(phases))

    started = time.monotonic()
    for phase in phases:
        result = run_phase(phase, ctx, notify=notify)
        if result.returncode != 0:
            error = (result.stderr or result.stdout or "unknown error").strip()
            print(f"Pipeline failed at phase '{phase.key}': {error}", file=sys.stderr)
            return result.returncode

        if phase.key == "train" and ctx.mlflow_skipped and not args.no_mlflow:
            reg_result = run_register_fallback(ctx, notify=notify)
            if reg_result.returncode != 0:
                error = (reg_result.stderr or reg_result.stdout or "unknown error").strip()
                print(f"Pipeline failed at phase 'register': {error}", file=sys.stderr)
                return reg_result.returncode

    duration = time.monotonic() - started
    extras_parts: list[str] = []
    if ctx.run_dir:
        extras_parts.append(f"run_dir={ctx.run_dir}")
    if ctx.serving_pid:
        extras_parts.append(f"serving=http://localhost:{SERVING_PORT} (pid={ctx.serving_pid})")
    extras = "\n".join(extras_parts)

    if notify:
        send_slack_pipeline_complete(args.month, duration, extras=extras)

    print(f"\nPipeline completed in {duration:.1f}s", flush=True)
    return 0


def run_airflow_pipeline(args: argparse.Namespace) -> int:
    root = repo_root()
    start_infra = not args.no_infra
    full = args.full
    with_monitoring = not args.no_monitoring or full
    phases = build_airflow_phases(
        args.month,
        legacy=args.legacy,
        skip_validation=args.skip_validation,
        use_sample=args.use_sample,
        max_rows=args.max_rows,
        start_infra=start_infra,
        with_monitoring=with_monitoring,
        with_flower=args.with_flower,
        no_serve=args.no_serve,
        full=full,
        skip_initial_monitoring=args.skip_initial_monitoring,
    )
    airflow_conf = build_airflow_conf(
        args.month,
        legacy=args.legacy,
        skip_validation=args.skip_validation,
        use_sample=args.use_sample,
        max_rows=args.max_rows,
    )
    notify = not args.no_slack

    if notify and not is_slack_configured():
        print("Slack is not configured; notifications will be skipped.", file=sys.stderr)

    ctx = PipelineContext(
        month=args.month,
        repo_root=root,
        full=full,
        monitor_reference_month=args.monitor_reference_month or args.month,
        skip_initial_monitoring=args.skip_initial_monitoring,
        skip_grafana_seed=args.skip_grafana_seed,
    )
    if notify:
        send_slack_pipeline_start(args.month, phase_keys(phases))

    started = time.monotonic()
    for phase in phases:
        result = run_airflow_phase(phase, ctx, notify=notify, airflow_conf=airflow_conf)
        if result.returncode != 0:
            error = (result.stderr or result.stdout or "unknown error").strip()
            print(f"Pipeline failed at phase '{phase.key}': {error}", file=sys.stderr)
            return result.returncode

        if notify and phase.key not in {"observability", "wait"}:
            detail = phase.detail
            if phase.key == "airflow-trigger" and ctx.dag_run_id:
                detail = f"run_id={ctx.dag_run_id}"
            elif phase.key == "serve" and ctx.run_dir:
                detail = f"model={ctx.run_dir}"
            send_slack_success(COMPONENT, phase.key, ctx.month, detail=detail)

    duration = time.monotonic() - started
    extras_parts: list[str] = []
    if ctx.dag_run_id:
        extras_parts.append(f"dag_run_id={ctx.dag_run_id}")
    if ctx.run_dir:
        extras_parts.append(f"run_dir={ctx.run_dir}")
    if not args.no_serve:
        extras_parts.append(f"serving=http://localhost:{SERVING_PORT}")
    extras = "\n".join(extras_parts)

    if notify:
        send_slack_pipeline_complete(args.month, duration, extras=extras)

    print(f"\nPipeline completed in {duration:.1f}s", flush=True)
    return 0


def run_pipeline(args: argparse.Namespace) -> int:
    if args.local:
        return run_local_pipeline(args)
    return run_airflow_pipeline(args)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", default=DEFAULT_MONTH, metavar="YYYY-MM", help="Shard month (default: 2013-01)")
    parser.add_argument(
        "--local",
        action="store_true",
        help="Run pipeline on host via uv run (legacy mode)",
    )
    parser.add_argument("--no-infra", action="store_true", help="Skip Docker infrastructure startup")
    parser.add_argument(
        "--no-monitoring",
        action="store_true",
        help="Skip monitoring profile when starting infrastructure (airflow mode only)",
    )
    parser.add_argument(
        "--with-monitoring",
        action="store_true",
        help="Include monitoring profile when starting infrastructure (local mode only)",
    )
    parser.add_argument(
        "--with-flower",
        action="store_true",
        help="Include Flower Celery UI (airflow mode only)",
    )
    parser.add_argument("--skip-validation", action="store_true", help="Skip validate and validate-ge phases")
    parser.add_argument("--use-sample", action="store_true", help="Cap rows for OOM-safe dev runs")
    parser.add_argument("--max-rows", type=int, default=None, help="Max games when --use-sample")
    parser.add_argument("--legacy", action="store_true", help="Use extract path instead of ELT")
    parser.add_argument("--no-slack", action="store_true", help="Disable Slack notifications")
    parser.add_argument("--no-serve", action="store_true", help="Stop after train (skip serving)")
    parser.add_argument(
        "--no-mlflow",
        action="store_true",
        help="Do not attempt MLflow register fallback after train (local mode only)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full stack: evidently + portal + initial monitoring + Grafana seed + URL manifest",
    )
    parser.add_argument(
        "--monitor-reference-month",
        default=None,
        metavar="YYYY-MM",
        help="Reference month for drift reports (default: same as --month)",
    )
    parser.add_argument(
        "--skip-initial-monitoring",
        action="store_true",
        help="Skip one-shot Evidently drift/quality reports after serve",
    )
    parser.add_argument(
        "--skip-grafana-seed",
        action="store_true",
        help="Skip sample /predict requests that populate Grafana serving metrics",
    )
    args = parser.parse_args(argv)
    return run_pipeline(args)


if __name__ == "__main__":
    raise SystemExit(main())
