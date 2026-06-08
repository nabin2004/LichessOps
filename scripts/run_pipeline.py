#!/usr/bin/env python3
"""Run the full Lichess MLOps pipeline end-to-end with Slack notifications."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
import urllib.error
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
SERVING_PORT = 8082
SERVING_PID_FILE = Path("artifacts/.serving.pid")

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
) -> list[Phase]:
    """Return ordered pipeline phases (excluding conditional register fallback)."""
    sample_extra: list[str] = []
    if use_sample:
        sample_extra.append("--use-sample")
        if max_rows is not None:
            sample_extra.extend(["--max-rows", str(max_rows)])

    phases: list[Phase] = []

    if start_infra:
        profiles = ["core", "ml", "pipeline"]
        if with_monitoring:
            profiles.append("monitoring")
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

    return phases


def phase_keys(phases: list[Phase]) -> list[str]:
    return [phase.key for phase in phases]


def _probe(url: str, timeout: float = 5.0) -> bool:
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return 200 <= response.status < 300
    except urllib.error.URLError:
        return False


def wait_for_services(timeout_s: float = 180.0) -> None:
    """Wait until MinIO and MLflow health endpoints respond."""
    checks = {
        "MinIO": "http://localhost:9000/minio/health/live",
        "MLflow": "http://localhost:5000/health",
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
        names = ", ".join(pending)
        raise RuntimeError(f"Timed out waiting for: {names}")


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
            capture_output=True,
        )
        if result.returncode == 0:
            try:
                wait_for_services()
            except RuntimeError as exc:
                result = subprocess.CompletedProcess(
                    phase.cmd,
                    returncode=1,
                    stdout=result.stdout,
                    stderr=(result.stderr or "") + str(exc),
                )
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


def run_pipeline(args: argparse.Namespace) -> int:
    root = repo_root()
    start_infra = not args.no_infra
    phases = build_phases(
        args.month,
        legacy=args.legacy,
        skip_validation=args.skip_validation,
        use_sample=args.use_sample,
        max_rows=args.max_rows,
        start_infra=start_infra,
        with_monitoring=args.with_monitoring,
        no_serve=args.no_serve,
    )
    notify = not args.no_slack

    if notify and not is_slack_configured():
        print("Slack is not configured; notifications will be skipped.", file=sys.stderr)

    ctx = PipelineContext(month=args.month, repo_root=root)
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", default=DEFAULT_MONTH, metavar="YYYY-MM", help="Shard month (default: 2013-01)")
    parser.add_argument("--no-infra", action="store_true", help="Skip Docker infrastructure startup")
    parser.add_argument(
        "--with-monitoring",
        action="store_true",
        help="Include monitoring profile when starting infrastructure",
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
        help="Do not attempt MLflow register fallback after train",
    )
    args = parser.parse_args(argv)
    return run_pipeline(args)


if __name__ == "__main__":
    raise SystemExit(main())
