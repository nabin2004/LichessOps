from __future__ import annotations

from datetime import datetime
import os
import subprocess
import sys

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


def _build_data_cmd(command: str, month: str | None, extra_args: list[str] | None = None) -> list[str]:
    cmd = [os.environ.get("PYTHON_BIN", "python"), "-m", "lichess_data", command]

    if month:
        cmd.extend(["--month", month])
    else:
        cmd.append("--previous-month")
    if extra_args:
        cmd.extend(extra_args)
    return cmd


def _build_features_cmd(command: str, month: str | None, extra_args: list[str] | None = None) -> list[str]:
    cmd = [os.environ.get("PYTHON_BIN", "python"), "-m", "lichess_features", command]
    if month:
        cmd.extend(["--month", month])
    else:
        cmd.append("--previous-month")
    if extra_args:
        cmd.extend(extra_args)
    return cmd


def _build_models_cmd(command: str, month: str | None, extra_args: list[str] | None = None) -> list[str]:
    cmd = [os.environ.get("PYTHON_BIN", "python"), "-m", "lichess_models", command]
    if month:
        cmd.extend(["--month", month])
    else:
        cmd.append("--previous-month")
    if extra_args:
        cmd.extend(extra_args)
    return cmd


def _package_env() -> dict[str, str]:
    env = os.environ.copy()
    package_paths = os.pathsep.join(
        [
            "/opt/airflow/project/libs/src",
            "/opt/airflow/project/packages/lichess_data/src",
            "/opt/airflow/project/packages/lichess_features/src",
            "/opt/airflow/project/packages/lichess_models/src",
        ]
    )
    existing_pythonpath = env.get("PYTHONPATH", "").strip()
    if existing_pythonpath:
        env["PYTHONPATH"] = f"{package_paths}{os.pathsep}{existing_pythonpath}"
    else:
        env["PYTHONPATH"] = package_paths
    return env


def _run_cmd(cmd: list[str]) -> None:
    print("Running:", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True, env=_package_env())


def _run_cmd_capture(cmd: list[str]) -> str:
    print("Running:", " ".join(cmd), flush=True)
    result = subprocess.run(cmd, check=True, env=_package_env(), capture_output=True, text=True)
    if result.stdout:
        print(result.stdout, end="", flush=True)
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr, flush=True)
    return result.stdout


def _get_params(context: dict | None) -> dict:
    if not context:
        return {}
    return context.get("params", {})


def _use_elt(params: dict) -> bool:
    if "use_elt" in params:
        return bool(params.get("use_elt"))
    return os.environ.get("LICHESS_STORAGE_BACKEND", "minio").strip().lower() == "minio"


@dag(
    dag_id="lichess_monthly_ingestion",
    description="Monthly Lichess ingestion: ELT (MinIO/Spark/ColumnStore) or local extract path.",
    schedule="0 3 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    on_failure_callback=[dag_failure_slack_webhook_notification],
    tags=["lichess", "ingestion"],
    params={
        "month": "",
        "year": "",
        "verify_checksum": True,
        "skip_existing": True,
        "test_size": 0.2,
        "run_validation": True,
        "run_training": True,
        "use_cv": False,
        "use_sample": False,
        "max_rows": 1000,
        "use_elt": True,
        "run_register": True,
    },
)
def lichess_monthly_ingestion():
    @task.python
    def resolve_months(**context) -> list[str | None]:
        params = _get_params(context)
        month = (params.get("month") or "").strip() or None
        if month:
            return [month]
        year = (params.get("year") or "").strip()
        if not year:
            return [None]
        return [f"{year}-{index:02d}" for index in range(1, 13)]

    @task.python
    def download_shard(month: str | None, **context) -> None:
        params = _get_params(context)
        verify = bool(params.get("verify_checksum", True))
        skip_existing = bool(params.get("skip_existing", True))

        extra: list[str] = []
        if not verify:
            extra.append("--no-verify")
        if not skip_existing:
            extra.append("--no-skip-existing")

        cmd = _build_data_cmd("download", month, extra)
        _run_cmd(cmd)

    @task.python
    def upload_raw(month: str | None, **context) -> None:
        params = _get_params(context)
        if not _use_elt(params):
            print("ELT disabled; skipping upload", flush=True)
            return
        cmd = _build_data_cmd("upload", month)
        _run_cmd(cmd)

    @task.python
    def spark_transform(month: str | None, **context) -> None:
        params = _get_params(context)
        if not _use_elt(params):
            print("ELT disabled; skipping spark-transform", flush=True)
            return
        cmd = _build_data_cmd("spark-transform", month)
        _run_cmd(cmd)

    @task.python
    def columnstore_sync(month: str | None, **context) -> None:
        params = _get_params(context)
        if not _use_elt(params):
            print("ELT disabled; skipping columnstore-sync", flush=True)
            return
        cmd = _build_data_cmd("columnstore-sync", month)
        _run_cmd(cmd)

    @task.python
    def extract_parquet(month: str | None, **context) -> None:
        params = _get_params(context)
        if _use_elt(params):
            print("ELT enabled; skipping legacy extract", flush=True)
            return
        cmd = _build_data_cmd("extract", month)
        _run_cmd(cmd)

    @task.python
    def preprocess_features(month: str | None, **context) -> None:
        params = _get_params(context)
        cmd = _build_data_cmd("preprocess", month)
        _run_cmd(cmd)

    @task.python
    def feast_split(month: str | None, **context) -> None:
        params = _get_params(context)
        test_size = params.get("test_size", 0.2)
        extra = ["--test-size", str(test_size)]
        if bool(params.get("use_sample", False)):
            extra.append("--use-sample")
            extra.extend(["--max-rows", str(int(params.get("max_rows", 1000)))])
        cmd = _build_features_cmd("split", month, extra)
        _run_cmd(cmd)

    @task.python
    def validate_shard(month: str | None, **context) -> None:
        params = _get_params(context)
        if not bool(params.get("run_validation", True)):
            print("Validation skipped by params.run_validation", flush=True)
            return
        cmd = _build_data_cmd("validate", month, ["--strict"])
        _run_cmd(cmd)

    @task.python
    def validate_ge(month: str | None, **context) -> None:
        params = _get_params(context)
        if not bool(params.get("run_validation", True)):
            print("Validation skipped by params.run_validation", flush=True)
            return
        cmd = _build_data_cmd("validate-ge", month, ["--stage", "all", "--strict"])
        _run_cmd(cmd)

    @task.python
    def train_model(month: str | None, **context) -> dict:
        params = _get_params(context)
        if not bool(params.get("run_training", True)):
            print("Training skipped by params.run_training", flush=True)
            return {"run_dir": "", "mlflow_skipped": False}
        extra: list[str] = []
        if bool(params.get("use_cv", False)):
            extra.append("--cv")
        if bool(params.get("use_sample", False)):
            extra.append("--use-sample")
            extra.extend(["--max-rows", str(int(params.get("max_rows", 1000)))])
        cmd = _build_models_cmd("train", month, extra)
        stdout = _run_cmd_capture(cmd)
        lines = [line.strip() for line in stdout.splitlines() if line.strip()]
        run_dir = lines[0] if lines else ""
        mlflow_skipped = any("MLflow logging skipped" in line for line in lines)
        return {"run_dir": run_dir, "mlflow_skipped": mlflow_skipped}

    @task.python
    def register_model(train_result: dict, month: str | None, **context) -> None:
        params = _get_params(context)
        if not bool(params.get("run_register", True)):
            print("Register skipped by params.run_register", flush=True)
            return
        if not train_result.get("mlflow_skipped"):
            print("MLflow logging succeeded; skipping register backfill", flush=True)
            return
        run_dir = (train_result.get("run_dir") or "").strip()
        if not run_dir:
            raise ValueError("Cannot register: train did not return run_dir")
        cmd = _build_models_cmd("register", month, ["--run-dir", run_dir])
        _run_cmd(cmd)

    months = resolve_months()

    downloaded = download_shard.expand(month=months)
    uploaded = upload_raw.expand(month=months)
    transformed = spark_transform.expand(month=months)
    synced = columnstore_sync.expand(month=months)
    extracted = extract_parquet.expand(month=months)
    preprocessed = preprocess_features.expand(month=months)
    split = feast_split.expand(month=months)
    validated = validate_shard.expand(month=months)
    validated_ge = validate_ge.expand(month=months)
    trained = train_model.expand(month=months)
    registered = register_model.expand(month=months, train_result=trained)

    downloaded >> [uploaded, extracted]
    uploaded >> transformed >> synced >> preprocessed
    extracted >> preprocessed
    preprocessed >> split >> validated >> validated_ge >> trained >> registered


lichess_monthly_ingestion()
