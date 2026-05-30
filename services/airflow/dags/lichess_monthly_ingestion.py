from __future__ import annotations

from datetime import datetime
import os
import subprocess

from airflow.decorators import get_current_context
from airflow.sdk import dag, task

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
}


def _build_data_cmd(command: str, month: str | None, extra_args: list[str] | None = None) -> list[str]:
    cmd = [os.environ.get("PYTHON_BIN", "python"), "-m", "lichess_data.cli", command]
    if month:
        cmd.extend(["--month", month])
    else:
        cmd.append("--previous-month")
    if extra_args:
        cmd.extend(extra_args)
    return cmd


def _build_features_cmd(command: str, month: str | None, extra_args: list[str] | None = None) -> list[str]:
    cmd = [os.environ.get("PYTHON_BIN", "python"), "-m", "lichess_features.cli", command]
    if month:
        cmd.extend(["--month", month])
    else:
        cmd.append("--previous-month")
    if extra_args:
        cmd.extend(extra_args)
    return cmd


def _build_models_cmd(command: str, month: str | None, extra_args: list[str] | None = None) -> list[str]:
    cmd = [os.environ.get("PYTHON_BIN", "python"), "-m", "lichess_models.cli", command]
    if month:
        cmd.extend(["--month", month])
    else:
        cmd.append("--previous-month")
    if extra_args:
        cmd.extend(extra_args)
    return cmd


def _run_cmd(cmd: list[str]) -> None:
    print("Running:", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


def _get_params() -> dict:
    context = get_current_context()
    return context.get("params", {})


def _use_elt(params: dict) -> bool:
    if "use_elt" in params:
        return bool(params.get("use_elt"))
    return os.environ.get("LICHESS_STORAGE_BACKEND", "minio").strip().lower() == "minio"


@dag(
    dag_id="lichess_monthly_ingestion",
    description="Monthly Lichess ingestion: ELT (MinIO/Spark/DuckDB) or local extract path.",
    schedule="0 3 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lichess", "ingestion"],
    params={
        "month": "",
        "verify_checksum": True,
        "skip_existing": True,
        "test_size": 0.2,
        "run_validation": True,
        "run_training": True,
        "use_elt": True,
    },
)
def lichess_monthly_ingestion():
    @task.python
    def download_shard() -> None:
        params = _get_params()
        month = (params.get("month") or "").strip() or None
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
    def upload_raw() -> None:
        params = _get_params()
        if not _use_elt(params):
            print("ELT disabled; skipping upload", flush=True)
            return
        month = (params.get("month") or "").strip() or None
        cmd = _build_data_cmd("upload", month)
        _run_cmd(cmd)

    @task.python
    def spark_transform() -> None:
        params = _get_params()
        if not _use_elt(params):
            print("ELT disabled; skipping spark-transform", flush=True)
            return
        month = (params.get("month") or "").strip() or None
        cmd = _build_data_cmd("spark-transform", month)
        _run_cmd(cmd)

    @task.python
    def duckdb_sync() -> None:
        params = _get_params()
        if not _use_elt(params):
            print("ELT disabled; skipping duckdb-sync", flush=True)
            return
        month = (params.get("month") or "").strip() or None
        cmd = _build_data_cmd("duckdb-sync", month)
        _run_cmd(cmd)

    @task.python
    def extract_parquet() -> None:
        params = _get_params()
        if _use_elt(params):
            print("ELT enabled; skipping legacy extract", flush=True)
            return
        month = (params.get("month") or "").strip() or None
        cmd = _build_data_cmd("extract", month)
        _run_cmd(cmd)

    @task.python
    def preprocess_features() -> None:
        params = _get_params()
        month = (params.get("month") or "").strip() or None
        cmd = _build_data_cmd("preprocess", month)
        _run_cmd(cmd)

    @task.python
    def feast_split() -> None:
        params = _get_params()
        month = (params.get("month") or "").strip() or None
        test_size = params.get("test_size", 0.2)
        extra = ["--test-size", str(test_size)]
        cmd = _build_features_cmd("split", month, extra)
        _run_cmd(cmd)

    @task.python
    def validate_shard() -> None:
        params = _get_params()
        if not bool(params.get("run_validation", True)):
            print("Validation skipped by params.run_validation", flush=True)
            return
        month = (params.get("month") or "").strip() or None
        cmd = _build_data_cmd("validate", month, ["--strict"])
        _run_cmd(cmd)

    @task.python
    def validate_ge() -> None:
        params = _get_params()
        if not bool(params.get("run_validation", True)):
            print("Validation skipped by params.run_validation", flush=True)
            return
        month = (params.get("month") or "").strip() or None
        cmd = _build_data_cmd("validate-ge", month, ["--stage", "all", "--strict"])
        _run_cmd(cmd)

    @task.python
    def train_model() -> None:
        params = _get_params()
        if not bool(params.get("run_training", True)):
            print("Training skipped by params.run_training", flush=True)
            return
        month = (params.get("month") or "").strip() or None
        cmd = _build_models_cmd("train", month, ["--no-mlflow"])
        _run_cmd(cmd)

    downloaded = download_shard()
    uploaded = upload_raw()
    transformed = spark_transform()
    synced = duckdb_sync()
    extracted = extract_parquet()
    preprocessed = preprocess_features()
    split = feast_split()
    validated = validate_shard()
    validated_ge = validate_ge()
    trained = train_model()

    downloaded >> [uploaded, extracted]
    uploaded >> transformed >> synced >> preprocessed
    extracted >> preprocessed
    preprocessed >> split >> validated >> validated_ge >> trained


lichess_monthly_ingestion()
