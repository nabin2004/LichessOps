"""PySpark cluster transform entrypoint."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from libs.shared import get_logger

_logger = get_logger(__name__)

DEFAULT_SPARK_MASTER = "spark://spark:7077"


def spark_master() -> str:
    return os.getenv("SPARK_MASTER_URL", DEFAULT_SPARK_MASTER)


def submit_transform(
    month: str,
    *,
    input_path: Path,
    config_path: Path | None = None,
    local: bool = False,
) -> None:
    """Submit the Spark transform job or run locally when ``local=True``."""
    if local:
        from lichess_data.spark.transform import run_local_transform
        from libs.shared import load_config

        cfg = load_config("lichess_data")
        run_local_transform(input_path, month, config=cfg)
        return

    repo_root = Path(__file__).resolve().parents[4]
    job_path = repo_root / "packages/lichess_data/src/lichess_data/spark/job.py"
    cmd = [
        "spark-submit",
        "--master",
        spark_master(),
        "--py-files",
        str(repo_root / "packages/lichess_data/src"),
        str(job_path),
        "--month",
        month,
        "--input",
        str(input_path),
    ]
    if config_path:
        cmd.extend(["--config", str(config_path)])

    env = os.environ.copy()
    _logger.info("Submitting Spark job: %s", " ".join(cmd))
    subprocess.run(cmd, check=True, env=env, cwd=repo_root)
