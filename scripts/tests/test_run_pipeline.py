"""Tests for pipeline orchestrator command/phase construction."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.run_pipeline import (  # noqa: E402
    _parse_run_id,
    _probe,
    _probe_airflow,
    _probe_tcp,
    build_airflow_conf,
    build_data_cmd,
    build_features_cmd,
    build_infra_cmd,
    build_infra_profiles,
    build_models_cmd,
    build_phases,
    build_airflow_phases,
    phase_keys,
    resolve_latest_model,
    trigger_dag,
    wait_for_dag_run,
    wait_for_services,
)


def test_build_data_cmd() -> None:
    cmd = build_data_cmd("download", "2013-01")
    assert cmd == ["uv", "run", "lichess-data", "download", "--month", "2013-01"]


def test_build_features_cmd_with_extra() -> None:
    cmd = build_features_cmd("split", "2013-01", ["--use-sample", "--max-rows", "1000"])
    assert "lichess-features" in cmd
    assert "--use-sample" in cmd


def test_build_models_cmd() -> None:
    cmd = build_models_cmd("train", "2013-01", ["--cv"])
    assert cmd[-1] == "--cv"


def test_build_phases_elt_default() -> None:
    phases = build_phases("2013-01", start_infra=False)
    keys = phase_keys(phases)
    assert keys[0] == "download"
    assert "upload" in keys
    assert "spark-transform" in keys
    assert "columnstore-sync" in keys
    assert "extract" not in keys
    assert keys[-1] == "serve"


def test_build_phases_legacy() -> None:
    phases = build_phases("2013-01", legacy=True, start_infra=False)
    keys = phase_keys(phases)
    assert "extract" in keys
    assert "upload" not in keys


def test_build_phases_skip_validation() -> None:
    phases = build_phases("2013-01", skip_validation=True, start_infra=False)
    keys = phase_keys(phases)
    assert "validate" not in keys
    assert "validate-ge" not in keys


def test_build_phases_with_infra_and_monitoring() -> None:
    phases = build_phases("2013-01", start_infra=True, with_monitoring=True)
    infra = phases[0]
    assert infra.key == "infra"
    assert "--profile" in infra.cmd
    assert "monitoring" in infra.cmd
    assert infra.cmd[-1] == "-d"


def test_build_infra_profiles_airflow_default() -> None:
    profiles = build_infra_profiles()
    assert profiles == ["core", "ml", "pipeline", "orchestration", "monitoring"]


def test_build_infra_profiles_with_flower() -> None:
    profiles = build_infra_profiles(with_flower=True)
    assert "flower" in profiles


def test_build_infra_cmd_includes_build() -> None:
    cmd = build_infra_cmd()
    assert cmd[0:2] == ["docker", "compose"]
    assert "orchestration" in cmd
    assert "monitoring" in cmd
    assert cmd[-2:] == ["-d", "--build"]


def test_build_airflow_conf() -> None:
    conf = build_airflow_conf(
        "2013-01",
        legacy=False,
        skip_validation=False,
        use_sample=True,
        max_rows=500,
    )
    assert conf == {
        "month": "2013-01",
        "use_elt": True,
        "run_validation": True,
        "run_training": True,
        "use_sample": True,
        "max_rows": 500,
    }


def test_build_airflow_conf_legacy() -> None:
    conf = build_airflow_conf("2013-01", legacy=True)
    assert conf["use_elt"] is False


def test_build_airflow_phases_default() -> None:
    phases = build_airflow_phases("2013-01", start_infra=True)
    keys = phase_keys(phases)
    assert keys[0] == "infra"
    assert "airflow-trigger" in keys
    assert "airflow-wait" in keys
    assert keys[-1] == "observability"
    assert keys[-2] == "serve"


def test_build_airflow_phases_no_serve() -> None:
    phases = build_airflow_phases("2013-01", start_infra=False, no_serve=True)
    keys = phase_keys(phases)
    assert "serve" not in keys
    assert keys[-1] == "observability"


def test_parse_run_id_from_table_output() -> None:
    stdout = "dag_run_id=manual__2024-01-01T00:00:00+00:00"
    assert _parse_run_id(stdout) == "manual__2024-01-01T00:00:00+00:00"


def test_parse_run_id_from_json() -> None:
    stdout = '{"run_id": "manual__2024-06-01T03:00:00+00:00"}'
    assert _parse_run_id(stdout) == "manual__2024-06-01T03:00:00+00:00"


def test_trigger_dag_parses_run_id(tmp_path: Path) -> None:
    ctx = MagicMock()
    ctx.repo_root = tmp_path

    unpause = MagicMock(returncode=0, stdout="", stderr="")
    trigger = MagicMock(
        returncode=0,
        stdout='{"run_id": "manual__2024-01-01T00:00:00+00:00"}',
        stderr="",
    )

    with patch("scripts.run_pipeline._compose_exec", side_effect=[unpause, trigger]):
        run_id = trigger_dag(ctx, {"month": "2013-01"})
    assert run_id == "manual__2024-01-01T00:00:00+00:00"


def test_wait_for_dag_run_success() -> None:
    ctx = MagicMock()
    with patch("scripts.run_pipeline._dag_run_state", return_value="success"):
        wait_for_dag_run(ctx, "manual__test", timeout_s=1)


def test_wait_for_dag_run_failed() -> None:
    ctx = MagicMock()
    task_states = MagicMock(returncode=0, stdout="train_model failed", stderr="")
    with (
        patch("scripts.run_pipeline._dag_run_state", return_value="failed"),
        patch("scripts.run_pipeline._compose_exec", return_value=task_states),
        pytest.raises(RuntimeError, match="failed"),
    ):
        wait_for_dag_run(ctx, "manual__test", timeout_s=1)


def test_resolve_latest_model(tmp_path: Path) -> None:
    import os

    models_root = tmp_path / "artifacts" / "lichess_models"
    older = models_root / "run-old"
    newer = models_root / "run-new"
    older.mkdir(parents=True)
    newer.mkdir(parents=True)
    (older / "model.joblib").write_text("old", encoding="utf-8")
    os.utime(older / "model.joblib", (1, 1))
    (newer / "model.joblib").write_text("new", encoding="utf-8")

    assert resolve_latest_model(tmp_path) == newer


def test_probe_airflow_healthy_payload() -> None:
    payload = json.dumps(
        {
            "metadatabase": {"status": "healthy"},
            "scheduler": {"status": "healthy"},
            "dag_processor": {"status": "healthy"},
        }
    ).encode()

    class FakeResponse:
        status = 200

        def read(self) -> bytes:
            return payload

        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> bool:
            return False

    with patch("scripts.run_pipeline.urllib.request.urlopen", return_value=FakeResponse()):
        assert _probe_airflow() is True


def test_probe_airflow_unhealthy_scheduler() -> None:
    payload = json.dumps(
        {
            "metadatabase": {"status": "healthy"},
            "scheduler": {"status": "unhealthy"},
            "dag_processor": {"status": "healthy"},
        }
    ).encode()

    class FakeResponse:
        status = 200

        def read(self) -> bytes:
            return payload

        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> bool:
            return False

    with patch("scripts.run_pipeline.urllib.request.urlopen", return_value=FakeResponse()):
        assert _probe_airflow() is False


def test_probe_connection_reset_returns_false() -> None:
    with patch("scripts.run_pipeline.urllib.request.urlopen", side_effect=ConnectionResetError):
        assert _probe("http://localhost:5000/health") is False


def test_probe_tcp_closed_port_returns_false() -> None:
    with patch("scripts.run_pipeline.socket.create_connection", side_effect=ConnectionRefusedError):
        assert _probe_tcp("127.0.0.1", 3307) is False


def test_probe_tcp_open_port_returns_true() -> None:
    with patch("scripts.run_pipeline.socket.create_connection") as mock_connect:
        mock_connect.return_value.__enter__ = MagicMock(return_value=None)
        mock_connect.return_value.__exit__ = MagicMock(return_value=False)
        assert _probe_tcp("127.0.0.1", 3307) is True


def test_wait_for_services_retries_until_ready() -> None:
    call_counts = {"mlflow": 0}

    def fake_probe(url: str, timeout: float = 5.0) -> bool:
        if "5000" in url:
            call_counts["mlflow"] += 1
            return call_counts["mlflow"] >= 2
        return True

    with (
        patch("scripts.run_pipeline._probe", side_effect=fake_probe),
        patch("scripts.run_pipeline._probe_tcp", return_value=True),
        patch("scripts.run_pipeline.time.sleep"),
    ):
        wait_for_services(timeout_s=30)

    assert call_counts["mlflow"] >= 2


def test_wait_for_services_times_out() -> None:
    with (
        patch("scripts.run_pipeline._probe", return_value=False),
        patch("scripts.run_pipeline._probe_tcp", return_value=False),
        patch("scripts.run_pipeline.time.sleep"),
    ):
        with pytest.raises(RuntimeError, match="Timed out waiting for"):
            wait_for_services(timeout_s=0.1)


def test_build_phases_no_serve() -> None:
    phases = build_phases("2013-01", start_infra=False, no_serve=True)
    assert phase_keys(phases)[-1] == "train"
