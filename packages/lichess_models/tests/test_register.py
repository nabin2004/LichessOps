"""Tests for MLflow registration helpers."""

from __future__ import annotations

from lichess_models.register import _candidate_artifact_path


def test_candidate_artifact_path_avoids_slashes() -> None:
    assert _candidate_artifact_path("logistic_regression") == "candidate_logistic_regression"
    assert "/" not in _candidate_artifact_path("logistic_regression")
    assert _candidate_artifact_path("foo/bar") == "candidate_foo_bar"
