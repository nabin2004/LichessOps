"""Tests for Feast materialize helpers."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lichess_features.materialize import _apply_repo_definitions, _feature_repo_path


def test_apply_repo_definitions_uses_feast_repo_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
	monkeypatch.chdir(tmp_path)
	# Console scripts set sys.path[0] to the entrypoint dir, not the process CWD.
	script_dir = str(tmp_path / "bin")
	monkeypatch.setattr(sys, "path", [script_dir, *sys.path[1:]])

	store = MagicMock()
	store.project = "lichess"
	store.registry = MagicMock()
	repo_path = _feature_repo_path()
	repo_str = str(repo_path)
	cwd_during_parse: list[str] = []
	path_during_parse: list[list[str]] = []

	def capture_parse(path: Path):
		cwd_during_parse.append(os.getcwd())
		path_during_parse.append(list(sys.path))
		return MagicMock()

	with (
		patch("feast.repo_operations.parse_repo", side_effect=capture_parse) as mock_parse,
		patch(
			"feast.repo_operations.extract_objects_for_apply_delete",
			return_value=([], [], [], []),
		),
	):
		_apply_repo_definitions(store)

	mock_parse.assert_called_once_with(repo_path)
	assert cwd_during_parse == [repo_str]
	assert path_during_parse[0][0] == repo_str
	assert os.getcwd() == str(tmp_path)
	assert sys.path[0] == script_dir
	store.apply.assert_called_once_with([], objects_to_delete=[], partial=False)
