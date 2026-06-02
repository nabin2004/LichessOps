"""Helpers to apply and materialize Feast features."""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
	from feast import FeatureStore


def _feature_repo_path() -> Path:
	return Path(__file__).resolve().parents[2] / "feast_repo"


def _require_feast():
	try:
		from feast import FeatureStore
	except ImportError as exc:
		raise ImportError(
			"Feast is required for this command. Install feast==0.46.0 in the "
			"runtime environment (for example the Airflow or feast-cli container)."
		) from exc
	return FeatureStore


def get_store() -> FeatureStore:
	FeatureStore = _require_feast()
	return FeatureStore(repo_path=str(_feature_repo_path()))


def _apply_repo_definitions(store: FeatureStore) -> None:
	"""Register all objects from ``feast_repo`` (Feast 0.46+ requires explicit objects)."""
	from feast.repo_operations import extract_objects_for_apply_delete, parse_repo

	repo = parse_repo(_feature_repo_path())
	objects_to_apply, objects_to_delete, _, _ = extract_objects_for_apply_delete(
		store.project, store.registry, repo
	)
	store.apply(
		objects_to_apply,
		objects_to_delete=objects_to_delete,
		partial=False,
	)


@contextmanager
def feast_source_path(path: str | Path):
	"""Temporarily point Feast FileSource at ``path`` for apply/retrieval."""
	resolved = str(Path(path).expanduser().resolve())
	previous = os.environ.get("FEAST_SOURCE_PATH")
	os.environ["FEAST_SOURCE_PATH"] = resolved
	try:
		yield resolved
	finally:
		if previous is None:
			os.environ.pop("FEAST_SOURCE_PATH", None)
		else:
			os.environ["FEAST_SOURCE_PATH"] = previous


def apply_with_source(path: str | Path) -> None:
	"""Apply feature definitions using ``path`` as the offline FileSource."""
	with feast_source_path(path):
		store = get_store()
		_apply_repo_definitions(store)


def apply() -> None:
	_apply_repo_definitions(get_store())


def materialize_days(days_back: int = 30) -> None:
	end = datetime.utcnow()
	start = end - timedelta(days=days_back)
	store = get_store()
	store.materialize(start, end)
