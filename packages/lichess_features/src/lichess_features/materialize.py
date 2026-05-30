"""Helpers to apply and materialize Feast features."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from feast import FeatureStore


def _feature_repo_path() -> Path:
	return Path(__file__).resolve().parents[2] / "feast_repo"


def get_store() -> FeatureStore:
	return FeatureStore(repo_path=str(_feature_repo_path()))


def apply() -> None:
	store = get_store()
	store.apply()


def materialize_days(days_back: int = 30) -> None:
	end = datetime.utcnow()
	start = end - timedelta(days=days_back)
	store = get_store()
	store.materialize(start, end)
