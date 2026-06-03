"""Tests for Feast-backed train/test split."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from lichess_features.split import ENTITY_COLUMNS, _ensure_result_label
from lichess_libs.shared.sampling import limit_games, temporal_split


@pytest.fixture
def ordered_entity_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "site": ["a", "b", "c"],
            "utc_datetime": pd.to_datetime(
                ["2013-01-01", "2013-01-02", "2013-01-03"]
            ),
            "result_label": [1, 0, -1],
            "white_elo": [1500.0, 1600.0, 1700.0],
        }
    )


def test_ensure_result_label_merges_from_full_df(ordered_entity_df: pd.DataFrame) -> None:
    retrieved = ordered_entity_df.drop(columns=["result_label"])
    merged = _ensure_result_label(retrieved, ordered_entity_df)
    assert "result_label" in merged.columns
    pd.testing.assert_series_equal(
        merged["result_label"], ordered_entity_df["result_label"], check_names=False
    )


def test_limit_games_keeps_earliest_rows(ordered_entity_df: pd.DataFrame) -> None:
    limited = limit_games(ordered_entity_df, use_sample=True, max_rows=2)
    assert len(limited) == 2
    assert limited["utc_datetime"].min() == ordered_entity_df["utc_datetime"].min()


def test_limit_games_noop_when_disabled(ordered_entity_df: pd.DataFrame) -> None:
    unchanged = limit_games(ordered_entity_df, use_sample=False, max_rows=1)
    assert len(unchanged) == len(ordered_entity_df)


def test_temporal_split_orders_by_time(ordered_entity_df: pd.DataFrame) -> None:
    shuffled = ordered_entity_df.sample(frac=1, random_state=0)
    train, test = temporal_split(shuffled, test_size=1 / 3)
    assert len(train) == 2
    assert len(test) == 1
    assert train["utc_datetime"].max() <= test["utc_datetime"].min()


@patch("lichess_features.split._register_saved_dataset")
@patch("lichess_features.split.apply_with_source")
@patch("lichess_features.split.get_store")
def test_run_split_writes_parquet_and_registers_datasets(
    mock_get_store: MagicMock,
    mock_apply: MagicMock,
    mock_register: MagicMock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ordered_entity_df: pd.DataFrame,
) -> None:
    from lichess_features.split import run_split

    month = "2013-01"
    features_path = tmp_path / "features.parquet"
    ordered_entity_df.to_parquet(features_path, index=False)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    def fake_artifact_path(component: str, subpath: str, *, create: bool = False) -> Path:
        if subpath.endswith("features.parquet"):
            return features_path
        if subpath.endswith(month):
            if create:
                out_dir.mkdir(parents=True, exist_ok=True)
            return out_dir
        raise AssertionError(f"unexpected artifact path: {component}/{subpath}")

    monkeypatch.setattr("lichess_features.split.get_artifact_path", fake_artifact_path)

    store = MagicMock()
    mock_get_store.return_value = store

    def fake_retrieve(*args, **kwargs):
        entity_df = kwargs["entity_df"] if "entity_df" in kwargs else args[1]
        retrieved = entity_df[list(ENTITY_COLUMNS)].merge(
            ordered_entity_df[["site", "white_elo"]],
            on="site",
            how="left",
        )
        job = MagicMock()
        job.to_df.return_value = retrieved
        return job

    store.get_feature_service.return_value = "pregame_service"
    store.get_historical_features.side_effect = fake_retrieve

    train_path, test_path = run_split(month, test_size=1 / 3)

    mock_apply.assert_called_once_with(features_path)
    assert store.get_historical_features.call_args.kwargs["entity_df"].shape[0] == 3
    assert train_path == out_dir / "train.parquet"
    assert test_path == out_dir / "test.parquet"
    assert train_path.exists()
    assert test_path.exists()
    train_df = pd.read_parquet(train_path)
    assert len(train_df) == 2
    assert len(pd.read_parquet(test_path)) == 1
    assert "result_label" in train_df.columns
    assert mock_register.call_count == 2


@patch("lichess_features.split._register_saved_dataset")
@patch("lichess_features.split.apply_with_source")
@patch("lichess_features.split.get_store")
def test_run_split_with_sampling_limits_feast_entities(
    mock_get_store: MagicMock,
    mock_apply: MagicMock,
    mock_register: MagicMock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ordered_entity_df: pd.DataFrame,
) -> None:
    from lichess_features.split import run_split

    month = "2013-01"
    extra_rows = ordered_entity_df.copy()
    extra_rows["site"] = ["d", "e", "f"]
    extra_rows["utc_datetime"] = pd.to_datetime(
        ["2013-01-04", "2013-01-05", "2013-01-06"]
    )
    full_df = pd.concat([ordered_entity_df, extra_rows], ignore_index=True)
    features_path = tmp_path / "features.parquet"
    full_df.to_parquet(features_path, index=False)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    def fake_artifact_path(component: str, subpath: str, *, create: bool = False) -> Path:
        if subpath.endswith("features.parquet"):
            return features_path
        if subpath.endswith(month):
            if create:
                out_dir.mkdir(parents=True, exist_ok=True)
            return out_dir
        raise AssertionError(f"unexpected artifact path: {component}/{subpath}")

    monkeypatch.setattr("lichess_features.split.get_artifact_path", fake_artifact_path)

    store = MagicMock()
    mock_get_store.return_value = store

    def fake_retrieve(*args, **kwargs):
        entity_df = kwargs["entity_df"] if "entity_df" in kwargs else args[1]
        retrieved = entity_df[list(ENTITY_COLUMNS)].merge(
            full_df[["site", "white_elo"]],
            on="site",
            how="left",
        )
        job = MagicMock()
        job.to_df.return_value = retrieved
        return job

    store.get_feature_service.return_value = "pregame_service"
    store.get_historical_features.side_effect = fake_retrieve

    train_path, test_path = run_split(
        month,
        test_size=0.5,
        use_sample=True,
        max_rows=2,
    )

    entity_df = store.get_historical_features.call_args.kwargs["entity_df"]
    assert len(entity_df) == 2
    assert len(pd.read_parquet(train_path)) + len(pd.read_parquet(test_path)) == 2


def test_pregame_source_uses_timestamp_field() -> None:
    defs_path = Path(__file__).resolve().parents[1] / "feast_repo" / "feature_defs.py"
    source = defs_path.read_text(encoding="utf-8")
    assert 'timestamp_field="utc_datetime"' in source
    assert "event_timestamp_column" not in source
