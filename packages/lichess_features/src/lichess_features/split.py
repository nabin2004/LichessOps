"""Chronological train/test split via Feast historical feature retrieval."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from lichess_libs.shared import get_artifact_path, get_logger, load_config

from lichess_features.materialize import apply_with_source, feast_source_path, get_store

log = get_logger("lichess_features.split")

ENTITY_COLUMNS = ("site", "utc_datetime")
LABEL_COLUMNS = ("result_label",)


def temporal_split(
    df: pd.DataFrame, test_size: float = 0.2
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Sort by ``utc_datetime`` and split chronologically."""
    log.info("Temporal train/test split (test_size=%.2f)", test_size)

    df = df.sort_values("utc_datetime").reset_index(drop=True)
    split_idx = int(len(df) * (1 - test_size))
    train, test = df.iloc[:split_idx], df.iloc[split_idx:]

    log.info(
        "  train: %d rows (%s → %s)",
        len(train),
        train["utc_datetime"].min(),
        train["utc_datetime"].max(),
    )
    log.info(
        "  test:  %d rows (%s → %s)",
        len(test),
        test["utc_datetime"].min(),
        test["utc_datetime"].max(),
    )
    return train, test


def _features_path(month: str, data_cfg: dict) -> Path:
    pp_cfg = data_cfg.get("preprocessing") or {}
    subpath = pp_cfg.get("output_subpath", "preprocessed")
    return get_artifact_path(
        "lichess_data", f"{subpath}/{month}/features.parquet", create=False
    )


def _output_dir(month: str, data_cfg: dict) -> Path:
    pp_cfg = data_cfg.get("preprocessing") or {}
    subpath = pp_cfg.get("output_subpath", "preprocessed")
    return get_artifact_path("lichess_data", f"{subpath}/{month}", create=True)


def _entity_columns(full_df: pd.DataFrame) -> list[str]:
    cols = [c for c in (*ENTITY_COLUMNS, *LABEL_COLUMNS) if c in full_df.columns]
    missing = [c for c in ENTITY_COLUMNS if c not in cols]
    if missing:
        raise ValueError(f"Features parquet missing entity columns: {missing}")
    return cols


def _retrieve_partition(store, entity_df: pd.DataFrame, feature_service_name: str) -> pd.DataFrame:
    feature_service = store.get_feature_service(feature_service_name)
    job = store.get_historical_features(
        entity_df=entity_df,
        features=feature_service,
    )
    return job.to_df()


def _ensure_result_label(features_df: pd.DataFrame, full_df: pd.DataFrame) -> pd.DataFrame:
    """Restore ``result_label`` when Feast retrieval omits it from the feature view."""
    if "result_label" in features_df.columns or "result_label" not in full_df.columns:
        return features_df
    labels = full_df[list(ENTITY_COLUMNS) + ["result_label"]].drop_duplicates(
        subset=list(ENTITY_COLUMNS)
    )
    merged = features_df.merge(labels, on=list(ENTITY_COLUMNS), how="left")
    log.info("Merged result_label from features parquet (%d rows)", len(merged))
    return merged


def _register_saved_dataset(
    store,
    df: pd.DataFrame,
    name: str,
    parquet_path: Path,
    feature_service_name: str,
) -> None:
    from feast.infra.offline_stores.file_source import SavedDatasetFileStorage

    entity_df = df[list(ENTITY_COLUMNS)].copy()
    if "result_label" in df.columns:
        entity_df["result_label"] = df["result_label"].values

    job = store.get_historical_features(
        entity_df=entity_df,
        features=store.get_feature_service(feature_service_name),
    )
    store.create_saved_dataset(
        from_=job,
        name=name,
        storage=SavedDatasetFileStorage(path=str(parquet_path)),
        allow_overwrite=True,
    )
    log.info("Registered Feast SavedDataset %r → %s", name, parquet_path)


def run_split(
    month: str,
    test_size: float | None = None,
    *,
    features_config: dict | None = None,
    data_config: dict | None = None,
) -> tuple[Path, Path]:
    """
    Apply Feast on full ``features.parquet``, split chronologically, persist outputs.

    Returns paths to ``train.parquet`` and ``test.parquet``.
    """
    features_cfg = features_config or load_config("lichess_features")
    data_cfg = data_config or load_config("lichess_data")
    feast_cfg = features_cfg.get("feast") or {}

    if test_size is None:
        test_size = float(feast_cfg.get("test_size", 0.2))

    features_path = _features_path(month, data_cfg)
    if not features_path.is_file():
        raise FileNotFoundError(f"Features parquet not found: {features_path}")

    out_dir = _output_dir(month, data_cfg)
    train_path = out_dir / "train.parquet"
    test_path = out_dir / "test.parquet"

    full_df = pd.read_parquet(features_path)
    entity_cols = _entity_columns(full_df)
    entity_df = full_df[entity_cols].copy()

    apply_with_source(features_path)

    feature_service_name = feast_cfg.get("feature_service", "pregame_service")
    prefix = feast_cfg.get("saved_dataset_prefix", "lichess")
    month_token = month.replace("-", "_")

    with feast_source_path(features_path):
        store = get_store()
        features_df = _retrieve_partition(store, entity_df, feature_service_name)
        features_df = _ensure_result_label(features_df, full_df)
        train_df, test_df = temporal_split(features_df, test_size=test_size)

        train_df.to_parquet(train_path, index=False)
        test_df.to_parquet(test_path, index=False)
        log.info("Saved → %s", train_path)
        log.info("Saved → %s", test_path)

        train_name = f"{prefix}_train_{month_token}"
        test_name = f"{prefix}_test_{month_token}"
        _register_saved_dataset(
            store, train_df, train_name, train_path, feature_service_name
        )
        _register_saved_dataset(
            store, test_df, test_name, test_path, feature_service_name
        )

    log.info("Split complete for %s", month)
    return train_path, test_path
