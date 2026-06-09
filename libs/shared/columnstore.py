"""MariaDB ColumnStore client helpers (mirror of lichess_libs.shared.columnstore)."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote_plus

import pandas as pd

from lichess_libs.shared.logger import get_logger
from lichess_libs.shared.storage_config import columnstore_settings, parse_month

_logger = get_logger(__name__)

WIDE_EXPORT_SQL = """
SELECT
    fg.event,
    fg.game_id AS site,
    dd.calendar_date AS `date`,
    NULL AS `round`,
    wp.username AS white,
    bp.username AS black,
    wp.title AS white_title,
    bp.title AS black_title,
    fg.result AS `result`,
    DATE_FORMAT(dd.calendar_date, '%Y.%m.%d') AS utc_date,
    SUBSTRING(fg.utc_datetime, 12, 8) AS utc_time,
    fg.white_elo,
    fg.black_elo,
    fg.white_rating_diff,
    fg.black_rating_diff,
    op.eco,
    op.opening_name AS opening,
    fg.time_control,
    fg.termination,
    CAST('[]' AS CHAR) AS moves
FROM fact_games fg
LEFT JOIN dim_player wp ON fg.white_player_id = wp.player_id
LEFT JOIN dim_player bp ON fg.black_player_id = bp.player_id
LEFT JOIN dim_opening op ON fg.opening_id = op.opening_id
LEFT JOIN dim_date dd ON fg.date_id = dd.date_id
WHERE fg.year = %s AND fg.month = %s
"""

CHUNK_SIZE = 10_000

WIDE_FROM_COLUMNSTORE = {
    "game_date": "date",
    "white_player": "white",
    "black_player": "black",
    "opening_name": "opening",
    "event_name": "event",
    "round_label": "round",
    "game_result": "result",
}


def columnstore_enabled() -> bool:
    return os.getenv("MARIADB_COLUMNSTORE_DISABLED", "").strip().lower() not in {
        "1",
        "true",
        "yes",
    }


@lru_cache(maxsize=1)
def _engine_url(config_key: str = "") -> str:
    settings = columnstore_settings()
    user = quote_plus(settings["user"])
    password = quote_plus(settings["password"])
    host = settings["host"]
    port = settings["port"]
    database = settings["database"]
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"


def get_engine(config: dict[str, Any] | None = None):
    from sqlalchemy import create_engine

    _ = config
    return create_engine(_engine_url(), pool_pre_ping=True, pool_recycle=3600)


@contextmanager
def get_connection(config: dict[str, Any] | None = None) -> Iterator[Any]:
    engine = get_engine(config)
    conn = engine.raw_connection()
    try:
        yield conn
    finally:
        conn.close()
        engine.dispose()


def ping(config: dict[str, Any] | None = None) -> bool:
    if not columnstore_enabled():
        return False
    try:
        with get_connection(config) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
        return True
    except Exception as exc:
        _logger.debug("ColumnStore ping failed: %s", exc)
        return False


def ensure_schema(config: dict[str, Any] | None = None) -> None:
    _ = config
    if not ping():
        raise ConnectionError("ColumnStore is not reachable; cannot ensure schema")


def query_dataframe(
    sql: str,
    params: tuple | list | dict | None = None,
    *,
    config: dict[str, Any] | None = None,
) -> pd.DataFrame:
    engine = get_engine(config)
    return pd.read_sql(sql, engine, params=params)


def _delete_month(cursor, table: str, year: int, month: int) -> None:
    cursor.execute(
        f"DELETE FROM {table} WHERE year = %s AND month = %s",
        (year, month),
    )


def _insert_dataframe(cursor, table: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    frame = df.astype(object).where(pd.notna(df), None)
    columns = list(frame.columns)
    placeholders = ", ".join(["%s"] * len(columns))
    col_sql = ", ".join(columns)
    sql = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"
    rows = [tuple(row) for row in frame.itertuples(index=False, name=None)]
    total = 0
    for start in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[start : start + CHUNK_SIZE]
        cursor.executemany(sql, chunk)
        total += len(chunk)
    return total


def bulk_upsert_month(
    table: str,
    df: pd.DataFrame,
    year: int,
    month: int,
    *,
    config: dict[str, Any] | None = None,
) -> int:
    with get_connection(config) as conn:
        cursor = conn.cursor()
        try:
            _delete_month(cursor, table, year, month)
            inserted = _insert_dataframe(cursor, table, df)
            conn.commit()
            return inserted
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def bulk_replace_dimension(
    table: str,
    df: pd.DataFrame,
    *,
    config: dict[str, Any] | None = None,
) -> int:
    with get_connection(config) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(f"DELETE FROM {table}")
            inserted = _insert_dataframe(cursor, table, df)
            conn.commit()
            return inserted
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def export_wide_parquet(
    month: str,
    out_path: Path,
    *,
    config: dict[str, Any] | None = None,
    prefer_wide_table: bool = True,
) -> Path:
    year, mon = parse_month(month)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if prefer_wide_table:
        wide = query_dataframe(
            "SELECT * FROM wide_games WHERE year = %s AND month = %s",
            (year, mon),
            config=config,
        )
        if not wide.empty:
            wide.rename(columns=WIDE_FROM_COLUMNSTORE, inplace=True)
            wide.to_parquet(out_path, index=False)
            _logger.info("Exported wide_games table to %s", out_path)
            return out_path

    frame = query_dataframe(WIDE_EXPORT_SQL, (year, mon), config=config)
    frame.to_parquet(out_path, index=False)
    _logger.info("Exported joined wide games to %s", out_path)
    return out_path


def insert_prediction_log(
    *,
    player_elo: int,
    opponent_elo: int,
    predicted_outcome: str,
    probabilities: dict[str, float] | None = None,
    player_color: str | None = None,
    eco: str | None = None,
    game_type: str | None = None,
    model_uri: str | None = None,
    source: str = "serving",
    inferred_at: datetime | None = None,
    config: dict[str, Any] | None = None,
) -> None:
    if not columnstore_enabled():
        return

    probs = probabilities or {}
    prediction = {"0": 0, "1": 1, "½": 2, "1/2-1/2": 2}.get(predicted_outcome)
    ts = inferred_at or datetime.now(tz=UTC)

    sql = """
        INSERT INTO prediction_logs (
            player_elo, opponent_elo, player_color, eco, game_type,
            predicted_outcome, prediction, prob_lose, prob_win, prob_draw,
            probabilities, model_uri, source, inferred_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        player_elo,
        opponent_elo,
        player_color,
        eco,
        game_type,
        predicted_outcome,
        prediction,
        probs.get("lose"),
        probs.get("win"),
        probs.get("draw"),
        json.dumps(probs) if probs else None,
        model_uri,
        source,
        ts.replace(tzinfo=None),
    )
    with get_connection(config) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def insert_batch_predictions(
    df: pd.DataFrame,
    *,
    run_id: str,
    month: str,
    model_uri: str | None = None,
    source: str = "evaluate",
    config: dict[str, Any] | None = None,
) -> int:
    if df.empty:
        return 0

    frame = df.copy()
    frame["run_id"] = run_id
    frame["month"] = month
    frame["model_uri"] = model_uri
    frame["source"] = source

    columns = [
        "run_id",
        "month",
        "y_true",
        "y_pred",
        "pred_display",
        "prob_lose",
        "prob_win",
        "prob_draw",
        "player_elo",
        "opponent_elo",
        "eco",
        "game_type",
        "model_uri",
        "source",
    ]
    for col in columns:
        if col not in frame.columns:
            frame[col] = None
    payload = frame[columns]
    with get_connection(config) as conn:
        cursor = conn.cursor()
        try:
            inserted = _insert_dataframe(cursor, "batch_predictions", payload)
            conn.commit()
            return inserted
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def record_inference_run(
    *,
    run_id: str,
    month: str,
    source: str,
    row_count: int,
    model_uri: str | None = None,
    metrics: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
) -> None:
    now = datetime.now(tz=UTC).replace(tzinfo=None)
    sql = """
        INSERT INTO inference_runs (
            run_id, month, model_uri, source, row_count, metrics_json, started_at, completed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            row_count = VALUES(row_count),
            metrics_json = VALUES(metrics_json),
            completed_at = VALUES(completed_at)
    """
    params = (
        run_id,
        month,
        model_uri,
        source,
        row_count,
        json.dumps(metrics) if metrics else None,
        now,
        now,
    )
    with get_connection(config) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def fetch_prediction_logs(limit: int = 100, *, config: dict[str, Any] | None = None) -> list[dict]:
    frame = query_dataframe(
        """
        SELECT player_elo, opponent_elo, game_type, prediction, probabilities,
               inferred_at AS timestamp
        FROM prediction_logs
        ORDER BY id DESC
        LIMIT %s
        """,
        (limit,),
        config=config,
    )
    if frame.empty:
        return []
    logs: list[dict] = []
    for row in frame.to_dict(orient="records"):
        probs = row.pop("probabilities", None)
        if isinstance(probs, str):
            try:
                probs = json.loads(probs)
            except json.JSONDecodeError:
                probs = None
        if probs is not None:
            row["probabilities"] = (
                [probs.get("lose"), probs.get("win"), probs.get("draw")]
                if isinstance(probs, dict)
                else probs
            )
        if row.get("timestamp") is not None:
            row["timestamp"] = str(row["timestamp"])
        logs.append(row)
    return logs


def fetch_batch_predictions_as_monitoring_frame(
    month: str,
    *,
    limit: int = 5000,
    config: dict[str, Any] | None = None,
) -> pd.DataFrame:
    return query_dataframe(
        """
        SELECT y_true AS target, y_pred AS prediction, player_elo, opponent_elo,
               eco, game_type, prob_lose, prob_win, prob_draw
        FROM batch_predictions
        WHERE month = %s
        ORDER BY id DESC
        LIMIT %s
        """,
        (month, limit),
        config=config,
    )
