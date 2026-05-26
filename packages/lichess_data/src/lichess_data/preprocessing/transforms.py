"""Feature transforms for raw Lichess Parquet game records."""

from __future__ import annotations

import re

import numpy as np
import pandas as pd

from libs.shared import get_logger

log = get_logger("lichess_data.preprocessing")

# ── Stage 1 — Event parsing ──────────────────────────────────────────────────
TIME_CONTROL_MAP = {
    "Rated Bullet game": "Bullet",
    "Rated Bullet tournament": "Bullet",
    "Rated Blitz game": "Blitz",
    "Rated Blitz tournament": "Blitz",
    "Rated Classical game": "Classical",
    "Rated Classical tournament": "Classical",
    "Rated Correspondence game": "Correspondence",
}
TIME_CONTROL_ORDER = ["Bullet", "Blitz", "Classical", "Correspondence"]

# ── Stage 2 — Result encoding ────────────────────────────────────────────────
RESULT_LABEL_MAP = {"1-0": 0, "0-1": 1, "1/2-1/2": 2}
WINNER_WHITE_MAP = {"1-0": 1, "0-1": -1, "1/2-1/2": 0}

# ── Stage 3 — Date features ──────────────────────────────────────────────────
MONTH_NAMES = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]

# ── Stage 6 — Opening / time-control parsing ───────────────────────────────────
_TC_RE = re.compile(r"^(\d+)\+(\d+)$")


def parse_event(df: pd.DataFrame) -> pd.DataFrame:
    """Extract event_name, tournament_url, time_control, is_tournament."""
    log.info("Stage 1 — parsing event column")

    if "time_control" in df.columns:
        df["time_control_raw"] = df["time_control"]

    extracted = df["event"].astype("string").str.extract(
        r"^(?P<event_name>.*?)(?:\s+(?P<tournament_url>https://\S+))?$"
    )
    df["event_name"] = extracted["event_name"].str.strip()
    df["tournament_url"] = extracted["tournament_url"]

    df["time_control"] = (
        df["event_name"]
        .map(TIME_CONTROL_MAP)
        .astype(pd.CategoricalDtype(categories=TIME_CONTROL_ORDER, ordered=True))
    )
    df["is_tournament"] = (
        df["event_name"].str.contains("tournament", case=False).astype(int)
    )

    unmapped = df["time_control"].isna().sum()
    if unmapped:
        log.warning(
            "  %d rows have unmapped time_control (unknown event_name)", unmapped
        )

    log.info(
        "  time_control distribution:\n%s",
        df["time_control"].value_counts().to_string(),
    )
    return df


def encode_result(df: pd.DataFrame) -> pd.DataFrame:
    """Multi-class label, binary targets, winner perspective, decisive flag."""
    log.info("Stage 2 — encoding result")

    df["result_label"] = df["result"].map(RESULT_LABEL_MAP)
    df["white_win"] = (df["result"] == "1-0").astype(int)
    df["black_win"] = (df["result"] == "0-1").astype(int)
    df["is_draw"] = (df["result"] == "1/2-1/2").astype(int)
    df["decisive_game"] = (df["is_draw"] == 0).astype(int)
    df["winner_white_perspective"] = df["result"].map(WINNER_WHITE_MAP)

    log.info(
        "  result distribution — White wins: %d | Black wins: %d | Draws: %d",
        df["white_win"].sum(),
        df["black_win"].sum(),
        df["is_draw"].sum(),
    )
    return df


def extract_date_features(df: pd.DataFrame) -> pd.DataFrame:
    """Year, month, day, day_of_week, quarter, is_weekend, days_since_start."""
    log.info("Stage 3 — extracting date features")

    df["utc_date"] = pd.to_datetime(df["utc_date"], format="%Y.%m.%d")

    df["year"] = df["utc_date"].dt.year
    df["month"] = df["utc_date"].dt.month
    df["day"] = df["utc_date"].dt.day
    df["day_of_week"] = df["utc_date"].dt.dayofweek
    df["quarter"] = df["utc_date"].dt.quarter
    df["day_of_year"] = df["utc_date"].dt.dayofyear
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    df["month_name"] = pd.Categorical(
        df["utc_date"].dt.strftime("%b"), categories=MONTH_NAMES, ordered=True
    )
    df["days_since_start"] = (df["utc_date"] - df["utc_date"].min()).dt.days

    log.info(
        "  date range: %s → %s",
        df["utc_date"].min().date(),
        df["utc_date"].max().date(),
    )
    return df


def extract_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """Hour, time_of_day, cyclical sin/cos, peak-gaming flag."""
    log.info("Stage 4 — extracting time features")

    time_dt = pd.to_datetime(df["utc_time"], format="%H:%M:%S")

    df["hour"] = time_dt.dt.hour
    df["minute"] = time_dt.dt.minute
    df["second"] = time_dt.dt.second
    df["seconds_since_midnight"] = (
        df["hour"] * 3600 + df["minute"] * 60 + df["second"]
    )

    df["time_of_day"] = pd.cut(
        df["hour"],
        bins=[-1, 5, 11, 17, 23],
        labels=["Night", "Morning", "Afternoon", "Evening"],
    )

    df["is_night"] = ((df["hour"] >= 22) | (df["hour"] <= 5)).astype(int)
    df["is_morning"] = ((df["hour"] >= 6) & (df["hour"] <= 11)).astype(int)
    df["is_afternoon"] = ((df["hour"] >= 12) & (df["hour"] <= 17)).astype(int)
    df["is_evening"] = ((df["hour"] >= 18) & (df["hour"] <= 21)).astype(int)
    df["is_peak_gaming"] = ((df["hour"] >= 16) & (df["hour"] <= 23)).astype(int)

    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)

    df["utc_datetime"] = pd.to_datetime(
        df["utc_date"].dt.strftime("%Y.%m.%d") + " " + df["utc_time"],
        format="%Y.%m.%d %H:%M:%S",
    )

    log.info(
        "  time_of_day distribution:\n%s",
        df["time_of_day"].value_counts().to_string(),
    )
    return df


def impute_ratings(df: pd.DataFrame) -> pd.DataFrame:
    """Group-median imputation by time_control; fallback to global median / 0."""
    log.info("Stage 5 — imputing Elo ratings")

    for col in ["white_elo", "black_elo"]:
        before = df[col].isna().sum()
        df[col] = df.groupby("time_control", observed=False)[col].transform(
            lambda x: x.fillna(x.median())
        )
        df[col] = df[col].fillna(df[col].median())
        log.info("  %s: filled %d NaNs", col, before)

    for col in ["white_rating_diff", "black_rating_diff"]:
        before = df[col].isna().sum()
        df[col] = df.groupby("time_control", observed=False)[col].transform(
            lambda x: x.fillna(x.median())
        )
        df[col] = df[col].fillna(0)
        log.info("  %s: filled %d NaNs", col, before)

    df["elo_diff"] = df["white_elo"] - df["black_elo"]
    df["elo_diff_abs"] = df["elo_diff"].abs()
    df["avg_elo"] = (df["white_elo"] + df["black_elo"]) / 2
    df["rating_diff_net"] = df["white_rating_diff"] - df["black_rating_diff"]

    return df


def _tc_to_seconds(tc_str: str) -> float:
    """Convert PGN ``TimeControl`` strings like ``300+0`` to total seconds."""
    m = _TC_RE.match(str(tc_str))
    if not m:
        return np.nan
    base, increment = int(m.group(1)), int(m.group(2))
    return base + 40 * increment


def extract_opening_features(df: pd.DataFrame) -> pd.DataFrame:
    """ECO area code, opening family, time_control seconds."""
    log.info("Stage 6 — extracting opening features")

    df["eco_area"] = df["eco"].astype(str).str[0]
    df["opening_family"] = (
        df["opening"].astype(str).str.split(r"[:|]").str[0].str.strip()
    )

    tc_col = "time_control_raw" if "time_control_raw" in df.columns else None
    if tc_col is not None:
        df["tc_seconds"] = df[tc_col].apply(_tc_to_seconds)

    log.info("  ECO areas: %s", df["eco_area"].value_counts().to_dict())
    log.info("  unique opening families: %d", df["opening_family"].nunique())
    return df


def extract_move_features(df: pd.DataFrame) -> pd.DataFrame:
    """Count half-moves (plies) from the moves column."""
    log.info("Stage 7 — extracting move features")

    if "moves" not in df.columns:
        log.warning("  'moves' column not found — skipping")
        return df

    def _count_moves(m) -> int | float:
        if m is None or (isinstance(m, float) and np.isnan(m)):
            return np.nan
        if isinstance(m, (list, tuple)):
            return len(m)
        if hasattr(m, "__len__") and not isinstance(m, str):
            return len(m)
        if isinstance(m, str):
            return len(m.split())
        return np.nan

    df["move_count"] = df["moves"].apply(_count_moves)

    valid = df["move_count"].dropna()
    if valid.empty:
        log.warning("  no valid move counts")
    else:
        log.info(
            "  move_count — mean: %.1f | median: %.1f | max: %.0f",
            valid.mean(),
            valid.median(),
            valid.max(),
        )
    return df
