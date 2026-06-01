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

TITLE_RANK = {
    "CM": 1,
    "WCM": 1,
    "FM": 2,
    "WFM": 2,
    "IM": 3,
    "WIM": 3,
    "GM": 4,
    "WGM": 4,
}

RATING_BUCKET_EDGES = [0, 2000, 2200, 2400, np.inf]
RATING_BUCKET_LABELS = ["<2000", "2000-2200", "2200-2400", ">2400"]

OPENING_TYPE_BY_ECO = {
    "A": "Flank",
    "B": "Semi-Open",
    "C": "Open",
    "D": "Closed",
    "E": "Closed",
}

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


def _normalize_optional_str(value: str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    value = str(value).strip()
    return value or None


def _infer_tournament_type(event_name: str | None, tournament_url: str | None) -> str:
    event_name = _normalize_optional_str(event_name)
    tournament_url = _normalize_optional_str(tournament_url)
    if tournament_url:
        lowered = tournament_url.lower()
        if "/swiss/" in lowered:
            return "Swiss"
        if "/tournament/" in lowered:
            return "Arena"
        if "round-robin" in lowered or "round_robin" in lowered:
            return "RoundRobin"
        if "knockout" in lowered or "/ko/" in lowered:
            return "Knockout"
        return "Tournament"
    if event_name and "tournament" in event_name.lower():
        return "Tournament"
    return "Game"


def parse_event(df: pd.DataFrame) -> pd.DataFrame:
    """Extract event_name, tournament_url, time_control, is_tournament, tournament_type."""
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
    df["tournament_type"] = [
        _infer_tournament_type(event, url)
        for event, url in zip(df["event_name"], df["tournament_url"])
    ]

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
    df["session_bucket"] = df["time_of_day"]

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
    log.info("Stage 6 — imputing Elo ratings")

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

    df["expected_white"] = 1 / (
        1 + 10 ** ((df["black_elo"] - df["white_elo"]) / 400)
    )

    df["white_rating_bucket"] = pd.cut(
        df["white_elo"], bins=RATING_BUCKET_EDGES, labels=RATING_BUCKET_LABELS
    )
    df["black_rating_bucket"] = pd.cut(
        df["black_elo"], bins=RATING_BUCKET_EDGES, labels=RATING_BUCKET_LABELS
    )
    df["white_rating_bucket_id"] = df["white_rating_bucket"].cat.codes
    df["black_rating_bucket_id"] = df["black_rating_bucket"].cat.codes
    df["rating_bucket_diff"] = (
        df["white_rating_bucket_id"] - df["black_rating_bucket_id"]
    )
    df["rating_bucket_pair"] = (
        df["white_rating_bucket"].astype(str)
        + "_vs_"
        + df["black_rating_bucket"].astype(str)
    )

    return df


def _parse_time_control(tc_str: str) -> tuple[float, float]:
    """Return base and increment seconds from PGN ``TimeControl`` strings."""
    m = _TC_RE.match(str(tc_str))
    if not m:
        return np.nan, np.nan
    return float(m.group(1)), float(m.group(2))


def extract_time_control_features(df: pd.DataFrame) -> pd.DataFrame:
    """Parse time control into base/increment and derived buckets."""
    log.info("Stage 5 — extracting time-control features")

    tc_col = "time_control_raw" if "time_control_raw" in df.columns else "time_control"
    base_inc = df[tc_col].apply(_parse_time_control)
    df["base_seconds"] = base_inc.apply(lambda x: x[0])
    df["increment_seconds"] = base_inc.apply(lambda x: x[1])
    df["estimated_40move_time"] = df["base_seconds"] + 40 * df["increment_seconds"]
    df["tc_seconds"] = df["estimated_40move_time"]
    df["base_minutes"] = df["base_seconds"] / 60

    df["is_blitz"] = (df["base_seconds"] < 300).astype(int)
    df["is_rapid"] = (
        (df["base_seconds"] >= 300) & (df["base_seconds"] <= 600)
    ).astype(int)
    df["is_classical"] = (df["base_seconds"] > 600).astype(int)
    df["base_x_increment"] = df["base_seconds"] * df["increment_seconds"]

    return df


def extract_title_features(df: pd.DataFrame) -> pd.DataFrame:
    """Map titles to ordinal ranks and compute title difference."""
    log.info("Stage 7 — extracting title features")

    for col in ["white_title", "black_title"]:
        if col not in df.columns:
            df[col] = None

    def _title_rank(value: str | None) -> int:
        if value is None:
            return 0
        return TITLE_RANK.get(str(value).upper(), 0)

    df["white_title_rank"] = df["white_title"].apply(_title_rank)
    df["black_title_rank"] = df["black_title"].apply(_title_rank)
    df["title_diff"] = df["white_title_rank"] - df["black_title_rank"]

    return df


def extract_opening_features(df: pd.DataFrame) -> pd.DataFrame:
    """ECO area code, opening family, and opening type flags."""
    log.info("Stage 8 — extracting opening features")

    df["eco_area"] = df["eco"].astype(str).str[0]
    df["opening_family"] = (
        df["opening"].astype(str).str.split(r"[:|]").str[0].str.strip()
    )

    df["opening_type"] = df["eco_area"].map(OPENING_TYPE_BY_ECO)
    df["is_gambit"] = (
        df["opening"].astype(str).str.contains("gambit", case=False).astype(int)
    )

    log.info("  ECO areas: %s", df["eco_area"].value_counts().to_dict())
    log.info("  unique opening families: %d", df["opening_family"].nunique())
    return df


def add_historical_pregame_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add leakage-safe historical aggregates based on prior games only."""
    log.info("Stage 9 — adding historical pregame aggregates")

    required = {"utc_datetime", "eco", "white_win", "black_win", "is_draw"}
    if not required.issubset(df.columns):
        missing = ", ".join(sorted(required - set(df.columns)))
        log.warning("  missing required columns for history: %s", missing)
        return df

    ordered = df.sort_values("utc_datetime").copy()

    ordered["total_prior_games"] = np.arange(len(ordered))

    eco_group = ordered.groupby("eco", observed=False)
    ordered["eco_prior_count"] = eco_group.cumcount()
    ordered["eco_prior_white_wins"] = (
        eco_group["white_win"].cumsum() - ordered["white_win"]
    )
    ordered["opening_white_win_rate"] = np.where(
        ordered["eco_prior_count"] > 0,
        ordered["eco_prior_white_wins"] / ordered["eco_prior_count"],
        0.5,
    )
    ordered["opening_frequency"] = np.where(
        ordered["total_prior_games"] > 0,
        ordered["eco_prior_count"] / ordered["total_prior_games"],
        0.0,
    )

    if {"white", "black"}.issubset(df.columns):
        ordered["white_score"] = ordered["white_win"] + 0.5 * ordered["is_draw"]
        ordered["black_score"] = ordered["black_win"] + 0.5 * ordered["is_draw"]

        white_eco_group = ordered.groupby(["white", "eco"], observed=False)
        ordered["white_eco_prior_count"] = white_eco_group.cumcount()
        ordered["white_eco_prior_score"] = (
            white_eco_group["white_score"].cumsum() - ordered["white_score"]
        )
        ordered["white_eco_score"] = np.where(
            ordered["white_eco_prior_count"] > 0,
            ordered["white_eco_prior_score"] / ordered["white_eco_prior_count"],
            0.5,
        )

        black_eco_group = ordered.groupby(["black", "eco"], observed=False)
        ordered["black_eco_prior_count"] = black_eco_group.cumcount()
        ordered["black_eco_prior_score"] = (
            black_eco_group["black_score"].cumsum() - ordered["black_score"]
        )
        ordered["black_eco_score"] = np.where(
            ordered["black_eco_prior_count"] > 0,
            ordered["black_eco_prior_score"] / ordered["black_eco_prior_count"],
            0.5,
        )

        h2h_group = ordered.groupby(["white", "black"], observed=False)
        ordered["h2h_total"] = h2h_group.cumcount()
        ordered["h2h_white_wins"] = (
            h2h_group["white_win"].cumsum() - ordered["white_win"]
        )
        ordered["h2h_draws"] = h2h_group["is_draw"].cumsum() - ordered["is_draw"]
        ordered["h2h_black_wins"] = (
            h2h_group["black_win"].cumsum() - ordered["black_win"]
        )

        ordered["h2h_white_win_rate"] = np.where(
            ordered["h2h_total"] > 0,
            ordered["h2h_white_wins"] / ordered["h2h_total"],
            1 / 3,
        )
        ordered["h2h_draw_rate"] = np.where(
            ordered["h2h_total"] > 0,
            ordered["h2h_draws"] / ordered["h2h_total"],
            1 / 3,
        )
        ordered["h2h_black_win_rate"] = np.where(
            ordered["h2h_total"] > 0,
            ordered["h2h_black_wins"] / ordered["h2h_total"],
            1 / 3,
        )

        if "expected_white" in ordered.columns:
            ordered["expected_black"] = 1 - ordered["expected_white"]
            ordered["white_perf"] = ordered["white_score"] - ordered["expected_white"]
            ordered["black_perf"] = ordered["black_score"] - ordered["expected_black"]

            white_group = ordered.groupby("white", observed=False)
            ordered["white_color_prior_count"] = white_group.cumcount()
            ordered["white_color_perf"] = (
                white_group["white_perf"].cumsum() - ordered["white_perf"]
            )
            ordered["white_color_perf"] = np.where(
                ordered["white_color_prior_count"] > 0,
                ordered["white_color_perf"] / ordered["white_color_prior_count"],
                0.0,
            )

            black_group = ordered.groupby("black", observed=False)
            ordered["black_color_prior_count"] = black_group.cumcount()
            ordered["black_color_perf"] = (
                black_group["black_perf"].cumsum() - ordered["black_perf"]
            )
            ordered["black_color_perf"] = np.where(
                ordered["black_color_prior_count"] > 0,
                ordered["black_color_perf"] / ordered["black_color_prior_count"],
                0.0,
            )

    ordered = ordered.drop(columns=[
        col
        for col in [
            "eco_prior_white_wins",
            "total_prior_games",
            "white_eco_prior_score",
            "black_eco_prior_score",
            "h2h_white_wins",
            "h2h_draws",
            "h2h_black_wins",
            "white_score",
            "black_score",
            "expected_black",
            "white_perf",
            "black_perf",
            "white_color_prior_count",
            "black_color_prior_count",
        ]
        if col in ordered.columns
    ])

    df = df.copy()
    df.loc[ordered.index, ordered.columns] = ordered
    return df


def extract_move_features(df: pd.DataFrame) -> pd.DataFrame:
    """Count half-moves (plies) from the moves column."""
    log.info("Stage 10 — extracting move features")

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
