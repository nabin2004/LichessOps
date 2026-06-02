"""Feast feature definitions for Lichess pre-game features."""

from __future__ import annotations

import os
from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, FileSource
from feast.types import Float32, Int64, String

from lichess_libs.shared import load_config

cfg = load_config("lichess_features")
feast_cfg = cfg.get("feast", {})
source_path = os.environ.get("FEAST_SOURCE_PATH") or feast_cfg.get(
	"source_path", "artifacts/lichess_data/preprocessed/features.parquet"
)
ttl_days = int(feast_cfg.get("ttl_days", 3650))

pregame_source = FileSource(
	path=source_path,
	event_timestamp_column="utc_datetime",
)

game = Entity(
	name="game_id",
	join_keys=["site"],
	description="Lichess game URL",
)

pregame_features = FeatureView(
	name="pregame_features",
	entities=[game],
	ttl=timedelta(days=ttl_days),
	schema=[
		Field(name="white_elo", dtype=Float32),
		Field(name="black_elo", dtype=Float32),
		Field(name="white_rating_diff", dtype=Float32),
		Field(name="black_rating_diff", dtype=Float32),
		Field(name="elo_diff", dtype=Float32),
		Field(name="elo_diff_abs", dtype=Float32),
		Field(name="avg_elo", dtype=Float32),
		Field(name="rating_diff_net", dtype=Float32),
		Field(name="expected_white", dtype=Float32),
		Field(name="white_rating_bucket", dtype=String),
		Field(name="black_rating_bucket", dtype=String),
		Field(name="white_rating_bucket_id", dtype=Int64),
		Field(name="black_rating_bucket_id", dtype=Int64),
		Field(name="rating_bucket_diff", dtype=Int64),
		Field(name="rating_bucket_pair", dtype=String),
		Field(name="white_title_rank", dtype=Int64),
		Field(name="black_title_rank", dtype=Int64),
		Field(name="title_diff", dtype=Int64),
		Field(name="time_control", dtype=String),
		Field(name="time_control_raw", dtype=String),
		Field(name="base_seconds", dtype=Float32),
		Field(name="increment_seconds", dtype=Float32),
		Field(name="estimated_40move_time", dtype=Float32),
		Field(name="base_minutes", dtype=Float32),
		Field(name="base_x_increment", dtype=Float32),
		Field(name="is_blitz", dtype=Int64),
		Field(name="is_rapid", dtype=Int64),
		Field(name="is_classical", dtype=Int64),
		Field(name="tournament_type", dtype=String),
		Field(name="is_tournament", dtype=Int64),
		Field(name="eco", dtype=String),
		Field(name="eco_area", dtype=String),
		Field(name="opening_family", dtype=String),
		Field(name="opening_type", dtype=String),
		Field(name="is_gambit", dtype=Int64),
		Field(name="opening_frequency", dtype=Float32),
		Field(name="opening_white_win_rate", dtype=Float32),
		Field(name="eco_prior_count", dtype=Int64),
		Field(name="white_eco_prior_count", dtype=Int64),
		Field(name="white_eco_score", dtype=Float32),
		Field(name="black_eco_prior_count", dtype=Int64),
		Field(name="black_eco_score", dtype=Float32),
		Field(name="h2h_total", dtype=Int64),
		Field(name="h2h_white_win_rate", dtype=Float32),
		Field(name="h2h_draw_rate", dtype=Float32),
		Field(name="h2h_black_win_rate", dtype=Float32),
		Field(name="white_color_perf", dtype=Float32),
		Field(name="black_color_perf", dtype=Float32),
		Field(name="day_of_week", dtype=Int64),
		Field(name="hour", dtype=Int64),
		Field(name="time_of_day", dtype=String),
		Field(name="session_bucket", dtype=String),
		Field(name="is_weekend", dtype=Int64),
		Field(name="is_night", dtype=Int64),
		Field(name="is_morning", dtype=Int64),
		Field(name="is_afternoon", dtype=Int64),
		Field(name="is_evening", dtype=Int64),
		Field(name="is_peak_gaming", dtype=Int64),
	],
	source=pregame_source,
	online=True,
)

pregame_service = FeatureService(
	name="pregame_service",
	features=[pregame_features],
)
