"""Request and response schemas for outcome prediction."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class PredictRequest(BaseModel):
    player_elo: int = Field(..., ge=400, le=3500)
    opponent_elo: int = Field(..., ge=400, le=3500)
    player_color: Literal["white", "black"]
    eco: str = Field(..., min_length=1, max_length=8)
    opening_family: str | None = None
    time_control: str = "Blitz"
    time_control_raw: str | None = None
    player_eco_score: float | None = Field(default=0.5, ge=0.0, le=1.0)
    player_h2h_win_rate: float | None = Field(default=0.5, ge=0.0, le=1.0)
    opening_population_win_rate: float | None = Field(default=0.5, ge=0.0, le=1.0)


class PredictResponse(BaseModel):
    predicted_outcome: Literal["1", "0", "½"]
    probabilities: dict[str, float]
    recommended_opening_score: float | None = None


class HealthResponse(BaseModel):
    status: str
    model_loaded: bool
