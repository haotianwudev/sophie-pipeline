"""Typed answer models. structured() output goes through these instead of free prose, so a
recommendation with no cited evidence fails validation rather than being politely hedged."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator


class OptionLeg(BaseModel):
    """Exactly the client's OptionLeg shape (client/src/lib/options/payoff.ts) so a resolved
    position is already a valid Options Viewer payload."""

    type: Literal["call", "put", "stock"]
    side: Literal["long", "short"]
    strike: float
    premium: float
    quantity: int = 1


class Citation(BaseModel):
    kind: Literal["wiki", "run", "chain", "sql", "graphql"]
    ref: str  # wiki path / config_hash / quote_date / query id
    as_of: str | None = None


class StrategyRecommendation(BaseModel):
    preset_id: str
    legs: list[OptionLeg]
    net_credit: float
    max_profit: float
    max_loss: float
    breakevens: list[float]
    probability_of_profit: float
    rationale: str
    evidence: list[Citation]
    evidence_strength: Literal["backtested", "conventional", "unsupported"]
    caveats: list[str] = Field(default_factory=list)
    confidence: float

    @field_validator("evidence")
    @classmethod
    def _evidence_required(cls, v: list[Citation]) -> list[Citation]:
        if not v:
            raise ValueError("evidence must be non-empty — a recommendation must cite at least one source")
        return v

    @field_validator("legs")
    @classmethod
    def _legs_required(cls, v: list[OptionLeg]) -> list[OptionLeg]:
        if not v:
            raise ValueError("legs must be non-empty")
        return v
