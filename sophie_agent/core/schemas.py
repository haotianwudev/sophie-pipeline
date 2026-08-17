"""Typed answer models. Structured output goes through these instead of free prose, so a
recommendation with no cited evidence fails validation rather than being politely hedged.

Constraints are declared with `Field(...)` rather than enforced in `field_validator` hooks. That is
not a style preference: `create_agent(response_format=ToolStrategy(...))` binds these models as an
output tool, so anything expressible in the JSON schema — `minItems`, `minimum`, `maximum` — is
visible to the model as part of the contract it is filling in. A `field_validator` is invisible until
after the model has already answered, which turns a knowable requirement into a retry.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field


class OptionLeg(BaseModel):
    """Exactly the client's OptionLeg shape (client/src/lib/options/payoff.ts) so a resolved
    position is already a valid Options Viewer payload."""

    type: Literal["call", "put", "stock"]
    side: Literal["long", "short"]
    strike: float
    premium: float
    quantity: Annotated[int, Field(ge=1, description="Contract count; direction is carried by `side`.")] = 1


class Citation(BaseModel):
    kind: Literal["wiki", "run", "chain", "sql", "graphql"]
    ref: Annotated[str, Field(min_length=1, description="wiki path / config_hash / quote_date / query id")]
    as_of: str | None = None


class StrategyRecommendation(BaseModel):
    preset_id: str
    legs: Annotated[list[OptionLeg], Field(min_length=1, description="Resolved legs; must be non-empty.")]
    net_credit: float
    max_profit: float
    max_loss: float
    breakevens: list[float]
    probability_of_profit: Annotated[float, Field(ge=0.0, le=1.0)]
    rationale: str
    evidence: Annotated[
        list[Citation],
        Field(min_length=1, description="At least one source must be cited for the recommendation."),
    ]
    evidence_strength: Literal["backtested", "conventional", "unsupported"]
    caveats: list[str] = Field(default_factory=list)
    confidence: Annotated[float, Field(ge=0.0, le=1.0)]
