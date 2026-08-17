"""Port of client/src/lib/options/liquidity.ts — composite liquidity tiering.

Spread gates the score rather than averaging into it: no amount of open interest makes a contract
"liquid" if you can't get filled near mid today.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

LiquidityTier = Literal["excellent", "good", "fair", "poor", "unknown"]


@dataclass
class LiquidityRead:
    spread_dollar: float | None
    spread_pct: float | None
    score: float | None
    tier: LiquidityTier


def _spread_score(spread_pct: float) -> float:
    if spread_pct <= 0.003:
        return 100.0
    if spread_pct <= 0.01:
        return 100 - ((spread_pct - 0.003) / (0.01 - 0.003)) * 30
    if spread_pct <= 0.03:
        return 70 - ((spread_pct - 0.01) / (0.03 - 0.01)) * 40
    if spread_pct <= 0.08:
        return 30 - ((spread_pct - 0.03) / (0.08 - 0.03)) * 30
    return 0.0


def _volume_score(volume: float) -> float:
    if volume <= 0:
        return 0.0
    return min(100.0, (math.log10(volume + 1) / math.log10(1001)) * 100)


def _oi_score(open_interest: float) -> float:
    if open_interest <= 0:
        return 0.0
    return min(100.0, (math.log10(open_interest + 1) / math.log10(5001)) * 100)


def _tier_for(score: float) -> LiquidityTier:
    if score >= 75:
        return "excellent"
    if score >= 50:
        return "good"
    if score >= 25:
        return "fair"
    return "poor"


def compute_liquidity(
    bid: float | None,
    ask: float | None,
    mid: float | None,
    volume: float | None,
    open_interest: float | None,
) -> LiquidityRead:
    if bid is None or ask is None or mid is None or mid <= 0 or ask < bid:
        return LiquidityRead(spread_dollar=None, spread_pct=None, score=None, tier="unknown")

    spread_dollar = ask - bid
    spread_pct = spread_dollar / mid

    s_score = _spread_score(spread_pct)
    activity_score = _volume_score(volume or 0) * 0.65 + _oi_score(open_interest or 0) * 0.35
    score = round(s_score * (0.55 + 0.45 * activity_score / 100))

    return LiquidityRead(spread_dollar=spread_dollar, spread_pct=spread_pct, score=score, tier=_tier_for(score))
