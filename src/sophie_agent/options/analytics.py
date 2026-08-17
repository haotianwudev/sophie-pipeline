"""Port of client/src/lib/options/analytics.ts — IV solving, net greeks, POP, implied range.

Kept close to the TS: same bisection tolerance, same risk-neutral lognormal model for
probability-of-profit and implied price range.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Literal

from .blackscholes import black_scholes, inverse_standard_normal_cdf, standard_normal_cdf
from ..schemas import OptionLeg

SPX_MULTIPLIER = 100
SPX_DEFAULT_RATE = 0.054
SPX_DEFAULT_DIV_YIELD = 0.013


@dataclass
class PricedLeg:
    leg: OptionLeg
    iv: float


def solve_implied_vol(price: float, S: float, K: float, T: float, r: float, q: float, option_type: Literal["call", "put"]) -> float:
    """Implied vol via bisection on black_scholes' price."""
    if T <= 0 or price <= 0:
        return 0.0
    bs_type = "Call" if option_type == "call" else "Put"
    lo, hi = 0.001, 5.0
    for _ in range(100):
        mid = (lo + hi) / 2
        p = black_scholes(S, K, T, r, mid, bs_type, q).price
        if p > price:
            hi = mid
        else:
            lo = mid
    return (lo + hi) / 2


@dataclass
class NetGreeks:
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0


def net_greeks(legs: list[PricedLeg], S: float, T: float, r: float, q: float, multiplier: float = SPX_MULTIPLIER) -> NetGreeks:
    totals = NetGreeks()
    for pl in legs:
        leg = pl.leg
        quantity = leg.quantity or 1
        sign = 1 if leg.side == "long" else -1
        scale = sign * quantity * multiplier
        if leg.type == "stock":
            totals.delta += scale
            continue
        bs_type = "Call" if leg.type == "call" else "Put"
        g = black_scholes(S, leg.strike, T, r, pl.iv, bs_type, q)
        totals.delta += scale * g.delta
        totals.gamma += scale * g.gamma
        totals.theta += scale * g.theta
        totals.vega += scale * g.vega
    return totals


def probability_of_profit(
    legs_pnl_at: Callable[[float], float],
    breakevens: list[float],
    S: float,
    T: float,
    r: float,
    q: float,
    atm_iv: float,
) -> float:
    if T <= 0 or atm_iv <= 0:
        return 1.0 if legs_pnl_at(S) > 0 else 0.0

    sorted_be = sorted(breakevens)
    bounds = [0.0, *sorted_be, math.inf]
    sigma_sqrt_t = atm_iv * math.sqrt(T)

    def prob_above(x: float) -> float:
        if x <= 0:
            return 1.0
        if x == math.inf:
            return 0.0
        d2 = (math.log(S / x) + (r - q - (atm_iv * atm_iv) / 2) * T) / sigma_sqrt_t
        return standard_normal_cdf(d2)

    pop = 0.0
    for i in range(len(bounds) - 1):
        lo, hi = bounds[i], bounds[i + 1]
        test_point = lo * 1.5 + 1 if hi == math.inf else (lo + hi) / 2
        if legs_pnl_at(test_point) > 0:
            pop += prob_above(lo) - prob_above(hi)
    return min(1.0, max(0.0, pop))


def implied_boundary_price(S: float, T: float, r: float, q: float, iv: float, confidence: float, side: Literal["lower", "upper"]) -> float:
    if T <= 0 or iv <= 0:
        return S
    z = inverse_standard_normal_cdf((1 + confidence) / 2)
    drift = (r - q - (iv * iv) / 2) * T
    spread = iv * math.sqrt(T) * z
    return S * math.exp(drift + (spread if side == "upper" else -spread))


def implied_price_range(S: float, T: float, r: float, q: float, iv: float, confidence: float) -> tuple[float, float]:
    if T <= 0 or iv <= 0:
        return S, S
    return (
        implied_boundary_price(S, T, r, q, iv, confidence, "lower"),
        implied_boundary_price(S, T, r, q, iv, confidence, "upper"),
    )
