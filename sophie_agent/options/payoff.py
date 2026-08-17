"""Port of client/src/lib/options/payoff.ts — universal multi-leg option payoff engine.

Kept function-for-function identical to the TS so the two can't drift. See schemas.OptionLeg for
the leg shape (same fields: type, side, strike, premium, quantity).
"""

from __future__ import annotations

from ..core.schemas import OptionLeg


def leg_pnl(price: float, leg: OptionLeg) -> float:
    """P&L of a single leg at expiration, at underlying price `price`."""
    quantity = leg.quantity or 1
    if leg.type == "stock":
        directional = (price - leg.strike) if leg.side == "long" else (leg.strike - price)
        return quantity * directional
    intrinsic = max(0.0, price - leg.strike) if leg.type == "call" else max(0.0, leg.strike - price)
    directional = intrinsic if leg.side == "long" else -intrinsic
    premium_flow = -leg.premium if leg.side == "long" else leg.premium
    return quantity * (directional + premium_flow)


def legs_pnl(price: float, legs: list[OptionLeg]) -> float:
    """Combined P&L of every leg at expiration, at underlying price `price`."""
    return sum(leg_pnl(price, leg) for leg in legs)


def net_premium(legs: list[OptionLeg]) -> float:
    """Net premium across all legs: positive = net credit, negative = net debit. Stock legs
    contribute nothing — buying/holding stock is a cost basis, not option premium."""
    total = 0.0
    for leg in legs:
        if leg.type == "stock":
            continue
        quantity = leg.quantity or 1
        flow = leg.premium if leg.side == "short" else -leg.premium
        total += quantity * flow
    return total


def find_breakevens(labels: list[float], payoff_data: list[float]) -> list[float]:
    """Every zero-crossing of a price/P&L series, interpolated so breakevens are exact rather than
    rounded to the nearest sampled price."""
    breakevens: list[float] = []
    for i in range(len(payoff_data) - 1):
        a, b = payoff_data[i], payoff_data[i + 1]
        if a == 0:
            breakevens.append(labels[i])
        elif (a < 0 < b) or (a > 0 > b):
            t = -a / (b - a)
            breakevens.append(labels[i] + t * (labels[i + 1] - labels[i]))
    if payoff_data[-1] == 0:
        breakevens.append(labels[-1])
    return breakevens


def max_profit_loss(legs: list[OptionLeg], price_grid: list[float]) -> tuple[float, float]:
    """Max profit / max loss over a sampled price grid, cross-checked against the algebraic net
    premium so build_strategy can raise on disagreement (deterministic-math cross-check)."""
    pnls = [legs_pnl(p, legs) for p in price_grid]
    return max(pnls), min(pnls)
