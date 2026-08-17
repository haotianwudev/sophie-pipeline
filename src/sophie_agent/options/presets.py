"""Port of client/src/lib/options/presets.ts — strategy leg resolution against a real chain.

Keep all 20 preset ids identical to the TS. Only `iron_condor` is backtest-derived (from
sophie-option-research's real iron_condor_45dte.yaml, 165 real trades) — every other preset's
deltas are conventional retail defaults, documented per group below exactly as in the TS.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Literal, Union

from .chain_types import ChainContract, ExpirationChain
from ..schemas import OptionLeg

DeltaTarget = Union[float, Literal["atm"]]


@dataclass
class LegTarget:
    type: Literal["call", "put"]
    side: Literal["long", "short"]
    delta: DeltaTarget
    further_than: int | None = None
    quantity: int | None = None


@dataclass
class StrategyPreset:
    id: str
    label: str
    legs: list[LegTarget]
    backtested: bool = False
    custom: Callable[[ExpirationChain, float], list[OptionLeg]] | None = None


def _nearest_by_delta(contracts: list[ChainContract], target_abs_delta: float) -> ChainContract:
    return min(contracts, key=lambda c: abs(abs(c.delta) - target_abs_delta))


def _nearest_by_strike(contracts: list[ChainContract], spot: float) -> ChainContract:
    return min(contracts, key=lambda c: abs(c.strike - spot))


def _build_call_butterfly_legs(chain: ExpirationChain, spot: float) -> list[OptionLeg]:
    middle = _nearest_by_strike(chain.calls, spot)
    upper_candidates = [c for c in chain.calls if c.strike > middle.strike]
    upper_wing = _nearest_by_delta(upper_candidates or chain.calls, 0.16)
    width = abs(upper_wing.strike - middle.strike)
    lower_target = middle.strike - width
    lower_wing = min(chain.calls, key=lambda c: abs(c.strike - lower_target))
    return [
        OptionLeg(type="call", side="long", strike=lower_wing.strike, premium=lower_wing.mid),
        OptionLeg(type="call", side="short", strike=middle.strike, premium=middle.mid, quantity=2),
        OptionLeg(type="call", side="long", strike=upper_wing.strike, premium=upper_wing.mid),
    ]


def _build_put_butterfly_legs(chain: ExpirationChain, spot: float) -> list[OptionLeg]:
    middle = _nearest_by_strike(chain.puts, spot)
    lower_candidates = [c for c in chain.puts if c.strike < middle.strike]
    lower_wing = _nearest_by_delta(lower_candidates or chain.puts, 0.16)
    width = abs(middle.strike - lower_wing.strike)
    upper_target = middle.strike + width
    upper_wing = min(chain.puts, key=lambda c: abs(c.strike - upper_target))
    return [
        OptionLeg(type="put", side="long", strike=lower_wing.strike, premium=lower_wing.mid),
        OptionLeg(type="put", side="short", strike=middle.strike, premium=middle.mid, quantity=2),
        OptionLeg(type="put", side="long", strike=upper_wing.strike, premium=upper_wing.mid),
    ]


def _build_covered_call_legs(chain: ExpirationChain, spot: float) -> list[OptionLeg]:
    short_call = _nearest_by_delta(chain.calls, 0.30)
    return [
        OptionLeg(type="stock", side="long", strike=spot, premium=0),
        OptionLeg(type="call", side="short", strike=short_call.strike, premium=short_call.mid),
    ]


def _build_collar_legs(chain: ExpirationChain, spot: float) -> list[OptionLeg]:
    long_put = _nearest_by_delta(chain.puts, 0.20)
    short_call = _nearest_by_delta(chain.calls, 0.20)
    return [
        OptionLeg(type="stock", side="long", strike=spot, premium=0),
        OptionLeg(type="put", side="long", strike=long_put.strike, premium=long_put.mid),
        OptionLeg(type="call", side="short", strike=short_call.strike, premium=short_call.mid),
    ]


def _build_buffered_legs(chain: ExpirationChain, spot: float) -> list[OptionLeg]:
    long_put = _nearest_by_delta(chain.puts, 0.35)
    further_puts = [c for c in chain.puts if c.strike < long_put.strike]
    short_put = _nearest_by_delta(further_puts or chain.puts, 0.10)
    short_call = _nearest_by_delta(chain.calls, 0.20)
    return [
        OptionLeg(type="stock", side="long", strike=spot, premium=0),
        OptionLeg(type="put", side="long", strike=long_put.strike, premium=long_put.mid),
        OptionLeg(type="put", side="short", strike=short_put.strike, premium=short_put.mid),
        OptionLeg(type="call", side="short", strike=short_call.strike, premium=short_call.mid),
    ]


STRATEGY_PRESETS: list[StrategyPreset] = [
    StrategyPreset(
        id="iron_condor", label="Iron Condor", backtested=True,
        legs=[
            LegTarget(type="put", side="long", delta=0.10, further_than=1),
            LegTarget(type="put", side="short", delta=0.16),
            LegTarget(type="call", side="short", delta=0.16),
            LegTarget(type="call", side="long", delta=0.10, further_than=2),
        ],
    ),
    StrategyPreset(
        id="iron_butterfly", label="Iron Butterfly",
        legs=[
            LegTarget(type="put", side="long", delta=0.10, further_than=1),
            LegTarget(type="put", side="short", delta="atm"),
            LegTarget(type="call", side="short", delta="atm"),
            LegTarget(type="call", side="long", delta=0.10, further_than=2),
        ],
    ),
    StrategyPreset(
        id="bull_put_spread", label="Bull Put Spread (credit)",
        legs=[
            LegTarget(type="put", side="short", delta=0.30),
            LegTarget(type="put", side="long", delta=0.10, further_than=0),
        ],
    ),
    StrategyPreset(
        id="bear_call_spread", label="Bear Call Spread (credit)",
        legs=[
            LegTarget(type="call", side="short", delta=0.30),
            LegTarget(type="call", side="long", delta=0.10, further_than=0),
        ],
    ),
    StrategyPreset(
        id="bull_call_spread", label="Bull Call Spread (debit)",
        legs=[
            LegTarget(type="call", side="long", delta="atm"),
            LegTarget(type="call", side="short", delta=0.20, further_than=0),
        ],
    ),
    StrategyPreset(
        id="bear_put_spread", label="Bear Put Spread (debit)",
        legs=[
            LegTarget(type="put", side="long", delta="atm"),
            LegTarget(type="put", side="short", delta=0.20, further_than=0),
        ],
    ),
    StrategyPreset(
        id="seagull_spread", label="Seagull Spread",
        legs=[
            LegTarget(type="call", side="long", delta=0.40),
            LegTarget(type="call", side="short", delta=0.20, further_than=0),
            LegTarget(type="put", side="short", delta=0.20),
        ],
    ),
    StrategyPreset(
        id="long_straddle", label="Long Straddle",
        legs=[
            LegTarget(type="call", side="long", delta="atm"),
            LegTarget(type="put", side="long", delta="atm"),
        ],
    ),
    StrategyPreset(
        id="short_straddle", label="Short Straddle",
        legs=[
            LegTarget(type="call", side="short", delta="atm"),
            LegTarget(type="put", side="short", delta="atm"),
        ],
    ),
    StrategyPreset(
        id="long_strangle", label="Long Strangle",
        legs=[
            LegTarget(type="call", side="long", delta=0.30),
            LegTarget(type="put", side="long", delta=0.30),
        ],
    ),
    StrategyPreset(
        id="short_strangle", label="Short Strangle",
        legs=[
            LegTarget(type="call", side="short", delta=0.16),
            LegTarget(type="put", side="short", delta=0.16),
        ],
    ),
    StrategyPreset(
        id="jade_lizard", label="Jade Lizard",
        legs=[
            LegTarget(type="put", side="short", delta=0.35),
            LegTarget(type="call", side="short", delta=0.16),
            LegTarget(type="call", side="long", delta=0.10, further_than=1),
        ],
    ),
    StrategyPreset(id="call_butterfly", label="Call Butterfly", legs=[], custom=_build_call_butterfly_legs),
    StrategyPreset(id="put_butterfly", label="Put Butterfly", legs=[], custom=_build_put_butterfly_legs),
    StrategyPreset(id="covered_call", label="Covered Call", legs=[], custom=_build_covered_call_legs),
    StrategyPreset(id="collar", label="Collar", legs=[], custom=_build_collar_legs),
    StrategyPreset(id="buffered", label="Buffered (Defined Outcome)", legs=[], custom=_build_buffered_legs),
    StrategyPreset(id="long_call", label="Long Call", legs=[LegTarget(type="call", side="long", delta="atm")]),
    StrategyPreset(id="long_put", label="Long Put", legs=[LegTarget(type="put", side="long", delta="atm")]),
    StrategyPreset(id="short_put", label="Short Put", legs=[LegTarget(type="put", side="short", delta=0.30)]),
    StrategyPreset(id="custom", label="Custom (start from scratch)", legs=[]),
]

PRESETS_BY_ID: dict[str, StrategyPreset] = {p.id: p for p in STRATEGY_PRESETS}


def build_preset_legs(chain: ExpirationChain, preset: StrategyPreset, spot: float | None = None) -> list[OptionLeg]:
    """Resolves a preset's leg targets against a real expiration chain. Two-pass so `further_than`
    anchors are resolved regardless of declaration order."""
    if preset.custom:
        if spot is None:
            raise ValueError(f'Preset "{preset.id}" requires a spot price')
        return preset.custom(chain, spot)

    resolved: list[ChainContract | None] = [None] * len(preset.legs)

    def resolve_one(i: int, target: LegTarget) -> None:
        pool = chain.calls if target.type == "call" else chain.puts
        candidates = pool
        if target.further_than is not None:
            anchor = resolved[target.further_than]
            if anchor is not None:
                further = [c for c in pool if (c.strike < anchor.strike if target.type == "put" else c.strike > anchor.strike)]
                if further:
                    candidates = further
        if target.delta == "atm":
            if spot is None:
                raise ValueError(f'Preset "{preset.id}" requires a spot price for its \'atm\' leg')
            resolved[i] = _nearest_by_strike(candidates, spot)
        else:
            resolved[i] = _nearest_by_delta(candidates, target.delta)

    for i, t in enumerate(preset.legs):
        if t.further_than is None:
            resolve_one(i, t)
    for i, t in enumerate(preset.legs):
        if t.further_than is not None:
            resolve_one(i, t)

    legs: list[OptionLeg] = []
    for t, c in zip(preset.legs, resolved):
        assert c is not None
        legs.append(OptionLeg(type=t.type, side=t.side, strike=c.strike, premium=c.mid, quantity=t.quantity or 1))
    return legs
