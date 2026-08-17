"""Port of client/src/lib/options/chain-types.ts — shared option-chain shapes.

Deliberately a subset of the fields already used by the live Options Viewer, so a live chain
source and a historical one both satisfy this same shape.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass
class ChainContract:
    strike: float
    bid: float
    ask: float
    mid: float
    delta: float  # signed: positive for calls, negative for puts
    iv: float | None = None
    gamma: float | None = None
    vega: float | None = None
    theta: float | None = None
    rho: float | None = None
    volume: float | None = None
    open_interest: float | None = None


@dataclass
class ExpirationChain:
    expiration: str  # ISO date
    dte: int
    calls: list[ChainContract]
    puts: list[ChainContract]


@dataclass
class OptionChainSnapshot:
    symbol: str
    quote_date: str
    underlying_price: float
    expirations: list[ExpirationChain]


OptionType = Literal["call", "put"]
