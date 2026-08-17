"""Toolkit registry — name -> class. profiles.py references toolkits by these string names so
adding a new toolkit to an existing profile (or a new profile) is a data edit, not a code edit."""

from .base import SophieToolkit
from .dataframe import DataFrameToolkit
from .delegate import DelegationToolkit
from .market import MarketDataToolkit
from .options import OptionChainToolkit
from .strategy import StrategyToolkit
from .wiki import WikiToolkit

TOOLKIT_REGISTRY: dict[str, type[SophieToolkit]] = {
    "wiki": WikiToolkit,
    "options": OptionChainToolkit,
    "strategy": StrategyToolkit,
    "dataframe": DataFrameToolkit,
    "market": MarketDataToolkit,
    "delegate": DelegationToolkit,
}

__all__ = [
    "SophieToolkit",
    "WikiToolkit",
    "OptionChainToolkit",
    "StrategyToolkit",
    "DataFrameToolkit",
    "MarketDataToolkit",
    "DelegationToolkit",
    "TOOLKIT_REGISTRY",
]
