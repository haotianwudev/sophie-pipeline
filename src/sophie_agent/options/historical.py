"""Reimplementation of sophie-option-research/src/lab/backtest.py::load_chains.

Deliberately NOT imported cross-repo — sophie-option-research runs its own venv on pandas 3.0.3
while sophie-pipeline pins pandas 2.2.3. Same prefilter-by-filename-then-by-date strategy as the
original, reading the OptionsDX-derived parquet files directly (see
sophie-option-research/src/convert_optionsdx.py for the schema this loader depends on:
underlying_symbol, underlying_price, option_type ('c'/'p'), expiration, quote_date, strike, bid,
ask, delta, volume — no IV, no open interest).
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import pandas as pd

MAX_MONTHS_WITHOUT_CONFIRM = 6


class HistoricalChainUnavailable(RuntimeError):
    """Raised (and caught by the calling tool) when pyarrow or the data directory is missing."""


def _pyarrow_available() -> bool:
    try:
        import pyarrow  # noqa: F401

        return True
    except ImportError:
        return False


@lru_cache(maxsize=8)
def _chain_files(chain_dir: str) -> tuple[Path, ...]:
    return tuple(sorted(Path(chain_dir).glob("*.parquet")))


def months_between(start: str, end: str) -> int:
    s = pd.Timestamp(start)
    e = pd.Timestamp(end)
    return (e.year - s.year) * 12 + (e.month - s.month) + 1


def load_historical_chain(
    chain_dir: Path,
    start_date: str,
    end_date: str,
    option_type: str | None = None,
    dte_min: int | None = None,
    dte_max: int | None = None,
    delta_min: float | None = None,
    delta_max: float | None = None,
    confirm: bool = False,
) -> pd.DataFrame:
    if not _pyarrow_available():
        raise HistoricalChainUnavailable(
            "Historical chains unavailable: pyarrow is not installed. Run `poetry add pyarrow`."
        )
    if not chain_dir.exists():
        raise HistoricalChainUnavailable(f"Historical chains unavailable: path {chain_dir} not found.")

    span = months_between(start_date, end_date)
    if span > MAX_MONTHS_WITHOUT_CONFIRM and not confirm:
        raise HistoricalChainUnavailable(
            f"Requested range spans {span} months (> {MAX_MONTHS_WITHOUT_CONFIRM}). "
            f"Pass confirm=True to load it anyway — this can be tens of millions of rows."
        )

    files = _chain_files(str(chain_dir))
    if not files:
        raise HistoricalChainUnavailable(f"Historical chains unavailable: no parquet files in {chain_dir}.")

    lo = pd.Timestamp(start_date).strftime("%Y%m")
    hi = pd.Timestamp(end_date).strftime("%Y%m")
    files = [f for f in files if lo <= f.stem[-6:] <= hi]
    if not files:
        return pd.DataFrame()

    df = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)
    df["quote_date"] = pd.to_datetime(df["quote_date"])
    df["expiration"] = pd.to_datetime(df["expiration"])
    df = df[(df["quote_date"] >= pd.Timestamp(start_date)) & (df["quote_date"] <= pd.Timestamp(end_date))]

    if option_type:
        normalized = option_type.strip().lower()[0]  # 'call'/'c' -> 'c', 'put'/'p' -> 'p'
        df = df[df["option_type"] == normalized]

    if "mid" not in df.columns:
        df["mid"] = (df["bid"] + df["ask"]) / 2

    df["dte"] = (df["expiration"] - df["quote_date"]).dt.days
    if dte_min is not None:
        df = df[df["dte"] >= dte_min]
    if dte_max is not None:
        df = df[df["dte"] <= dte_max]
    if delta_min is not None:
        df = df[df["delta"].abs() >= delta_min]
    if delta_max is not None:
        df = df[df["delta"].abs() <= delta_max]

    return df.reset_index(drop=True)
