"""OptionChainToolkit — live SPX chains (Cboe) and historical EOD chains (OptionsDX parquet).

Two sources, kept clearly separate: live is 15-min delayed but carries IV + full greeks; historical
carries delta only, no IV, no open interest. See system_prompt_fragment().
"""

from __future__ import annotations

from typing import Annotated, ClassVar, Literal

import pandas as pd
from langchain.tools import ToolRuntime
from langchain_core.tools import BaseTool, tool
from pydantic import Field

from src.tools import api_cboe

from ...options.historical import HistoricalChainUnavailable, load_historical_chain
from ..base import SophieToolkit
from ..strategy.toolkit import build_strategy, list_strategy_presets
from ..ui_envelope import ui_envelope

_LIVE_DISABLED = "Live chain tools are disabled in point-in-time mode (as_of is set)."


@tool
def spx_chain_metadata(runtime: ToolRuntime) -> str:
    """Get current SPX spot price and the list of available option expirations from the live
    (15-min delayed) Cboe feed. Cheap — call this before pulling a full chain."""
    if not runtime.context.run_ctx.is_live:
        return _LIVE_DISABLED
    meta = api_cboe.get_spx_metadata()
    exps = ", ".join(meta["expirations"][:10])
    more = f" (+{meta['total_expirations'] - 10} more)" if meta["total_expirations"] > 10 else ""
    return (
        f"SPX spot: {meta['spot_price']}, as of {meta['cboe_timestamp']}. "
        f"{meta['total_expirations']} expirations: {exps}{more}"
    )


@tool
def spx_option_chain(
    runtime: ToolRuntime,
    expiration: Annotated[str | None, Field(description="Expiration date in 'YYYY-MM-DD' format (defaults to nearest available expiration).")] = None,
    option_type: Annotated[Literal["CALL", "PUT"] | None, Field(description="Optional filter for contract type: 'CALL' or 'PUT'.")] = None,
    strike_range: Annotated[float | None, Field(gt=0, description="Filter to contracts within +/- this many points around current SPX spot price.")] = None,
    strike_min: Annotated[float | None, Field(gt=0, description="Minimum strike price to include.")] = None,
    strike_max: Annotated[float | None, Field(gt=0, description="Maximum strike price to include.")] = None,
) -> str:
    """Pull the live (15-min delayed) SPX option chain from Cboe, with IV and full greeks.
    `expiration` is 'YYYY-MM-DD' (defaults to nearest); `option_type` is 'CALL'/'PUT';
    `strike_range` filters to +/- that many points around spot. Registers the full result in the
    DataFrame store and returns a preview — use the DataFrame toolkit to analyze it further."""
    ctx = runtime.context
    if not ctx.run_ctx.is_live:
        return f"{_LIVE_DISABLED} Use spx_historical_chain instead."
    spot, exp_used, contracts = api_cboe.get_spx_option_chain(
        expiration=expiration,
        option_type=option_type,
        strike_min=strike_min,
        strike_max=strike_max,
        strike_range=strike_range,
    )
    if not contracts:
        return f"No contracts found for expiration {exp_used}."
    df = pd.DataFrame(contracts)
    handle = ctx.store.put(f"chain_{exp_used}", df)
    return f"spot={spot}, expiration={exp_used}, {len(df)} contracts.\n\n{ctx.store.preview(handle)}"


@tool
def spx_gex(
    runtime: ToolRuntime,
    expiration: Annotated[str | None, Field(description="Expiration date in 'YYYY-MM-DD' format (defaults to nearest).")] = None,
    strike_range: Annotated[float, Field(gt=0, description="+/- strike points around spot price to compute Gamma Exposure (default 150.0).")] = 150.0,
) -> str:
    """Calculate net Gamma Exposure (GEX) by strike for SPX from the live Cboe chain. Returns the
    by-strike table directly (it's small) plus total net/call/put GEX in $M."""
    ctx = runtime.context
    if not ctx.run_ctx.is_live:
        return _LIVE_DISABLED
    result = api_cboe.calculate_spx_gex(expiration=expiration, strike_range=strike_range)
    handle = ctx.store.put(f"gex_{result['expiration']}", pd.DataFrame(result["strikes"]))
    text = (
        f"spot={result['spot_price']}, expiration={result['expiration']}, "
        f"total_net_gex_m={result['total_net_gex_m']}, "
        f"total_call_gex_m={result['total_call_gex_m']}, total_put_gex_m={result['total_put_gex_m']}\n\n"
        f"{ctx.store.preview(handle)}"
    )
    return ui_envelope(
        text, "gex_chart",
        spot=result["spot_price"], expiration=result["expiration"],
        total_net_gex_m=result["total_net_gex_m"], total_call_gex_m=result["total_call_gex_m"],
        total_put_gex_m=result["total_put_gex_m"], strikes=result["strikes"],
    )


@tool
def spx_historical_chain(
    runtime: ToolRuntime,
    start_date: Annotated[str, Field(description="Start date in 'YYYY-MM-DD' format (between 2010 and 2023).")],
    end_date: Annotated[str, Field(description="End date in 'YYYY-MM-DD' format (clamped to as_of if in point-in-time mode).")],
    option_type: Annotated[Literal["call", "put", "c", "p"] | None, Field(description="Optional filter for option type: 'call' or 'put'.")] = None,
    dte_min: Annotated[int | None, Field(ge=0, description="Minimum days to expiration.")] = None,
    dte_max: Annotated[int | None, Field(ge=0, description="Maximum days to expiration.")] = None,
    delta_min: Annotated[float | None, Field(description="Minimum option delta target.")] = None,
    delta_max: Annotated[float | None, Field(description="Maximum option delta target.")] = None,
    confirm: Annotated[bool, Field(description="Must be set to True if loading a date range greater than 6 months.")] = False,
) -> str:
    """Load historical EOD SPX option chains (OptionsDX, 2010-2023) for a date range. Columns:
    underlying_price, option_type, expiration, quote_date, strike, bid, ask, mid, delta, dte,
    volume. NO implied vol and NO open interest in this source. Dates beyond the current as_of (if
    point-in-time mode is on) are clamped. Ranges over 6 months need confirm=True (can be tens of
    millions of rows)."""
    ctx = runtime.context
    effective_end = end_date
    if ctx.run_ctx.as_of is not None:
        effective_end = min(end_date, ctx.run_ctx.as_of.isoformat())
    try:
        df = load_historical_chain(
            chain_dir=ctx.config.historical_chain_dir,
            start_date=start_date,
            end_date=effective_end,
            option_type=option_type,
            dte_min=dte_min,
            dte_max=dte_max,
            delta_min=delta_min,
            delta_max=delta_max,
            confirm=confirm,
        )
    except HistoricalChainUnavailable as exc:
        return str(exc)
    if df.empty:
        return f"No historical chain rows found for {start_date}..{effective_end}."
    handle = ctx.store.put(f"hist_chain_{start_date}_{effective_end}", df)
    return f"{len(df)} rows, {start_date}..{effective_end}.\n\n{ctx.store.preview(handle)}"


class OptionChainToolkit(SophieToolkit):
    toolkit_name: ClassVar[str] = "options"

    def get_tools(self) -> list[BaseTool]:
        # build_strategy/list_strategy_presets are defined in ..strategy.toolkit (the resolve/price
        # logic lives there — see that module's docstring) but exposed here too so a profile only
        # needs "options" in its toolkits tuple to both read chains and build a position from one.
        return [
            spx_chain_metadata, spx_option_chain, spx_gex, spx_historical_chain,
            list_strategy_presets, build_strategy,
        ]

    def system_prompt_fragment(self) -> str:
        return (
            "OPTION CHAIN TOOLKIT: two SPX data sources. LIVE (spx_chain_metadata, "
            "spx_option_chain, spx_gex) is Cboe's 15-min-delayed feed — carries IV and full greeks, "
            "but disabled entirely in point-in-time (as_of) mode since live quotes are never "
            "point-in-time. HISTORICAL (spx_historical_chain) is OptionsDX EOD data 2010-2023 — "
            "carries delta but NO implied vol and NO open interest; build_strategy solves IV itself "
            "when needed. Never confuse the two sources in one analysis without saying so.\n\n"
            "Resolving a position: list_strategy_presets() for the 20 preset ids, then "
            "build_strategy() to price one against a real chain — it does all the payoff/Greeks "
            "arithmetic itself, never compute those numbers yourself. ONLY 'iron_condor' is "
            "backtest-derived (0.10-delta wings / 0.16-delta shorts, from sophie-option-research's "
            "real iron_condor_45dte.yaml); every other preset's deltas are conventional retail "
            "defaults — always state which applies."
        )
