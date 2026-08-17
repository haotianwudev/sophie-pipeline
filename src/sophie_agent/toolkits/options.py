"""OptionChainToolkit — live SPX chains (Cboe) and historical EOD chains (OptionsDX parquet).

Two sources, kept clearly separate: live is 15-min delayed but carries IV + full greeks; historical
carries delta only, no IV, no open interest. See system_prompt_fragment().
"""

from __future__ import annotations

from typing import ClassVar

import pandas as pd
from langchain_core.tools import BaseTool, tool

# src/__init__.py inserts the project root onto sys.path on first import of the `src` package,
# which always happens before this submodule (src.sophie_agent.toolkits.options) is importable —
# see docs/SOPHIE_AGENT.md's import-convention note.
from src.tools import api_cboe

from ..options.historical import HistoricalChainUnavailable, load_historical_chain
from .base import SophieToolkit
from .ui_envelope import ui_envelope


class OptionChainToolkit(SophieToolkit):
    toolkit_name: ClassVar[str] = "options"

    def get_tools(self) -> list[BaseTool]:
        store = self.store
        run_ctx = self.run_ctx
        chain_dir = self.config.historical_chain_dir

        @tool
        def spx_chain_metadata() -> str:
            """Get current SPX spot price and the list of available option expirations from the
            live (15-min delayed) Cboe feed. Cheap — call this before pulling a full chain."""
            if not run_ctx.is_live:
                return "Live chain tools are disabled in point-in-time mode (as_of is set)."
            meta = api_cboe.get_spx_metadata()
            exps = ", ".join(meta["expirations"][:10])
            more = f" (+{meta['total_expirations'] - 10} more)" if meta["total_expirations"] > 10 else ""
            return (
                f"SPX spot: {meta['spot_price']}, as of {meta['cboe_timestamp']}. "
                f"{meta['total_expirations']} expirations: {exps}{more}"
            )

        @tool
        def spx_option_chain(
            expiration: str | None = None,
            option_type: str | None = None,
            strike_range: float | None = None,
            strike_min: float | None = None,
            strike_max: float | None = None,
        ) -> str:
            """Pull the live (15-min delayed) SPX option chain from Cboe, with IV and full greeks.
            `expiration` is 'YYYY-MM-DD' (defaults to nearest); `option_type` is 'CALL'/'PUT';
            `strike_range` filters to +/- that many points around spot. Registers the full result
            in the DataFrame store and returns a preview — use the DataFrame toolkit to analyze it
            further."""
            if not run_ctx.is_live:
                return "Live chain tools are disabled in point-in-time mode (as_of is set). Use spx_historical_chain instead."
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
            handle = store.put(f"chain_{exp_used}", df)
            return f"spot={spot}, expiration={exp_used}, {len(df)} contracts.\n\n{store.preview(handle)}"

        @tool
        def spx_gex(expiration: str | None = None, strike_range: float = 150.0) -> str:
            """Calculate net Gamma Exposure (GEX) by strike for SPX from the live Cboe chain.
            Returns the by-strike table directly (it's small) plus total net/call/put GEX in $M."""
            if not run_ctx.is_live:
                return "Live chain tools are disabled in point-in-time mode (as_of is set)."
            result = api_cboe.calculate_spx_gex(expiration=expiration, strike_range=strike_range)
            df = pd.DataFrame(result["strikes"])
            handle = store.put(f"gex_{result['expiration']}", df)
            text = (
                f"spot={result['spot_price']}, expiration={result['expiration']}, "
                f"total_net_gex_m={result['total_net_gex_m']}, "
                f"total_call_gex_m={result['total_call_gex_m']}, total_put_gex_m={result['total_put_gex_m']}\n\n"
                f"{store.preview(handle)}"
            )
            return ui_envelope(
                text, "gex_chart",
                spot=result["spot_price"], expiration=result["expiration"],
                total_net_gex_m=result["total_net_gex_m"], total_call_gex_m=result["total_call_gex_m"],
                total_put_gex_m=result["total_put_gex_m"], strikes=result["strikes"],
            )

        @tool
        def spx_historical_chain(
            start_date: str,
            end_date: str,
            option_type: str | None = None,
            dte_min: int | None = None,
            dte_max: int | None = None,
            delta_min: float | None = None,
            delta_max: float | None = None,
            confirm: bool = False,
        ) -> str:
            """Load historical EOD SPX option chains (OptionsDX, 2010-2023) for a date range.
            Columns: underlying_price, option_type, expiration, quote_date, strike, bid, ask, mid,
            delta, dte, volume. NO implied vol and NO open interest in this source. Dates beyond
            the current as_of (if point-in-time mode is on) are clamped. Ranges over 6 months need
            confirm=True (can be tens of millions of rows)."""
            effective_end = end_date
            if run_ctx.as_of is not None:
                effective_end = min(end_date, run_ctx.as_of.isoformat())
            try:
                df = load_historical_chain(
                    chain_dir=chain_dir,
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
            handle = store.put(f"hist_chain_{start_date}_{effective_end}", df)
            return f"{len(df)} rows, {start_date}..{effective_end}.\n\n{store.preview(handle)}"

        return [spx_chain_metadata, spx_option_chain, spx_gex, spx_historical_chain]

    def system_prompt_fragment(self) -> str:
        return (
            "OPTION CHAIN TOOLKIT: two SPX data sources. LIVE (spx_chain_metadata, "
            "spx_option_chain, spx_gex) is Cboe's 15-min-delayed feed — carries IV and full greeks, "
            "but disabled entirely in point-in-time (as_of) mode since live quotes are never "
            "point-in-time. HISTORICAL (spx_historical_chain) is OptionsDX EOD data 2010-2023 — "
            "carries delta but NO implied vol and NO open interest; anything requiring IV/greeks on "
            "historical data must first solve IV (the strategy toolkit does this). Never confuse the "
            "two sources in one analysis without saying so."
        )
