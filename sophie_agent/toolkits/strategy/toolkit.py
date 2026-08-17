"""StrategyToolkit — the headline toolkit: resolve, price, and back-test-justify option strategies.

Ports client/src/lib/options/{presets,payoff,analytics,liquidity}.ts (see sophie_agent/options/)
so a resolved position is already a valid Options Viewer OptionLeg payload, and keeps all 20 preset
ids identical to the client.
"""

from __future__ import annotations

import dataclasses
from datetime import date, datetime, timezone
from typing import Annotated, ClassVar, Literal

import pandas as pd
from langchain.tools import ToolRuntime
from langchain_core.tools import BaseTool, tool
from pydantic import Field

from src.tools import api_cboe
from src.tools.api_db import get_db_connection

from ...options.analytics import (
    SPX_DEFAULT_DIV_YIELD,
    SPX_DEFAULT_RATE,
    SPX_MULTIPLIER,
    PricedLeg,
    net_greeks,
    probability_of_profit,
    solve_implied_vol,
)
from ...options.chain_types import ChainContract, ExpirationChain
from ...options.historical import HistoricalChainUnavailable, load_historical_chain
from ...options.liquidity import compute_liquidity
from ...options.payoff import find_breakevens, legs_pnl, net_premium
from ...options.presets import PRESETS_BY_ID, STRATEGY_PRESETS, LegTarget, build_preset_legs
from ..base import SophieToolkit
from ..ui_envelope import ui_envelope


def _price_grid(spot: float, n: int = 400) -> list[float]:
    lo, hi = spot * 0.6, spot * 1.4
    step = (hi - lo) / n
    return [lo + i * step for i in range(n + 1)]


def _build_live_chain(target_dte: int) -> tuple[ExpirationChain, float, dict]:
    meta = api_cboe.get_spx_metadata()
    today = datetime.now(timezone.utc).date()

    def dte_of(exp: str) -> int:
        return (date.fromisoformat(exp) - today).days

    expirations = meta["expirations"]
    if not expirations:
        raise ValueError("No live expirations available from Cboe.")
    chosen = min(expirations, key=lambda e: abs(dte_of(e) - target_dte))
    spot, exp_used, contracts = api_cboe.get_spx_option_chain(expiration=chosen)

    calls, puts = [], []
    iv_lookup: dict[tuple[str, float], float] = {}
    for c in contracts:
        mid = (c["bid"] + c["ask"]) / 2
        contract = ChainContract(
            strike=c["strike"], bid=c["bid"], ask=c["ask"], mid=mid, delta=c["delta"],
            iv=c["iv"], gamma=c["gamma"], vega=c["vega"], theta=c["theta"], rho=c["rho"],
            volume=c["volume"], open_interest=c["open_interest"],
        )
        (calls if c["type"] == "CALL" else puts).append(contract)
        key = ("call" if c["type"] == "CALL" else "put", c["strike"])
        iv_lookup[key] = c["iv"]

    chain = ExpirationChain(expiration=exp_used, dte=dte_of(exp_used), calls=calls, puts=puts)
    return chain, spot, iv_lookup


def _build_historical_chain(chain_dir, quote_date: str, target_dte: int) -> tuple[ExpirationChain, float, dict]:
    df = load_historical_chain(chain_dir=chain_dir, start_date=quote_date, end_date=quote_date)
    if df.empty:
        raise HistoricalChainUnavailable(f"No historical chain rows for quote_date {quote_date}.")

    dte_by_exp = df.groupby("expiration")["dte"].first()
    chosen_exp = (dte_by_exp - target_dte).abs().idxmin()
    slice_df = df[df["expiration"] == chosen_exp]
    spot = float(slice_df["underlying_price"].iloc[0])
    dte = int(slice_df["dte"].iloc[0])
    T = dte / 365.0

    calls, puts = [], []
    iv_lookup: dict[tuple[str, float], float] = {}
    for row in slice_df.itertuples():
        option_type = "call" if row.option_type == "c" else "put"
        mid = (row.bid + row.ask) / 2
        iv = solve_implied_vol(mid, spot, row.strike, T, SPX_DEFAULT_RATE, SPX_DEFAULT_DIV_YIELD, option_type)
        contract = ChainContract(
            strike=row.strike, bid=row.bid, ask=row.ask, mid=mid, delta=row.delta,
            iv=iv, volume=getattr(row, "volume", None), open_interest=None,
        )
        (calls if option_type == "call" else puts).append(contract)
        iv_lookup[(option_type, row.strike)] = iv

    chain = ExpirationChain(expiration=str(chosen_exp.date()), dte=dte, calls=calls, puts=puts)
    return chain, spot, iv_lookup


def _resolve_and_price(
    chain: ExpirationChain,
    spot: float,
    iv_lookup: dict,
    preset_id: str,
    delta_overrides: dict[int, float] | None = None,
) -> dict:
    preset = PRESETS_BY_ID.get(preset_id)
    if preset is None:
        raise ValueError(f"Unknown preset '{preset_id}'. Call list_strategy_presets() for valid ids.")
    if preset.id == "custom":
        raise ValueError("'custom' has no leg targets to resolve — it's a start-from-scratch placeholder.")

    working_preset = preset
    if delta_overrides:
        new_legs = []
        for i, leg in enumerate(preset.legs):
            if i in delta_overrides:
                new_legs.append(dataclasses.replace(leg, delta=delta_overrides[i]))
            else:
                new_legs.append(leg)
        working_preset = dataclasses.replace(preset, legs=new_legs)

    legs = build_preset_legs(chain, working_preset, spot=spot)

    priced_legs = [
        PricedLeg(leg=leg, iv=iv_lookup.get((leg.type, leg.strike), 0.0)) for leg in legs if leg.type != "stock"
    ]
    atm_iv = priced_legs[0].iv if priced_legs else 0.0
    T = chain.dte / 365.0

    grid = _price_grid(spot)
    pnls = [legs_pnl(p, legs) for p in grid]
    breakevens = find_breakevens(grid, pnls)
    credit = net_premium(legs)
    greeks = net_greeks(priced_legs, spot, T, SPX_DEFAULT_RATE, SPX_DEFAULT_DIV_YIELD)
    pop = probability_of_profit(
        lambda p: legs_pnl(p, legs), breakevens, spot, T, SPX_DEFAULT_RATE, SPX_DEFAULT_DIV_YIELD, atm_iv
    )

    contracts_by_leg = []
    all_contracts = chain.calls + chain.puts
    for leg in legs:
        match = next((c for c in all_contracts if c.strike == leg.strike), None)
        liq = compute_liquidity(match.bid, match.ask, match.mid, match.volume, match.open_interest) if match else None
        contracts_by_leg.append(liq)

    return {
        "preset": working_preset,
        "legs": legs,
        "spot": spot,
        "expiration": chain.expiration,
        "dte": chain.dte,
        "net_credit": round(credit, 2),
        "max_profit": round(max(pnls), 2),
        "max_loss": round(min(pnls), 2),
        "breakevens": [round(b, 2) for b in breakevens],
        "probability_of_profit": round(pop, 4),
        "net_greeks": greeks,
        "liquidity": contracts_by_leg,
        "backtested": preset.backtested,
    }


@tool
def list_strategy_presets() -> str:
    """List every option strategy preset id, its label, and whether its delta targets are
    backtest-derived or conventional retail defaults. Only 'iron_condor' is backtested (from
    sophie-option-research's real iron_condor_45dte.yaml config) — do not present any other
    preset's deltas as backtest-justified."""
    lines = []
    rows = []
    for p in STRATEGY_PRESETS:
        if p.id == "custom":
            continue
        basis = "BACKTESTED" if p.backtested else "conventional"
        leg_desc = (
            "custom builder"
            if p.custom
            else ", ".join(f"{t.side} {t.type} delta={t.delta}" for t in p.legs)
        )
        lines.append(f"- {p.id} ({p.label}) [{basis}]: {leg_desc}")
        rows.append({"id": p.id, "label": p.label, "backtested": p.backtested, "legs": leg_desc})
    return ui_envelope("\n".join(lines), "presets_list", presets=rows)


@tool
def build_strategy(
    runtime: ToolRuntime,
    preset_id: Annotated[str, Field(description="Strategy preset ID from list_strategy_presets (e.g. 'iron_condor', 'vertical_spread', 'straddle').")],
    target_dte: Annotated[int, Field(ge=0, description="Target days to expiration (default 45).")] = 45,
    source: Annotated[Literal["live", "historical"], Field(description="Data source: 'live' (Cboe feed, today) or 'historical' (OptionsDX EOD data).")] = "live",
    quote_date: Annotated[str | None, Field(description="Required if source='historical' ('YYYY-MM-DD').")] = None,
    delta_overrides: Annotated[dict[int, float] | None, Field(description="Optional mapping of 0-indexed leg position to replacement delta target (e.g. {1: 0.20}).")] = None,
) -> str:
    """Resolve a strategy preset against a real chain and price it: legs (in Options Viewer
    OptionLeg shape), net credit/debit, max profit/loss, breakevens, probability of profit, and net
    greeks. `source` is 'live' (Cboe, today) or 'historical' (OptionsDX, requires
    `quote_date`='YYYY-MM-DD'). `delta_overrides` maps a 0-indexed leg position (as listed by
    list_strategy_presets) to a replacement delta target, for what-if analysis without
    hand-computing a whole new preset."""
    ctx = runtime.context
    try:
        if source == "live":
            if not ctx.run_ctx.is_live:
                return "Live chain is disabled in point-in-time mode. Use source='historical'."
            chain, spot, iv_lookup = _build_live_chain(target_dte)
        elif source == "historical":
            if not quote_date:
                return "source='historical' requires quote_date='YYYY-MM-DD'."
            if ctx.run_ctx.as_of and quote_date > ctx.run_ctx.as_of.isoformat():
                return f"quote_date {quote_date} is after as_of {ctx.run_ctx.as_of.isoformat()}; not permitted."
            chain, spot, iv_lookup = _build_historical_chain(
                ctx.config.historical_chain_dir, quote_date, target_dte
            )
        else:
            return "source must be 'live' or 'historical'."
    except HistoricalChainUnavailable as exc:
        return str(exc)
    except ValueError as exc:
        return str(exc)

    try:
        result = _resolve_and_price(chain, spot, iv_lookup, preset_id, delta_overrides)
    except ValueError as exc:
        return str(exc)

    legs_df = pd.DataFrame(
        [
            {
                "type": l.type, "side": l.side, "strike": l.strike, "premium": l.premium,
                "quantity": l.quantity, "liquidity": liq.tier if liq else "unknown",
            }
            for l, liq in zip(result["legs"], result["liquidity"])
        ]
    )
    handle = ctx.store.put(f"strategy_{preset_id}", legs_df)
    g = result["net_greeks"]
    basis = (
        "BACKTESTED (see strategy_backtest_evidence)"
        if result["backtested"]
        else "conventional default, not backtested"
    )
    text = (
        f"{preset_id} on {result['expiration']} (dte={result['dte']}), spot={result['spot']} [{basis}]\n"
        f"net_credit={result['net_credit']}, max_profit={result['max_profit']}, "
        f"max_loss={result['max_loss']}, breakevens={result['breakevens']}, "
        f"pop={result['probability_of_profit']}\n"
        f"net_greeks: delta={round(g.delta, 1)}, gamma={round(g.gamma, 4)}, "
        f"theta={round(g.theta, 1)}, vega={round(g.vega, 1)}\n\n"
        f"{ctx.store.preview(handle)}"
    )
    return ui_envelope(
        text, "strategy_legs",
        preset_id=preset_id, expiration=result["expiration"], dte=result["dte"],
        spot=result["spot"], backtested=result["backtested"],
        legs=[
            {"type": l.type, "side": l.side, "strike": l.strike, "premium": l.premium, "quantity": l.quantity}
            for l in result["legs"]
        ],
        metrics={
            "net_credit": result["net_credit"], "max_profit": result["max_profit"],
            "max_loss": result["max_loss"], "breakevens": result["breakevens"],
            "probability_of_profit": result["probability_of_profit"],
            "net_greeks": {"delta": g.delta, "gamma": g.gamma, "theta": g.theta, "vega": g.vega},
        },
    )


@tool
def compare_strategy_variants(
    runtime: ToolRuntime,
    preset_id: Annotated[str, Field(description="Strategy preset ID (e.g. 'iron_condor').")],
    leg_index: Annotated[int, Field(ge=0, description="0-indexed leg position to sweep delta targets for.")],
    delta_grid: Annotated[list[float], Field(min_length=1, description="Delta values to evaluate across variants (e.g. [0.10, 0.16, 0.20, 0.25]).")],
    target_dte: Annotated[int, Field(ge=0, description="Target days to expiration (default 45).")] = 45,
) -> str:
    """Sweep one leg's delta target across `delta_grid` (e.g. the short strike[s] of an iron
    condor) and compare credit, max loss, credit-to-width, POP, and liquidity for each variant.
    This is what actually answers "which strikes should I pick" — prefer it over calling
    build_strategy repeatedly by hand."""
    ctx = runtime.context
    if not ctx.run_ctx.is_live:
        return "This tool currently requires live mode (as_of unset)."
    try:
        chain, spot, iv_lookup = _build_live_chain(target_dte)
    except ValueError as exc:
        return str(exc)

    rows = []
    for d in delta_grid:
        try:
            result = _resolve_and_price(chain, spot, iv_lookup, preset_id, {leg_index: d})
        except ValueError as exc:
            rows.append({"delta": d, "error": str(exc)})
            continue
        strikes = [l.strike for l in result["legs"]]
        width = max(strikes) - min(strikes) if len(strikes) > 1 else 0
        credit = result["net_credit"]
        rows.append(
            {
                "delta": d,
                "net_credit": credit,
                "max_loss": result["max_loss"],
                "credit_to_width": round(credit / width, 4) if width else None,
                "probability_of_profit": result["probability_of_profit"],
            }
        )
    handle = ctx.store.put(f"variants_{preset_id}", pd.DataFrame(rows))
    return ui_envelope(
        ctx.store.preview(handle), "variant_table",
        preset_id=preset_id, leg_index=leg_index, rows=rows,
    )


@tool
def strategy_backtest_evidence(
    runtime: ToolRuntime,
    strategy: Annotated[str, Field(description="Strategy name to search backtest runs for in Postgres (e.g. 'iron_condor', 'straddle').")],
) -> str:
    """Look up sophie-option-research backtest runs (option_research_run) for a strategy name —
    params (delta/DTE targets actually used) alongside metrics (Sharpe, CAGR, max drawdown, win
    rate) per config, so a recommendation can cite real evidence instead of folklore. `strategy`
    matches loosely (e.g. 'iron_condor')."""
    ctx = runtime.context
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT config_hash, name, strategy, study_tag, params, metrics, window_start, window_end "
            "FROM option_research_run WHERE strategy ILIKE %s ORDER BY updated_at DESC LIMIT 50",
            (f"%{strategy}%",),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        cur.close()
        conn.close()
    except Exception as exc:
        return f"Could not query option_research_run: {exc}"
    if not rows:
        return f"No backtest runs found for strategy matching '{strategy}'."
    handle = ctx.store.put(f"evidence_{strategy}", pd.DataFrame(rows, columns=cols))
    return ctx.store.preview(handle)


class StrategyToolkit(SophieToolkit):
    toolkit_name: ClassVar[str] = "strategy"

    def get_tools(self) -> list[BaseTool]:
        return [
            list_strategy_presets,
            build_strategy,
            compare_strategy_variants,
            strategy_backtest_evidence,
        ]

    def system_prompt_fragment(self) -> str:
        return (
            "STRATEGY TOOLKIT: resolves option strategy presets (20 ids — call "
            "list_strategy_presets() for the full list) against a real chain and prices them. "
            "ONLY 'iron_condor' is backtest-derived (0.10-delta wings / 0.16-delta shorts, from "
            "sophie-option-research's real iron_condor_45dte.yaml); every other preset's deltas are "
            "conventional retail defaults. Always state which applies — never present a "
            "conventional default as if it were backtested. The LLM never computes payoff numbers "
            "itself; build_strategy and compare_strategy_variants do all arithmetic. Use "
            "strategy_backtest_evidence to cite real Sharpe/drawdown/win-rate numbers when they "
            "exist for a strategy."
        )
