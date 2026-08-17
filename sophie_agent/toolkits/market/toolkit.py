"""MarketDataToolkit — read-only Postgres SQL + Sophie's public GraphQL API."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pandas as pd
import requests
from langchain.tools import ToolRuntime
from langchain_core.tools import BaseTool, tool
from pydantic import Field

from src.tools.api_db import get_db_connection

from ..base import SophieToolkit

# name -> (one-line description, has biz_date column for as_of enforcement)
CURATED_TABLES: dict[str, tuple[str, bool]] = {
    "ai_analysis": ("Per-analyst AI signal/confidence/reasoning by ticker and biz_date.", True),
    "company_facts": ("Static company metadata (sector, exchange, market cap, ...).", False),
    "company_news": ("Company news headlines with sentiment.", False),
    "company_news_alphavantage": ("Alpha Vantage news with relevance/sentiment scores.", False),
    "financial_metrics": ("~45 valuation/profitability/growth ratios per report period.", False),
    "fundamentals": ("Deterministic fundamentals scoring (profitability/growth/health/valuation).", True),
    "insider_trades": ("Insider transactions by ticker.", False),
    "investment_clock_data": ("Monthly FRED macro signals + z-scores.", True),
    "investment_clock_evaluation": ("AI-generated Investment Clock phase evaluations.", True),
    "line_items": ("Raw financial statement line items per report period.", False),
    "option_research_run": ("SPX option strategy backtest runs — params + metrics JSONB.", True),
    "option_research_equity": ("Equity curve points per backtest run.", True),
    "option_research_evaluation": ("AI research memos per backtest study.", True),
    "prices": ("Daily OHLCV prices by ticker.", True),
    "quant_trending_items": ("Scraped quant-finance trending items (ArXiv/GitHub/Reddit/HN).", False),
    "sentiment": ("Insider + news sentiment scoring by ticker/biz_date.", True),
    "sophie_analysis": ("Meta-agent synthesis across all analyst signals.", True),
    "technicals": ("Multi-strategy technical analysis (trend/mean-reversion/momentum/vol/stat-arb).", True),
    "valuation": ("Per-method + weighted intrinsic-value estimates.", True),
}

_BIZ_DATE_TABLES = tuple(t for t, (_, has_biz_date) in CURATED_TABLES.items() if has_biz_date)

_READ_ONLY_RE = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
_LIMIT_RE = re.compile(r"\bLIMIT\b", re.IGNORECASE)
# Single-quoted SQL string literals, '' being an escaped quote. Blanked before any structural
# check so a semicolon or the word LIMIT inside a literal can't be mistaken for syntax.
_STRING_LITERAL_RE = re.compile(r"'(?:[^']|'')*'")
# A literal that is exactly a bare ISO date. The as_of bound check needs date literals to stay
# visible (blanking them would make every `biz_date <= DATE '...'` undetectable) while every other
# literal is still blanked — so prose that merely *contains* a date can't masquerade as a bound.
_DATE_LITERAL_RE = re.compile(r"^'\d{4}-\d{2}-\d{2}'$")
# An explicit upper bound the model wrote itself, e.g. `biz_date <= DATE '2023-06-30'`.
_BIZ_DATE_BOUND_RE = re.compile(
    r"\bbiz_date\s*(<=|<)\s*(?:DATE\s*)?'(\d{4}-\d{2}-\d{2})'", re.IGNORECASE
)
_MAX_INLINE_CHARS = 4000
_PIT_ALIAS = "_pit_guarded"

# Postgres raises this when the point-in-time wrapper references a biz_date the inner query didn't
# project. Translated into actionable guidance rather than surfaced as a raw driver error.
_MISSING_BIZ_DATE_HINT = (
    f"Point-in-time mode is ON, so this query was wrapped in an outer "
    f"`biz_date <= as_of` filter — but your query does not expose a `biz_date` column for that "
    f"filter to act on. Either add `biz_date` to your SELECT list, or write the bound yourself "
    f"(e.g. `WHERE biz_date <= DATE 'AS_OF'`), which is what aggregate queries should do."
)


def _blank_literals(sql: str, keep_dates: bool = False) -> str:
    """Replace string literals with `''` so structural checks can't be fooled by their contents.
    With keep_dates, bare ISO-date literals are preserved for the as_of bound check."""
    if not keep_dates:
        return _STRING_LITERAL_RE.sub("''", sql)
    return _STRING_LITERAL_RE.sub(
        lambda m: m.group(0) if _DATE_LITERAL_RE.match(m.group(0)) else "''", sql
    )


@dataclass(frozen=True)
class SqlGuardResult:
    """Outcome of the read-only/point-in-time guard. `sql` is set iff the query is permitted."""

    sql: str | None = None
    rejection: str | None = None

    @property
    def ok(self) -> bool:
        return self.sql is not None


def _guard_and_prepare_sql(sql: str, as_of: str | None) -> SqlGuardResult:
    """Enforce read-only, single-statement, bounded-size, and point-in-time constraints.

    Point-in-time enforcement is REAL, not best-effort. If `as_of` is set and the query touches any
    biz_date-bearing table, the query must either carry its own `biz_date <= <date>` bound that is
    no later than `as_of`, or it gets wrapped in an outer filter that does the clamping here. A
    query that can satisfy neither is rejected outright.

    (The previous implementation wrapped the query in `... WHERE TRUE`, which filtered nothing while
    the tool docstring and system prompt both promised clamping — every `--as-of` run touching these
    tables was silently contaminated with post-as_of rows.)
    """
    stripped = sql.strip().rstrip(";")
    # Structural checks run against a literal-blanked copy so `WHERE note = 'a;b'` isn't read as
    # two statements, and `WHERE msg = 'no LIMIT here'` isn't read as already being limited.
    blanked = _blank_literals(stripped)

    if ";" in blanked:
        return SqlGuardResult(rejection="Rejected: only single-statement queries are permitted.")
    if not _READ_ONLY_RE.match(blanked):
        return SqlGuardResult(rejection="Rejected: only read-only SELECT/WITH queries are permitted.")

    if as_of:
        touches = [t for t in _BIZ_DATE_TABLES if re.search(rf"\b{t}\b", blanked, re.IGNORECASE)]
        if touches:
            bound = _BIZ_DATE_BOUND_RE.search(_blank_literals(stripped, keep_dates=True))
            if bound is not None:
                # The model wrote its own bound — permitted only if it is no later than as_of.
                if bound.group(2) > as_of:
                    return SqlGuardResult(
                        rejection=(
                            f"Rejected: query bounds biz_date at {bound.group(2)}, which is after "
                            f"the current as_of ({as_of}). Point-in-time mode forbids reading data "
                            f"dated after as_of."
                        )
                    )
            else:
                # No bound of its own: clamp it here. Requires biz_date to be projected; if it
                # isn't, execution fails and _explain_sql_error turns that into guidance.
                stripped = (
                    f"SELECT * FROM (\n{stripped}\n) AS {_PIT_ALIAS} "
                    f"WHERE {_PIT_ALIAS}.biz_date <= DATE '{as_of}'"
                )

    if not _LIMIT_RE.search(_blank_literals(stripped)):
        stripped = f"{stripped}\nLIMIT 1000"
    return SqlGuardResult(sql=stripped)


def _explain_sql_error(exc: Exception, as_of: str | None) -> str:
    """Turn the one driver error our point-in-time wrapper can cause into actionable guidance."""
    message = str(exc)
    if as_of and _PIT_ALIAS in message and "biz_date" in message:
        return _MISSING_BIZ_DATE_HINT.replace("AS_OF", as_of)
    return f"Query failed: {message}"


@tool
def sql_list_tables() -> str:
    """List the curated market-data Postgres tables available to sql_query, with a one-line
    description of each. Tables marked (point-in-time) carry a biz_date column and are subject to
    as_of clamping."""
    return "\n".join(
        f"- {t}: {desc}" + (" (point-in-time)" if has_biz_date else "")
        for t, (desc, has_biz_date) in CURATED_TABLES.items()
    )


@tool
def sql_schema(
    table: Annotated[str, Field(description="Table name from sql_list_tables (e.g. 'option_research_run', 'prices', 'technicals').")],
) -> str:
    """Show column names and types for a table via information_schema.columns."""
    if table not in CURATED_TABLES:
        return f"'{table}' is not in the curated table list. Call sql_list_tables() first."
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = %s ORDER BY ordinal_position",
            (table,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as exc:
        return f"Could not fetch schema for '{table}': {exc}"
    return "\n".join(f"{c}: {t}" for c, t in rows)


@tool
def sql_query(
    runtime: ToolRuntime,
    sql: Annotated[str, Field(description="Single read-only SELECT or WITH query against market-data Postgres (auto LIMIT 1000 applied).")],
) -> str:
    """Run a read-only SELECT/WITH query against the market-data Postgres. Rejected if it isn't
    SELECT/WITH or contains multiple statements. A LIMIT 1000 is auto-appended if you don't specify
    one. In point-in-time mode, any query touching a biz_date table is clamped to as_of — include
    `biz_date` in your SELECT list, or write your own `WHERE biz_date <= DATE '<as_of>'` bound
    (required for aggregate queries, which project no biz_date to clamp)."""
    ctx = runtime.context
    as_of = ctx.run_ctx.as_of_iso()
    guard = _guard_and_prepare_sql(sql, as_of)
    if not guard.ok:
        return guard.rejection
    try:
        conn = get_db_connection()
        conn.set_session(readonly=True)
        cur = conn.cursor()
        cur.execute(guard.sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        cur.close()
        conn.close()
    except Exception as exc:
        return _explain_sql_error(exc, as_of)
    df = pd.DataFrame(rows, columns=cols)
    handle = ctx.store.put("sql_result", df)
    return ctx.store.preview(handle)


@tool
def graphql_schema(
    runtime: ToolRuntime,
    type_name: Annotated[str | None, Field(description="Optional GraphQL type name to narrow introspection (e.g. 'Query', 'Stock', 'Financials').")] = None,
) -> str:
    """Condensed introspection of Sophie's public GraphQL API: field names and types only (never
    the raw introspection blob). Pass `type_name` (e.g. 'Query', 'Stock') to narrow."""
    ctx = runtime.context
    if not ctx.run_ctx.is_live:
        return "GraphQL is disabled in point-in-time mode — it only serves latest-only views."
    query = """
    query IntrospectionQuery {
      __schema {
        types {
          name
          fields { name type { name kind ofType { name kind } } }
        }
      }
    }
    """
    try:
        resp = requests.post(ctx.config.graphql_url, json={"query": query}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        return f"Introspection failed: {exc}"
    types = data.get("data", {}).get("__schema", {}).get("types", [])
    lines = []
    for t in types:
        if t["name"].startswith("__"):
            continue
        if type_name and t["name"] != type_name:
            continue
        fields = t.get("fields") or []
        if not fields:
            continue
        lines.append(f"{t['name']}: {', '.join(f.get('name', '') for f in fields[:20])}")
    return "\n".join(lines) if lines else "No matching type found."


@tool
def graphql_query(
    runtime: ToolRuntime,
    query: Annotated[str, Field(description="GraphQL query string to execute against Sophie's public GraphQL API.")],
    variables: Annotated[dict[str, Any] | None, Field(description="Optional dictionary of GraphQL variables (e.g. {'ticker': 'AAPL'}).")] = None,
) -> str:
    """Run a GraphQL query against Sophie's public API (composed views like stock(ticker),
    investmentClock, quantTrending). Flattens a tabular top-level list into the DataFrame store
    when possible."""
    ctx = runtime.context
    if not ctx.run_ctx.is_live:
        return "GraphQL is disabled in point-in-time mode — it only serves latest-only views."
    try:
        resp = requests.post(
            ctx.config.graphql_url, json={"query": query, "variables": variables or {}}, timeout=20
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        return f"GraphQL request failed: {exc}"
    if "errors" in payload:
        return f"GraphQL errors: {payload['errors']}"
    data = payload.get("data", {})
    for value in data.values():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            handle = ctx.store.put("graphql_result", pd.DataFrame(value))
            return ctx.store.preview(handle)
    return str(data)[:_MAX_INLINE_CHARS]


class MarketDataToolkit(SophieToolkit):
    toolkit_name: ClassVar[str] = "market"

    def get_tools(self) -> list[BaseTool]:
        return [sql_list_tables, sql_schema, sql_query, graphql_schema, graphql_query]

    def system_prompt_fragment(self) -> str:
        return (
            "MARKET DATA TOOLKIT: read-only SQL against the market-data Postgres (sql_list_tables "
            "for the curated table inventory, sql_schema, sql_query — SELECT/WITH only, auto-"
            "LIMIT'd) and Sophie's public GraphQL API (graphql_schema, graphql_query — composed "
            "views like stock(ticker), investmentClock). SQL reaches raw history and tables "
            "GraphQL doesn't expose (option_research_*, line_items); GraphQL gives the same "
            "composed views the live site uses. In point-in-time mode GraphQL is disabled entirely, "
            "and SQL against any biz_date table is clamped to as_of — put `biz_date` in your SELECT "
            "list so the clamp has a column to act on, or write the `biz_date <=` bound yourself."
        )
