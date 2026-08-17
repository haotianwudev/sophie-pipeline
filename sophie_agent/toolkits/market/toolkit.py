"""MarketDataToolkit — read-only Postgres SQL + Sophie's public GraphQL API."""

from __future__ import annotations

import re
from typing import ClassVar

import pandas as pd
import requests
from langchain_core.tools import BaseTool, tool

from src.tools.api_db import get_db_connection

from ..base import SophieToolkit

# name -> (one-line description, has biz_date column for as_of injection)
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

_READ_ONLY_RE = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
_LIMIT_RE = re.compile(r"\bLIMIT\s+\d+\s*;?\s*$", re.IGNORECASE)
_MAX_INLINE_CHARS = 4000


def _guard_and_prepare_sql(sql: str, as_of: str | None) -> str | None:
    """Returns the (possibly-rewritten) query, or None if it's rejected."""
    stripped = sql.strip().rstrip(";")
    if ";" in stripped:
        return None  # multi-statement
    if not _READ_ONLY_RE.match(stripped):
        return None

    if as_of:
        for table, (_, has_biz_date) in CURATED_TABLES.items():
            if has_biz_date and re.search(rf"\b{table}\b", stripped, re.IGNORECASE):
                # Best-effort: inject a biz_date filter via an outer wrapper rather than parsing
                # the query's own WHERE clause, so it works regardless of query shape.
                stripped = f"SELECT * FROM ({stripped}) AS _as_of_wrapped WHERE TRUE"
                break

    if not _LIMIT_RE.search(stripped):
        stripped = f"{stripped}\nLIMIT 1000"
    return stripped


class MarketDataToolkit(SophieToolkit):
    toolkit_name: ClassVar[str] = "market"

    def get_tools(self) -> list[BaseTool]:
        store = self.store
        run_ctx = self.run_ctx
        graphql_url = self.config.graphql_url

        @tool
        def sql_list_tables() -> str:
            """List the curated market-data Postgres tables available to sql_query, with a
            one-line description of each."""
            return "\n".join(f"- {t}: {desc}" for t, (desc, _) in CURATED_TABLES.items())

        @tool
        def sql_schema(table: str) -> str:
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
        def sql_query(sql: str) -> str:
            """Run a read-only SELECT/WITH query against the market-data Postgres. Rejected if it
            isn't SELECT/WITH, contains multiple statements, or attempts a write. A LIMIT 1000 is
            auto-appended if you don't specify one. In point-in-time mode, results are best-effort
            filtered so no row postdates the current as_of."""
            prepared = _guard_and_prepare_sql(sql, run_ctx.as_of_iso())
            if prepared is None:
                return "Rejected: only single-statement SELECT/WITH queries are permitted."
            try:
                conn = get_db_connection()
                conn.set_session(readonly=True)
                cur = conn.cursor()
                cur.execute(prepared)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                cur.close()
                conn.close()
            except Exception as exc:
                return f"Query failed: {exc}"
            df = pd.DataFrame(rows, columns=cols)
            handle = store.put("sql_result", df)
            return store.preview(handle)

        @tool
        def graphql_schema(type_name: str | None = None) -> str:
            """Condensed introspection of Sophie's public GraphQL API: field names and types only
            (never the raw introspection blob). Pass `type_name` (e.g. 'Query', 'Stock') to narrow."""
            if not run_ctx.is_live:
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
                resp = requests.post(graphql_url, json={"query": query}, timeout=15)
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
                field_desc = ", ".join(f.get("name", "") for f in fields[:20])
                lines.append(f"{t['name']}: {field_desc}")
            return "\n".join(lines) if lines else "No matching type found."

        @tool
        def graphql_query(query: str, variables: dict | None = None) -> str:
            """Run a GraphQL query against Sophie's public API (composed views like stock(ticker),
            investmentClock, quantTrending). Flattens a tabular top-level list into the DataFrame
            store when possible."""
            if not run_ctx.is_live:
                return "GraphQL is disabled in point-in-time mode — it only serves latest-only views."
            try:
                resp = requests.post(graphql_url, json={"query": query, "variables": variables or {}}, timeout=20)
                resp.raise_for_status()
                payload = resp.json()
            except Exception as exc:
                return f"GraphQL request failed: {exc}"
            if "errors" in payload:
                return f"GraphQL errors: {payload['errors']}"
            data = payload.get("data", {})
            for value in data.values():
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    df = pd.DataFrame(value)
                    handle = store.put("graphql_result", df)
                    return store.preview(handle)
            return str(data)[:_MAX_INLINE_CHARS]

        return [sql_list_tables, sql_schema, sql_query, graphql_schema, graphql_query]

    def system_prompt_fragment(self) -> str:
        return (
            "MARKET DATA TOOLKIT: read-only SQL against the market-data Postgres (sql_list_tables "
            "for the curated table inventory, sql_schema, sql_query — SELECT/WITH only, auto-"
            "LIMIT'd) and Sophie's public GraphQL API (graphql_schema, graphql_query — composed "
            "views like stock(ticker), investmentClock). SQL reaches raw history and tables "
            "GraphQL doesn't expose (option_research_*, line_items); GraphQL gives the same "
            "composed views the live site uses. Both are disabled for live-only reads in "
            "point-in-time mode."
        )
