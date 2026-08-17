"""DataFrameToolkit — inspect and analyze every DataFrame the session has produced.

df_python executes arbitrary Python in-process (PythonAstREPLTool-style: ast.parse, exec all
nodes but the last, eval the last if it's an expression). That is a deliberate trade-off for a
local personal tool and what makes real options/backtest analysis possible — gated by
SOPHIE_AGENT_ALLOW_PYTHON, not pretended safe by a blocklist.
"""

from __future__ import annotations

import ast
import io
from contextlib import redirect_stdout
from typing import ClassVar

import numpy as np
import pandas as pd
from langchain_core.tools import BaseTool, tool

from ..base import SophieToolkit

_MAX_REPR_CHARS = 4000


class DataFrameToolkit(SophieToolkit):
    toolkit_name: ClassVar[str] = "dataframe"

    def get_tools(self) -> list[BaseTool]:
        store = self.store
        allow_python = self.config.allow_python

        @tool
        def df_list() -> str:
            """List every DataFrame currently in the shared store, with its handle and shape."""
            return store.context_listing()

        @tool
        def df_schema(name: str) -> str:
            """Show dtypes, null counts, and a numeric describe() for a stored DataFrame."""
            try:
                df = store.get(name)
            except KeyError as exc:
                return str(exc)
            dtypes = df.dtypes.astype(str)
            nulls = df.isnull().sum()
            schema_lines = [f"{c}: {dtypes[c]} (nulls={nulls[c]})" for c in df.columns]
            describe = df.describe(include="number").to_markdown() if not df.select_dtypes("number").empty else "(no numeric columns)"
            return "\n".join(schema_lines) + "\n\n" + describe

        @tool
        def df_head(name: str, n: int = 10) -> str:
            """Show the first n rows of a stored DataFrame as markdown."""
            try:
                return store.preview(name, n=n)
            except KeyError as exc:
                return str(exc)

        @tool
        def df_python(code: str, save_as: str | None = None) -> str:
            """Execute Python against every stored DataFrame (available in the namespace by its
            handle name), plus `pd` and `np`. The result of the last expression (if any) is
            returned; if it's a DataFrame it is auto-registered under `save_as` (or an auto name)
            and you get shape + a preview instead of a dump. Use this for anything the other
            DataFrame tools don't cover directly — grouping, pivoting, custom joins, plotting data
            prep, etc."""
            if not allow_python:
                return "df_python is disabled (SOPHIE_AGENT_ALLOW_PYTHON=0)."

            namespace: dict = {"pd": pd, "np": np}
            for name, _ in store.list():
                namespace[name] = store.get(name)

            try:
                tree = ast.parse(code, mode="exec")
            except SyntaxError as exc:
                return f"SyntaxError: {exc}"

            stdout = io.StringIO()
            result = None
            try:
                with redirect_stdout(stdout):
                    if tree.body and isinstance(tree.body[-1], ast.Expr):
                        *body, last = tree.body
                        if body:
                            exec(compile(ast.Module(body=body, type_ignores=[]), "<df_python>", "exec"), namespace)
                        result = eval(
                            compile(ast.Expression(body=last.value), "<df_python>", "eval"), namespace
                        )
                    else:
                        exec(compile(tree, "<df_python>", "exec"), namespace)
            except Exception as exc:
                captured = stdout.getvalue()
                return (captured + f"\n{type(exc).__name__}: {exc}").strip()

            captured = stdout.getvalue()
            if isinstance(result, pd.DataFrame):
                handle = store.put(save_as or "df_python_result", result)
                return (captured + f"\n{store.preview(handle)}").strip()
            if result is not None:
                text = repr(result)
                if len(text) > _MAX_REPR_CHARS:
                    text = text[:_MAX_REPR_CHARS] + "... (truncated)"
                return (captured + f"\n{text}").strip()
            return captured.strip() or "(no output)"

        return [df_list, df_schema, df_head, df_python]

    def system_prompt_fragment(self) -> str:
        return (
            "DATAFRAME TOOLKIT: every bulk result from other toolkits lands in a shared handle "
            "store, never dumped directly into your context. df_list/df_schema/df_head inspect a "
            "handle; df_python runs arbitrary pandas/numpy code against every stored frame by name "
            "(plus pd/np) for anything the typed tools don't cover — grouping, pivots, custom "
            "filters. Never compute financial numbers (P&L, greeks, Sharpe) by hand in prose; use "
            "this or the strategy/market toolkits, which do the arithmetic deterministically."
        )
