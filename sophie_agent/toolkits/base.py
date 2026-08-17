"""SophieToolkit — the base every data toolkit subclasses.

A toolkit is a stateless *declaration* of tools. It holds no run state: everything a tool needs at
call time (RunContext/as_of, the DataFrameStore, AgentConfig) arrives through the
`runtime: ToolRuntime` parameter LangGraph injects, as `runtime.context` — a
`context.agent_context.SophieContext`.

That parameter is injected by name+annotation and is automatically excluded from the JSON schema the
model sees, so it never appears as a tool argument the LLM could set.

IMPORTANT — tools here must NOT pass `args_schema=` to `@tool`. Verified against
langchain 1.3.15: supplying an explicit `args_schema` makes LangChain treat it as the verbatim
invocation contract and skip registering `runtime` for injection, so the tool fails at call time
with `TypeError: missing 1 required positional argument: 'runtime'`. Declare argument types,
constraints, and descriptions with `Annotated[T, Field(...)]` on the signature instead — that
produces an identical JSON schema (bounds and descriptions included) from a single source of truth.
"""

from __future__ import annotations

from typing import ClassVar

from langchain_core.tools import BaseTool, BaseToolkit


class SophieToolkit(BaseToolkit):
    toolkit_name: ClassVar[str] = "base"

    def get_tools(self) -> list[BaseTool]:  # pragma: no cover - overridden by subclasses
        raise NotImplementedError

    def system_prompt_fragment(self) -> str:
        """A short paragraph describing what this toolkit covers and when to reach for it.
        Concatenated into every agent's system prompt that carries this toolkit."""
        return ""
