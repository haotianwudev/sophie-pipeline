"""SophieToolkit — the base every data toolkit subclasses.

BaseToolkit is a pydantic v2 model in langchain-core 0.3, so subclasses need
arbitrary_types_allowed to hold a DataFrameStore / RunContext / AgentConfig.
"""

from __future__ import annotations

from typing import ClassVar

from langchain_core.tools import BaseTool, BaseToolkit
from pydantic import ConfigDict, SkipValidation

from ..context.runcontext import RunContext
from ..context.store import DataFrameStore
from ..core.config import AgentConfig


class SophieToolkit(BaseToolkit):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    toolkit_name: ClassVar[str] = "base"

    store: DataFrameStore
    # SkipValidation: RunContext is a stdlib dataclass nesting a threading.Lock (inside
    # UsageTracker) — pydantic's auto dataclass-schema generation can't model a Lock and warns.
    # We want RunContext held opaquely and mutated by reference anyway, never validated/coerced.
    run_ctx: SkipValidation[RunContext]
    config: AgentConfig

    def get_tools(self) -> list[BaseTool]:  # pragma: no cover - overridden by subclasses
        raise NotImplementedError

    def system_prompt_fragment(self) -> str:
        """A short paragraph describing what this toolkit covers and when to reach for it.
        Concatenated into every agent's system prompt that carries this toolkit."""
        return ""
