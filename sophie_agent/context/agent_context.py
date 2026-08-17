"""SophieContext — invoke-time runtime context, delivered by LangGraph to tools and middleware.

Replaces the previous arrangement where every toolkit held `store`/`run_ctx`/`config` as pydantic
model fields and closed over them inside `get_tools()`. That had two costs:

1. Every delegated sub-agent needs its own RunContext (deeper `depth`, shared usage accumulator),
   so `AgentRuntime` had to re-instantiate every toolkit — and therefore rebuild every tool — on
   each `delegate()` call. `WikiToolkit._get_wiki_store`'s module-level `lru_cache` existed purely
   to stop that from re-parsing 240 markdown files each time.
2. Holding a `RunContext` (which nests a `threading.Lock`) inside a `BaseToolkit` pydantic model
   required `arbitrary_types_allowed`, `SkipValidation`, and a `model_rebuild(_types_namespace=...)`
   call in runtime.py to resolve a forward reference pydantic couldn't see.

Both disappear when the context travels per-invocation instead of per-construction: toolkits become
stateless tool *declarations*, agents are built once and cached, and every tool reads the live
context through `runtime: ToolRuntime`.

`AgentConfig` is imported only under TYPE_CHECKING: `sophie_agent.core.__init__` imports `agent.py`,
which imports this package, so an eager `..core.config` import here would close a cycle.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from .runcontext import RunContext
from .store import DataFrameStore

if TYPE_CHECKING:
    from ..core.config import AgentConfig
    from ..core.runtime import AgentRuntime


@dataclass
class SophieContext:
    """Passed to `agent.invoke(..., context=...)` and read back inside tools via
    `runtime.context`. Not a pydantic model — it deliberately holds live, mutable, un-validated
    objects (a lock-bearing RunContext, a thread-safe DataFrameStore, the AgentRuntime itself)."""

    run_ctx: RunContext
    store: DataFrameStore
    config: "AgentConfig"
    # Only populated for profiles carrying the delegation toolkit; it's how `delegate()` reaches
    # back into the runtime to build a specialist. Kept off the toolkit so nothing else can use it.
    runtime: "AgentRuntime | None" = None

    def child(self) -> "SophieContext":
        """Context for a delegated sub-agent: a deeper RunContext (which shares this run's usage
        accumulator and token budget), but the same store, config, and runtime — so a DataFrame
        pulled by one specialist stays visible to its siblings and to the supervisor."""
        return replace(self, run_ctx=self.run_ctx.child())
