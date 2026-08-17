"""SophieAgent — the backbone. One class; everything domain-specific is injected via toolkits, the
profile, and the per-invocation SophieContext. Built on LangChain 1.x `create_agent`.

This class deliberately owns as little machinery as possible — `create_agent` accepts
`system_prompt`, `middleware`, `response_format`, `context_schema`, `checkpointer`, `store` and
`cache`, so the pieces that used to be hand-rolled here are now configuration:

  hand-rolled before                -> package feature now
  ---------------------------------    ---------------------------------------------------
  manual SystemMessage prepend         `@dynamic_prompt` middleware (prompt varies per turn:
                                       the store listing and as_of change between calls)
  `self.chat_history` list             `checkpointer=` + `thread_id`, which also preserves
                                       ToolMessages across turns (the manual list dropped them)
  `structured()`'s 2nd LLM call        `response_format=ToolStrategy(...)`
  `max_iterations` (silently unused)   `ModelCallLimitMiddleware(run_limit=...)`
  nothing (context grew unbounded)     `ContextEditingMiddleware([ClearToolUsesEdit(...)])`
  a fallback-model *hint* in an error  `ModelFallbackMiddleware(...)` when a profile opts in

`intermediate_steps` is still exposed, because the eval harness asserts on tool trajectory — but it
is now derived from the message list rather than tracked separately.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, AsyncIterator, Type

from langchain.agents import create_agent
from langchain.agents.middleware import (
    ClearToolUsesEdit,
    ContextEditingMiddleware,
    ModelCallLimitMiddleware,
    ModelFallbackMiddleware,
    dynamic_prompt,
)
from langchain.agents.structured_output import ToolStrategy
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.tools import BaseTool
from langgraph.checkpoint.memory import InMemorySaver
from pydantic import BaseModel

from ..context.agent_context import SophieContext
from ..context.run_record import write_run_record
from .callbacks import UsageCallbackHandler
from .config import DEFAULT_CONFIG, AgentConfig
from .models import ToolCallingNotSupportedError, build_chat_model, provider_from_str

__all__ = ["SophieAgent", "ToolAction", "ToolCallingNotSupportedError", "tool_trajectory"]

DEFAULT_THREAD_ID = "default"


@dataclass(frozen=True)
class ToolAction:
    """A tool invocation, for trajectory inspection by the eval harness."""

    tool: str
    tool_input: Any
    log: str = ""


def tool_trajectory(messages: list[BaseMessage]) -> list[tuple[ToolAction, str]]:
    """Reconstruct (action, observation) pairs from a message list, newest last.

    The messages *are* the trajectory in LangChain 1.x — this derives the legacy
    `intermediate_steps` shape from them rather than maintaining a parallel record.
    """
    observations = {
        m.tool_call_id: (m.content if isinstance(m.content, str) else str(m.content))
        for m in messages
        if isinstance(m, ToolMessage) and getattr(m, "tool_call_id", None)
    }
    steps: list[tuple[ToolAction, str]] = []
    for m in messages:
        if isinstance(m, AIMessage) and getattr(m, "tool_calls", None):
            for tc in m.tool_calls:
                steps.append(
                    (
                        ToolAction(tool=tc.get("name", ""), tool_input=tc.get("args", {})),
                        observations.get(tc.get("id", ""), ""),
                    )
                )
    return steps


def _final_text(messages: list[BaseMessage]) -> str:
    if not messages:
        return ""
    last = messages[-1]
    return last.content if isinstance(last.content, str) else str(last.content)


class SophieAgent:
    def __init__(
        self,
        default_context: SophieContext,
        toolkits: list | None = None,
        model_name: str | None = None,
        provider: str | None = None,
        config: AgentConfig | None = None,
        system_prompt: str | None = None,
        answer_model: Type[BaseModel] | None = None,
        name: str = "sophie",
        verbose: bool = False,
        max_iterations: int = 15,
        record_runs: bool = True,
        fallback_models: tuple[str, ...] = (),
        llm: Any = None,
    ) -> None:
        """`llm` injects a prebuilt chat model instead of constructing one from
        model_name/provider — used by the offline test suite, and by any caller that already holds a
        configured model."""
        self.name = name
        self.record_runs = record_runs
        self.config = config or DEFAULT_CONFIG
        self.answer_model = answer_model
        self.toolkits = toolkits or []
        # The context this agent uses when a caller doesn't pass one. Delegated sub-agents always
        # get an explicit child context instead, which is why this can be a shared cached agent.
        self.default_context = default_context

        self.tools: list[BaseTool] = []
        seen: set[str] = set()
        for tk in self.toolkits:
            for t in tk.get_tools():
                if t.name in seen:
                    raise ValueError(
                        f"Duplicate tool name '{t.name}' across toolkits attached to agent '{name}'."
                    )
                seen.add(t.name)
                self.tools.append(t)

        self._model_name = model_name or self.config.default_model_name
        self._provider = provider_from_str(provider or self.config.default_provider)
        self.llm = llm if llm is not None else build_chat_model(self._model_name, self._provider)

        self._base_system_prompt = system_prompt or (
            "You are Sophie, a research assistant over the Sophie finance platform."
        )
        self._fragments = "\n\n".join(
            f for tk in self.toolkits for f in [tk.system_prompt_fragment()] if f
        )
        self._max_iterations = max_iterations
        self._fallback_models = fallback_models
        self._verbose = verbose

        self._checkpointer = InMemorySaver()
        self.agent = create_agent(
            model=self.llm,
            tools=self.tools,
            middleware=self._build_middleware(),
            context_schema=SophieContext,
            checkpointer=self._checkpointer,
            debug=verbose,
        )
        # Built lazily per answer schema by structured(); a response_format-bound graph always
        # returns typed output, so it can't double as the prose graph above.
        self._structured_agents: dict[type, Any] = {}

    # ---------------------------------------------------------------- prompt / middleware

    def _build_middleware(self) -> list:
        base_prompt = self._base_system_prompt
        fragments = self._fragments

        @dynamic_prompt
        def sophie_prompt(request) -> str:
            """Reassembled every turn: the DataFrame store listing and the point-in-time banner
            both change as a run progresses, so a static system_prompt would go stale."""
            ctx: SophieContext = request.runtime.context
            parts = [
                base_prompt,
                fragments,
                ctx.run_ctx.prompt_fragment(),
                f"Stored DataFrames (shared across this run):\n{ctx.store.context_listing()}",
            ]
            return "\n\n".join(p for p in parts if p)

        middleware: list = [
            sophie_prompt,
            # Replaces the max_iterations parameter that AgentExecutor used to honor and that
            # create_agent silently ignored. 'end' stops the loop cleanly with whatever the model
            # has produced, rather than raising mid-run.
            ModelCallLimitMiddleware(run_limit=self._max_iterations, exit_behavior="end"),
            # Bulk tool output is the dominant context cost here. Clearing older tool results is
            # safe in this design precisely because bulk results never lived in the messages: they
            # live in the DataFrameStore under a handle, which stays valid after the text is cleared.
            ContextEditingMiddleware(edits=[ClearToolUsesEdit(trigger=120_000, keep=3)]),
        ]
        if self._fallback_models:
            middleware.append(
                ModelFallbackMiddleware(
                    *[
                        build_chat_model(m, self._provider) if ":" not in m else m
                        for m in self._fallback_models
                    ]
                )
            )
        return middleware

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def provider_name(self) -> str:
        return self._provider.value

    def preview_system_prompt(self, context: SophieContext | None = None) -> str:
        """Render the system prompt as the model would see it right now.

        The prompt is assembled by middleware at call time, so there is no static string to print —
        this reproduces the same assembly for `run.py --list-tools` and for debugging.
        """
        ctx = context or self.default_context
        parts = [
            self._base_system_prompt,
            self._fragments,
            ctx.run_ctx.prompt_fragment(),
            f"Stored DataFrames (shared across this run):\n{ctx.store.context_listing()}",
        ]
        return "\n\n".join(p for p in parts if p)

    # ---------------------------------------------------------------- internals

    def _config(self, thread_id: str | None) -> dict:
        return {"configurable": {"thread_id": thread_id or DEFAULT_THREAD_ID}}

    def _invoke_config(self, ctx: SophieContext, thread_id: str | None) -> dict:
        cfg = self._config(thread_id)
        cfg["callbacks"] = [UsageCallbackHandler(ctx.run_ctx)]
        return cfg

    def _record(self, ctx: SophieContext, message: str, messages: list[BaseMessage]) -> None:
        """Run records are diagnostic; a failure here must never lose the actual answer."""
        if not self.record_runs:
            return
        try:
            write_run_record(
                runs_dir=self.config.runs_dir,
                run_id=ctx.run_ctx.run_id,
                agent_name=self.name,
                model_name=self._model_name,
                provider=self._provider.value,
                as_of=ctx.run_ctx.as_of_iso(),
                message=message,
                output=_final_text(messages),
                intermediate_steps=tool_trajectory(messages),
                usage=ctx.run_ctx.usage.as_dict(),
            )
        except Exception:
            pass

    # ---------------------------------------------------------------- public API

    def invoke(
        self,
        message: str,
        *,
        context: SophieContext | None = None,
        thread_id: str | None = None,
        seed_history: list[BaseMessage] | None = None,
    ) -> dict:
        """Run one turn. Conversation state for `thread_id` is held by the checkpointer, so only
        the new message is passed in; `seed_history` prepends messages when adopting a history that
        originated elsewhere (see server.py's AG-UI thread seeding)."""
        ctx = context or self.default_context
        inputs = [*(seed_history or []), HumanMessage(content=message)]
        result = self.agent.invoke(
            {"messages": inputs},
            context=ctx,
            config=self._invoke_config(ctx, thread_id),
        )
        messages = result.get("messages", [])
        self._record(ctx, message, messages)
        return {
            "output": _final_text(messages),
            "messages": messages,
            "intermediate_steps": tool_trajectory(messages),
        }

    def chat(
        self,
        message: str,
        *,
        context: SophieContext | None = None,
        thread_id: str | None = None,
    ) -> str:
        return self.invoke(message, context=context, thread_id=thread_id)["output"]

    def structured(
        self,
        message: str,
        model: Type[BaseModel] | None = None,
        *,
        context: SophieContext | None = None,
        thread_id: str | None = None,
    ) -> BaseModel:
        """Return a validated typed answer.

        Previously this ran the whole tool loop and then made a *second* LLM call to repackage the
        result into the schema. `response_format` folds that into the same graph: the schema is
        bound as an output tool, so the model emits it directly and LangChain validates it (retrying
        on validation failure) without a second round trip.
        """
        target = model or self.answer_model
        if target is None:
            raise ValueError(
                f"Agent '{self.name}' has no answer_model configured; pass `model` explicitly."
            )
        if target not in self._structured_agents:
            self._structured_agents[target] = create_agent(
                model=self.llm,
                tools=self.tools,
                middleware=self._build_middleware(),
                context_schema=SophieContext,
                response_format=ToolStrategy(target),
                debug=self._verbose,
            )
        ctx = context or self.default_context
        result = self._structured_agents[target].invoke(
            {"messages": [HumanMessage(content=message)]},
            context=ctx,
            config=self._invoke_config(ctx, thread_id),
        )
        self._record(ctx, message, result.get("messages", []))
        return result["structured_response"]

    async def stream(
        self,
        message: str,
        *,
        context: SophieContext | None = None,
        thread_id: str | None = None,
        seed_history: list[BaseMessage] | None = None,
    ) -> AsyncIterator[dict]:
        """Yield raw astream_events v2 events. v2 remains the right choice: v3 exists but its own
        docstring marks it beta, and ag_ui_mapper.py was built against measured v2 event shapes."""
        ctx = context or self.default_context
        cfg = self._invoke_config(ctx, thread_id)
        inputs = [*(seed_history or []), HumanMessage(content=message)]
        async for event in self.agent.astream_events(
            {"messages": inputs}, version="v2", context=ctx, config=cfg
        ):
            yield event
        # Streamed runs used to skip run records entirely — which meant the chat widget, the only
        # consumer of stream(), produced none at all. The final state comes from the checkpointer.
        self._record(ctx, message, self.history(thread_id))

    # ---------------------------------------------------------------- history

    def history(self, thread_id: str | None = None) -> list[BaseMessage]:
        """Full message history for a thread, ToolMessages included."""
        state = self.agent.get_state(self._config(thread_id))
        return list(state.values.get("messages", []))

    def has_history(self, thread_id: str | None = None) -> bool:
        return bool(self.agent.get_state(self._config(thread_id)).values.get("messages"))

    def reset(self, thread_id: str | None = None) -> None:
        """Drop a thread's conversation state. A fresh thread_id is a fresh conversation, so this
        just clears the checkpoint for the given one."""
        self._checkpointer.delete_thread((thread_id or DEFAULT_THREAD_ID))
