"""DelegationToolkit — attached only to profiles with can_delegate=True.

Spins up specialist agents from the AgentProfile registry, one at a time or fanned out in parallel.
Guards: depth limit (via RunContext.child()), no delegating to another can_delegate profile, bounded
concurrency, and a shared token budget checked before each spawn.

Thread safety: specialist agents are now built once and cached by AgentRuntime, so parallel tasks
share one CompiledStateGraph. That is safe because a LangGraph agent holds no per-run state — the
state travels in the invocation (its own SophieContext with its own child RunContext, and its own
thread_id, both created per task). The DataFrameStore they all write into is itself lock-guarded.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Annotated, ClassVar

from langchain.tools import ToolRuntime
from langchain_core.tools import BaseTool, tool
from pydantic import BaseModel, Field

from ...context.agent_context import SophieContext
from ...core.profiles import AGENT_PROFILES
from ..base import SophieToolkit


class ParallelTask(BaseModel):
    """One specialist task in delegate_parallel. Defined here rather than in a separate schemas
    module so the signature that declares it and the body that consumes it can't drift."""

    agent: Annotated[str, Field(description="Specialist agent key (from list_agents).")]
    task: Annotated[str, Field(description="Actionable instruction or question for this specialist.")]
    context: Annotated[str | None, Field(description="Optional background context for this specific task.")] = None


def _run_one(ctx: SophieContext, agent_key: str, task: str, extra_context: str | None) -> str:
    """Run one specialist to completion on its own child context. Never raises — a failure is
    returned as text so one bad sub-task can't abort a whole fan-out."""
    profile = AGENT_PROFILES.get(agent_key)
    if profile is None:
        return f"Unknown agent '{agent_key}'. Call list_agents() for valid keys."
    if profile.can_delegate:
        return f"'{agent_key}' is itself a supervisor and cannot be delegated to."
    if ctx.runtime is None:
        return "Delegation is unavailable: no AgentRuntime is attached to this run's context."

    try:
        child = ctx.child()
    except RuntimeError as exc:
        return str(exc)
    try:
        child.run_ctx.check_budget()
    except Exception as exc:
        return f"Budget exceeded before delegating to '{agent_key}': {exc}"

    prompt = f"{extra_context}\n\n{task}" if extra_context else task
    try:
        sub_agent = ctx.runtime.build_agent(agent_key)
        return sub_agent.chat(prompt, context=child, thread_id=f"delegate-{agent_key}-{id(child)}")
    except Exception as exc:
        return f"Sub-agent '{agent_key}' failed: {exc}"


@tool
def list_agents() -> str:
    """List every available specialist agent profile: key, description, and toolkits. Supervisors
    (can_delegate profiles) are excluded — they cannot be delegated to."""
    return "\n".join(
        f"- {key}: {p.description} (toolkits: {', '.join(p.toolkits)})"
        for key, p in AGENT_PROFILES.items()
        if not p.can_delegate
    )


@tool
def delegate(
    runtime: ToolRuntime,
    agent: Annotated[str, Field(description="Key of the specialist agent to run (from list_agents, e.g. 'option_strategist', 'wiki_researcher').")],
    task: Annotated[str, Field(description="Actionable instruction or question to be solved by the specialist agent.")],
    context: Annotated[str | None, Field(description="Optional background context to prepend to the task.")] = None,
) -> str:
    """Run one specialist agent (a key from list_agents()) to completion and return its final
    answer. `context` is optional extra background prepended to the task. Any DataFrames the
    specialist produces remain readable by you and by siblings via the shared DataFrame store."""
    return _run_one(runtime.context, agent, task, context)


@tool
def delegate_parallel(
    runtime: ToolRuntime,
    tasks: Annotated[
        list[ParallelTask],
        Field(min_length=1, max_length=10, description="Tasks to execute concurrently across specialists."),
    ],
) -> str:
    """Fan multiple specialist tasks out in parallel. Each item is {"agent": key, "task": str,
    "context": optional str}. Results are returned in submission order; a per-task failure is
    captured as an error string rather than failing the whole batch. Bounded to a small worker pool
    — don't submit more tasks than you actually need answered independently."""
    ctx = runtime.context
    max_workers = min(ctx.config.max_workers, len(tasks))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        # executor.map preserves submission order, so results line up with `tasks` without the
        # index bookkeeping the previous as_completed() version needed.
        results = list(pool.map(lambda t: _run_one(ctx, t.agent, t.task, t.context), tasks))
    return "\n\n".join(f"[{t.agent}] {r}" for t, r in zip(tasks, results))


class DelegationToolkit(SophieToolkit):
    toolkit_name: ClassVar[str] = "delegate"

    def get_tools(self) -> list[BaseTool]:
        return [list_agents, delegate, delegate_parallel]

    def system_prompt_fragment(self) -> str:
        return (
            "DELEGATION TOOLKIT: you are a supervisor. list_agents() shows the specialist agents "
            "you can hand work to. Use delegate() for one task, delegate_parallel() to fan several "
            "independent sub-questions out at once (e.g. a wiki lookup, a backtest-evidence query, "
            "and a live chain pricing, all in one batch). Synthesize their answers yourself — never "
            "just relay a single specialist's answer verbatim if the question needed more than one."
        )
