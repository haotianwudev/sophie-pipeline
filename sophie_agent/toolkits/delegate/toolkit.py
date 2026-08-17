"""DelegationToolkit — attached only to profiles with can_delegate=True.

Spins up specialist agents from the AgentProfile registry, one at a time or fanned out in
parallel. Guards: depth limit (via RunContext.child()), no delegating to another can_delegate
profile, bounded concurrency, a fresh AgentExecutor per parallel task (LangChain executors are not
thread-safe; the shared DataFrameStore they write into is).
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, ClassVar

from langchain_core.tools import BaseTool, tool

from ...core.profiles import AGENT_PROFILES
from ..base import SophieToolkit

if TYPE_CHECKING:
    from ...core.runtime import AgentRuntime


class DelegationToolkit(SophieToolkit):
    toolkit_name: ClassVar[str] = "delegate"

    runtime: "AgentRuntime"

    def get_tools(self) -> list[BaseTool]:
        runtime = self.runtime
        run_ctx = self.run_ctx
        max_workers = self.config.max_workers

        def run_one(agent_key: str, task: str, context: str | None) -> str:
            profile = AGENT_PROFILES.get(agent_key)
            if profile is None:
                return f"Unknown agent '{agent_key}'. Call list_agents() for valid keys."
            if profile.can_delegate:
                return f"'{agent_key}' is itself a supervisor and cannot be delegated to."
            try:
                child_ctx = run_ctx.child()
            except RuntimeError as exc:
                return str(exc)
            try:
                child_ctx.check_budget()
            except Exception as exc:
                return f"Budget exceeded before delegating to '{agent_key}': {exc}"
            sub_agent = runtime.build_agent(agent_key, run_ctx=child_ctx)
            prompt = f"{context}\n\n{task}" if context else task
            try:
                return sub_agent.chat(prompt)
            except Exception as exc:
                return f"Sub-agent '{agent_key}' failed: {exc}"

        @tool
        def list_agents() -> str:
            """List every available specialist agent profile: key, description, and toolkits.
            Supervisors (can_delegate profiles) are excluded — they cannot be delegated to."""
            lines = [
                f"- {key}: {p.description} (toolkits: {', '.join(p.toolkits)})"
                for key, p in AGENT_PROFILES.items()
                if not p.can_delegate
            ]
            return "\n".join(lines)

        @tool
        def delegate(agent: str, task: str, context: str | None = None) -> str:
            """Run one specialist agent (a key from list_agents()) to completion and return its
            final answer. `context` is optional extra background prepended to the task. Any
            DataFrames the specialist produces remain readable by you and by siblings via the
            shared DataFrame store."""
            return run_one(agent, task, context)

        @tool
        def delegate_parallel(tasks: list[dict]) -> str:
            """Fan multiple specialist tasks out in parallel. Each item is
            {"agent": key, "task": str, "context": optional str}. Results are returned in
            submission order; a per-task failure is captured as an error string rather than
            failing the whole batch. Bounded to a small worker pool — don't submit more tasks than
            you actually need answered independently."""
            if not tasks:
                return "No tasks given."
            results: list[str] = [""] * len(tasks)
            with ThreadPoolExecutor(max_workers=min(max_workers, len(tasks))) as pool:
                futures = {
                    pool.submit(run_one, t.get("agent", ""), t.get("task", ""), t.get("context")): i
                    for i, t in enumerate(tasks)
                }
                for future in as_completed(futures):
                    i = futures[future]
                    try:
                        results[i] = future.result()
                    except Exception as exc:
                        results[i] = f"ERROR: {exc}"
            return "\n\n".join(f"[{tasks[i].get('agent')}] {results[i]}" for i in range(len(tasks)))

        return [list_agents, delegate, delegate_parallel]

    def system_prompt_fragment(self) -> str:
        return (
            "DELEGATION TOOLKIT: you are a supervisor. list_agents() shows the specialist agents "
            "you can hand work to. Use delegate() for one task, delegate_parallel() to fan several "
            "independent sub-questions out at once (e.g. a wiki lookup, a backtest-evidence query, "
            "and a live chain pricing, all in one batch). Synthesize their answers yourself — never "
            "just relay a single specialist's answer verbatim if the question needed more than one."
        )
