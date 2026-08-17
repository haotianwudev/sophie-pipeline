"""AgentRuntime — owns the shared DataFrameStore and AgentConfig, builds agents from profiles.

One runtime per CLI session / eval run. Every agent it builds (top-level or delegated) shares the
same DataFrameStore, so a chain pulled by one specialist stays visible to a sibling or the
supervisor that spawned it.
"""

from __future__ import annotations

from langchain_core.globals import set_llm_cache

from ..context.cache import SqliteCache
from ..context.runcontext import RunContext
from ..context.store import DataFrameStore
from ..toolkits import TOOLKIT_REGISTRY
from ..toolkits.delegate import DelegationToolkit
from .config import DEFAULT_CONFIG, AgentConfig
from .profiles import AgentProfile, get_profile

_cache_installed = False


def _ensure_llm_cache(config: AgentConfig) -> None:
    """Installs the process-wide LangChain LLM cache once. Safe to call from every AgentRuntime
    construction — set_llm_cache is a global, so re-installing it repeatedly would just discard
    the same cache and reopen the sqlite file for no reason."""
    global _cache_installed
    if _cache_installed:
        return
    set_llm_cache(SqliteCache(config.cache_db_path))
    _cache_installed = True


class AgentRuntime:
    def __init__(
        self,
        config: AgentConfig | None = None,
        root_run_ctx: RunContext | None = None,
        force_model: str | None = None,
        force_provider=None,
    ) -> None:
        self.config = config or DEFAULT_CONFIG
        _ensure_llm_cache(self.config)
        self.store = DataFrameStore()
        self.root_run_ctx = root_run_ctx or RunContext(
            token_budget=self.config.token_budget,
            as_of=_parse_as_of(self.config.as_of),
        )
        # Set by --all-local (or any caller wanting every profile, including delegated
        # sub-agents, pinned to one model) — overrides every profile's own model_name/provider.
        self.force_model = force_model
        self.force_provider = force_provider

    def build_toolkits(self, profile: AgentProfile, run_ctx: RunContext) -> list:
        instances = []
        for name in profile.toolkits:
            cls = TOOLKIT_REGISTRY.get(name)
            if cls is None:
                raise KeyError(f"Unknown toolkit '{name}' referenced by profile '{profile.key}'.")
            if cls is DelegationToolkit:
                instances.append(cls(store=self.store, run_ctx=run_ctx, config=self.config, runtime=self))
            else:
                instances.append(cls(store=self.store, run_ctx=run_ctx, config=self.config))
        return instances

    def build_agent(self, key: str, run_ctx: RunContext | None = None, **overrides):
        # Deferred import: agent.py imports nothing from runtime.py, but this keeps the module
        # graph acyclic and obvious regardless.
        from .agent import SophieAgent

        profile = get_profile(key)
        ctx = run_ctx or self.root_run_ctx
        toolkits = self.build_toolkits(profile, ctx)
        model_name = overrides.get("model_name") or self.force_model or profile.model_name or self.config.default_model_name
        provider = overrides.get("provider") or self.force_provider or profile.provider
        return SophieAgent(
            toolkits=toolkits,
            model_name=model_name,
            provider=provider,
            config=self.config,
            run_ctx=ctx,
            system_prompt=profile.system_prompt,
            answer_model=profile.answer_model,
            name=profile.key,
            max_iterations=profile.max_iterations,
            verbose=overrides.get("verbose", False),
        )


def _parse_as_of(as_of_str: str | None):
    if not as_of_str:
        return None
    from datetime import date

    return date.fromisoformat(as_of_str)


# DelegationToolkit's `runtime: "AgentRuntime"` field is a forward reference resolved only under
# TYPE_CHECKING in toolkits/delegate.py (to avoid a real circular import at module-load time), so
# pydantic can't find the name in that module's own globals. Rebuild it here, once, now that
# AgentRuntime actually exists — this must run before any DelegationToolkit is instantiated.
DelegationToolkit.model_rebuild(_types_namespace={"AgentRuntime": AgentRuntime})
