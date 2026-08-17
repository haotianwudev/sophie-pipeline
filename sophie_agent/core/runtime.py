"""AgentRuntime — owns the shared DataFrameStore and AgentConfig, builds agents from profiles.

One runtime per CLI session / eval run / chat thread. Every agent it builds (top-level or delegated)
shares the same DataFrameStore, so a chain pulled by one specialist stays visible to a sibling or the
supervisor that spawned it.

Toolkits and agents are both cached now. They used to be rebuilt on every `delegate()` call, because
each sub-agent needs its own RunContext and the RunContext was a toolkit constructor field. With the
context moved to the invocation (`SophieContext`), a toolkit is a stateless tool declaration and an
agent is a reusable compiled graph — so a delegation-heavy run constructs each specialist once
instead of once per delegated task.
"""

from __future__ import annotations

from datetime import date

from langchain_core.globals import set_llm_cache

from ..context.agent_context import SophieContext
from ..context.cache import SqliteCache
from ..context.runcontext import RunContext
from ..context.store import DataFrameStore
from ..toolkits import TOOLKIT_REGISTRY, SophieToolkit
from .config import DEFAULT_CONFIG, AgentConfig
from .models import provider_from_str
from .profiles import AgentProfile, get_profile

_cache_installed = False


def _ensure_llm_cache(config: AgentConfig) -> None:
    """Installs the process-wide LangChain LLM cache once. Safe to call from every AgentRuntime
    construction — set_llm_cache is a global, so re-installing it repeatedly would just discard
    the same cache and reopen the sqlite file for no reason."""
    global _cache_installed
    if _cache_installed or not config.llm_cache:
        return
    set_llm_cache(SqliteCache(config.cache_db_path))
    _cache_installed = True


def _parse_as_of(as_of_str: str | None) -> date | None:
    return date.fromisoformat(as_of_str) if as_of_str else None


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
        self._toolkits: dict[str, SophieToolkit] = {}
        self._agents: dict[tuple, object] = {}

    def root_context(self) -> SophieContext:
        """The context top-level (non-delegated) invocations run under."""
        return SophieContext(
            run_ctx=self.root_run_ctx,
            store=self.store,
            config=self.config,
            runtime=self,
        )

    def toolkit(self, name: str) -> SophieToolkit:
        cls = TOOLKIT_REGISTRY.get(name)
        if cls is None:
            raise KeyError(f"Unknown toolkit '{name}'.")
        if name not in self._toolkits:
            self._toolkits[name] = cls()
        return self._toolkits[name]

    def build_toolkits(self, profile: AgentProfile) -> list[SophieToolkit]:
        for name in profile.toolkits:
            if name not in TOOLKIT_REGISTRY:
                raise KeyError(f"Unknown toolkit '{name}' referenced by profile '{profile.key}'.")
        return [self.toolkit(name) for name in profile.toolkits]

    def build_agent(self, key: str, verbose: bool = False, **overrides):
        # Deferred import: agent.py imports nothing from runtime.py, but this keeps the module
        # graph acyclic and obvious regardless.
        from .agent import SophieAgent

        profile = get_profile(key)
        model_name = (
            overrides.get("model_name")
            or self.force_model
            or profile.model_name
            or self.config.default_model_name
        )
        raw_provider = (
            overrides.get("provider") or self.force_provider or profile.provider or self.config.default_provider
        )
        provider = provider_from_str(raw_provider)

        cache_key = (key, model_name, provider.value, verbose)
        if cache_key not in self._agents:
            self._agents[cache_key] = SophieAgent(
                default_context=self.root_context(),
                toolkits=self.build_toolkits(profile),
                model_name=model_name,
                provider=provider.value,
                config=self.config,
                system_prompt=profile.system_prompt,
                answer_model=profile.answer_model,
                name=profile.key,
                max_iterations=profile.max_iterations,
                fallback_models=profile.fallback_models,
                verbose=verbose,
            )
        return self._agents[cache_key]
