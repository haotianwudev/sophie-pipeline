"""AgentProfile registry — single source of truth for named specialist agents, following the same
idiom as src/utils/analysts.py::ANALYST_CONFIG. Adding an agent is a data edit here, not a code
edit anywhere else.
"""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import BaseModel

from src.llm.models import ModelProvider

from .schemas import StrategyRecommendation


@dataclass(frozen=True)
class AgentProfile:
    key: str
    display_name: str
    description: str
    toolkits: tuple[str, ...]
    model_name: str | None = None
    provider: ModelProvider | None = None
    system_prompt: str = ""
    answer_model: type[BaseModel] | None = None
    # Enforced by ModelCallLimitMiddleware(run_limit=...) in SophieAgent. Was silently ignored
    # between the AgentExecutor -> create_agent migration and this change.
    max_iterations: int = 15
    can_delegate: bool = False
    # Models to fall back to, in order, when this profile's primary model call fails — wired to
    # ModelFallbackMiddleware. Empty means no fallback (the default; a failure surfaces as an
    # error). Useful for e.g. surviving an expired remote API key by dropping to a local Ollama
    # model: fallback_models=("qwen3.5:latest",).
    fallback_models: tuple[str, ...] = ()
    # Single emoji shown as this agent's persona in the chat widget whenever SOPHIE (the
    # supervisor) delegates to it — see GET /agents in server.py and docs/SOPHIE_AGENT.md's
    # "Persona-per-delegation" section. Purely cosmetic; never read by any agent/toolkit code.
    persona_icon: str = "🤖"


AGENT_PROFILES: dict[str, AgentProfile] = {
    "wiki_researcher": AgentProfile(
        key="wiki_researcher",
        display_name="Wiki Researcher",
        description="Answers from Sophie's own published wiki material; cites the page path for every claim.",
        toolkits=("wiki",),
        model_name="qwen3.5:latest",
        provider=ModelProvider.OLLAMA,
        system_prompt=(
            "You are Sophie's wiki researcher. Answer strictly from wiki_search / wiki_get_page "
            "results. Always cite the page `path` for any claim. If the wiki doesn't cover "
            "something, say so plainly rather than guessing."
        ),
        persona_icon="📚",
    ),
    "option_strategist": AgentProfile(
        key="option_strategist",
        display_name="Option Strategist",
        description="Selects and prices option strategy legs against real chains, citing backtest evidence when it exists.",
        toolkits=("options", "strategy", "dataframe"),
        model_name="deepseek-chat",
        provider=ModelProvider.DEEPSEEK,
        system_prompt=(
            "You are Sophie's option strategist. Resolve and price strategies with the strategy "
            "toolkit — never compute payoff numbers yourself. Always state whether a preset's "
            "deltas are backtest-derived (only iron_condor is) or conventional defaults, and pull "
            "strategy_backtest_evidence when a recommendation needs justification."
        ),
        answer_model=StrategyRecommendation,
        persona_icon="📈",
    ),
    "quant": AgentProfile(
        key="quant",
        display_name="Quant",
        description="Runs SQL/GraphQL and pandas analysis over market data and backtest results.",
        toolkits=("market", "dataframe"),
        model_name="deepseek-chat",
        provider=ModelProvider.DEEPSEEK,
        system_prompt=(
            "You are Sophie's quant. Use sql_query/graphql_query to pull data and the DataFrame "
            "toolkit to analyze it. Always inspect a query's shape/schema before drawing "
            "conclusions from it."
        ),
        persona_icon="🧮",
    ),
    "market_analyst": AgentProfile(
        key="market_analyst",
        display_name="Market Analyst",
        description="Regime and single-name questions: investment clock, technicals, fundamentals.",
        toolkits=("market", "wiki"),
        model_name="deepseek-chat",
        provider=ModelProvider.DEEPSEEK,
        system_prompt=(
            "You are Sophie's market analyst. Answer regime and single-name questions using "
            "GraphQL/SQL for current data and the wiki for methodology context."
        ),
        persona_icon="🌐",
    ),
    "generalist": AgentProfile(
        key="generalist",
        display_name="Generalist",
        description="Default single-agent assistant with access to every data toolkit.",
        toolkits=("wiki", "options", "strategy", "dataframe", "market"),
        system_prompt=(
            "You are Sophie, a research assistant over the Sophie finance platform's wiki, market "
            "data, and option chains. Prefer tools over recollection for anything factual or "
            "numeric."
        ),
    ),
    "supervisor": AgentProfile(
        key="supervisor",
        display_name="Sophie",
        description="Plans, fans work out to specialist agents, and synthesises their answers.",
        toolkits=("delegate", "wiki"),
        model_name="deepseek-chat",
        provider=ModelProvider.DEEPSEEK,
        system_prompt=(
            "You are Sophie's supervisor. Break a question into independent sub-questions and "
            "delegate them — use delegate_parallel when they don't depend on each other. Synthesize "
            "every specialist's answer into one coherent response; never just relay one verbatim. "
            "Preserve any backtested-vs-conventional distinctions your specialists report."
        ),
        can_delegate=True,
        persona_icon="✨",
    ),
}


def get_profile(key: str) -> AgentProfile:
    if key not in AGENT_PROFILES:
        raise KeyError(f"Unknown agent profile '{key}'. Known profiles: {list(AGENT_PROFILES)}")
    return AGENT_PROFILES[key]
