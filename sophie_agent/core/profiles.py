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
    # False keeps a profile usable from the CLI/tests/evals (AGENT_PROFILES[key] still resolves)
    # while removing it from list_agents()/delegate()'s supervisor-facing set — see
    # toolkits/delegate/toolkit.py. Distinct from can_delegate: this is about being a target, not
    # about being able to delegate.
    delegatable: bool = True
    # Models to fall back to, in order, when this profile's primary model call fails — wired to
    # ModelFallbackMiddleware. Empty means no fallback (the default; a failure surfaces as an
    # error). Useful for e.g. surviving an expired remote API key by dropping to a local Ollama
    # model: fallback_models=("qwen3.5:latest",).
    fallback_models: tuple[str, ...] = ()
    # Single emoji shown as this agent's persona in the chat widget whenever SOPHIE (the
    # supervisor) delegates to it — see GET /agents in server.py and docs/SOPHIE_AGENT.md's
    # "Persona-per-delegation" section. Purely cosmetic; never read by any agent/toolkit code.
    persona_icon: str = "🤖"


# Trimmed to the agents actually in active use (trade-suggestion + payoff-diagram flow) — not
# deleted, just not registered, since they're not referenced by key anywhere. Re-enable by moving
# an entry back into AGENT_PROFILES below.
#
# "wiki_researcher": AgentProfile(
#     key="wiki_researcher",
#     display_name="Wiki Researcher",
#     description="Answers from Sophie's own published wiki material; cites the page path for every claim.",
#     toolkits=("wiki",),
#     model_name="qwen3.5:latest",
#     provider=ModelProvider.OLLAMA,
#     system_prompt=(
#         "You are Sophie's wiki researcher. Answer strictly from wiki_search / wiki_get_page "
#         "results. Always cite the page `path` for any claim. If the wiki doesn't cover "
#         "something, say so plainly rather than guessing."
#     ),
#     persona_icon="📚",
# ),
#
# "market_analyst": AgentProfile(
#     key="market_analyst",
#     display_name="Market Analyst",
#     description="Regime and single-name questions: investment clock, technicals, fundamentals.",
#     toolkits=("market", "wiki"),
#     model_name="deepseek-chat",
#     provider=ModelProvider.DEEPSEEK,
#     system_prompt=(
#         "You are Sophie's market analyst. Answer regime and single-name questions using "
#         "GraphQL/SQL for current data and the wiki for methodology context."
#     ),
#     persona_icon="🌐",
# ),

AGENT_PROFILES: dict[str, AgentProfile] = {
    "option_strategist": AgentProfile(
        key="option_strategist",
        display_name="Option Strategist",
        description="Selects and prices option strategy legs against real chains.",
        toolkits=("options", "dataframe", "wiki_options"),
        system_prompt=(
            "You are Sophie's option strategist. Resolve and price strategies with build_strategy "
            "— never compute payoff numbers yourself. Always state whether a preset's "
            "deltas are backtest-derived (only iron_condor is, per sophie-option-research's real "
            "iron_condor_45dte.yaml) or conventional retail defaults — you don't have a tool to "
            "pull the underlying backtest numbers right now, so state the distinction plainly "
            "without claiming to cite specific metrics. When you call "
            "build_strategy, the legs also appear as a suggestion banner in the user's SPX Payoff "
            "Builder if they have it open — mention this in your answer ('I've set this up in your "
            "Payoff Builder — click Apply to load it') rather than only describing the legs in text. "
            "Use option_wiki_search to cite Sophie's own strategy/mechanics explanations when they "
            "add real context — it only covers option-strategy content, not market/macro wiki pages."
        ),
        answer_model=StrategyRecommendation,
        persona_icon="📈",
    ),
    "quant": AgentProfile(
        key="quant",
        display_name="Quant",
        description="Runs SQL/GraphQL and pandas analysis over market data and backtest results.",
        toolkits=("market", "dataframe"),
        system_prompt=(
            "You are Sophie's quant. Use sql_query/graphql_query to pull data and the DataFrame "
            "toolkit to analyze it. Always inspect a query's shape/schema before drawing "
            "conclusions from it."
        ),
        persona_icon="🧮",
        # Kept live (unlike wiki_researcher/market_analyst above, which are fully commented out)
        # because test_sophie_agent.py and test_tool_schemas.py reference AGENT_PROFILES["quant"]
        # directly — commenting out the entry would break those tests, not just this feature.
        delegatable=False,
    ),
    "generalist": AgentProfile(
        key="generalist",
        display_name="Generalist",
        description="Default single-agent assistant with access to every data toolkit.",
        # "strategy" dropped: build_strategy/list_strategy_presets are now also exposed via
        # "options" (see toolkits/options/toolkit.py), and that toolkit's other two tools
        # (compare_strategy_variants, strategy_backtest_evidence) are currently withheld from
        # get_tools() anyway — including "strategy" here would just duplicate tool names.
        toolkits=("wiki", "options", "dataframe", "market"),
        system_prompt=(
            "You are Sophie, a research assistant over the Sophie finance platform's wiki, market "
            "data, and option chains. Prefer tools over recollection for anything factual or "
            "numeric."
        ),
        # CLI/direct-use only (see run.py's --agent default) — not offered as a delegate target,
        # since it's a strict toolkit superset of the real specialists and the chat widget never
        # reaches it (supervisor is always the entry point there). Keeping it out of
        # list_agents() is what "only the key agents" for delegation actually means in code.
        delegatable=False,
    ),
    "supervisor": AgentProfile(
        key="supervisor",
        display_name="Sophie",
        description="Plans, fans work out to specialist agents, and synthesises their answers.",
        toolkits=("delegate", "wiki"),
        system_prompt=(
            "You are Sophie's supervisor. Call list_agents() to see which specialists are "
            "currently available for delegation — the roster is intentionally trimmed right now, "
            "so don't assume specialists mentioned in older context (market/regime analysis, wiki "
            "research) still exist; check first.\n\n"
            "For a trade-suggestion request ('build me a position', 'suggest an iron condor'), "
            "delegate to option_strategist — it can size, price, and justify legs on its own via "
            "strategy_backtest_evidence, and its build_strategy calls also populate the user's SPX "
            "Payoff Builder directly. For anything else you can answer yourself using your own wiki "
            "tools rather than delegating. Synthesize any specialist's answer into your own "
            "response; never just relay it verbatim, and preserve any backtested-vs-conventional "
            "distinction it reports."
        ),
        can_delegate=True,
        persona_icon="✨",
    ),
}


def get_profile(key: str) -> AgentProfile:
    if key not in AGENT_PROFILES:
        raise KeyError(f"Unknown agent profile '{key}'. Known profiles: {list(AGENT_PROFILES)}")
    return AGENT_PROFILES[key]
