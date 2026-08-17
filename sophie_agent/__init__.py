"""Sophie Agent — Modular multi-profile tool-calling agent platform for financial research."""

# Core orchestrators and configurations
from .core.agent import SophieAgent, ToolCallingNotSupportedError
from .core.callbacks import UsageCallbackHandler
from .core.config import DEFAULT_CONFIG, AgentConfig
from .core.profiles import AGENT_PROFILES, AgentProfile, get_profile
from .core.runtime import AgentRuntime
from .core.schemas import Citation, OptionLeg, StrategyRecommendation

# Context, state, and stores
from .context.cache import SqliteCache
from .context.run_record import write_run_record
from .context.runcontext import BudgetExceededError, RunContext, UsageTracker
from .context.store import DataFrameStore
from .context.wiki_store import WikiPage, WikiStore

# Server and protocol mappers
from .server.ag_ui_mapper import stream_agui_events
from .server.server import app

# Toolkits
from .toolkits import TOOLKIT_REGISTRY, SophieToolkit

__all__ = [
    # Core
    "SophieAgent",
    "ToolCallingNotSupportedError",
    "UsageCallbackHandler",
    "AgentConfig",
    "DEFAULT_CONFIG",
    "AgentProfile",
    "AGENT_PROFILES",
    "get_profile",
    "AgentRuntime",
    "Citation",
    "OptionLeg",
    "StrategyRecommendation",
    # Context
    "RunContext",
    "UsageTracker",
    "BudgetExceededError",
    "DataFrameStore",
    "WikiStore",
    "WikiPage",
    "SqliteCache",
    "write_run_record",
    # Server
    "app",
    "stream_agui_events",
    # Toolkits
    "SophieToolkit",
    "TOOLKIT_REGISTRY",
]
