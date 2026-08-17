"""Sophie Agent — Modular multi-profile tool-calling agent platform for financial research."""

import warnings

# LangGraph's internal config model types its `context` field as optional-None, so handing
# create_agent any custom context_schema type makes pydantic emit a serializer warning on every
# single model call and tool call. Verified to be inherent to context_schema in langgraph 1.2.x
# (reproduces with a two-field str dataclass, nothing to do with SophieContext's contents) and
# purely cosmetic — the context object arrives intact, which the offline agent-wiring tests assert.
# Filtered narrowly by message and category so no other pydantic warning is hidden.
# (?s) so `.` spans newlines: filterwarnings match()es from the start of the message, and pydantic
# puts the "Pydantic serializer warnings:" header on its own line before the detail.
warnings.filterwarnings(
    "ignore",
    message=r"(?s).*PydanticSerializationUnexpectedValue.*field_name='context'.*",
    category=UserWarning,
)

# Core orchestrators and configurations
from .core.agent import SophieAgent, ToolAction, tool_trajectory
from .core.callbacks import UsageCallbackHandler
from .core.config import DEFAULT_CONFIG, AgentConfig
from .core.models import ToolCallingNotSupportedError, build_chat_model, provider_from_str
from .core.profiles import AGENT_PROFILES, AgentProfile, get_profile
from .core.runtime import AgentRuntime
from .core.schemas import Citation, OptionLeg, StrategyRecommendation

# Context, state, and stores
from .context.agent_context import SophieContext
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
    "ToolAction",
    "tool_trajectory",
    "ToolCallingNotSupportedError",
    "build_chat_model",
    "provider_from_str",
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
    "SophieContext",
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
