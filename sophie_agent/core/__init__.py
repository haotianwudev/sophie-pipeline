"""Core agent models, profile registry, config, and runtime orchestration."""

from .agent import SophieAgent, ToolAction, tool_trajectory
from .callbacks import UsageCallbackHandler
from .config import DEFAULT_CONFIG, AgentConfig
from .models import ToolCallingNotSupportedError, build_chat_model, provider_from_str
from .profiles import AGENT_PROFILES, AgentProfile, get_profile
from .runtime import AgentRuntime
from .schemas import Citation, OptionLeg, StrategyRecommendation

__all__ = [
    "SophieAgent",
    "ToolAction",
    "tool_trajectory",
    "ToolCallingNotSupportedError",
    "build_chat_model",
    "provider_from_str",
    "UsageCallbackHandler",
    "DEFAULT_CONFIG",
    "AgentConfig",
    "AGENT_PROFILES",
    "AgentProfile",
    "get_profile",
    "AgentRuntime",
    "Citation",
    "OptionLeg",
    "StrategyRecommendation",
]
