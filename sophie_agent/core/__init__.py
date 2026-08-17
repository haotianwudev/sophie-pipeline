"""Core agent models, profile registry, config, and runtime orchestration."""

from .agent import SophieAgent, ToolCallingNotSupportedError
from .callbacks import UsageCallbackHandler
from .config import DEFAULT_CONFIG, AgentConfig
from .profiles import AGENT_PROFILES, AgentProfile, get_profile
from .runtime import AgentRuntime
from .schemas import Citation, OptionLeg, StrategyRecommendation

__all__ = [
    "SophieAgent",
    "ToolCallingNotSupportedError",
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
