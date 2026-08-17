"""Shared pytest configuration.

The important thing here is disabling the persistent LLM cache. `AgentRuntime` installs a
process-wide sqlite cache (`logs/sophie_agent_cache.sqlite3`) keyed on (prompt, llm_string); it
persists across processes, which is exactly what makes repeated real research cheap. In tests it is
a correctness hazard: a cached reply is replayed without the model being invoked, so any assertion
about call counts or about a scripted model's response sequence silently depends on what earlier
runs left in that file. Tests were observed changing behavior between two identical invocations
because of it.
"""

from __future__ import annotations

import os

# Set before sophie_agent is imported anywhere, since AgentConfig reads it at class-definition time
# via default_factory and DEFAULT_CONFIG is built at import.
os.environ["SOPHIE_AGENT_LLM_CACHE"] = "0"

import pytest  # noqa: E402
from langchain_core.globals import set_llm_cache  # noqa: E402


@pytest.fixture(autouse=True)
def _no_llm_cache():
    """Belt-and-braces: even if something installed a cache, clear it for every test."""
    set_llm_cache(None)
    yield
    set_llm_cache(None)
