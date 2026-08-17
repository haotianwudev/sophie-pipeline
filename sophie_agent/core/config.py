"""Single source of truth for every path/URL/knob the agent needs.

All defaults point at the current F:/workspace layout, but every one of them is a plain env
override — no other module in this package should reach into os.environ directly for a path.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# sophie-pipeline's own .env carries ANTHROPIC_API_KEY / DEEPSEEK_API_KEY / DATABASE_URL / etc.
load_dotenv()

_PIPELINE_ROOT = Path(__file__).resolve().parents[2]
_WORKSPACE_ROOT = _PIPELINE_ROOT.parent


def _env_path(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    return Path(value) if value else default


@dataclass(frozen=True)
class AgentConfig:
    # Cross-repo paths
    client_repo: Path = field(
        default_factory=lambda: _env_path("SOPHIE_CLIENT_REPO", _WORKSPACE_ROOT / "ai-stock-suggestion-client")
    )
    option_research_repo: Path = field(
        default_factory=lambda: _env_path("SOPHIE_OPTION_RESEARCH_DIR", _WORKSPACE_ROOT / "sophie-option-research")
    )

    # Remote endpoints
    graphql_url: str = field(
        default_factory=lambda: os.environ.get(
            "SOPHIE_GRAPHQL_URL",
            "https://ai-stock-suggestion-server-282034489414.us-central1.run.app/graphql",
        )
    )

    # Model defaults. DeepSeek, not Anthropic: the ANTHROPIC_API_KEY in this repo's .env is
    # currently invalid (401 on every call — a credentials issue, not a code issue; verified
    # during Phase 2's live testing). Flip SOPHIE_AGENT_MODEL/SOPHIE_AGENT_PROVIDER once fixed.
    default_model_name: str = field(default_factory=lambda: os.environ.get("SOPHIE_AGENT_MODEL", "deepseek-chat"))
    default_provider: str = field(default_factory=lambda: os.environ.get("SOPHIE_AGENT_PROVIDER", "DeepSeek"))

    # Safety / operational knobs
    allow_python: bool = field(
        default_factory=lambda: os.environ.get("SOPHIE_AGENT_ALLOW_PYTHON", "1") not in ("0", "false", "False")
    )
    max_workers: int = field(default_factory=lambda: int(os.environ.get("SOPHIE_AGENT_MAX_WORKERS", "4")))
    as_of: str | None = field(default_factory=lambda: os.environ.get("SOPHIE_AGENT_AS_OF") or None)
    token_budget: int | None = field(
        default_factory=lambda: (
            int(os.environ["SOPHIE_AGENT_TOKEN_BUDGET"]) if os.environ.get("SOPHIE_AGENT_TOKEN_BUDGET") else None
        )
    )
    ollama_base_url: str = field(
        default_factory=lambda: os.environ.get("OLLAMA_BASE_URL", f"http://{os.environ.get('OLLAMA_HOST', 'localhost')}:11434")
    )

    @property
    def wiki_dir(self) -> Path:
        override = os.environ.get("SOPHIE_WIKI_DIR")
        return Path(override) if override else self.client_repo / "public" / "wiki"

    @property
    def wiki_registry_path(self) -> Path:
        return self.client_repo / "src" / "data" / "wiki" / "index.ts"

    @property
    def historical_chain_dir(self) -> Path:
        return self.option_research_repo / "data" / "processed"

    @property
    def cache_db_path(self) -> Path:
        return _PIPELINE_ROOT / "logs" / "sophie_agent_cache.sqlite3"

    @property
    def runs_dir(self) -> Path:
        return _PIPELINE_ROOT / "runs"


DEFAULT_CONFIG = AgentConfig()
