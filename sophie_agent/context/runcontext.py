"""RunContext — the point-in-time and budget spine threaded through every toolkit and agent.

The model never controls `as_of` directly: it is injected state the agent only *sees* described in
its system prompt, not a tool argument. That is what keeps research free of look-ahead bias.
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone


class BudgetExceededError(RuntimeError):
    """Raised when a run's token budget is exhausted mid-tree."""


@dataclass
class UsageTracker:
    """Thread-safe token/cost accumulator shared across an entire delegation tree."""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    calls: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def add(self, prompt_tokens: int, completion_tokens: int) -> None:
        with self._lock:
            self.prompt_tokens += prompt_tokens
            self.completion_tokens += completion_tokens
            self.total_tokens += prompt_tokens + completion_tokens
            self.calls += 1

    def as_dict(self) -> dict:
        with self._lock:
            return {
                "prompt_tokens": self.prompt_tokens,
                "completion_tokens": self.completion_tokens,
                "total_tokens": self.total_tokens,
                "calls": self.calls,
            }


@dataclass
class RunContext:
    """Carried by reference into every toolkit and every spawned agent in a run."""

    as_of: date | None = None
    depth: int = 0
    max_depth: int = 2
    token_budget: int | None = None
    usage: UsageTracker = field(default_factory=UsageTracker)
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def is_live(self) -> bool:
        """True when no as_of is set — tools may hit live/latest-only sources."""
        return self.as_of is None

    def as_of_iso(self) -> str | None:
        return self.as_of.isoformat() if self.as_of else None

    def check_budget(self) -> None:
        if self.token_budget is not None and self.usage.total_tokens >= self.token_budget:
            raise BudgetExceededError(
                f"Run {self.run_id} exceeded its token budget of {self.token_budget} "
                f"(used {self.usage.total_tokens})."
            )

    def child(self) -> "RunContext":
        """A context for a delegated sub-agent: same run_id/usage/budget, deeper depth."""
        if self.depth >= self.max_depth:
            raise RuntimeError(
                f"Delegation depth limit ({self.max_depth}) reached — cannot spawn a further sub-agent."
            )
        return RunContext(
            as_of=self.as_of,
            depth=self.depth + 1,
            max_depth=self.max_depth,
            token_budget=self.token_budget,
            usage=self.usage,  # shared accumulator, not copied
            run_id=self.run_id,
            started_at=self.started_at,
        )

    def prompt_fragment(self) -> str:
        if self.as_of is None:
            return "Point-in-time mode: OFF (live). Live/latest-only data sources are permitted."
        return (
            f"Point-in-time mode: ON. as_of = {self.as_of.isoformat()}. You must never use, cite, or "
            f"reason about data dated after this. Live-only tools (real-time chain, GraphQL) are "
            f"disabled in this mode; historical tools automatically clamp to this date."
        )
