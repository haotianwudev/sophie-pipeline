"""Run records — one JSON file per agent turn, mirroring the config_hash reproducibility culture
already in sophie-option-research (see docs/SOPHIE_AGENT.md). Any recommendation can be replayed
and audited: which agent, which model, what as_of, which tools fired with what arguments, and the
final answer."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def write_run_record(
    runs_dir: Path,
    run_id: str,
    agent_name: str,
    model_name: str,
    provider: str,
    as_of: str | None,
    message: str,
    output: str,
    intermediate_steps: list[tuple[Any, Any]],
    usage: dict,
) -> Path:
    runs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    path = runs_dir / f"{run_id}_{timestamp}.json"

    tool_trace = [
        {"tool": action.tool, "input": _jsonable(action.tool_input), "observation": _truncate(str(observation))}
        for action, observation in intermediate_steps
    ]

    record = {
        "run_id": run_id,
        "timestamp": timestamp,
        "agent": agent_name,
        "model": model_name,
        "provider": provider,
        "as_of": as_of,
        "input": message,
        "output": output,
        "tool_trace": tool_trace,
        "usage": usage,
    }
    path.write_text(json.dumps(record, indent=2, default=str), encoding="utf-8")
    return path


def _jsonable(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        return str(value)


def _truncate(text: str, limit: int = 2000) -> str:
    return text if len(text) <= limit else text[:limit] + "... (truncated)"
