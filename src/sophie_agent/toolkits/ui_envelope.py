"""Shared envelope for tools that can drive a chat UI component (Phase 2 — see docs/SOPHIE_AGENT.md).

Bulk-producing tools (option chains, SQL, df_python) must NEVER use this — they stay
markdown+handle only, per the Phase 1 DataFrameStore discipline. Only small, structurally-final
results (a resolved strategy, a GEX table, wiki search hits) get a `ui` payload.
"""

from __future__ import annotations

import json
from typing import Any


def ui_envelope(text: str, component: str, **ui_fields: Any) -> str:
    """Returns a JSON string `{"text": ..., "ui": {"component": ..., ...}}`.

    `text` is what the LLM reads and reasons over — keep it exactly as informative as the old
    plain-text return value. `ui` is what the frontend's tool-UI registry renders; a component with
    no matching frontend entry just falls back to showing `text`, so this is always additive.
    """
    return json.dumps({"text": text, "ui": {"component": component, **ui_fields}}, default=str)
