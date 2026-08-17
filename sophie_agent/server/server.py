"""AG-UI protocol FastAPI server over SophieAgent — LOCAL-ONLY, never deployed.

Binds 127.0.0.1 (see sophie-agent/serve.py). All five toolkits are enabled, including df_python
(arbitrary Python execution) and raw SQL — this is safe only because the service never leaves your
machine. See docs/SOPHIE_AGENT.md's Phase 2 section for the full design and why there is
deliberately no hosted/production path here.
"""

from __future__ import annotations

from collections import OrderedDict

import requests
from ag_ui.core import AssistantMessage, RunAgentInput, UserMessage
from ag_ui.encoder import EventEncoder
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from langchain_core.messages import AIMessage, HumanMessage

from src.llm.models import AVAILABLE_MODELS, OLLAMA_MODELS, ModelProvider, get_model_info

from ..core.config import DEFAULT_CONFIG
from ..core.profiles import AGENT_PROFILES
from ..core.runtime import AgentRuntime
from .ag_ui_mapper import stream_agui_events

app = FastAPI(title="Sophie Agent (AG-UI)", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# thread_id -> AgentRuntime, so DataFrameStore persists across turns within a thread. Chat history
# is NOT tracked here — AG-UI resends the full message list every run (see _convert_history), so
# tracking it separately would double-track and risk drifting from what the client believes it sent.
_MAX_THREADS = 50
_runtimes: "OrderedDict[str, AgentRuntime]" = OrderedDict()


def _get_runtime(thread_id: str) -> AgentRuntime:
    if thread_id in _runtimes:
        _runtimes.move_to_end(thread_id)
        return _runtimes[thread_id]
    runtime = AgentRuntime()
    _runtimes[thread_id] = runtime
    if len(_runtimes) > _MAX_THREADS:
        _runtimes.popitem(last=False)
    return runtime


def _convert_history(messages: list) -> list:
    """AG-UI history -> LangChain chat_history. System/tool/developer messages are dropped: the
    agent supplies its own system prompt, and tool observations are already folded into the prior
    assistant text the model produced from them."""
    converted = []
    for m in messages:
        if isinstance(m, UserMessage):
            converted.append(HumanMessage(content=m.content))
        elif isinstance(m, AssistantMessage) and m.content:
            converted.append(AIMessage(content=m.content))
    return converted


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "profiles": list(AGENT_PROFILES.keys())}


@app.get("/agents")
def get_agents() -> dict:
    """Persona registry for the chat widget's delegation UI (see docs/SOPHIE_AGENT.md's
    "Persona-per-delegation" section). Excludes can_delegate profiles (supervisors, i.e. SOPHIE
    herself) — those are never a delegate() target, mirroring DelegationToolkit.list_agents()'s
    own filter, so the two stay consistent by construction rather than by convention."""
    return {
        "agents": [
            {
                "key": key,
                "displayName": p.display_name,
                "description": p.description,
                "icon": p.persona_icon,
            }
            for key, p in AGENT_PROFILES.items()
            if not p.can_delegate
        ],
        "supervisor": {
            "key": "supervisor",
            "displayName": AGENT_PROFILES["supervisor"].display_name,
            "icon": AGENT_PROFILES["supervisor"].persona_icon,
        },
    }


# Ollama tags for embedding-only models (e.g. bge-m3, nomic-embed-text) — pulled for the wiki
# toolkit's own use, never chat models. get_model_info() doesn't know about them (they're not in
# AVAILABLE_MODELS/OLLAMA_MODELS), so without this exclusion they default to
# supportsToolCalling=True purely because they're unrecognized, and show up in the chat model
# picker as if they were usable — they're not, they have no chat/completion capability at all.
_EMBEDDING_MODEL_MARKERS = ("bge", "embed", "e5-", "gte-", "minilm")


def _is_embedding_model(name: str) -> bool:
    lowered = name.lower()
    return any(marker in lowered for marker in _EMBEDDING_MODEL_MARKERS)


@app.get("/models")
def get_models() -> dict:
    """Return live discovered Ollama models alongside configured models.

    Only models that actually support tool calling are returned — sophie_agent's toolkits are
    exposed via bind_tools()/create_tool_calling_agent(), so a model without tool-calling support
    (e.g. deepseek-reasoner / any local R1 distill) can't drive this agent at all, and listing it
    would just be a picker entry that silently produces prose-only, tool-less runs.
    """
    ollama_tags = []
    try:
        resp = requests.get(f"{DEFAULT_CONFIG.ollama_base_url}/api/tags", timeout=2)
        if resp.status_code == 200:
            data = resp.json()
            for m in data.get("models", []):
                name = m.get("name")
                if name and not _is_embedding_model(name):
                    ollama_tags.append(name)
    except Exception:
        pass

    models_list = []
    seen = set()

    for tag in ollama_tags:
        seen.add(tag)
        info = get_model_info(tag)
        supports_tools = info.supports_tool_calling() if info else True
        if not supports_tools:
            continue
        models_list.append({
            "name": tag,
            "provider": "ollama",
            "displayName": tag,
            "supportsToolCalling": True,
            "isLocal": True,
            "pulled": True,
        })

    for info in AVAILABLE_MODELS:
        model_name = info.model_name
        if model_name in seen:
            continue
        seen.add(model_name)
        if not info.supports_tool_calling():
            continue
        is_ollama = info.provider == ModelProvider.OLLAMA
        models_list.append({
            "name": model_name,
            "provider": info.provider.value.lower(),
            "displayName": info.display_name,
            "supportsToolCalling": True,
            "isLocal": is_ollama,
            "pulled": not is_ollama,
        })

    return {
        "models": models_list,
        "defaultModel": DEFAULT_CONFIG.default_model_name,
        "defaultProvider": DEFAULT_CONFIG.default_provider.lower(),
    }


@app.post("/agent/{profile}")
async def run_agent(
    profile: str,
    request: Request,
    model: str | None = Query(default=None),
    provider: str | None = Query(default=None),
) -> StreamingResponse:
    if profile not in AGENT_PROFILES:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown profile '{profile}'. Known profiles: {list(AGENT_PROFILES.keys())}",
        )

    try:
        body = await request.json()
        input_data = RunAgentInput.model_validate(body)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid AG-UI payload: {exc}") from exc

    runtime = _get_runtime(input_data.thread_id)

    overrides = {}
    if model:
        overrides["model_name"] = model
    if provider:
        overrides["provider"] = provider

    agent = runtime.build_agent(profile, **overrides)

    history = _convert_history(input_data.messages[:-1])
    agent.chat_history = history

    latest = input_data.messages[-1] if input_data.messages else None
    user_text = latest.content if isinstance(latest, UserMessage) else ""

    async def event_generator():
        encoder = EventEncoder()
        async for event in stream_agui_events(
            agent=agent,
            message=user_text,
            thread_id=input_data.thread_id,
            run_id=input_data.run_id,
        ):
            yield encoder.encode(event)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
