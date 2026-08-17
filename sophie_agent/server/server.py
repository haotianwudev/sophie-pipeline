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


@app.get("/models")
def get_models() -> dict:
    """Return live discovered Ollama models alongside configured models."""
    ollama_tags = []
    try:
        resp = requests.get(f"{DEFAULT_CONFIG.ollama_base_url}/api/tags", timeout=2)
        if resp.status_code == 200:
            data = resp.json()
            for m in data.get("models", []):
                name = m.get("name")
                if name:
                    ollama_tags.append(name)
    except Exception:
        pass

    models_list = []
    seen = set()

    for tag in ollama_tags:
        seen.add(tag)
        info = get_model_info(tag)
        models_list.append({
            "name": tag,
            "provider": "ollama",
            "displayName": tag,
            "supportsToolCalling": info.supports_tool_calling() if info else True,
            "isLocal": True,
            "pulled": True,
        })

    for model_name, info in AVAILABLE_MODELS.items():
        if model_name in seen:
            continue
        seen.add(model_name)
        is_ollama = info.provider == ModelProvider.OLLAMA
        models_list.append({
            "name": model_name,
            "provider": info.provider.value.lower(),
            "displayName": info.name,
            "supportsToolCalling": info.supports_tool_calling(),
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
