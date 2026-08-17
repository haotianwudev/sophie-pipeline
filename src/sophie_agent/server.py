"""AG-UI protocol FastAPI server over SophieAgent — LOCAL-ONLY, never deployed.

Binds 127.0.0.1 (see sophie-agent/serve.py). All five toolkits are enabled, including df_python
(arbitrary Python execution) and raw SQL — this is safe only because the service never leaves your
machine. See docs/SOPHIE_AGENT.md's Phase 2 section for the full design and why there is
deliberately no hosted/production path here.
"""

from __future__ import annotations

from collections import OrderedDict

from ag_ui.core import AssistantMessage, RunAgentInput, UserMessage
from ag_ui.encoder import EventEncoder
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from langchain_core.messages import AIMessage, HumanMessage

from .ag_ui_mapper import stream_agui_events
from .profiles import AGENT_PROFILES
from .runtime import AgentRuntime

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
            converted.append(HumanMessage(content=m.content or ""))
        elif isinstance(m, AssistantMessage):
            if m.content:
                converted.append(AIMessage(content=m.content))
        # SystemMessage, ToolMessage, DeveloperMessage, etc.: intentionally skipped.
    return converted


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/agents")
def list_agents() -> list[dict]:
    return [
        {
            "key": key,
            "display_name": p.display_name,
            "description": p.description,
            "toolkits": list(p.toolkits),
            "can_delegate": p.can_delegate,
        }
        for key, p in AGENT_PROFILES.items()
    ]


@app.post("/agent/{profile}")
async def run_agent(profile: str, input_data: RunAgentInput, request: Request) -> StreamingResponse:
    if profile not in AGENT_PROFILES:
        raise HTTPException(status_code=404, detail=f"Unknown profile '{profile}'. See GET /agents.")
    if not input_data.messages:
        raise HTTPException(status_code=400, detail="messages must be non-empty.")

    encoder = EventEncoder(accept=request.headers.get("accept"))
    runtime = _get_runtime(input_data.thread_id)
    agent = runtime.build_agent(profile)
    agent.chat_history = _convert_history(input_data.messages[:-1])

    last = input_data.messages[-1]
    message_text = last.content or ""

    async def event_generator():
        async for agui_event in stream_agui_events(agent, message_text, input_data.thread_id, input_data.run_id):
            yield encoder.encode(agui_event)

    return StreamingResponse(event_generator(), media_type="text/event-stream")
