"""Maps SophieAgent.stream()'s astream_events(version="v2") output to AG-UI protocol events.

Measured against a real run (see docs/SOPHIE_AGENT.md) rather than assumed: on Ollama, tool call
args arrive whole at `on_chat_model_end`, never incrementally via `tool_call_chunks` during
`on_chat_model_stream` — so this mapper drives tool-call events off `on_tool_start`/`on_tool_end`
(which always carry the fully-parsed args/output) rather than trying to reconstruct partial JSON
from streamed chunks. That also makes the mapper provider-agnostic: it doesn't matter whether a
given provider streams tool-call deltas or not.

Text streaming (the final answer) drives off `on_chat_model_stream` chunks with non-empty
`.content`, bracketed by TextMessageStart on the first non-empty chunk for a given LLM-call run_id
and TextMessageEnd at that run's `on_chat_model_end` — which correctly produces zero text events for
an LLM call that turns out to be a tool call (content stays empty for those).

Pure function of (agent, message) -> event stream; no FastAPI/network here, so it's directly unit
testable against recorded event fixtures.
"""

from __future__ import annotations

import json
import uuid
from typing import Any, AsyncIterator

from ag_ui.core import (
    BaseEvent,
    EventType,
    RunErrorEvent,
    RunFinishedEvent,
    RunStartedEvent,
    TextMessageContentEvent,
    TextMessageEndEvent,
    TextMessageStartEvent,
    ToolCallArgsEvent,
    ToolCallEndEvent,
    ToolCallResultEvent,
    ToolCallStartEvent,
)


async def stream_agui_events(
    agent: Any, message: str, thread_id: str, run_id: str
) -> AsyncIterator[BaseEvent]:
    yield RunStartedEvent(type=EventType.RUN_STARTED, thread_id=thread_id, run_id=run_id)

    top_run_id: str | None = None
    active_text_message_ids: dict[str, str] = {}  # langchain run_id -> agui message_id

    try:
        async for event in agent.stream(message):
            name = event.get("event")
            data = event.get("data", {}) or {}
            lc_run_id = event.get("run_id")

            if top_run_id is None:
                top_run_id = lc_run_id

            if name == "on_chat_model_stream":
                chunk = data.get("chunk")
                content = getattr(chunk, "content", None)
                if content:
                    if lc_run_id not in active_text_message_ids:
                        message_id = str(uuid.uuid4())
                        active_text_message_ids[lc_run_id] = message_id
                        yield TextMessageStartEvent(
                            type=EventType.TEXT_MESSAGE_START, message_id=message_id, role="assistant"
                        )
                    yield TextMessageContentEvent(
                        type=EventType.TEXT_MESSAGE_CONTENT,
                        message_id=active_text_message_ids[lc_run_id],
                        delta=content,
                    )

            elif name == "on_chat_model_end":
                message_id = active_text_message_ids.pop(lc_run_id, None)
                if message_id:
                    yield TextMessageEndEvent(type=EventType.TEXT_MESSAGE_END, message_id=message_id)

            elif name == "on_tool_start":
                tool_call_id = lc_run_id
                tool_name = event.get("name", "unknown_tool")
                args = data.get("input", {})
                yield ToolCallStartEvent(
                    type=EventType.TOOL_CALL_START, tool_call_id=tool_call_id, tool_call_name=tool_name
                )
                yield ToolCallArgsEvent(
                    type=EventType.TOOL_CALL_ARGS, tool_call_id=tool_call_id,
                    delta=json.dumps(args, default=str),
                )
                yield ToolCallEndEvent(type=EventType.TOOL_CALL_END, tool_call_id=tool_call_id)

            elif name == "on_tool_end":
                tool_call_id = lc_run_id
                output = data.get("output")
                content = output if isinstance(output, str) else str(output)
                yield ToolCallResultEvent(
                    type=EventType.TOOL_CALL_RESULT,
                    message_id=str(uuid.uuid4()),
                    tool_call_id=tool_call_id,
                    content=content,
                    role="tool",
                )

            elif name == "on_chain_end" and lc_run_id == top_run_id:
                # The top-level AgentExecutor run has finished. Anything after this (there
                # shouldn't be anything, astream_events ends the iterator here) is ignored.
                yield RunFinishedEvent(type=EventType.RUN_FINISHED, thread_id=thread_id, run_id=run_id)
                return

        # Stream ended without an on_chain_end for the top run — still close the run cleanly.
        yield RunFinishedEvent(type=EventType.RUN_FINISHED, thread_id=thread_id, run_id=run_id)

    except Exception as exc:  # noqa: BLE001 - deliberately broad: any failure must close the run
        yield RunErrorEvent(type=EventType.RUN_ERROR, message=str(exc), code=type(exc).__name__)
