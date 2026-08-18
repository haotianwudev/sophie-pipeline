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
    StateSnapshotEvent,
    TextMessageContentEvent,
    TextMessageEndEvent,
    TextMessageStartEvent,
    TokenUsage,
    ToolCallArgsEvent,
    ToolCallEndEvent,
    ToolCallResultEvent,
    ToolCallStartEvent,
)


def _usage(agent: Any, context: Any) -> list[TokenUsage] | None:
    """AG-UI's RunFinished/RunError events carry an optional `usage` list, and the agent already
    accumulates exact per-run token counts in RunContext.usage (via UsageCallbackHandler). Reporting
    it costs nothing and is the protocol's own field for it — previously it was tracked and then
    dropped, so the UI had no way to show run cost."""
    if context is None:
        return None
    try:
        totals = context.run_ctx.usage.as_dict()
    except AttributeError:
        return None
    if not totals.get("calls"):
        return None
    return [
        TokenUsage(
            provider=getattr(agent, "provider_name", None),
            model=getattr(agent, "model_name", None),
            input_tokens=totals.get("prompt_tokens"),
            output_tokens=totals.get("completion_tokens"),
            total_tokens=totals.get("total_tokens"),
        )
    ]


async def stream_agui_events(
    agent: Any,
    message: str,
    thread_id: str,
    run_id: str,
    context: Any = None,
    seed_history: list | None = None,
) -> AsyncIterator[BaseEvent]:
    """`thread_id` doubles as the agent's checkpointer thread, so an AG-UI thread and the agent's
    conversation state stay keyed the same way. `context` is the run's SophieContext; `seed_history`
    is only non-empty when adopting a transcript for a thread with no checkpoint yet."""
    yield RunStartedEvent(type=EventType.RUN_STARTED, thread_id=thread_id, run_id=run_id)

    top_run_id: str | None = None
    active_text_message_ids: dict[str, str] = {}  # langchain run_id -> agui message_id

    try:
        async for event in agent.stream(
            message, context=context, thread_id=thread_id, seed_history=seed_history
        ):
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
                tool_name = event.get("name", "unknown_tool")
                output = data.get("output")
                content = output if isinstance(output, str) else str(output)
                yield ToolCallResultEvent(
                    type=EventType.TOOL_CALL_RESULT,
                    message_id=str(uuid.uuid4()),
                    tool_call_id=tool_call_id,
                    content=content,
                    role="tool",
                )

                # Mirror build_strategy's result into AG-UI shared state so the frontend's SPX
                # Payoff Builder (a completely separate React subtree from the chat) can offer to
                # load it, via PayoffBridgeProvider — see client/src/hooks/use-payoff-bridge.tsx.
                # ui_envelope() always returns valid JSON (json.dumps, not a bare dict repr), so a
                # parse failure here just means "not an envelope-shaped tool," not a real error.
                if tool_name == "build_strategy":
                    try:
                        ui = json.loads(content).get("ui")
                    except (json.JSONDecodeError, AttributeError):
                        ui = None
                    if isinstance(ui, dict) and ui.get("component") == "strategy_legs":
                        yield StateSnapshotEvent(
                            type=EventType.STATE_SNAPSHOT,
                            snapshot={
                                "payoffBuilder": {
                                    "legs": ui.get("legs", []),
                                    "presetId": ui.get("preset_id"),
                                    "expiration": ui.get("expiration"),
                                    "dte": ui.get("dte"),
                                    "backtested": ui.get("backtested"),
                                }
                            },
                        )

            elif name == "on_chain_end" and lc_run_id == top_run_id:
                # The top-level graph run has finished. Anything after this (there shouldn't be
                # anything, astream_events ends the iterator here) is ignored.
                yield RunFinishedEvent(
                    type=EventType.RUN_FINISHED,
                    thread_id=thread_id,
                    run_id=run_id,
                    usage=_usage(agent, context),
                )
                return

        # Stream ended without an on_chain_end for the top run — still close the run cleanly.
        yield RunFinishedEvent(
            type=EventType.RUN_FINISHED,
            thread_id=thread_id,
            run_id=run_id,
            usage=_usage(agent, context),
        )

    except Exception as exc:  # noqa: BLE001 - deliberately broad: any failure must close the run
        yield RunErrorEvent(
            type=EventType.RUN_ERROR,
            message=str(exc),
            code=type(exc).__name__,
            usage=_usage(agent, context),
        )
