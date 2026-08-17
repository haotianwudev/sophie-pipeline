"""Offline tests for the AG-UI layer added in Phase 2 — see docs/SOPHIE_AGENT.md.

The event mapper is tested as a pure function against a recorded fixture (shaped exactly like the
real astream_events(version="v2") output captured from a live Ollama run — see that doc's Phase 2
section) so no network or LLM call is needed. The tool-envelope tests confirm the JSON `{text, ui}`
shape without touching the DB/Cboe network paths those tools would otherwise need.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import pytest
import test_utils  # noqa: F401

from sophie_agent.server.ag_ui_mapper import stream_agui_events
from sophie_agent.toolkits.ui_envelope import ui_envelope

TOP_RUN_ID = "top-run"


@dataclass
class _Chunk:
    content: str = ""


def _event(event: str, run_id: str, name: str = "", data: dict | None = None) -> dict:
    return {"event": event, "run_id": run_id, "name": name, "data": data or {}}


async def _fake_events(events: list[dict]):
    for e in events:
        yield e


@dataclass
class _FakeAgent:
    events: list[dict] = field(default_factory=list)
    calls: list[dict] = field(default_factory=list)

    async def stream(self, message: str, **kwargs):
        # Record the kwargs so a test can assert the mapper forwards context/thread_id/seed_history.
        self.calls.append({"message": message, **kwargs})
        async for e in _fake_events(self.events):
            yield e


def _tool_call_fixture() -> list[dict]:
    """Mirrors a real run: the LangGraph agent starts, the LLM calls wiki_search (no streamed
    content, per the measured Ollama behavior), the tool runs, then the LLM streams a final text
    answer."""
    return [
        _event("on_chain_start", TOP_RUN_ID, "LangGraph"),
        _event("on_chat_model_start", "llm-1"),
        _event("on_chat_model_stream", "llm-1", data={"chunk": _Chunk("")}),
        _event("on_chat_model_end", "llm-1"),
        _event("on_tool_start", "tool-1", "wiki_search", data={"input": {"query": "gamma exposure"}}),
        _event("on_tool_end", "tool-1", "wiki_search", data={"output": "- option-strategy/gex — GEX"}),
        _event("on_chat_model_start", "llm-2"),
        _event("on_chat_model_stream", "llm-2", data={"chunk": _Chunk("Hello")}),
        _event("on_chat_model_stream", "llm-2", data={"chunk": _Chunk(" world")}),
        _event("on_chat_model_stream", "llm-2", data={"chunk": _Chunk("!")}),
        _event("on_chat_model_end", "llm-2"),
        _event("on_chain_end", TOP_RUN_ID, "LangGraph"),
    ]


class TestAgUiMapper:
    @pytest.mark.asyncio
    async def test_event_sequence_and_types(self):
        agent = _FakeAgent(events=_tool_call_fixture())
        events = [e async for e in stream_agui_events(agent, "what is GEX?", "thread-1", "run-1")]
        types = [type(e).__name__ for e in events]

        assert types[0] == "RunStartedEvent"
        assert types[-1] == "RunFinishedEvent"
        assert "ToolCallStartEvent" in types
        assert "ToolCallArgsEvent" in types
        assert "ToolCallEndEvent" in types
        assert "ToolCallResultEvent" in types
        assert "TextMessageStartEvent" in types
        assert "TextMessageEndEvent" in types
        # Exactly one text message started (the tool-call turn streamed no content, so it must not
        # produce a spurious TextMessageStart/End pair).
        assert types.count("TextMessageStartEvent") == 1
        assert types.count("TextMessageEndEvent") == 1
        assert types.count("TextMessageContentEvent") == 3

    @pytest.mark.asyncio
    async def test_tool_call_args_are_json_encoded_input(self):
        agent = _FakeAgent(events=_tool_call_fixture())
        events = [e async for e in stream_agui_events(agent, "q", "t", "r")]
        args_event = next(e for e in events if type(e).__name__ == "ToolCallArgsEvent")
        assert json.loads(args_event.delta) == {"query": "gamma exposure"}

    @pytest.mark.asyncio
    async def test_tool_call_result_carries_tool_output(self):
        agent = _FakeAgent(events=_tool_call_fixture())
        events = [e async for e in stream_agui_events(agent, "q", "t", "r")]
        result_event = next(e for e in events if type(e).__name__ == "ToolCallResultEvent")
        assert "option-strategy/gex" in result_event.content

    @pytest.mark.asyncio
    async def test_text_content_deltas_concatenate_to_full_answer(self):
        agent = _FakeAgent(events=_tool_call_fixture())
        events = [e async for e in stream_agui_events(agent, "q", "t", "r")]
        content_events = [e for e in events if type(e).__name__ == "TextMessageContentEvent"]
        assert "".join(e.delta for e in content_events) == "Hello world!"
        # Start/content/end must all share one message_id.
        start = next(e for e in events if type(e).__name__ == "TextMessageStartEvent")
        end = next(e for e in events if type(e).__name__ == "TextMessageEndEvent")
        assert start.message_id == end.message_id
        assert all(e.message_id == start.message_id for e in content_events)

    @pytest.mark.asyncio
    async def test_run_started_and_finished_carry_thread_and_run_id(self):
        agent = _FakeAgent(events=_tool_call_fixture())
        events = [e async for e in stream_agui_events(agent, "q", "thread-42", "run-99")]
        started = events[0]
        finished = events[-1]
        assert started.thread_id == "thread-42" and started.run_id == "run-99"
        assert finished.thread_id == "thread-42" and finished.run_id == "run-99"

    @pytest.mark.asyncio
    async def test_exception_yields_run_error_event(self):
        class _BrokenAgent:
            async def stream(self, message: str, **kwargs):
                raise RuntimeError("boom")
                yield  # pragma: no cover - makes this an async generator

        events = [e async for e in stream_agui_events(_BrokenAgent(), "q", "t", "r")]
        assert type(events[-1]).__name__ == "RunErrorEvent"
        assert "boom" in events[-1].message

    @pytest.mark.asyncio
    async def test_run_finished_reports_token_usage(self):
        """AG-UI has a first-class `usage` field on RunFinished; the agent already accumulates exact
        counts, so dropping them left the UI unable to show run cost."""
        import test_utils  # noqa: F401
        from sophie_agent import DEFAULT_CONFIG, DataFrameStore, RunContext, SophieContext

        ctx = SophieContext(run_ctx=RunContext(), store=DataFrameStore(), config=DEFAULT_CONFIG)
        ctx.run_ctx.usage.add(120, 30)

        class _AgentWithModel(_FakeAgent):
            model_name = "deepseek-chat"
            provider_name = "DeepSeek"

        agent = _AgentWithModel(events=_tool_call_fixture())
        events = [e async for e in stream_agui_events(agent, "q", "t", "r", context=ctx)]
        finished = events[-1]
        assert type(finished).__name__ == "RunFinishedEvent"
        assert finished.usage is not None
        usage = finished.usage[0]
        assert (usage.input_tokens, usage.output_tokens, usage.total_tokens) == (120, 30, 150)
        assert usage.model == "deepseek-chat"

    @pytest.mark.asyncio
    async def test_usage_omitted_when_no_llm_calls_were_made(self):
        agent = _FakeAgent(events=_tool_call_fixture())
        events = [e async for e in stream_agui_events(agent, "q", "t", "r", context=None)]
        assert events[-1].usage is None

    @pytest.mark.asyncio
    async def test_forwards_thread_context_and_seed_history_to_the_agent(self):
        """The AG-UI thread_id must reach the agent, since it doubles as the checkpointer thread —
        otherwise every turn would start a fresh conversation."""
        agent = _FakeAgent(events=_tool_call_fixture())
        sentinel_ctx = object()
        seed = ["prior-message"]
        _ = [
            e
            async for e in stream_agui_events(
                agent, "q", "thread-7", "run-7", context=sentinel_ctx, seed_history=seed
            )
        ]
        assert agent.calls[0]["thread_id"] == "thread-7"
        assert agent.calls[0]["context"] is sentinel_ctx
        assert agent.calls[0]["seed_history"] == seed

    @pytest.mark.asyncio
    async def test_pure_text_run_no_tool_calls(self):
        """A run with no tool call at all must still produce a clean text message + RunFinished."""
        events_in = [
            _event("on_chain_start", TOP_RUN_ID, "LangGraph"),
            _event("on_chat_model_start", "llm-1"),
            _event("on_chat_model_stream", "llm-1", data={"chunk": _Chunk("Hi")}),
            _event("on_chat_model_end", "llm-1"),
            _event("on_chain_end", TOP_RUN_ID, "LangGraph"),
        ]
        agent = _FakeAgent(events=events_in)
        events = [e async for e in stream_agui_events(agent, "q", "t", "r")]
        types = [type(e).__name__ for e in events]
        assert types == [
            "RunStartedEvent", "TextMessageStartEvent", "TextMessageContentEvent",
            "TextMessageEndEvent", "RunFinishedEvent",
        ]


class TestUiEnvelope:
    def test_shape(self):
        raw = ui_envelope("some text", "strategy_legs", legs=[1, 2], metrics={"a": 1})
        parsed = json.loads(raw)
        assert parsed["text"] == "some text"
        assert parsed["ui"]["component"] == "strategy_legs"
        assert parsed["ui"]["legs"] == [1, 2]
        assert parsed["ui"]["metrics"] == {"a": 1}

    def test_round_trips_non_jsonable_defaults_to_str(self):
        class Weird:
            def __str__(self):
                return "weird!"

        raw = ui_envelope("t", "c", value=Weird())
        parsed = json.loads(raw)
        assert parsed["ui"]["value"] == "weird!"
