"""Offline test suite for src/sophie_agent — no API key or network access required.

Covers: wiki retrieval, the strategy-preset port (anti-drift against the real client sample
chain), point-in-time gating, the DataFrame REPL, the SQL read-only guard, tool-name uniqueness,
delegation guards, typed-output validation, and provider gating. See docs/SOPHIE_AGENT.md.
"""

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
import test_utils  # noqa: F401  (adds project root to sys.path — see test/test_utils.py)

from langchain.tools import ToolRuntime
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from src.llm.models import ModelProvider, get_model, get_model_info
from sophie_agent import (
    DEFAULT_CONFIG,
    AGENT_PROFILES,
    AgentConfig,
    AgentRuntime,
    Citation,
    DataFrameStore,
    OptionLeg,
    RunContext,
    SophieContext,
    StrategyRecommendation,
    ToolCallingNotSupportedError,
    WikiStore,
    build_chat_model,
)
from sophie_agent.options.chain_types import ChainContract, ExpirationChain
from sophie_agent.options.historical import HistoricalChainUnavailable, load_historical_chain
from sophie_agent.options.payoff import find_breakevens, legs_pnl, net_premium
from sophie_agent.options.presets import PRESETS_BY_ID, build_preset_legs
from sophie_agent.toolkits.dataframe import DataFrameToolkit
from sophie_agent.toolkits.delegate.toolkit import _run_one
from sophie_agent.toolkits.market import _guard_and_prepare_sql
from sophie_agent.toolkits.options import OptionChainToolkit

SAMPLE_CHAIN_PATH = (
    Path(__file__).resolve().parents[2]
    / "ai-stock-suggestion-client"
    / "public"
    / "data"
    / "spx-chain-sample.json"
)


def _context(as_of=None, config=None, store=None, runtime=None) -> SophieContext:
    """A SophieContext standing in for what AgentRuntime would supply at invoke time."""
    return SophieContext(
        run_ctx=RunContext(as_of=as_of),
        store=store if store is not None else DataFrameStore(),
        config=config or DEFAULT_CONFIG,
        runtime=runtime,
    )


def _tool_runtime(context: SophieContext) -> ToolRuntime:
    """Build the ToolRuntime that LangGraph would normally inject, so a tool can be unit-tested
    without standing up a graph. Only `context` matters to Sophie's tools; the rest are the
    dataclass's required fields."""
    return ToolRuntime(
        state={"messages": []},
        context=context,
        config={},
        stream_writer=lambda *_: None,
        tool_call_id="test-call",
        store=None,
        tools=[],
        execution_info=None,
        server_info=None,
    )


def _call(tool, context: SophieContext, **kwargs):
    """Invoke a context-dependent tool through its normal validation path."""
    return tool.invoke({"runtime": _tool_runtime(context), **kwargs})


# --------------------------------------------------------------------------------------
# Wiki
# --------------------------------------------------------------------------------------


class TestWiki:
    @pytest.fixture(scope="class")
    def store(self):
        return WikiStore(DEFAULT_CONFIG.wiki_dir, DEFAULT_CONFIG.wiki_registry_path)

    def test_loads_all_pages(self, store):
        assert len(store) >= 200  # 240 at time of writing; tolerant of future additions

    def test_search_ranks_gex_first(self, store):
        results = store.search("gamma exposure")
        assert results
        assert results[0]["path"] == "option-strategy/gex"

    def test_every_page_has_title_and_path(self, store):
        for page in store._pages.values():
            assert page.title
            assert page.path

    def test_form13f_category_normalized(self, store):
        cats = store.list_categories()
        assert "form13f" in cats
        assert "form-13f" not in cats

    def test_get_page_roundtrip(self, store):
        page = store.get_page("option-strategy/gex")
        assert page is not None
        assert "Gamma Exposure" in page.title


# --------------------------------------------------------------------------------------
# Strategy presets (anti-drift against the real client sample chain) + payoff math
# --------------------------------------------------------------------------------------


def _load_sample_expiration(target_dte: int = 45) -> tuple[ExpirationChain, float]:
    raw = json.loads(SAMPLE_CHAIN_PATH.read_text(encoding="utf-8"))
    exp = min(raw["expirations"], key=lambda e: abs(e["dte"] - target_dte))

    def to_contracts(entries):
        return [
            ChainContract(
                strike=c["strike"], bid=c["bid"], ask=c["ask"], mid=c["mid"], delta=c["delta"],
                iv=c.get("iv"), gamma=c.get("gamma"), vega=c.get("vega"), theta=c.get("theta"),
                rho=c.get("rho"), volume=c.get("volume"), open_interest=c.get("openInterest"),
            )
            for c in entries
        ]

    chain = ExpirationChain(
        expiration=exp["expiration"], dte=exp["dte"],
        calls=to_contracts(exp["calls"]), puts=to_contracts(exp["puts"]),
    )
    return chain, raw["underlyingPrice"]


@pytest.mark.skipif(not SAMPLE_CHAIN_PATH.exists(), reason="client sample chain fixture not present")
class TestStrategyPresetsAntiDrift:
    def test_iron_condor_deltas_match_targets(self):
        """The direct anti-drift check: resolving against the real chain must pick contracts
        whose |delta| is nearest 0.10 (wings) / 0.16 (shorts) — independently re-derived here,
        not just re-asserting our own resolver's output."""
        chain, spot = _load_sample_expiration(45)
        preset = PRESETS_BY_ID["iron_condor"]
        legs = build_preset_legs(chain, preset, spot=spot)
        assert len(legs) == 4

        by_role = {(l.type, l.side): l for l in legs}
        long_put, short_put = by_role[("put", "long")], by_role[("put", "short")]
        short_call, long_call = by_role[("call", "short")], by_role[("call", "long")]

        # Independent expected values: brute-force nearest-|delta| over the raw contract lists.
        expected_short_put = min(chain.puts, key=lambda c: abs(abs(c.delta) - 0.16))
        expected_short_call = min(chain.calls, key=lambda c: abs(abs(c.delta) - 0.16))
        assert short_put.strike == expected_short_put.strike
        assert short_call.strike == expected_short_call.strike

        # Structural invariants that must hold regardless of the exact chain snapshot.
        assert long_put.strike < short_put.strike < short_call.strike < long_call.strike
        assert preset.backtested is True

    def test_only_iron_condor_is_backtested(self):
        for preset_id, preset in PRESETS_BY_ID.items():
            if preset_id == "iron_condor":
                assert preset.backtested is True
            else:
                assert preset.backtested is False, f"{preset_id} should not be marked backtested"

    def test_further_than_respected_for_long_wings(self):
        """A sparse chain shouldn't let a protective wing resolve onto (or past) its short."""
        chain, spot = _load_sample_expiration(45)
        legs = build_preset_legs(chain, PRESETS_BY_ID["iron_condor"], spot=spot)
        by_role = {(l.type, l.side): l for l in legs}
        assert by_role[("put", "long")].strike < by_role[("put", "short")].strike
        assert by_role[("call", "long")].strike > by_role[("call", "short")].strike

    def test_all_20_presets_have_stable_ids(self):
        expected = {
            "iron_condor", "iron_butterfly", "bull_put_spread", "bear_call_spread",
            "bull_call_spread", "bear_put_spread", "seagull_spread", "long_straddle",
            "short_straddle", "long_strangle", "short_strangle", "jade_lizard",
            "call_butterfly", "put_butterfly", "covered_call", "collar", "buffered",
            "long_call", "long_put", "short_put", "custom",
        }
        assert set(PRESETS_BY_ID) == expected


class TestPayoffMath:
    def test_find_breakevens_long_call(self):
        # Long call, strike 100, premium 5: breakeven at 105.
        prices = [95, 100, 105, 110]
        leg = OptionLeg(type="call", side="long", strike=100, premium=5)
        pnls = [legs_pnl(p, [leg]) for p in prices]
        breakevens = find_breakevens(prices, pnls)
        assert breakevens == pytest.approx([105.0])

    def test_net_premium_credit_vs_debit(self):
        short_leg = OptionLeg(type="put", side="short", strike=100, premium=3)
        long_leg = OptionLeg(type="put", side="long", strike=90, premium=1)
        assert net_premium([short_leg, long_leg]) == pytest.approx(2.0)  # net credit

    def test_max_profit_loss_bull_put_spread(self):
        short_put = OptionLeg(type="put", side="short", strike=100, premium=3)
        long_put = OptionLeg(type="put", side="long", strike=90, premium=1)
        legs = [short_put, long_put]
        # Max profit = net credit (2), realized at/above 100. Max loss = width - credit = -8, below 90.
        assert legs_pnl(150, legs) == pytest.approx(2.0)
        assert legs_pnl(50, legs) == pytest.approx(-8.0)


# --------------------------------------------------------------------------------------
# Point-in-time gating
# --------------------------------------------------------------------------------------


class TestPointInTime:
    def test_live_chain_tools_refuse_when_as_of_set(self):
        tools = {t.name: t for t in OptionChainToolkit().get_tools()}
        result = _call(tools["spx_chain_metadata"], _context(as_of=date(2023, 6, 30)))
        assert "point-in-time" in result.lower()

    def test_live_chain_tools_ok_when_live(self):
        tools = {t.name: t for t in OptionChainToolkit().get_tools()}
        # We only assert it does NOT return the point-in-time refusal message; a real network
        # call may or may not succeed in this environment, so don't assert on live data.
        try:
            result = _call(tools["spx_chain_metadata"], _context(as_of=None))
        except Exception:
            return
        assert "point-in-time" not in result.lower()

    def test_run_context_prompt_fragment_differs(self):
        live = RunContext(as_of=None)
        pit = RunContext(as_of=date(2023, 6, 30))
        assert "OFF" in live.prompt_fragment()
        assert "2023-06-30" in pit.prompt_fragment()

    def test_run_context_child_depth_limit(self):
        ctx = RunContext(max_depth=1)
        child = ctx.child()
        assert child.depth == 1
        with pytest.raises(RuntimeError):
            child.child()


# --------------------------------------------------------------------------------------
# DataFrame REPL
# --------------------------------------------------------------------------------------


class TestDataFrameToolkit:
    @staticmethod
    def _tools():
        return {t.name: t for t in DataFrameToolkit().get_tools()}

    def test_df_python_shape_and_stdout(self):
        store = DataFrameStore()
        store.put("sample", pd.DataFrame({"a": [1, 2, 3]}))
        result = _call(
            self._tools()["df_python"], _context(store=store), code="print('hi')\nsample.shape"
        )
        assert "hi" in result
        assert "(3, 1)" in result

    def test_df_python_auto_registers_dataframe_result(self):
        store = DataFrameStore()
        store.put("sample", pd.DataFrame({"a": [1, 2, 3]}))
        _call(
            self._tools()["df_python"],
            _context(store=store),
            code="sample.assign(b=sample.a * 2)",
            save_as="doubled",
        )
        assert "doubled" in dict(store.list())

    def test_df_python_disabled_via_config(self):
        ctx = _context(config=AgentConfig(allow_python=False))
        result = _call(self._tools()["df_python"], ctx, code="1 + 1")
        assert "disabled" in result.lower()

    def test_store_is_shared_through_the_context(self):
        """The store reaching a tool via runtime.context is the mechanism that lets a delegated
        specialist see a sibling's DataFrames — assert the write is visible on the caller's store."""
        store = DataFrameStore()
        ctx = _context(store=store)
        _call(self._tools()["df_python"], ctx, code="pd.DataFrame({'x': [1]})", save_as="made_by_tool")
        assert "made_by_tool" in dict(store.list())


# --------------------------------------------------------------------------------------
# SQL guard
# --------------------------------------------------------------------------------------


class TestSqlGuard:
    def test_rejects_delete(self):
        result = _guard_and_prepare_sql("DELETE FROM prices", None)
        assert not result.ok
        assert "read-only" in result.rejection

    def test_rejects_multi_statement(self):
        result = _guard_and_prepare_sql("SELECT 1; DROP TABLE prices", None)
        assert not result.ok
        assert "single-statement" in result.rejection

    def test_appends_limit_when_absent(self):
        result = _guard_and_prepare_sql("select 1", None)
        assert result.ok
        assert "LIMIT 1000" in result.sql

    def test_leaves_existing_limit_alone(self):
        result = _guard_and_prepare_sql("select * from prices limit 5", None)
        assert result.ok
        assert result.sql.lower().count("limit") == 1

    def test_semicolon_inside_a_string_literal_is_not_a_second_statement(self):
        """The multi-statement check must look past string literals; `'a;b'` is one statement."""
        result = _guard_and_prepare_sql("SELECT * FROM company_news WHERE title = 'a;b'", None)
        assert result.ok, result.rejection

    def test_limit_with_offset_is_not_double_limited(self):
        """`LIMIT 10 OFFSET 5` already bounds the query; appending a second LIMIT after OFFSET is a
        Postgres syntax error."""
        result = _guard_and_prepare_sql("SELECT * FROM company_facts LIMIT 10 OFFSET 5", None)
        assert result.ok
        assert result.sql.lower().count("limit") == 1


class TestPointInTimeSqlEnforcement:
    """Regression tests for a real look-ahead-bias bug: the as_of clamp used to be
    `... WHERE TRUE`, which filters nothing, while the tool docstring and system prompt both
    promised clamping. Point-in-time SQL was silently unfiltered."""

    AS_OF = "2023-06-30"

    def test_clamp_is_a_real_predicate_not_where_true(self):
        result = _guard_and_prepare_sql("SELECT biz_date, close FROM prices", self.AS_OF)
        assert result.ok
        assert "WHERE TRUE" not in result.sql.upper()
        assert f"biz_date <= DATE '{self.AS_OF}'" in result.sql

    def test_clamp_applies_to_every_biz_date_table(self):
        for table in ("prices", "technicals", "option_research_run", "investment_clock_data"):
            result = _guard_and_prepare_sql(f"SELECT biz_date FROM {table}", self.AS_OF)
            assert result.ok, f"{table}: {result.rejection}"
            assert self.AS_OF in result.sql, f"{table} was not clamped"

    def test_no_clamp_for_tables_without_biz_date(self):
        result = _guard_and_prepare_sql("SELECT * FROM company_facts", self.AS_OF)
        assert result.ok
        assert self.AS_OF not in result.sql

    def test_no_clamp_when_live(self):
        result = _guard_and_prepare_sql("SELECT biz_date FROM prices", None)
        assert result.ok
        assert "_pit_guarded" not in result.sql

    def test_models_own_bound_is_respected_and_not_double_wrapped(self):
        sql = f"SELECT biz_date FROM prices WHERE biz_date <= DATE '2023-01-01'"
        result = _guard_and_prepare_sql(sql, self.AS_OF)
        assert result.ok
        assert "_pit_guarded" not in result.sql

    def test_bound_after_as_of_is_rejected(self):
        """A query explicitly reaching past as_of must fail loudly rather than be silently rewritten
        or silently allowed."""
        result = _guard_and_prepare_sql(
            "SELECT biz_date FROM prices WHERE biz_date <= DATE '2025-01-01'", self.AS_OF
        )
        assert not result.ok
        assert "after" in result.rejection and self.AS_OF in result.rejection

    def test_aggregate_without_biz_date_still_gets_wrapped_and_will_error_loudly(self):
        """An aggregate projects no biz_date, so the wrapper cannot clamp it. It is still applied —
        Postgres then errors, and the tool translates that into guidance. What must NOT happen is
        the query running unclamped."""
        result = _guard_and_prepare_sql("SELECT AVG(close) FROM prices", self.AS_OF)
        assert result.ok
        assert "_pit_guarded" in result.sql
        assert f"biz_date <= DATE '{self.AS_OF}'" in result.sql


# --------------------------------------------------------------------------------------
# Structure: profiles, tool-name uniqueness, delegation guards
# --------------------------------------------------------------------------------------


class TestStructure:
    def test_every_profile_toolkits_resolve(self):
        runtime = AgentRuntime()
        for key, profile in AGENT_PROFILES.items():
            toolkits = runtime.build_toolkits(profile)
            assert len(toolkits) == len(profile.toolkits)

    def test_unknown_toolkit_name_is_rejected(self):
        from sophie_agent import AgentProfile

        runtime = AgentRuntime()
        bogus = AgentProfile(key="b", display_name="B", description="", toolkits=("nope",))
        with pytest.raises(KeyError):
            runtime.build_toolkits(bogus)

    def test_tool_names_unique_within_every_agent(self):
        runtime = AgentRuntime()
        for key in AGENT_PROFILES:
            agent = runtime.build_agent(key)
            names = [t.name for t in agent.tools]
            assert len(names) == len(set(names)), f"duplicate tool name in profile '{key}'"

    def test_toolkits_are_cached_not_rebuilt_per_call(self):
        """Toolkits are stateless now, so a delegation-heavy run must reuse them rather than
        reconstructing every tool for each delegated task."""
        runtime = AgentRuntime()
        first = runtime.build_toolkits(AGENT_PROFILES["generalist"])
        second = runtime.build_toolkits(AGENT_PROFILES["generalist"])
        assert all(a is b for a, b in zip(first, second))

    def test_agents_are_cached_per_profile_and_model(self):
        runtime = AgentRuntime()
        assert runtime.build_agent("quant") is runtime.build_agent("quant")

    def test_delegate_refuses_supervisor_target(self):
        runtime = AgentRuntime()
        result = _run_one(_context(runtime=runtime), "supervisor", "anything", None)
        assert "cannot be delegated to" in result

    def test_delegate_refuses_unknown_agent(self):
        runtime = AgentRuntime()
        result = _run_one(_context(runtime=runtime), "not_a_real_agent", "anything", None)
        assert "Unknown agent" in result

    def test_delegate_refuses_when_no_runtime_on_context(self):
        result = _run_one(_context(runtime=None), "quant", "anything", None)
        assert "Delegation is unavailable" in result

    def test_delegate_respects_depth_limit(self):
        """A specialist must not be able to spawn another level once max_depth is reached."""
        runtime = AgentRuntime()
        ctx = SophieContext(
            run_ctx=RunContext(depth=2, max_depth=2),
            store=DataFrameStore(),
            config=DEFAULT_CONFIG,
            runtime=runtime,
        )
        result = _run_one(ctx, "quant", "anything", None)
        assert "depth limit" in result


# --------------------------------------------------------------------------------------
# Typed output validation
# --------------------------------------------------------------------------------------


class TestSchemas:
    def _base_kwargs(self):
        return dict(
            preset_id="iron_condor",
            legs=[OptionLeg(type="put", side="long", strike=90, premium=1)],
            net_credit=2.0, max_profit=2.0, max_loss=-8.0,
            breakevens=[98.0], probability_of_profit=0.6,
            rationale="test", evidence_strength="backtested", confidence=0.8,
        )

    def test_empty_evidence_rejected(self):
        with pytest.raises(Exception):
            StrategyRecommendation(evidence=[], **self._base_kwargs())

    def test_valid_recommendation_accepted(self):
        rec = StrategyRecommendation(
            evidence=[Citation(kind="run", ref="abc123")], **self._base_kwargs()
        )
        assert rec.evidence_strength == "backtested"

    def test_empty_legs_rejected(self):
        kwargs = self._base_kwargs()
        kwargs["legs"] = []
        with pytest.raises(Exception):
            StrategyRecommendation(evidence=[Citation(kind="run", ref="x")], **kwargs)


# --------------------------------------------------------------------------------------
# Provider gating
# --------------------------------------------------------------------------------------


class TestProviderGating:
    @pytest.mark.parametrize(
        "model_name,provider,expected",
        [
            ("deepseek-chat", ModelProvider.DEEPSEEK, True),
            ("deepseek-reasoner", ModelProvider.DEEPSEEK, False),
            ("qwen3.5:latest", ModelProvider.OLLAMA, True),
            ("gemma4:12b", ModelProvider.OLLAMA, True),  # verified via run.py --check-models
            ("deepseek-r1:14b", ModelProvider.OLLAMA, False),
        ],
    )
    def test_supports_tool_calling(self, model_name, provider, expected):
        info = get_model_info(model_name)
        assert info is not None, f"{model_name} missing from the catalog"
        assert info.supports_tool_calling() is expected

    def test_model_construction_refuses_non_tool_calling_model(self):
        with pytest.raises(ToolCallingNotSupportedError):
            build_chat_model("deepseek-r1:14b", ModelProvider.OLLAMA)

    def test_build_chat_model_applies_ollama_tool_calling_defaults(self):
        """Ollama's 4096 default context can't hold the tool schemas plus a chain preview, and the
        analyst factory's `stop` tokens truncate tool-call payloads."""
        llm = build_chat_model("qwen3.5:latest", ModelProvider.OLLAMA)
        assert llm.num_ctx == 16384
        assert llm.temperature == 0
        assert llm.stop is None

    def test_build_chat_model_overrides_win(self):
        llm = build_chat_model("qwen3.5:latest", ModelProvider.OLLAMA, num_ctx=2048)
        assert llm.num_ctx == 2048

    def test_build_chat_model_accepts_provider_as_string(self):
        assert build_chat_model("qwen3.5:latest", "Ollama") is not None

    def test_get_model_ollama_defaults_unchanged_with_no_kwargs(self):
        llm = get_model("deepseek-r1:14b", ModelProvider.OLLAMA)
        assert llm.num_ctx == 4096
        assert llm.temperature == 0.1
        assert llm.stop == ["</s>", "Human:", "Assistant:"]

    def test_get_model_ollama_kwargs_override(self):
        llm = get_model("qwen3.5:latest", ModelProvider.OLLAMA, num_ctx=16384, temperature=0, stop=None)
        assert llm.num_ctx == 16384
        assert llm.temperature == 0
        assert llm.stop is None

    def test_json_mode_models_all_support_tool_calling(self):
        """langchain-core >= 1.0's with_structured_output no longer has a raw-JSON-content
        'json_mode' path — it silently drops the `method` kwarg and always binds the schema via
        bind_tools(tool_choice='any'). src/utils/llm.py::call_llm routes every has_json_mode()==True
        model through with_structured_output(..., method="json_mode"), so any such model that
        can't tool-call would degrade to create_default_response() after 3 failed retries instead
        of failing clearly. Verified (offline, with a properly tool-calling fake model standing in
        for a real provider) that the tool-calling path itself works correctly; this guards the
        catalog invariant that makes it always reachable."""
        from src.llm.models import AVAILABLE_MODELS, OLLAMA_MODELS

        offenders = [
            m.model_name
            for m in AVAILABLE_MODELS + OLLAMA_MODELS
            if m.has_json_mode() and not m.supports_tool_calling()
        ]
        assert offenders == [], (
            f"models with has_json_mode()=True but supports_tool_calling()=False: {offenders} — "
            "call_llm() would silently degrade to default responses for these"
        )


# --------------------------------------------------------------------------------------
# Agent wiring, end to end, offline
# --------------------------------------------------------------------------------------


class _ScriptedModel(BaseChatModel):
    """A tool-capable fake: replays scripted AIMessages and records the system prompt it saw.

    Enough to exercise the real create_agent graph — tool injection, dynamic prompt assembly,
    checkpointer threading and the call limit — with no network.
    """

    script: list = []
    calls: int = 0
    seen_systems: list = []

    @property
    def _llm_type(self) -> str:
        return "scripted"

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        self.seen_systems.append(messages[0].content)
        msg = self.script[min(self.calls, len(self.script) - 1)]
        object.__setattr__(self, "calls", self.calls + 1)
        return ChatResult(generations=[ChatGeneration(message=msg.model_copy())])


def _scripted_agent(script, context, **kwargs):
    from sophie_agent import SophieAgent
    from sophie_agent.toolkits.dataframe import DataFrameToolkit

    return SophieAgent(
        default_context=context,
        toolkits=[DataFrameToolkit()],
        llm=_ScriptedModel(script=script, calls=0, seen_systems=[]),
        record_runs=False,
        **kwargs,
    )


class TestAgentWiring:
    def test_context_reaches_tools_through_the_graph(self):
        """The end-to-end proof of the context refactor: a tool call issued by the model must land
        in the tool body with the caller's live store attached."""
        store = DataFrameStore()
        ctx = _context(store=store)
        agent = _scripted_agent(
            [
                AIMessage(
                    content="",
                    tool_calls=[{
                        "name": "df_python",
                        "args": {"code": "pd.DataFrame({'x':[1,2]})", "save_as": "from_graph"},
                        "id": "c1",
                    }],
                ),
                AIMessage(content="done"),
            ],
            ctx,
        )
        result = agent.invoke("make a frame", thread_id="t1")
        assert result["output"] == "done"
        assert "from_graph" in dict(store.list())

    def test_intermediate_steps_are_derived_from_messages(self):
        agent = _scripted_agent(
            [
                AIMessage(content="", tool_calls=[{"name": "df_list", "args": {}, "id": "c1"}]),
                AIMessage(content="fin"),
            ],
            _context(),
        )
        steps = agent.invoke("list", thread_id="t1")["intermediate_steps"]
        assert [action.tool for action, _ in steps] == ["df_list"]

    def test_dynamic_prompt_reflects_as_of_and_store_contents(self):
        """The system prompt is reassembled per turn, so it must show point-in-time state and the
        live store listing rather than a stale snapshot."""
        store = DataFrameStore()
        store.put("preexisting", pd.DataFrame({"a": [1]}))
        agent = _scripted_agent(
            [AIMessage(content="ok")], _context(as_of=date(2023, 6, 30), store=store)
        )
        agent.invoke("hello", thread_id="t1")
        system = agent.llm.seen_systems[0]
        assert "2023-06-30" in system
        assert "preexisting" in system
        assert "DATAFRAME TOOLKIT" in system  # the toolkit's own fragment

    def test_history_persists_across_turns_and_includes_tool_messages(self):
        """The checkpointer replaces the hand-maintained chat_history list — and unlike it, retains
        ToolMessages, so a later turn can still see what a tool returned."""
        agent = _scripted_agent(
            [
                AIMessage(content="", tool_calls=[{"name": "df_list", "args": {}, "id": "c1"}]),
                AIMessage(content="first"),
                AIMessage(content="second"),
            ],
            _context(),
        )
        agent.invoke("turn one", thread_id="conv")
        agent.invoke("turn two", thread_id="conv")
        history = agent.history("conv")
        assert sum(isinstance(m, HumanMessage) for m in history) == 2
        assert any(isinstance(m, ToolMessage) for m in history)

    def test_threads_are_isolated(self):
        agent = _scripted_agent([AIMessage(content="ok")], _context())
        agent.invoke("a", thread_id="thread-a")
        assert agent.has_history("thread-a")
        assert not agent.has_history("thread-b")

    def test_reset_clears_only_the_named_thread(self):
        agent = _scripted_agent([AIMessage(content="ok")], _context())
        agent.invoke("a", thread_id="keep")
        agent.invoke("b", thread_id="drop")
        agent.reset("drop")
        assert agent.has_history("keep")
        assert not agent.has_history("drop")

    def test_max_iterations_is_actually_enforced(self):
        """Regression: max_iterations was accepted by the constructor, threaded in from the profile,
        and then never used — create_agent ignored it, leaving only LangGraph's recursion limit."""
        looping = AIMessage(content="", tool_calls=[{"name": "df_list", "args": {}, "id": "c1"}])
        agent = _scripted_agent([looping], _context(), max_iterations=3)
        result = agent.invoke("loop forever", thread_id="t1")
        assert agent.llm.calls == 3
        assert result["messages"]

    def test_seed_history_is_prepended_once(self):
        agent = _scripted_agent([AIMessage(content="ok")], _context())
        agent.invoke(
            "now",
            thread_id="seeded",
            seed_history=[HumanMessage(content="earlier"), AIMessage(content="earlier reply")],
        )
        contents = [m.content for m in agent.history("seeded")]
        assert "earlier" in contents
        assert contents.count("earlier") == 1

    def test_preview_system_prompt_matches_what_the_model_receives(self):
        ctx = _context(as_of=date(2022, 1, 31))
        agent = _scripted_agent([AIMessage(content="ok")], ctx)
        agent.invoke("hi", thread_id="t1")
        assert agent.preview_system_prompt() == agent.llm.seen_systems[0]


class _SchemaEmittingModel(BaseChatModel):
    """Emits whatever structured-output tool ToolStrategy bound, so response_format can be tested
    without a live model."""

    payload: dict = {}
    bound: list = []
    calls: int = 0

    @property
    def _llm_type(self) -> str:
        return "schema-emitting"

    def bind_tools(self, tools, **kwargs):
        object.__setattr__(self, "bound", [getattr(t, "name", str(t)) for t in tools])
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        object.__setattr__(self, "calls", self.calls + 1)
        target = next((n for n in self.bound if "StrategyRecommendation" in str(n)), None)
        if target is None:
            return ChatResult(generations=[ChatGeneration(message=AIMessage(content="no schema tool"))])
        return ChatResult(generations=[ChatGeneration(message=AIMessage(
            content="", tool_calls=[{"name": target, "args": self.payload, "id": "s1"}]
        ))])


_VALID_REC = {
    "preset_id": "iron_condor",
    "legs": [{"type": "put", "side": "short", "strike": 5200.0, "premium": 12.5, "quantity": 1}],
    "net_credit": 25.95,
    "max_profit": 25.95,
    "max_loss": -179.05,
    "breakevens": [5174.05, 5625.95],
    "probability_of_profit": 0.68,
    "rationale": "0.16-delta shorts are the backtest-derived choice.",
    "evidence": [{"kind": "run", "ref": "abc123", "as_of": None}],
    "evidence_strength": "backtested",
    "caveats": ["delayed quotes"],
    "confidence": 0.75,
}


class TestStructuredOutput:
    def _agent(self, payload):
        from sophie_agent import SophieAgent
        from sophie_agent.toolkits.dataframe import DataFrameToolkit

        return SophieAgent(
            default_context=_context(),
            toolkits=[DataFrameToolkit()],
            llm=_SchemaEmittingModel(payload=payload, bound=[], calls=0),
            answer_model=StrategyRecommendation,
            record_runs=False,
        )

    def test_returns_a_validated_model(self):
        agent = self._agent(_VALID_REC)
        out = agent.structured("recommend something", thread_id="s1")
        assert isinstance(out, StrategyRecommendation)
        assert out.preset_id == "iron_condor"
        assert [c.ref for c in out.evidence] == ["abc123"]

    def test_costs_exactly_one_llm_call(self):
        """The previous implementation ran the full tool loop and then made a SECOND call purely to
        repackage the answer into the schema. response_format folds that into the same graph."""
        agent = self._agent(_VALID_REC)
        agent.structured("recommend something", thread_id="s1")
        assert agent.llm.calls == 1

    def test_schema_is_bound_as_an_output_tool(self):
        agent = self._agent(_VALID_REC)
        agent.structured("recommend something", thread_id="s1")
        assert any("StrategyRecommendation" in str(n) for n in agent.llm.bound)

    def test_raises_without_an_answer_model(self):
        from sophie_agent import SophieAgent
        from sophie_agent.toolkits.dataframe import DataFrameToolkit

        agent = SophieAgent(
            default_context=_context(),
            toolkits=[DataFrameToolkit()],
            llm=_SchemaEmittingModel(payload={}, bound=[], calls=0),
            record_runs=False,
        )
        with pytest.raises(ValueError, match="answer_model"):
            agent.structured("no schema configured")


class TestTypedAnswerConstraintsAreVisibleToTheModel:
    """Declaring constraints via Field (not field_validator) is what puts them in the JSON schema the
    model fills in, instead of only failing after it has answered."""

    def test_evidence_min_items_in_schema(self):
        schema = StrategyRecommendation.model_json_schema()
        assert schema["properties"]["evidence"]["minItems"] == 1
        assert schema["properties"]["legs"]["minItems"] == 1

    def test_probability_bounds_in_schema(self):
        props = StrategyRecommendation.model_json_schema()["properties"]
        for field in ("confidence", "probability_of_profit"):
            assert props[field]["minimum"] == 0.0
            assert props[field]["maximum"] == 1.0


# --------------------------------------------------------------------------------------
# Graceful degradation
# --------------------------------------------------------------------------------------


class TestGracefulDegradation:
    def test_historical_chain_missing_directory(self):
        with pytest.raises(HistoricalChainUnavailable, match="not found"):
            load_historical_chain(
                chain_dir=Path("Z:/definitely/not/a/real/path"),
                start_date="2023-01-01", end_date="2023-01-31",
            )

    def test_historical_chain_large_range_requires_confirm(self):
        with pytest.raises(HistoricalChainUnavailable, match="confirm"):
            load_historical_chain(
                chain_dir=DEFAULT_CONFIG.historical_chain_dir,
                start_date="2010-01-01", end_date="2015-01-01",
            )

    @pytest.mark.skipif(
        not DEFAULT_CONFIG.historical_chain_dir.exists(), reason="historical parquet data not present"
    )
    def test_historical_chain_loads_real_data(self):
        df = load_historical_chain(
            chain_dir=DEFAULT_CONFIG.historical_chain_dir,
            start_date="2023-01-03", end_date="2023-01-05",
        )
        assert not df.empty
        assert {"quote_date", "expiration", "strike", "delta", "dte"}.issubset(df.columns)
