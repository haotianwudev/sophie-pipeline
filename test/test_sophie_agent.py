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
    SophieAgent,
    StrategyRecommendation,
    ToolCallingNotSupportedError,
    WikiStore,
)
from sophie_agent.options.chain_types import ChainContract, ExpirationChain
from sophie_agent.options.historical import HistoricalChainUnavailable, load_historical_chain
from sophie_agent.options.payoff import find_breakevens, legs_pnl, net_premium
from sophie_agent.options.presets import PRESETS_BY_ID, build_preset_legs
from sophie_agent.toolkits.dataframe import DataFrameToolkit
from sophie_agent.toolkits.market import _guard_and_prepare_sql
from sophie_agent.toolkits.options import OptionChainToolkit

SAMPLE_CHAIN_PATH = (
    Path(__file__).resolve().parents[2]
    / "ai-stock-suggestion-client"
    / "public"
    / "data"
    / "spx-chain-sample.json"
)


def _toolkit_kwargs(as_of=None, config=None):
    return dict(
        store=DataFrameStore(),
        run_ctx=RunContext(as_of=as_of),
        config=config or DEFAULT_CONFIG,
    )


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
        toolkit = OptionChainToolkit(**_toolkit_kwargs(as_of=date(2023, 6, 30)))
        tools = {t.name: t for t in toolkit.get_tools()}
        result = tools["spx_chain_metadata"].invoke({})
        assert "point-in-time" in result.lower()

    def test_live_chain_tools_ok_when_live(self):
        toolkit = OptionChainToolkit(**_toolkit_kwargs(as_of=None))
        tools = {t.name: t for t in toolkit.get_tools()}
        # We only assert it does NOT return the point-in-time refusal message; a real network
        # call may or may not succeed in this environment, so don't assert on live data.
        try:
            result = tools["spx_chain_metadata"].invoke({})
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
    def test_df_python_shape_and_stdout(self):
        store = DataFrameStore()
        store.put("sample", pd.DataFrame({"a": [1, 2, 3]}))
        toolkit = DataFrameToolkit(store=store, run_ctx=RunContext(), config=DEFAULT_CONFIG)
        tools = {t.name: t for t in toolkit.get_tools()}

        result = tools["df_python"].invoke({"code": "print('hi')\nsample.shape"})
        assert "hi" in result
        assert "(3, 1)" in result

    def test_df_python_auto_registers_dataframe_result(self):
        store = DataFrameStore()
        store.put("sample", pd.DataFrame({"a": [1, 2, 3]}))
        toolkit = DataFrameToolkit(store=store, run_ctx=RunContext(), config=DEFAULT_CONFIG)
        tools = {t.name: t for t in toolkit.get_tools()}

        tools["df_python"].invoke({"code": "sample.assign(b=sample.a * 2)", "save_as": "doubled"})
        assert "doubled" in dict(store.list())

    def test_df_python_disabled_via_config(self):
        cfg = AgentConfig(allow_python=False)
        store = DataFrameStore()
        toolkit = DataFrameToolkit(store=store, run_ctx=RunContext(), config=cfg)
        tools = {t.name: t for t in toolkit.get_tools()}
        result = tools["df_python"].invoke({"code": "1 + 1"})
        assert "disabled" in result.lower()


# --------------------------------------------------------------------------------------
# SQL guard
# --------------------------------------------------------------------------------------


class TestSqlGuard:
    def test_rejects_delete(self):
        assert _guard_and_prepare_sql("DELETE FROM prices", None) is None

    def test_rejects_multi_statement(self):
        assert _guard_and_prepare_sql("SELECT 1; DROP TABLE prices", None) is None

    def test_appends_limit_when_absent(self):
        prepared = _guard_and_prepare_sql("select 1", None)
        assert prepared is not None
        assert "LIMIT 1000" in prepared

    def test_leaves_existing_limit_alone(self):
        prepared = _guard_and_prepare_sql("select * from prices limit 5", None)
        assert prepared is not None
        assert prepared.lower().count("limit") == 1


# --------------------------------------------------------------------------------------
# Structure: profiles, tool-name uniqueness, delegation guards
# --------------------------------------------------------------------------------------


class TestStructure:
    def test_every_profile_toolkits_resolve(self):
        runtime = AgentRuntime()
        for key, profile in AGENT_PROFILES.items():
            toolkits = runtime.build_toolkits(profile, runtime.root_run_ctx)
            assert len(toolkits) == len(profile.toolkits)

    def test_tool_names_unique_within_every_agent(self):
        runtime = AgentRuntime()
        for key in AGENT_PROFILES:
            agent = runtime.build_agent(key)
            names = [t.name for t in agent.tools]
            assert len(names) == len(set(names)), f"duplicate tool name in profile '{key}'"

    def test_delegate_refuses_supervisor_target(self):
        runtime = AgentRuntime()
        supervisor = runtime.build_agent("supervisor")
        delegate_tool = next(t for t in supervisor.tools if t.name == "delegate")
        result = delegate_tool.invoke({"agent": "supervisor", "task": "anything"})
        assert "cannot be delegated to" in result

    def test_delegate_refuses_unknown_agent(self):
        runtime = AgentRuntime()
        supervisor = runtime.build_agent("supervisor")
        delegate_tool = next(t for t in supervisor.tools if t.name == "delegate")
        result = delegate_tool.invoke({"agent": "not_a_real_agent", "task": "anything"})
        assert "Unknown agent" in result


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

    def test_agent_construction_refuses_non_tool_calling_model(self):
        with pytest.raises(ToolCallingNotSupportedError):
            SophieAgent(toolkits=[], model_name="deepseek-r1:14b", provider=ModelProvider.OLLAMA)

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
