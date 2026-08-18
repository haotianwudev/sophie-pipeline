"""Tool-schema tests: assert on the schema the MODEL actually receives.

These replace an earlier suite that constructed the per-tool `args_schema` pydantic models directly.
That suite could pass while the tool was broken, because the schema and the function signature were
two separate declarations — and they had already drifted (delegate_parallel declared
`list[ParallelTaskItem]` in its schema but `list[dict | ParallelTaskItem]` in its signature).

Tool schemas are now inferred from `Annotated[...]` signatures, so the single thing worth asserting
is the generated contract. Note `tool.args_schema.model_json_schema()` is NOT usable here: the
inferred model retains the injected `runtime: ToolRuntime` field, whose callable members pydantic
cannot render as JSON Schema. `tool.args` and `convert_to_openai_tool()` both apply LangChain's
injected-argument filter first, which is exactly the provider-facing view we want.
"""

from __future__ import annotations

import pydantic
import pytest
import test_utils  # noqa: F401  (adds project root to sys.path — see test/test_utils.py)
from langchain_core.utils.function_calling import convert_to_openai_tool
from pydantic import ValidationError

from sophie_agent import AGENT_PROFILES, AgentRuntime
from sophie_agent.toolkits.delegate.toolkit import ParallelTask
from sophie_agent.toolkits.strategy.toolkit import compare_strategy_variants


@pytest.fixture(scope="module")
def tools_by_name():
    runtime = AgentRuntime()
    tools = {}
    for profile in AGENT_PROFILES.values():
        for toolkit in runtime.build_toolkits(profile):
            for t in toolkit.get_tools():
                tools[t.name] = t
    # compare_strategy_variants is temporarily withheld from every profile's StrategyToolkit
    # (see get_tools()) but its own schema contract is still worth asserting on directly.
    tools[compare_strategy_variants.name] = compare_strategy_variants
    return tools


def _params(tool):
    return convert_to_openai_tool(tool)["function"].get("parameters", {})


def _props(tool):
    return _params(tool).get("properties", {})


class TestInjectedRuntimeIsNeverExposed:
    """The whole point of ToolRuntime injection: the model must not see it, or it will try to fill
    it in. Verified across every tool in every profile, not just a sample."""

    def test_no_tool_exposes_runtime(self, tools_by_name):
        offenders = [name for name, t in tools_by_name.items() if "runtime" in _props(t)]
        assert offenders == [], f"tools leaking `runtime` into the LLM schema: {offenders}"

    def test_every_tool_schema_is_generatable(self, tools_by_name):
        """A tool whose schema can't be converted is unusable — it fails at bind_tools time, i.e.
        on the first real call, not at import."""
        for name, t in tools_by_name.items():
            spec = convert_to_openai_tool(t)
            assert spec["function"]["name"] == name
            assert spec["function"]["description"], f"{name} has no description"

    def test_every_tool_has_described_arguments(self, tools_by_name):
        """Every LLM-visible argument needs a description; an undescribed one is a schema the model
        has to guess at."""
        missing = [
            f"{name}.{arg}"
            for name, t in tools_by_name.items()
            for arg, spec in _props(t).items()
            if not spec.get("description")
        ]
        assert missing == [], f"arguments with no description: {missing}"


class TestConstraintsSurviveIntoTheSchema:
    """Constraints declared with Annotated[..., Field(...)] must appear in the emitted schema —
    that is the reason to declare them there rather than validating after the fact."""

    def test_wiki_search_limit_bounds(self, tools_by_name):
        limit = _props(tools_by_name["wiki_search"])["limit"]
        assert limit["minimum"] == 1
        assert limit["maximum"] == 50
        assert limit["default"] == 8

    def test_df_head_n_bounds(self, tools_by_name):
        n = _props(tools_by_name["df_head"])["n"]
        assert (n["minimum"], n["maximum"]) == (1, 100)

    def test_spx_gex_strike_range_is_exclusive_positive(self, tools_by_name):
        assert _props(tools_by_name["spx_gex"])["strike_range"]["exclusiveMinimum"] == 0

    def test_option_type_is_an_enum_not_a_free_string(self, tools_by_name):
        prop = _props(tools_by_name["spx_option_chain"])["option_type"]
        enums = [v for branch in prop["anyOf"] for v in branch.get("enum", [])]
        assert set(enums) == {"CALL", "PUT"}

    def test_build_strategy_source_is_an_enum(self, tools_by_name):
        assert set(_props(tools_by_name["build_strategy"])["source"]["enum"]) == {"live", "historical"}

    def test_compare_variants_requires_a_nonempty_delta_grid(self, tools_by_name):
        assert _props(tools_by_name["compare_strategy_variants"])["delta_grid"]["minItems"] == 1

    def test_delegate_parallel_task_list_is_bounded(self, tools_by_name):
        tasks = _props(tools_by_name["delegate_parallel"])["tasks"]
        assert tasks["minItems"] == 1
        assert tasks["maxItems"] == 10

    def test_delegate_parallel_tasks_are_structured_not_freeform(self, tools_by_name):
        """The nested item schema is what stops the model sending bare strings — which is what the
        old implementation's isinstance() ladder existed to cope with. convert_to_openai_tool
        inlines the nested model rather than emitting a $defs reference."""
        item = _props(tools_by_name["delegate_parallel"])["tasks"]["items"]
        assert item["type"] == "object"
        assert set(item["properties"]) == {"agent", "task", "context"}
        assert item["required"] == ["agent", "task"]


class TestRequiredVsOptional:
    def test_required_arguments_are_marked_required(self, tools_by_name):
        assert _params(tools_by_name["wiki_get_page"])["required"] == ["path"]
        assert set(_params(tools_by_name["spx_historical_chain"])["required"]) == {
            "start_date",
            "end_date",
        }

    def test_fully_optional_tool_has_no_required_list(self, tools_by_name):
        assert _params(tools_by_name["spx_option_chain"]).get("required", []) == []

    def test_zero_argument_tools_expose_no_properties(self, tools_by_name):
        for name in ("sql_list_tables", "list_strategy_presets", "list_agents"):
            assert _props(tools_by_name[name]) == {}, f"{name} should take no arguments"


class TestRuntimeValidationIsEnforced:
    """The schema is also the validator — a violating call is rejected before the body runs."""

    @pytest.mark.parametrize(
        "tool_name,bad_args",
        [
            ("wiki_search", {"query": "x", "limit": 0}),
            ("wiki_search", {"query": "x", "limit": 999}),
            ("df_head", {"name": "h", "n": 0}),
            ("spx_gex", {"strike_range": -10}),
            ("build_strategy", {"preset_id": "iron_condor", "source": "nonsense"}),
            ("compare_strategy_variants", {"preset_id": "x", "leg_index": 0, "delta_grid": []}),
            ("delegate_parallel", {"tasks": []}),
        ],
    )
    def test_invalid_arguments_rejected(self, tools_by_name, tool_name, bad_args):
        with pytest.raises(ValidationError):
            tools_by_name[tool_name].invoke(bad_args)


class TestParallelTaskModel:
    def test_coerces_dicts_to_typed_items(self):
        """delegate_parallel's body indexes `t.agent`/`t.task` directly, with no isinstance
        branching — so validation must hand it real ParallelTask objects, not dicts.

        Validated through a standalone model rather than `delegate_parallel.args_schema`, because
        the inferred args_schema also carries the injected `runtime` field and would demand it here.
        """

        class _Tasks(pydantic.BaseModel):
            tasks: list[ParallelTask]

        validated = _Tasks.model_validate({"tasks": [{"agent": "quant", "task": "compute"}]})
        assert isinstance(validated.tasks[0], ParallelTask)
        assert validated.tasks[0].agent == "quant"
        assert validated.tasks[0].context is None

    def test_agent_and_task_are_mandatory(self):
        with pytest.raises(ValidationError):
            ParallelTask(agent="quant")
