"""SophieAgent — the backbone. One class; everything domain-specific is injected via toolkits,
profile, and RunContext. See docs/SOPHIE_AGENT.md for the four-layer design this sits in.
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Type

from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.tools import BaseTool
from pydantic import BaseModel

from src.llm.models import ModelProvider, get_model, get_model_info

from ..context.run_record import write_run_record
from ..context.runcontext import RunContext
from .callbacks import UsageCallbackHandler
from .config import DEFAULT_CONFIG, AgentConfig


class ToolCallingNotSupportedError(RuntimeError):
    """Raised at construction time rather than letting a non-tool-calling model silently loop."""


_FALLBACKS = {
    ModelProvider.OLLAMA: "qwen3.5:latest",
    ModelProvider.DEEPSEEK: "deepseek-chat",
}


def _provider_from_str(value: str | ModelProvider) -> ModelProvider:
    if isinstance(value, ModelProvider):
        return value
    for p in ModelProvider:
        if p.value.lower() == str(value).lower() or p.name.lower() == str(value).lower():
            return p
    raise ValueError(f"Unknown model provider '{value}'.")


class SophieAgent:
    def __init__(
        self,
        toolkits: list | None = None,
        model_name: str | None = None,
        provider: str | ModelProvider | None = None,
        config: AgentConfig | None = None,
        run_ctx: RunContext | None = None,
        system_prompt: str | None = None,
        answer_model: Type[BaseModel] | None = None,
        name: str = "sophie",
        verbose: bool = False,
        max_iterations: int = 15,
        record_runs: bool = True,
    ) -> None:
        self.name = name
        self.record_runs = record_runs
        self.config = config or DEFAULT_CONFIG
        self.run_ctx = run_ctx or RunContext(token_budget=self.config.token_budget)
        self.answer_model = answer_model
        self.toolkits = toolkits or []

        self.tools: list[BaseTool] = []
        seen: set[str] = set()
        for tk in self.toolkits:
            for t in tk.get_tools():
                if t.name in seen:
                    raise ValueError(f"Duplicate tool name '{t.name}' across toolkits attached to agent '{name}'.")
                seen.add(t.name)
                self.tools.append(t)

        resolved_provider = _provider_from_str(provider or self.config.default_provider)
        resolved_model_name = model_name or self.config.default_model_name
        self._model_name = resolved_model_name
        self._provider = resolved_provider

        model_info = get_model_info(resolved_model_name)
        if model_info is not None and not model_info.supports_tool_calling():
            alt = _FALLBACKS.get(resolved_provider, "claude-sonnet-5")
            raise ToolCallingNotSupportedError(
                f"Model '{resolved_model_name}' ({resolved_provider.value}) does not support tool "
                f"calling. Try '{alt}' instead, or a different provider."
            )

        llm_kwargs: dict[str, Any] = {"temperature": 0}
        if resolved_provider == ModelProvider.OLLAMA:
            llm_kwargs = {"num_ctx": 16384, "temperature": 0, "stop": None}
        self.llm = get_model(resolved_model_name, resolved_provider, **llm_kwargs)

        self._usage_callback = UsageCallbackHandler(self.run_ctx)
        self._base_system_prompt = system_prompt or "You are Sophie, a research assistant over the Sophie finance platform."
        self._fragments = "\n\n".join(
            f for tk in self.toolkits for f in [tk.system_prompt_fragment()] if f
        )

        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", "{system_message}"),
                MessagesPlaceholder("chat_history"),
                ("human", "{input}"),
                MessagesPlaceholder("agent_scratchpad"),
            ]
        )
        agent = create_tool_calling_agent(self.llm, self.tools, prompt)
        self.executor = AgentExecutor(
            agent=agent,
            tools=self.tools,
            verbose=verbose,
            handle_parsing_errors=True,
            return_intermediate_steps=True,
            max_iterations=max_iterations,
        )
        self.chat_history: list[BaseMessage] = []

    def _store_listing(self) -> str:
        for tk in self.toolkits:
            store = getattr(tk, "store", None)
            if store is not None:
                return store.context_listing()
        return "(no DataFrame store attached)"

    def _system_message(self) -> str:
        return (
            f"{self._base_system_prompt}\n\n{self._fragments}\n\n"
            f"{self.run_ctx.prompt_fragment()}\n\n"
            f"Stored DataFrames (shared across this run):\n{self._store_listing()}"
        )

    def invoke(self, message: str) -> dict:
        result = self.executor.invoke(
            {"input": message, "chat_history": self.chat_history, "system_message": self._system_message()},
            config={"callbacks": [self._usage_callback]},
        )
        self.chat_history.append(HumanMessage(content=message))
        self.chat_history.append(AIMessage(content=result["output"]))

        if self.record_runs:
            try:
                write_run_record(
                    runs_dir=self.config.runs_dir,
                    run_id=self.run_ctx.run_id,
                    agent_name=self.name,
                    model_name=self._model_name,
                    provider=self._provider.value,
                    as_of=self.run_ctx.as_of_iso(),
                    message=message,
                    output=result["output"],
                    intermediate_steps=result.get("intermediate_steps", []),
                    usage=self.run_ctx.usage.as_dict(),
                )
            except Exception:
                pass  # run records are diagnostic, never fatal to the actual answer

        return result

    def chat(self, message: str) -> str:
        return self.invoke(message)["output"]

    def structured(self, message: str, model: Type[BaseModel] | None = None) -> BaseModel:
        """Runs the tool loop to ground every number in a tool call, then asks the LLM to
        repackage — not recompute — that grounded answer into the typed schema."""
        target_model = model or self.answer_model
        if target_model is None:
            raise ValueError(f"Agent '{self.name}' has no answer_model configured; pass `model` explicitly.")

        result = self.invoke(message)
        tool_trace = "\n\n".join(
            f"Tool {action.tool}({action.tool_input}) ->\n{observation}"
            for action, observation in result.get("intermediate_steps", [])
        )

        model_info = get_model_info(self._model_name)
        use_function_calling = model_info is not None and not model_info.has_json_mode() and model_info.supports_tool_calling()
        structuring_llm = (
            self.llm.with_structured_output(target_model, method="function_calling")
            if use_function_calling
            else self.llm.with_structured_output(target_model)
        )
        packaging_prompt = (
            "Package the research below into the required schema. Do not invent, estimate, or "
            "recompute any numeric value — use only what already appears in the tool outputs and "
            f"final answer.\n\nFinal answer:\n{result['output']}\n\nTool outputs:\n{tool_trace}"
        )
        return structuring_llm.invoke(packaging_prompt)

    async def stream(self, message: str) -> AsyncIterator[dict]:
        async for event in self.executor.astream_events(
            {"input": message, "chat_history": self.chat_history, "system_message": self._system_message()},
            version="v2",
            config={"callbacks": [self._usage_callback]},
        ):
            yield event

    def reset(self) -> None:
        self.chat_history = []
