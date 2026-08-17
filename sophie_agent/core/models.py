"""Chat-model construction for sophie_agent, delegated to LangChain's own `init_chat_model`.

`src/llm/models.py::get_model` is a hand-rolled six-branch provider factory that predates
`init_chat_model`. It is deliberately left untouched — the 15 analyst agents in `src/agents/` depend
on its exact behavior, including its Ollama sampling defaults — but sophie_agent has no reason to
carry a bespoke factory: `init_chat_model` resolves every provider this repo uses (verified against
the installed integration packages: anthropic, deepseek, google_genai, groq, openai, ollama) and
passes provider-specific kwargs like `num_ctx` straight through.

What is NOT delegated is `LLMModel.supports_tool_calling()`. That gate is maintained empirically
(see `run.py --check-models` and the provider matrix in docs/SOPHIE_AGENT.md) because tool-calling
support is per-model, not per-provider, and no library exposes it reliably. It stays the one piece of
hand-maintained model metadata, and it is checked here at construction time so a model that cannot
tool-call fails loudly instead of silently running prose-only, tool-less turns.
"""

from __future__ import annotations

from typing import Any

from langchain.chat_models import init_chat_model
from langchain_core.language_models import BaseChatModel

from src.llm.models import ModelProvider, get_model_info


class ToolCallingNotSupportedError(RuntimeError):
    """Raised at construction time rather than letting a non-tool-calling model silently loop."""


# ModelProvider -> the provider id init_chat_model expects.
_PROVIDER_IDS: dict[ModelProvider, str] = {
    ModelProvider.ANTHROPIC: "anthropic",
    ModelProvider.DEEPSEEK: "deepseek",
    ModelProvider.GEMINI: "google_genai",
    ModelProvider.GROQ: "groq",
    ModelProvider.OPENAI: "openai",
    ModelProvider.OLLAMA: "ollama",
}

# Suggested alternative when a chosen model can't tool-call, per provider.
_FALLBACK_HINTS: dict[ModelProvider, str] = {
    ModelProvider.OLLAMA: "qwen3.5:latest",
    ModelProvider.DEEPSEEK: "deepseek-chat",
}

# temperature=0 everywhere: the LLM is an orchestrator here, never a source of numbers, and the
# LLM cache keys on the prompt so determinism makes it actually hit.
_BASE_KWARGS: dict[str, Any] = {"temperature": 0}
# Ollama needs a much larger context than its 4096 default to hold tool schemas + a chain preview,
# and the sampling `stop` tokens the analyst factory sets truncate tool-call payloads.
_OLLAMA_KWARGS: dict[str, Any] = {"temperature": 0, "num_ctx": 16384, "stop": None}


def provider_from_str(value: str | ModelProvider) -> ModelProvider:
    if isinstance(value, ModelProvider):
        return value
    for p in ModelProvider:
        if p.value.lower() == str(value).lower() or p.name.lower() == str(value).lower():
            return p
    raise ValueError(f"Unknown model provider '{value}'.")


def assert_supports_tool_calling(model_name: str, provider: ModelProvider) -> None:
    """Fail fast for a model known not to expose function calling. Unknown models (e.g. a freshly
    pulled Ollama tag absent from the catalog) are allowed through — `--check-models` is how they
    get verified, and refusing everything unrecognized would be worse than optimistic."""
    info = get_model_info(model_name)
    if info is not None and not info.supports_tool_calling():
        alt = _FALLBACK_HINTS.get(provider, "deepseek-chat")
        raise ToolCallingNotSupportedError(
            f"Model '{model_name}' ({provider.value}) does not support tool calling. "
            f"Try '{alt}' instead, or a different provider."
        )


def build_chat_model(
    model_name: str, provider: str | ModelProvider, **overrides: Any
) -> BaseChatModel:
    """Construct a tool-calling-capable chat model for the given provider."""
    resolved = provider_from_str(provider)
    assert_supports_tool_calling(model_name, resolved)

    kwargs = dict(_OLLAMA_KWARGS if resolved == ModelProvider.OLLAMA else _BASE_KWARGS)
    kwargs.update(overrides)
    return init_chat_model(model_name, model_provider=_PROVIDER_IDS[resolved], **kwargs)
