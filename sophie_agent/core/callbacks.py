"""UsageCallbackHandler — provider-agnostic token accounting.

langchain_community.callbacks.get_openai_callback only works for OpenAI. Every provider's chat
model attaches `usage_metadata` to the returned AIMessage (LangChain's normalized usage dict since
core 0.3), so reading that off each generation works uniformly across Anthropic/OpenAI/DeepSeek/
Ollama/Groq/Gemini.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult

if TYPE_CHECKING:
    from ..context.runcontext import RunContext


class UsageCallbackHandler(BaseCallbackHandler):
    def __init__(self, run_ctx: "RunContext") -> None:
        self._run_ctx = run_ctx

    def on_llm_end(self, response: LLMResult, *, run_id: UUID, **kwargs: Any) -> None:
        for generation_list in response.generations:
            for generation in generation_list:
                message = getattr(generation, "message", None)
                usage = getattr(message, "usage_metadata", None) if message else None
                if not usage:
                    continue
                prompt_tokens = usage.get("input_tokens", 0) or 0
                completion_tokens = usage.get("output_tokens", 0) or 0
                self._run_ctx.usage.add(prompt_tokens, completion_tokens)
        self._run_ctx.check_budget()
