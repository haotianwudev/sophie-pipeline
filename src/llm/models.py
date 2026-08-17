import os
from langchain_anthropic import ChatAnthropic
from langchain_deepseek import ChatDeepSeek
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_groq import ChatGroq
from langchain_openai import ChatOpenAI
from langchain_ollama import ChatOllama
from enum import Enum
from pydantic import BaseModel
from typing import Tuple


class ModelProvider(str, Enum):
    """Enum for supported LLM providers"""
    ANTHROPIC = "Anthropic"
    DEEPSEEK = "DeepSeek"
    GEMINI = "Gemini"
    GROQ = "Groq"
    OPENAI = "OpenAI"
    OLLAMA = "Ollama"



class LLMModel(BaseModel):
    """Represents an LLM model configuration"""
    display_name: str
    model_name: str
    provider: ModelProvider

    def to_choice_tuple(self) -> Tuple[str, str, str]:
        """Convert to format needed for questionary choices"""
        return (self.display_name, self.model_name, self.provider.value)
    
    def has_json_mode(self) -> bool:
        """Check if the model supports JSON mode"""
        if self.is_deepseek() or self.is_gemini():
            return False
        # Only certain Ollama models support JSON mode
        if self.is_ollama():
            return "llama3" in self.model_name or "neural-chat" in self.model_name
        return True
    
    def is_deepseek(self) -> bool:
        """Check if the model is a DeepSeek model"""
        return self.model_name.startswith("deepseek")
    
    def is_gemini(self) -> bool:
        """Check if the model is a Gemini model"""
        return self.model_name.startswith("gemini")
        
    def is_ollama(self) -> bool:
        """Check if the model is an Ollama model"""
        return self.provider == ModelProvider.OLLAMA

    def supports_tool_calling(self) -> bool:
        """Whether this model can be used with bind_tools()/create_tool_calling_agent().

        Tool-calling support is per-model, not per-provider — verified empirically (see
        sophie-pipeline/docs/SOPHIE_AGENT.md's provider matrix) rather than assumed from the
        provider name. In particular: DeepSeek's reasoning models (deepseek-reasoner / any local
        R1 distill) do not expose function calling, and Ollama's tool support depends on the
        underlying model family's chat template, not on Ollama itself.
        """
        if self.provider == ModelProvider.DEEPSEEK:
            return self.model_name == "deepseek-chat"
        if self.is_ollama():
            name = self.model_name.lower()
            if "deepseek-r1" in name:
                return False
            if "gemma" in name:
                # Empirically verified with `sophie-agent/run.py --check-models` against the
                # locally pulled builds: gemma3 does not return tool calls, gemma4 does (Google
                # added function-calling support to the Gemma 4 chat template). Don't assume any
                # other gemma generation without re-verifying.
                return "gemma4" in name
            return any(family in name for family in ("qwen", "llama3.1", "llama3.3", "mistral", "hermes", "firefunction"))
        # Anthropic / OpenAI / Groq / Gemini: every model in this catalog supports tool calling.
        return True


# Define available models
AVAILABLE_MODELS = [
    LLMModel(
        display_name="[anthropic] claude-3.5-haiku",
        model_name="claude-3-5-haiku-latest",
        provider=ModelProvider.ANTHROPIC
    ),
    LLMModel(
        display_name="[anthropic] claude-3.5-sonnet",
        model_name="claude-3-5-sonnet-latest",
        provider=ModelProvider.ANTHROPIC
    ),
    LLMModel(
        display_name="[anthropic] claude-3.7-sonnet",
        model_name="claude-3-7-sonnet-latest",
        provider=ModelProvider.ANTHROPIC
    ),
    LLMModel(
        display_name="[deepseek] deepseek-r1",
        model_name="deepseek-reasoner",
        provider=ModelProvider.DEEPSEEK
    ),
    LLMModel(
        display_name="[deepseek] deepseek-v3",
        model_name="deepseek-v4-pro",
        provider=ModelProvider.DEEPSEEK
    ),
    LLMModel(
        display_name="[gemini] gemini-2.0-flash",
        model_name="gemini-2.0-flash",
        provider=ModelProvider.GEMINI
    ),
    LLMModel(
        display_name="[gemini] gemini-2.5-pro",
        model_name="gemini-2.5-pro-exp-03-25",
        provider=ModelProvider.GEMINI
    ),
    LLMModel(
        display_name="[groq] llama-4-scout-17b",
        model_name="meta-llama/llama-4-scout-17b-16e-instruct",
        provider=ModelProvider.GROQ
    ),
    LLMModel(
        display_name="[groq] llama-4-maverick-17b",
        model_name="meta-llama/llama-4-maverick-17b-128e-instruct",
        provider=ModelProvider.GROQ
    ),
    LLMModel(
        display_name="[openai] gpt-4.5",
        model_name="gpt-4.5-preview",
        provider=ModelProvider.OPENAI
    ),
    LLMModel(
        display_name="[openai] gpt-4o",
        model_name="gpt-4o",
        provider=ModelProvider.OPENAI
    ),
    LLMModel(
        display_name="[openai] o3",
        model_name="o3",
        provider=ModelProvider.OPENAI
    ),
    LLMModel(
        display_name="[openai] o4-mini",
        model_name="o4-mini",
        provider=ModelProvider.OPENAI
    ),
    # --- Appended for sophie_agent (see docs/SOPHIE_AGENT.md); strictly additive, existing
    # entries above are untouched so ANALYST_CONFIG-driven callers see no behavior change. ---
    LLMModel(
        display_name="[anthropic] claude-sonnet-5",
        model_name="claude-sonnet-5",
        provider=ModelProvider.ANTHROPIC
    ),
    LLMModel(
        display_name="[anthropic] claude-opus-5",
        model_name="claude-opus-5",
        provider=ModelProvider.ANTHROPIC
    ),
    LLMModel(
        display_name="[anthropic] claude-haiku-4.5",
        model_name="claude-haiku-4-5",
        provider=ModelProvider.ANTHROPIC
    ),
    LLMModel(
        # The only tool-calling-capable DeepSeek API model — deepseek-reasoner (R1) above does
        # not support function calling.
        display_name="[deepseek] deepseek-chat",
        model_name="deepseek-chat",
        provider=ModelProvider.DEEPSEEK
    ),
]

# Define Ollama models separately
OLLAMA_MODELS = [
    LLMModel(
        display_name="[google] gemma3 (4B)",
        model_name="gemma3:4b",
        provider=ModelProvider.OLLAMA
    ),
    LLMModel(
        display_name="[alibaba] qwen3 (4B)",
        model_name="qwen3:4b",
        provider=ModelProvider.OLLAMA
    ),
    LLMModel(
        display_name="[meta] llama3.1 (8B)",
        model_name="llama3.1:latest",
        provider=ModelProvider.OLLAMA
    ),
    LLMModel(
        display_name="[google] gemma3 (12B)",
        model_name="gemma3:12b",
        provider=ModelProvider.OLLAMA
    ),
    LLMModel(
        display_name="[mistral] mistral-small3.1 (24B)",
        model_name="mistral-small3.1",
        provider=ModelProvider.OLLAMA
    ),
    LLMModel(
        display_name="[google] gemma3 (27B)",
        model_name="gemma3:27b",
        provider=ModelProvider.OLLAMA
    ),
    LLMModel(
        display_name="[alibaba] qwen3 (30B-a3B)",
        model_name="qwen3:30b-a3b",
        provider=ModelProvider.OLLAMA
    ),
    LLMModel(
        display_name="[meta] llama-3.3 (70B)",
        model_name="llama3.3:70b-instruct-q4_0",
        provider=ModelProvider.OLLAMA
    ),
    # --- Appended for sophie_agent: the models actually pulled on this machine as of this
    # writing (none of the eight above are). Verify with `sophie-agent/run.py --check-models`
    # rather than trusting this list going forward — see docs/SOPHIE_AGENT.md. ---
    LLMModel(
        display_name="[alibaba] qwen3.5 (local, tool-calling)",
        model_name="qwen3.5:latest",
        provider=ModelProvider.OLLAMA
    ),
    LLMModel(
        # Verified WORKS via `run.py --check-models` — Gemma 4's chat template (unlike Gemma 3)
        # returns real tool calls.
        display_name="[google] gemma4 12B (local, tool-calling)",
        model_name="gemma4:12b",
        provider=ModelProvider.OLLAMA
    ),
    LLMModel(
        display_name="[deepseek] deepseek-r1 14B (local, no tool-calling)",
        model_name="deepseek-r1:14b",
        provider=ModelProvider.OLLAMA
    ),
]

# Create LLM_ORDER in the format expected by the UI
LLM_ORDER = [model.to_choice_tuple() for model in AVAILABLE_MODELS]

# Create Ollama LLM_ORDER separately
OLLAMA_LLM_ORDER = [model.to_choice_tuple() for model in OLLAMA_MODELS]

def get_model_info(model_name: str) -> LLMModel | None:
    """Get model information by model_name"""
    all_models = AVAILABLE_MODELS + OLLAMA_MODELS
    return next((model for model in all_models if model.model_name == model_name), None)

def get_model(model_name: str, model_provider: ModelProvider, **kwargs) -> ChatOpenAI | ChatGroq | ChatOllama | None:
    """`**kwargs` lets callers override provider-specific construction params (e.g. sophie_agent
    needs a larger Ollama num_ctx and no stop tokens for tool calling). Every existing call site
    passes none, so behavior for them is byte-for-byte unchanged — only the Ollama branch below
    reads from kwargs, with today's exact values as defaults.
    """
    if model_provider == ModelProvider.GROQ:
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            # Print error to console
            print(f"API Key Error: Please make sure GROQ_API_KEY is set in your .env file.")
            raise ValueError("Groq API key not found.  Please make sure GROQ_API_KEY is set in your .env file.")
        return ChatGroq(model=model_name, api_key=api_key, **kwargs)
    elif model_provider == ModelProvider.OPENAI:
        # Get and validate API key
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            # Print error to console
            print(f"API Key Error: Please make sure OPENAI_API_KEY is set in your .env file.")
            raise ValueError("OpenAI API key not found.  Please make sure OPENAI_API_KEY is set in your .env file.")
        return ChatOpenAI(model=model_name, api_key=api_key, **kwargs)
    elif model_provider == ModelProvider.ANTHROPIC:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            print(f"API Key Error: Please make sure ANTHROPIC_API_KEY is set in your .env file.")
            raise ValueError("Anthropic API key not found.  Please make sure ANTHROPIC_API_KEY is set in your .env file.")
        return ChatAnthropic(model=model_name, api_key=api_key, **kwargs)
    elif model_provider == ModelProvider.DEEPSEEK:
        api_key = os.getenv("DEEPSEEK_API_KEY")
        if not api_key:
            print(f"API Key Error: Please make sure DEEPSEEK_API_KEY is set in your .env file.")
            raise ValueError("DeepSeek API key not found.  Please make sure DEEPSEEK_API_KEY is set in your .env file.")
        return ChatDeepSeek(model=model_name, api_key=api_key, **kwargs)
    elif model_provider == ModelProvider.GEMINI:
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            print(f"API Key Error: Please make sure GOOGLE_API_KEY is set in your .env file.")
            raise ValueError("Google API key not found.  Please make sure GOOGLE_API_KEY is set in your .env file.")
        return ChatGoogleGenerativeAI(model=model_name, api_key=api_key, **kwargs)
    elif model_provider == ModelProvider.OLLAMA:
        # For Ollama, we use a base URL instead of an API key
        # Check if OLLAMA_HOST is set (for Docker on macOS)
        ollama_host = os.getenv("OLLAMA_HOST", "localhost")
        base_url = os.getenv("OLLAMA_BASE_URL", f"http://{ollama_host}:11434")
        ollama_kwargs = dict(
            temperature=0.1,  # Lower temperature for more deterministic output
            top_p=0.1,  # Lower top_p for more focused sampling
            top_k=10,  # Lower top_k for more focused sampling
            num_ctx=4096,  # Context window
            repeat_penalty=1.1,  # Slight penalty for repetition
            stop=["</s>", "Human:", "Assistant:"]  # Stop tokens
        )
        ollama_kwargs.update(kwargs)
        return ChatOllama(model=model_name, base_url=base_url, **ollama_kwargs)