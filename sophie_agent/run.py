#!/usr/bin/env python
"""Entrypoint for the Sophie agent — matches the investment-clock/run.py convention.

Usage (from the sophie-pipeline root):
    poetry run python sophie_agent/run.py                          # generalist REPL
    poetry run python sophie_agent/run.py --agent supervisor
    poetry run python sophie_agent/run.py --agent option_strategist --model claude-sonnet-5
    poetry run python sophie_agent/run.py --all-local               # every profile on Ollama qwen3.5
    poetry run python sophie_agent/run.py --as-of 2023-06-30 --agent quant
    poetry run python sophie_agent/run.py --list-agents
    poetry run python sophie_agent/run.py --list-tools --agent generalist
    poetry run python sophie_agent/run.py --check-models
"""

import argparse
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.llm.models import AVAILABLE_MODELS, ModelProvider  # noqa: E402
from sophie_agent import (  # noqa: E402
    AGENT_PROFILES,
    DEFAULT_CONFIG,
    AgentRuntime,
    RunContext,
    provider_from_str,
)
from sophie_agent.cli import run_repl  # noqa: E402


def _print_agents() -> None:
    for key, p in AGENT_PROFILES.items():
        model = p.model_name or DEFAULT_CONFIG.default_model_name
        provider = p.provider.value if p.provider else DEFAULT_CONFIG.default_provider
        print(f"{key:20s} [{provider}/{model}] toolkits={list(p.toolkits)}")
        print(f"{'':20s} {p.description}")


def _print_tools(runtime: AgentRuntime, agent_key: str) -> None:
    agent = runtime.build_agent(agent_key)
    print(f"--- system prompt for '{agent_key}' ---\n")
    print(agent.preview_system_prompt())
    print(f"\n--- {len(agent.tools)} tools ---")
    for t in agent.tools:
        print(f"- {t.name}: {t.description.splitlines()[0]}")


def _check_models() -> None:
    import requests
    from langchain_core.tools import tool

    @tool
    def _probe_tool(x: int) -> int:
        """Return x plus one."""
        return x + 1

    print("--- Ollama daemon ---")
    try:
        resp = requests.get(f"{DEFAULT_CONFIG.ollama_base_url}/api/tags", timeout=5)
        resp.raise_for_status()
        pulled = [m["name"] for m in resp.json().get("models", [])]
    except Exception as exc:
        print(f"  Could not reach Ollama at {DEFAULT_CONFIG.ollama_base_url}: {exc}")
        pulled = []

    for name in pulled:
        from langchain_ollama import ChatOllama

        try:
            llm = ChatOllama(model=name, base_url=DEFAULT_CONFIG.ollama_base_url, num_ctx=4096).bind_tools([_probe_tool])
            result = llm.invoke("Call _probe_tool with x=1.")
            works = bool(getattr(result, "tool_calls", None))
            print(f"  {name:30s} {'WORKS' if works else 'NO TOOL CALL RETURNED'}")
        except Exception as exc:
            print(f"  {name:30s} FAILS ({type(exc).__name__}: {exc})")

    print("\n--- Remote catalog (from static supports_tool_calling(), not live-pinged) ---")
    for m in AVAILABLE_MODELS:
        print(f"  [{m.provider.value:10s}] {m.model_name:30s} {'yes' if m.supports_tool_calling() else 'no'}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--agent", default="generalist", choices=list(AGENT_PROFILES))
    parser.add_argument("--model", default=None, help="Override the profile's model name.")
    parser.add_argument("--provider", default=None, help="Override the profile's provider (e.g. Anthropic, Ollama).")
    parser.add_argument("--as-of", default=None, help="YYYY-MM-DD: run in point-in-time mode.")
    parser.add_argument("--all-local", action="store_true", help="Force every profile onto local Ollama qwen3.5.")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--list-agents", action="store_true")
    parser.add_argument("--list-tools", action="store_true")
    parser.add_argument("--check-models", action="store_true")
    args = parser.parse_args()

    if args.list_agents:
        _print_agents()
        return
    if args.check_models:
        _check_models()
        return

    force_model, force_provider = (None, None)
    if args.all_local:
        force_model, force_provider = "qwen3.5:latest", ModelProvider.OLLAMA

    root_ctx = RunContext(
        as_of=date.fromisoformat(args.as_of) if args.as_of else None,
        token_budget=DEFAULT_CONFIG.token_budget,
    )
    runtime = AgentRuntime(root_run_ctx=root_ctx, force_model=force_model, force_provider=force_provider)

    if args.list_tools:
        _print_tools(runtime, args.agent)
        return

    provider = provider_from_str(args.provider) if args.provider else None

    agent = runtime.build_agent(
        args.agent, model_name=args.model, provider=provider, verbose=args.verbose
    )
    run_repl(agent)


if __name__ == "__main__":
    main()
