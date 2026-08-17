"""A minimal REPL over a SophieAgent."""

from __future__ import annotations

import uuid

from ..core.agent import SophieAgent

_HELP = "Commands: /reset, /tools, /history, /prompt, /usage, exit."


def run_repl(agent: SophieAgent) -> None:
    # One thread per REPL session: conversation state lives in the agent's checkpointer under this
    # id, so history (tool messages included) survives across turns without being tracked here.
    thread_id = f"cli-{uuid.uuid4().hex[:8]}"
    print(f"Sophie agent '{agent.name}' ready. {_HELP}")
    while True:
        try:
            line = input("\n> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not line:
            continue
        if line in ("exit", "quit", "/quit"):
            break
        if line == "/reset":
            agent.reset(thread_id)
            print("(history cleared)")
            continue
        if line == "/tools":
            for t in agent.tools:
                print(f"- {t.name}: {t.description.splitlines()[0]}")
            continue
        if line == "/history":
            for m in agent.history(thread_id):
                content = m.content if isinstance(m.content, str) else str(m.content)
                print(f"[{m.type}] {content[:2000]}")
            continue
        if line == "/prompt":
            print(agent.preview_system_prompt())
            continue
        if line == "/usage":
            print(agent.default_context.run_ctx.usage.as_dict())
            continue
        try:
            answer = agent.chat(line, thread_id=thread_id)
        except Exception as exc:
            print(f"Error: {exc}")
            continue
        print(answer)
