"""A minimal REPL over a SophieAgent."""

from __future__ import annotations

from .agent import SophieAgent


def run_repl(agent: SophieAgent) -> None:
    print(f"Sophie agent '{agent.name}' ready. Commands: /reset, /tools, /history, exit.")
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
            agent.reset()
            print("(history cleared)")
            continue
        if line == "/tools":
            for t in agent.tools:
                print(f"- {t.name}: {t.description.splitlines()[0]}")
            continue
        if line == "/history":
            for m in agent.chat_history:
                print(f"[{m.type}] {m.content}")
            continue
        try:
            answer = agent.chat(line)
        except Exception as exc:
            print(f"Error: {exc}")
            continue
        print(answer)
