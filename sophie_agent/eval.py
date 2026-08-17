#!/usr/bin/env python
"""Golden-set eval harness — asserts on tool TRAJECTORY (which tools fired), not just final text.

Usage:
    poetry run python sophie-agent/eval.py                 # remote tier, per-case default models
    poetry run python sophie-agent/eval.py --all-local      # every case forced onto Ollama qwen3.5
    poetry run python sophie-agent/eval.py --cases path.yaml
"""

import argparse
import sys
import time
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.llm.models import ModelProvider  # noqa: E402
from sophie_agent import AgentRuntime  # noqa: E402

DEFAULT_CASES = ROOT / "test" / "agent_evals" / "cases.yaml"


def run_case(runtime: AgentRuntime, case: dict) -> dict:
    started = time.monotonic()
    try:
        agent = runtime.build_agent(case["profile"])
        result = agent.invoke(case["question"])
    except Exception as exc:
        return {"id": case["id"], "passed": False, "error": str(exc), "latency_s": time.monotonic() - started}

    fired = [action.tool for action, _ in result.get("intermediate_steps", [])]
    expected = case.get("expects_tools", [])
    if case.get("ordered"):
        # expected must appear as a (not-necessarily-contiguous) subsequence of fired, in order
        it = iter(fired)
        passed = all(t in it for t in expected)
    else:
        passed = set(expected).issubset(set(fired))

    return {
        "id": case["id"],
        "passed": passed,
        "expected_tools": expected,
        "fired_tools": fired,
        "latency_s": round(time.monotonic() - started, 2),
        "usage": runtime.root_run_ctx.usage.as_dict(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", default=str(DEFAULT_CASES))
    parser.add_argument("--all-local", action="store_true")
    args = parser.parse_args()

    cases = yaml.safe_load(Path(args.cases).read_text(encoding="utf-8"))

    force_model, force_provider = (None, None)
    if args.all_local:
        force_model, force_provider = "qwen3.5:latest", ModelProvider.OLLAMA

    results = []
    for case in cases:
        # Fresh runtime per case so usage/store don't bleed between independent evals.
        runtime = AgentRuntime(force_model=force_model, force_provider=force_provider)
        result = run_case(runtime, case)
        results.append(result)
        status = "PASS" if result["passed"] else "FAIL"
        print(f"[{status}] {result['id']} ({result.get('latency_s', '?')}s)")
        if not result["passed"]:
            print(f"       expected>={result.get('expected_tools')} fired={result.get('fired_tools', result.get('error'))}")

    n_passed = sum(r["passed"] for r in results)
    print(f"\n{n_passed}/{len(results)} passed.")
    sys.exit(0 if n_passed == len(results) else 1)


if __name__ == "__main__":
    main()
