"""
Investment Clock — combined runner.
Step 1: Fetch FRED data, apply HP filter, upsert to investment_clock_data.
Step 2: Generate Gemini Deep Research prompt and save to investment-clock/prompts/.

Usage:
    cd F:/workspace/sophie-pipeline
    poetry run python investment-clock/run.py
"""

import sys
import pathlib

ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.agents.investment_clock import run_etl

# Load generate_prompt from the same directory as this script
import importlib.util
_spec = importlib.util.spec_from_file_location(
    "generate_prompt",
    pathlib.Path(__file__).parent / "generate_prompt.py"
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
generate_prompt = _mod.generate_prompt
save_and_print = _mod.save_and_print


def main():
    print("=" * 60)
    print("STEP 1: Running FRED ETL + HP Filter")
    print("=" * 60)
    run_etl()

    print()
    print("=" * 60)
    print("STEP 2: Generating Gemini Deep Research Prompt")
    print("=" * 60)
    prompt, today = generate_prompt()
    save_and_print(prompt, today)

    print()
    print("Next step: paste the prompt into Gemini Deep Research,")
    print(f"then save the result to investment-clock/prompts/Result-{today}.md")
    print(f"and run: /investment-clock-analyze F:/workspace/sophie-pipeline/investment-clock/prompts/Result-{today}.md")


if __name__ == "__main__":
    main()
