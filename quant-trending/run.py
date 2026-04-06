"""
Quant Trending runner.
Usage: cd F:/workspace/sophie-pipeline && poetry run python quant-trending/run.py
"""

import sys
import pathlib

ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.agents.quant_trending import run_etl

if __name__ == "__main__":
    run_etl()
