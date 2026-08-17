#!/usr/bin/env python
"""Entrypoint for the AG-UI server — LOCAL-ONLY. Binds 127.0.0.1, never 0.0.0.0.

Usage (from the sophie-pipeline root):
    poetry run python sophie-agent/serve.py
    poetry run python sophie-agent/serve.py --port 8001
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import uvicorn  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()

    uvicorn.run(
        "src.sophie_agent.server:app",
        host="127.0.0.1",  # never 0.0.0.0 — see docs/SOPHIE_AGENT.md's Phase 2 section
        port=args.port,
        reload=args.reload,
        app_dir=str(ROOT),
    )


if __name__ == "__main__":
    main()
