"""
Volatility Regime / VRP — runner.

Step 1: refresh SPX + VIX daily bars in `prices` (trailing window, self-healing).
Step 2: recompute the VRP/regime signals and upsert to `vol_regime_data`.

Both steps are idempotent, so re-running after a missed day just backfills it.

Usage:
    cd F:/workspace/sophie-pipeline
    poetry run python vol-regime/run.py              # daily: trailing window
    poetry run python vol-regime/run.py --full       # rebuild all history
"""

import argparse
import pathlib
import subprocess
import sys
from datetime import datetime, timedelta

ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.agents.vol_regime import run_etl

# Prices refresh window. Wider than the regime write window so the rolling
# stats always have fresh inputs even after a few missed runs.
PRICE_LOOKBACK_DAYS = 10
# Regime rows to rewrite on a daily run. Signals are causal, so only recent
# rows can change; --full rewrites everything.
REGIME_LOOKBACK_DAYS = 30


def refresh_prices() -> None:
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=PRICE_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    cmd = [
        "poetry", "run", "python", "src/upload/raw_data_table_uploader.py",
        "--tickers", "SPX,VIX", "--table", "prices",
        "--start-date", start_date, "--end-date", end_date,
    ]
    print(f"Refreshing SPX/VIX prices ({start_date} -> {end_date})...")
    subprocess.run(cmd, check=True, cwd=ROOT)


def main() -> None:
    parser = argparse.ArgumentParser(description="Volatility regime / VRP ETL")
    parser.add_argument("--full", action="store_true",
                        help="Rewrite all history instead of the trailing window")
    parser.add_argument("--skip-prices", action="store_true",
                        help="Skip the SPX/VIX price refresh (signals only)")
    args = parser.parse_args()

    print("=" * 60)
    print("STEP 1: Refreshing SPX / VIX prices")
    print("=" * 60)
    if args.skip_prices:
        print("Skipped (--skip-prices)")
    else:
        refresh_prices()

    print()
    print("=" * 60)
    print("STEP 2: Computing volatility regime / VRP signals")
    print("=" * 60)
    run_etl(lookback_days=None if args.full else REGIME_LOOKBACK_DAYS)


if __name__ == "__main__":
    main()
