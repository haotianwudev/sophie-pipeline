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
import sys
from datetime import datetime, timedelta

ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.agents.vol_regime import run_etl
from src.tools.db_upload import upload_prices

# Prices refresh window. Wider than the regime write window so the rolling
# stats always have fresh inputs even after a few missed runs.
PRICE_LOOKBACK_DAYS = 10
# Regime rows to rewrite on a daily run. Signals are causal, so only recent
# rows can change; --full rewrites everything.
REGIME_LOOKBACK_DAYS = 30


def refresh_prices() -> None:
    """Refresh SPX/VIX daily bars in `prices`.

    Calls upload_prices() directly rather than shelling out to
    `poetry run python src/upload/raw_data_table_uploader.py --table prices`. That CLI resolves
    to this same function via TABLE_UPLOAD_CONFIG, so the work is identical, but the subprocess
    form assumed a Poetry environment on PATH -- which a container does not have -- and dragged
    in the uploader's full import surface (company facts, news, insider trades, line items) for
    a job that fetches two index series. In-process keeps local and cloud on one code path.
    """
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=PRICE_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    print(f"Refreshing SPX/VIX prices ({start_date} -> {end_date})...")

    # Pinned to yfinance rather than left on the 'auto' waterfall. Auto already hardcodes
    # SPX/VIX/SPY/VVIX to Yahoo, so this changes nothing about which source is used -- but it
    # removes the fallback branches into Polygon and Financial Datasets, which need API keys
    # this job is not given and whose modules the container image does not carry. Better to fail
    # loudly on a Yahoo outage than to silently reach for a paid API that cannot authenticate.
    result = upload_prices(["SPX", "VIX"], start_date, end_date, verbose=True,
                           data_source="yfinance")

    # upload_prices() reports per-ticker outcomes rather than raising, so a partial failure would
    # otherwise sail through into step 2 and compute a regime off a stale tape.
    failed = result.get("failed") or []
    if failed:
        raise RuntimeError(f"Price refresh failed for: {', '.join(failed)}")


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
