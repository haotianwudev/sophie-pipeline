"""
Runner for the SPX option-chain snapshot ETL.

Local:       poetry run python spx-option-snapshot/run.py
Dry run:     poetry run python spx-option-snapshot/run.py --dry-run
Cloud Run:   this file is the job's entry point (see services/spx-snapshot-etl/)

--dry-run fetches and computes everything but touches neither Postgres nor GCS, which makes it
the fastest way to check the feed is healthy and the metrics look sane before a real write.
"""

import sys
import json
import pathlib
import logging
import argparse

ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.agents.spx_option_snapshot import (  # noqa: E402
    run_etl,
    build_snapshot,
    is_trading_session,
)
from src.tools.api_cboe import fetch_cboe_spx_raw  # noqa: E402


def _dry_run() -> int:
    payload = fetch_cboe_spx_raw()
    snap = build_snapshot(payload)
    s = snap["summary"]

    print(f"\n  biz_date        {s['biz_date']}  (trading session: {is_trading_session(s['biz_date'])})")
    print(f"  spot            {s['spot']:,.2f}")
    print(f"  reference cycle {s['ref_expiration']}  ({s['ref_dte']} DTE)")
    print("\n  --- surface ---")
    for k in ("atm_iv", "put25_iv", "call25_iv", "rr25", "fly25", "normalized_skew"):
        v = s[k]
        print(f"  {k:16s}{'—' if v is None else f'{v:,.3f}'}")
    print("\n  --- term structure ---")
    for k in ("front_atm_iv", "back_atm_iv", "term_slope"):
        v = s[k]
        print(f"  {k:16s}{'—' if v is None else f'{v:,.2f}'}")
    print("\n  --- book ---")
    for k in ("pcr_volume", "pcr_oi", "total_volume", "total_open_interest",
              "net_gex_m", "call_wall", "put_wall"):
        v = s[k]
        print(f"  {k:20s}{'—' if v is None else f'{v:,.2f}'}")
    print("\n  --- volume ---")
    print(f"  expirations     {s['expiration_count']}")
    print(f"  contracts live  {s['contract_count']:,}")
    print(f"  slice stored    {len(snap['slice_rows']):,}  "
          f"({len(snap['slice_rows']) / max(1, len(snap['all_rows'])):.1%} of chain)")
    print("\n  nothing written (dry run)\n")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="SPX option-chain snapshot ETL")
    parser.add_argument("--dry-run", action="store_true",
                        help="fetch and compute, but write nothing")
    parser.add_argument("--force", action="store_true",
                        help="run even when the date is not an NYSE trading session")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.dry_run:
        return _dry_run()

    result = run_etl(force=args.force)
    print(json.dumps(result, indent=2, default=str))
    # A skipped holiday is a success, not a failure -- exiting non-zero would trip the job's
    # retry and alerting for something entirely expected.
    return 0 if result["status"] in ("ok", "skipped") else 1


if __name__ == "__main__":
    sys.exit(main())
