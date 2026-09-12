"""Reshape the ThetaData gap-backfill archive (data/spx_chain_archive, 2024-01-02
-> 2026-08-20) into the canonical unified schema (data/spx_chain_unified). Pure
column mapping -- IV/greeks were already solved via the same Black-Scholes
engine (backfill.py imports src.tools.options_math, same module the OptionsDX
re-derivation uses), so nothing gets recomputed here.

Per the spx-option-chain-unify skill: backfill's `symbol` field is actually the
root (SPX/SPXW) and its synthetic `option` field does NOT match Cboe's real OSI
symbol format, so `option` is dropped and canonical `symbol` stays null for
this whole range -- do not carry the non-matching format forward.
"""

import pathlib
import re
import sys
from datetime import date

import pandas as pd

ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.tools.spx_chain_schema import write_canonical_chain, SOURCE_THETADATA_BACKFILL  # noqa: E402

SRC_ROOT = ROOT / "data" / "spx_chain_archive"
OUT_ROOT = ROOT / "data" / "spx_chain_unified"

PATH_RE = re.compile(r"year=(\d+)[\\/]month=(\d+)[\\/]day=(\d+)")


def parse_biz_date(path: pathlib.Path) -> date:
    m = PATH_RE.search(str(path))
    if not m:
        raise ValueError(f"Can't parse biz_date from path: {path}")
    y, mo, d = (int(x) for x in m.groups())
    return date(y, mo, d)


def reshape_one(src_file: pathlib.Path) -> int:
    biz_date = parse_biz_date(src_file)
    df = pd.read_parquet(src_file)

    out = pd.DataFrame({
        "biz_date": biz_date,
        "root": df["symbol"],  # backfill's "symbol" col is actually the root (SPX/SPXW)
        "symbol": None,  # backfill's synthetic `option` field doesn't match Cboe OSI format -- drop, don't carry forward
        "expiration": df["expiration"],
        "strike": df["strike"],
        "type": df["type"],
        "dte": df["dte"],
        "bid": df["bid"],
        "ask": df["ask"],
        "last": df["close"],
        "iv": df["iv"],
        "delta": df["delta"],
        "gamma": df["gamma"],
        "theta": df["theta"],
        "vega": df["vega"],
        "rho": None,
        "volume": df["volume"],
        "open_interest": df["open_interest"],
        "theo": None,
        "change": None,
        "percent_change": None,
        "prev_day_close": None,
        "last_trade_time": None,
        "source": SOURCE_THETADATA_BACKFILL,
    })

    rows = out.to_dict(orient="records")
    write_canonical_chain(biz_date, rows, OUT_ROOT)
    return len(rows)


def main():
    files = sorted(SRC_ROOT.glob("year=*/month=*/day=*/chain.parquet"))
    print(f"Found {len(files)} backfill chain files to reshape")
    total = 0
    for i, f in enumerate(files, 1):
        n = reshape_one(f)
        total += n
        if i % 50 == 0 or i == len(files):
            print(f"[{i}/{len(files)}] {f} -> {n} rows")
    print(f"BACKFILL RESHAPE COMPLETE: {len(files)} days, {total:,} total contract rows")


if __name__ == "__main__":
    main()
