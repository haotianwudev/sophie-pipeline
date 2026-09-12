"""Reshape the live Cboe ETL's GCS chain archive (pulled locally to
data/live_daily_gcs, 2026-08-21+) into the canonical unified schema
(data/spx_chain_unified). Pure column mapping -- Cboe's own IV/greeks are left
untouched (spx-option-chain-unify skill, decision point 2: never recompute the
live feed's greeks locally, production already trusts Cboe's numbers).

biz_date isn't a column in the source files -- derived from the filename
(spx_chain_YYYY-MM-DD.parquet), the only place it's encoded.
"""

import pathlib
import re
import sys
from datetime import date

import pandas as pd

ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.tools.spx_chain_schema import write_canonical_chain, SOURCE_CBOE_LIVE  # noqa: E402

SRC_ROOT = ROOT / "data" / "live_daily_gcs"
OUT_ROOT = ROOT / "data" / "spx_chain_unified"

FNAME_RE = re.compile(r"spx_chain_(\d{4})-(\d{2})-(\d{2})\.parquet$")


def parse_biz_date(path: pathlib.Path) -> date:
    m = FNAME_RE.search(path.name)
    if not m:
        raise ValueError(f"Can't parse biz_date from filename: {path.name}")
    y, mo, d = (int(x) for x in m.groups())
    return date(y, mo, d)


def reshape_one(src_file: pathlib.Path) -> int:
    biz_date = parse_biz_date(src_file)
    df = pd.read_parquet(src_file)

    expiration_d = pd.to_datetime(df["expiration"]).dt.date
    dte = expiration_d.apply(lambda d: (d - biz_date).days)

    out = pd.DataFrame({
        "biz_date": biz_date,
        "root": df["root"],
        "symbol": df["symbol"],  # real Cboe OSI-format symbol
        "expiration": df["expiration"],
        "strike": df["strike"],
        "type": df["type"],
        "dte": dte,
        "bid": df["bid"],
        "ask": df["ask"],
        "last": df["last"],
        "iv": df["iv"],
        "delta": df["delta"],
        "gamma": df["gamma"],
        "theta": df["theta"],
        "vega": df["vega"],
        "rho": df["rho"],
        "volume": df["volume"],
        "open_interest": df["open_interest"],
        "theo": df["theo"],
        "change": df["change"],
        "percent_change": df["percent_change"],
        "prev_day_close": df["prev_day_close"],
        "last_trade_time": df["last_trade_time"],
        "source": SOURCE_CBOE_LIVE,
    })

    rows = out.to_dict(orient="records")
    write_canonical_chain(biz_date, rows, OUT_ROOT)
    return len(rows)


def main():
    files = sorted(SRC_ROOT.rglob("spx_chain_*.parquet"))
    print(f"Found {len(files)} live GCS chain files to reshape")
    total = 0
    for i, f in enumerate(files, 1):
        n = reshape_one(f)
        total += n
        print(f"[{i}/{len(files)}] {f.name} -> {n} rows")
    print(f"LIVE GCS RESHAPE COMPLETE: {len(files)} days, {total:,} total contract rows")


if __name__ == "__main__":
    main()
