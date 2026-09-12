"""Validate data/spx_chain_unified against the spx-option-chain-unify skill's
checklist: schema consistency, date coverage (vs. a freshly-pulled real NYSE
calendar, not the stale local spx_daily.parquet reference), boundary
continuity at both source seams, the open_interest honesty transition, and
the source-tag partition. Read-only -- writes nothing.

Deliberately avoids pyarrow.dataset()'s automatic cross-fragment schema
unification -- open_interest is real int32 in the live-Cboe files but
all-None (inferred as an Arrow null-typed column) in the OptionsDX/backfill
files, and letting pyarrow pick a single unified type across fragments hits
`ArrowNotImplementedError: Unsupported cast from int64 to null`. Reading each
file's needed columns directly with pandas and casting in Python sidesteps
that entirely, and is fast enough at 4,174 files for the columns each check needs.
"""

import pathlib
import sys
from datetime import date, timedelta

import pandas as pd

ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
from src.tools.spx_chain_schema import CANONICAL_COLUMNS  # noqa: E402

UNIFIED_ROOT = ROOT / "data" / "spx_chain_unified"


def list_files():
    return sorted(UNIFIED_ROOT.glob("year=*/month=*/day=*/chain.parquet"))


def fresh_trading_calendar(start: str, end: str) -> set:
    import yfinance as yf
    df = yf.download("^GSPC", start=start, end=end, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        idx = df["Close"]["^GSPC"].dropna().index
    else:
        idx = df["Close"].dropna().index
    return {ts.date() for ts in idx}


def check_schema(files):
    print("\n=== Schema consistency ===")
    cols = pd.read_parquet(files[0]).columns.tolist()
    missing = [c for c in CANONICAL_COLUMNS if c not in cols]
    extra = [c for c in cols if c not in CANONICAL_COLUMNS]
    print(f"Columns present (sample file): {cols}")
    print(f"Missing from canonical set: {missing or 'none'}")
    print(f"Extra beyond canonical set: {extra or 'none'}")

    # Spot-check column names match across a few files spanning all 3 sources
    for f in (files[0], files[len(files) // 2], files[-1]):
        c = pd.read_parquet(f).columns.tolist()
        assert c == cols, f"Schema mismatch in {f}: {c}"
    print("Column set identical across early/mid/late sample files: OK")


def per_day_summary(files):
    """One pass over all files, pulling only the light columns every later
    check needs -- avoids re-reading 4,174 files per check."""
    records = []
    for f in files:
        df = pd.read_parquet(f, columns=["biz_date", "source", "open_interest", "iv"])
        biz_date = pd.to_datetime(df["biz_date"].iloc[0]).date()
        source = df["source"].iloc[0]
        n_sources = df["source"].nunique()
        oi_nonzero = pd.to_numeric(df["open_interest"], errors="coerce").fillna(0).ne(0).sum()
        iv_pos = df.loc[df["iv"] > 0, "iv"]
        records.append({
            "biz_date": biz_date, "source": source, "n_sources": n_sources,
            "n_contracts": len(df), "oi_nonzero": oi_nonzero,
            "iv_median": iv_pos.median() if len(iv_pos) else float("nan"),
        })
    return pd.DataFrame(records)


def check_dates_and_source(summary: pd.DataFrame):
    print("\n=== Date coverage + source tagging ===")
    present_days = set(summary["biz_date"])
    print(f"Unique sessions in archive: {len(present_days)}")
    print(f"Range: {min(present_days)} -> {max(present_days)}")
    print(f"Sessions per source:\n{summary['source'].value_counts()}")
    print(f"Sessions with null source: {summary['source'].isna().sum()}")
    print(f"Days with >1 distinct source in one file (should be 0): {(summary['n_sources'] > 1).sum()}")

    print("\nPulling a fresh NYSE trading calendar via yfinance (not the stale local reference)...")
    cal = fresh_trading_calendar(
        (min(present_days) - timedelta(days=1)).isoformat(),
        (max(present_days) + timedelta(days=1)).isoformat(),
    )
    missing_sessions = sorted(cal - present_days)
    extra_sessions = sorted(present_days - cal)
    print(f"Real trading days missing from archive: {len(missing_sessions)}")
    if missing_sessions:
        print(f"  {missing_sessions}")
    print(f"Archive days NOT in the real calendar: {len(extra_sessions)}")
    if extra_sessions:
        print(f"  {extra_sessions}")


def check_open_interest_transition(summary: pd.DataFrame):
    print("\n=== open_interest honesty transition ===")
    pre = summary[summary["biz_date"] < date(2026, 8, 21)]
    post = summary[summary["biz_date"] >= date(2026, 8, 21)]
    print(f"Pre-2026-08-21: {pre['oi_nonzero'].sum()} nonzero-OI rows across {len(pre)} sessions (should be 0)")
    print(f"Post-2026-08-21 (live): {post['oi_nonzero'].sum()} nonzero-OI rows across {len(post)} sessions (should be most rows most days)")


def check_boundary_continuity(summary: pd.DataFrame):
    print("\n=== Boundary continuity ===")
    s = summary.set_index("biz_date").sort_index()
    for label, d1, d2 in [
        ("OptionsDX -> ThetaData backfill", date(2023, 12, 29), date(2024, 1, 2)),
        ("ThetaData backfill -> Cboe live", date(2026, 8, 20), date(2026, 8, 21)),
    ]:
        print(f"\n{label}:")
        for d in (d1, d2):
            if d in s.index:
                row = s.loc[d]
                print(f"  {d} [{row['source']}]: {row['n_contracts']:,} contracts, median iv={row['iv_median']:.4f}")
            else:
                print(f"  {d}: NO DATA FOUND")


def main():
    files = list_files()
    print(f"Found {len(files)} chain.parquet files")
    check_schema(files)
    summary = per_day_summary(files)
    check_dates_and_source(summary)
    check_open_interest_transition(summary)
    check_boundary_continuity(summary)


if __name__ == "__main__":
    main()
