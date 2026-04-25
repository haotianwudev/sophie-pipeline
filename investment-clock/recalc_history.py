"""
Point-in-time recalculation of Investment Clock Z-scores.

For each closed month M already in the DB, recomputes growth_z / inflation_z / phase
using FRED data truncated to M (i.e. only data available at month-end M).
This eliminates retroactive EWM drift caused by later FRED releases/revisions.

The current month (date_trunc = today's month) is left unchanged — the regular
ETL owns that row.

Run once:
    poetry run python investment-clock/recalc_history.py
"""

import sys
import math
import datetime
import pathlib

import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.tools.api_db import get_db_connection
from src.agents.investment_clock import (
    FRED_SERIES_IDS,
    GROWTH_WEIGHTS,
    INFLATION_WEIGHTS,
    FED_TARGET,
    PHASE_MAP,
    fetch_fred_series,
    ewm_z_score,
    compute_cpi_yoy,
    compute_cpi_mom_annualized,
    clock_angle_from_z_scores,
    safe_float,
)

import os
from dotenv import load_dotenv
load_dotenv()


def build_monthly_df(raw: dict) -> pd.DataFrame:
    """Resample all FRED series to month-end, same logic as ETL."""
    monthly = {}
    for sid, series in raw.items():
        if sid in ("ICSA", "T5YIE"):
            monthly[sid] = series.resample("ME").mean()
        elif sid == "GDPC1":
            monthly[sid] = series.resample("ME").last().ffill()
        else:
            monthly[sid] = series.resample("ME").last().ffill()
    return pd.DataFrame(monthly).dropna(how="all").ffill()


def compute_z_scores(combined: pd.DataFrame):
    """Return (growth_z, inflation_z) Series using the same EWM formula as ETL."""
    cpi_yoy     = compute_cpi_yoy(combined["CPILFESL"])
    cpi_mom_ann = compute_cpi_mom_annualized(combined["CPILFESL"])
    indpro_yoy  = combined["INDPRO"].pct_change(12) * 100
    icsa_yoy    = combined["ICSA"].pct_change(12) * 100
    unrate_diff = combined["UNRATE"].diff(12)
    ppi_yoy     = combined["PPIFID"].pct_change(12) * 100

    cli_deviation = combined["USALOLITONOSTSAM"] - 100

    z = {
        "USALOLITONOSTSAM": ewm_z_score(cli_deviation),
        "ICSA_INV":         ewm_z_score(-icsa_yoy),
        "INDPRO":           ewm_z_score(indpro_yoy),
        "UNRATE_INV":       ewm_z_score(-unrate_diff),
        "T5YIE":            ewm_z_score(combined["T5YIE"] - FED_TARGET),
        "CPI_YOY":          ewm_z_score(cpi_yoy - FED_TARGET),
        "PPI_YOY":          ewm_z_score(ppi_yoy),
        "CPI_MOM_ANN":      ewm_z_score(cpi_mom_ann - FED_TARGET),
        "TCU":              ewm_z_score(combined["TCU"]),
    }

    growth_z    = sum(z[k] * w for k, w in GROWTH_WEIGHTS.items())
    inflation_z = sum(z[k] * w for k, w in INFLATION_WEIGHTS.items())
    return growth_z, inflation_z


def run():
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        raise ValueError("FRED_API_KEY not set")

    start_date = (datetime.date.today() - datetime.timedelta(days=365 * 10)).strftime("%Y-%m-%d")
    current_month = pd.Timestamp(datetime.date.today()).to_period("M").to_timestamp("M")

    # Fetch all FRED data once
    print("Fetching FRED data...")
    raw = {}
    for sid in FRED_SERIES_IDS:
        print(f"  {sid}...")
        raw[sid] = fetch_fred_series(sid, start_date, api_key)

    # Get closed months from DB (everything except current month)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT biz_date FROM investment_clock_data
        WHERE date_trunc('month', biz_date) < date_trunc('month', CURRENT_DATE)
        ORDER BY biz_date
    """)
    closed_dates = [r[0] for r in cur.fetchall()]
    print(f"\nFound {len(closed_dates)} closed months to recalculate.")

    updated = 0
    skipped = 0

    for biz_date in closed_dates:
        cutoff = pd.Timestamp(biz_date)  # month-end date = data cutoff

        # Truncate all series to data available at this month's end
        raw_cut = {sid: s[s.index <= cutoff] for sid, s in raw.items()}

        # Need at least 2 years of data for EWM warm-up
        if len(raw_cut.get("CPILFESL", pd.Series())) < 24:
            skipped += 1
            continue

        combined = build_monthly_df(raw_cut)
        if cutoff not in combined.index:
            skipped += 1
            continue

        growth_z_series, inflation_z_series = compute_z_scores(combined)

        g = safe_float(growth_z_series, cutoff)
        i = safe_float(inflation_z_series, cutoff)
        if g is None or i is None or math.isnan(g) or math.isnan(i):
            skipped += 1
            continue

        phase = PHASE_MAP[(g > 0, i > 0)]
        angle = clock_angle_from_z_scores(g, i)

        cur.execute("""
            UPDATE investment_clock_data
            SET growth_z_score    = %s,
                inflation_z_score = %s,
                data_phase        = %s,
                clock_angle       = %s
            WHERE biz_date = %s
        """, (round(g, 4), round(i, 4), phase, angle, biz_date))

        print(f"  {biz_date}  growth_z={g:+.4f}  inflation_z={i:+.4f}  {phase}")
        updated += cur.rowcount

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nDone. {updated} rows updated, {skipped} skipped.")


if __name__ == "__main__":
    run()
