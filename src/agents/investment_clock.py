"""
Investment Clock ETL Agent

Fetches FRED macroeconomic data, computes exponential-weighted rolling Z-scores,
and determines the current Investment Clock phase
(Reflation / Recovery / Overheat / Stagflation).

Methodology:
  Growth composite  = 50% OECD CLI + 20% INDPRO + 15% inv. ICSA + 15% inv. UNRATE
  Inflation composite = 30% 5Y Breakeven (vs 2%) + 25% Core PCE YoY (vs 2%)
                      + 20% PPI Final Demand YoY (vs 2%) + 15% Core PCE MoM ann (vs 2%) + 10% TCU
  Normalization: EWM Z-score (span=24). Inflation components (except TCU) use
  target_anchored_z against the Fed's 2% goal; growth components stay relative to trend.

  Core PCE (PCEPILFE), not core CPI, drives the inflation composite: the FOMC's 2%
  target is defined on PCE, and core CPI carries a structural +0.3pp wedge over it
  (2000-present average), so anchoring core CPI to 2.0 would bake in a hawkish bias.
  Core CPI is still fetched and stored for display — it lands ~2 weeks earlier.

Run manually:
    python -m src.agents.investment_clock

Phase logic (Merrill Lynch Investment Clock):
    growth_z > 0, inflation_z < 0  -> Recovery
    growth_z > 0, inflation_z > 0  -> Overheat
    growth_z < 0, inflation_z > 0  -> Stagflation
    growth_z < 0, inflation_z < 0  -> Reflation
"""

import os
import math
import datetime
import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from colorama import Fore, Style

load_dotenv()

# ---------------------------------------------------------------------------
# FRED series to fetch
# ---------------------------------------------------------------------------
FRED_SERIES_IDS = [
    "USALOLITONOSTSAM",  # OECD CLI (monthly)
    "ICSA",              # Initial Jobless Claims (weekly)
    "INDPRO",            # Industrial Production Index (monthly)
    "UNRATE",            # Unemployment Rate (monthly)
    "CPILFESL",          # Core CPI (monthly, index level) — display only
    "PCEPILFE",          # Core PCE (monthly, index level) — drives inflation composite
    "TCU",               # Capacity Utilization (monthly)
    "T5YIE",             # 5-Year Breakeven Inflation Rate (daily)
    "PPIFID",            # PPI: Final Demand (monthly)
    "GDPC1",             # Real GDP (quarterly, for display only)
]

# ---------------------------------------------------------------------------
# Composite weights
# ---------------------------------------------------------------------------
GROWTH_WEIGHTS = {
    "USALOLITONOSTSAM": 0.50,   # Leading (3-6m forward)
    "ICSA_INV":         0.15,   # Leading labor (inverted)
    "INDPRO":           0.20,   # Coincident output
    "UNRATE_INV":       0.15,   # Lagging confirmation (inverted)
}

INFLATION_WEIGHTS = {
    "T5YIE":       0.30,   # Leading: market expectation vs 2% target
    "PCE_YOY":     0.25,   # Lagging confirmer vs 2% target (the FOMC's own gauge)
    "PPI_YOY":     0.20,   # Pipeline leading (leads CPI 2-6 months), anchored to 2% target
    "PCE_MOM_ANN": 0.15,   # Real-time inflection vs 2% target
    "TCU":         0.10,   # Capacity pressure
}

FED_TARGET = 2.0  # Fed's 2% inflation target — the neutral baseline

# FRED lookback. Needs to comfortably exceed the table's earliest row (2014-04), because
# the first ~24 months are consumed by the 12-month YoY lag plus the ewm min_periods=12
# warm-up and produce no composite. Too short a window silently leaves early rows holding
# whatever methodology last wrote them. Binding constraints on going further back are
# PPIFID (starts 2009) and T5YIE (2003).
LOOKBACK_YEARS = 15

PHASE_MAP = {
    (True, False):  "Recovery",    # growth above trend, inflation below trend
    (True, True):   "Overheat",    # growth above trend, inflation above trend
    (False, True):  "Stagflation", # growth below trend, inflation above trend
    (False, False): "Reflation",   # growth below trend, inflation below trend
}

PHASE_COLORS = {
    "Recovery":    "green",
    "Overheat":    "red",
    "Stagflation": "yellow",
    "Reflation":   "blue",
}


def get_db_connection():
    db_user = os.environ.get("DB_USER", "")
    db_password = os.environ.get("DB_PASSWORD", "")
    db_host = os.environ.get("DB_HOST", "")
    db_name = os.environ.get("DB_NAME", "")
    db_sslmode = os.environ.get("DB_SSLMODE", "require")
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}?sslmode={db_sslmode}"
    connection_string = os.environ.get("DATABASE_URL", connection_string)
    return psycopg2.connect(connection_string)


def fetch_fred_series(series_id: str, start_date: str, api_key: str) -> pd.Series:
    """Fetch a FRED series and return as a pandas Series indexed by date."""
    try:
        from fredapi import Fred
        fred = Fred(api_key=api_key)
        data = fred.get_series(series_id, observation_start=start_date)
        data.index = pd.to_datetime(data.index)
        return data.dropna()
    except Exception as e:
        print(f"{Fore.RED}Error fetching FRED series {series_id}: {e}{Style.RESET_ALL}")
        raise


def ewm_z_score(series: pd.Series, span: int = 24, min_periods: int = 12) -> pd.Series:
    """Exponential-weighted rolling Z-score. No end-point bias.
    NaN inputs (missing data) are forward-filled so the last valid Z-score carries
    forward rather than collapsing to 0 (at-the-mean)."""
    ewm_mean = series.ewm(span=span, min_periods=min_periods, ignore_na=True).mean()
    ewm_std  = series.ewm(span=span, min_periods=min_periods, ignore_na=True).std()
    z = (series - ewm_mean) / ewm_std.replace(0, float('nan'))
    return z.ffill().fillna(0)


def target_anchored_z(series: pd.Series, target: float,
                      span: int = 24, min_periods: int = 12) -> pd.Series:
    """Z-score measured against a FIXED target rather than the series' own moving mean.

    Only the scale (EWM std) is rolling; the centre is pinned to `target`. This is the
    difference between "inflation vs its own recent average" and "inflation vs the Fed's
    2% goal" — the latter is what the Investment Clock quadrants actually mean.

    Do NOT substitute ewm_z_score(series - target): subtracting a constant before an
    EWM Z-score is algebraically a no-op, because ewm_mean(x - c) == ewm_mean(x) - c and
    ewm_std(x - c) == ewm_std(x), so the constant cancels. A series parked at 3.4% for
    two years scores z ~= 0 under that formulation, hiding persistent inflation entirely.
    """
    ewm_std = series.ewm(span=span, min_periods=min_periods, ignore_na=True).std()
    z = (series - target) / ewm_std.replace(0, float('nan'))
    return z.ffill().fillna(0)


def compute_cpi_yoy(cpi: pd.Series) -> pd.Series:
    """12-month percent change of CPI index level.

    fill_method=None is required: pct_change pads NaN internally by default, which
    would compare a stale index level against a 12-month-old one and report the result
    as a fresh YoY. An unreleased month must stay NaN so callers can carry the last
    genuine reading forward instead of inventing a new one.
    """
    return cpi.pct_change(12, fill_method=None) * 100


def compute_cpi_mom_annualized(cpi: pd.Series) -> pd.Series:
    """Compound-annualized month-over-month CPI change.
    Unreleased months are NaN (see compute_cpi_yoy on fill_method)."""
    mom = cpi.pct_change(1, fill_method=None)
    return ((1 + mom) ** 12 - 1) * 100


def clock_angle_from_z_scores(growth_z: float, inflation_z: float) -> float:
    """
    Map (growth_z, inflation_z) Cartesian coordinates to clock angle degrees.

    Convention (Merrill Lynch clock, clockwise from 12 o'clock):
      12 o'clock (0°)   = top of Recovery quadrant (high growth, zero inflation)
      3 o'clock (90°)   = top of Overheat quadrant (high inflation, zero growth)
      6 o'clock (180°)  = bottom of Stagflation quadrant (low growth, low inflation border)
      9 o'clock (270°)  = top of Reflation quadrant (low inflation, low growth)

    We use atan2(growth_z, inflation_z) so that growth maps to the y-axis (north = 12
    o'clock) and inflation maps to the x-axis (east = 3 o'clock), matching the Merrill
    Lynch convention where Recovery (growth↑, inflation low) is at 9-12 o'clock.
    """
    math_angle_rad = math.atan2(growth_z, inflation_z)
    math_angle_deg = math.degrees(math_angle_rad)
    clock_angle = (90 - math_angle_deg) % 360
    return round(clock_angle, 4)


def safe_float(series: pd.Series, idx) -> float | None:
    """Extract a float from a pandas Series, returning None for NaN/missing."""
    val = series.get(idx)
    if val is None:
        return None
    try:
        f = float(val)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def run_etl(backfill: bool = False):
    """
    Main ETL function. Fetches FRED data, computes EWM Z-scores,
    determines phases and clock angles, then upserts all monthly rows to DB.

    backfill=False (default): closed months keep their stored Z-scores/phase.
    backfill=True: recompute and overwrite every month. Only for methodology changes.
    """
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        raise ValueError("FRED_API_KEY environment variable not set")

    # see LOOKBACK_YEARS — needs ~2yr warm-up for ewm(span=24) before the first usable row
    start_date = (datetime.date.today()
                  - datetime.timedelta(days=365 * LOOKBACK_YEARS)).strftime("%Y-%m-%d")

    print(f"{Fore.CYAN}Fetching FRED series...{Style.RESET_ALL}")

    raw = {}
    for sid in FRED_SERIES_IDS:
        print(f"  Fetching {sid}...")
        raw[sid] = fetch_fred_series(sid, start_date, api_key)

    # Resample to monthly (end of month)
    print(f"{Fore.CYAN}Resampling to monthly...{Style.RESET_ALL}")
    monthly = {}
    for sid, series in raw.items():
        if sid == "ICSA":
            monthly[sid] = series.resample("ME").mean()          # weekly → monthly average
        elif sid == "T5YIE":
            monthly[sid] = series.resample("ME").mean()          # daily → monthly average
        elif sid == "GDPC1":
            monthly[sid] = series.resample("ME").last().ffill()  # quarterly → monthly ffill
        else:
            monthly[sid] = series.resample("ME").last().ffill()

    # `reported` leaves each series NaN in months it has not published yet; `combined`
    # forward-fills those gaps for display. Year-over-year signals must be derived from
    # `reported`: computing them from the forward-filled frame compares a stale index
    # level against one a full 12 months old, manufacturing a move that never happened.
    # (e.g. Sept 2026 core CPI YoY read 2.22% off August's ffilled index vs 2.45% actual.)
    reported = pd.DataFrame(monthly).dropna(how="all")
    combined = reported.ffill()

    # --- Derived signals (computed pre-ffill, then carried forward) ---
    # Core PCE feeds the composite; core CPI is carried for display only.
    pce_yoy = compute_cpi_yoy(reported["PCEPILFE"]).ffill()
    pce_mom_ann = compute_cpi_mom_annualized(reported["PCEPILFE"]).ffill()
    cpi_yoy = compute_cpi_yoy(reported["CPILFESL"]).ffill()
    cpi_mom_ann = compute_cpi_mom_annualized(reported["CPILFESL"]).ffill()
    indpro_yoy = (reported["INDPRO"].pct_change(12, fill_method=None) * 100).ffill()
    icsa_yoy = (reported["ICSA"].pct_change(12, fill_method=None) * 100).ffill()
    unrate_diff = reported["UNRATE"].diff(12).ffill()   # diff() does not pad
    ppi_yoy = (reported["PPIFID"].pct_change(12, fill_method=None) * 100).ffill()

    # --- EWM Z-scores ---
    print(f"{Fore.CYAN}Computing EWM Z-scores...{Style.RESET_ALL}")

    # GROWTH: CLI is pre-normalized by OECD (100 = long-run trend).
    # Use ewm_z_score on (CLI-100) for consistency with all other components —
    # avoids full-sample std rescaling all historical values each run.
    cli_deviation = combined["USALOLITONOSTSAM"] - 100
    cli_z = ewm_z_score(cli_deviation)

    # GROWTH components stay relative (vs own trend) — "above/below trend" is the
    # meaningful question for growth, and none of them has a fixed policy target.
    # INFLATION components are anchored to the Fed's 2% target via target_anchored_z,
    # so a persistently-above-target print stays positive instead of being absorbed
    # into a drifting mean. TCU is excluded: capacity utilisation has no 2% analogue.
    z = {
        # Growth components
        "USALOLITONOSTSAM": cli_z,
        "ICSA_INV":         ewm_z_score(-icsa_yoy),
        "INDPRO":           ewm_z_score(indpro_yoy),
        "UNRATE_INV":       ewm_z_score(-unrate_diff),
        # Inflation components — all anchored to 2% target
        "T5YIE":            target_anchored_z(combined["T5YIE"], FED_TARGET),
        "PCE_YOY":          target_anchored_z(pce_yoy, FED_TARGET),
        "PPI_YOY":          target_anchored_z(ppi_yoy, FED_TARGET),
        "PCE_MOM_ANN":      target_anchored_z(pce_mom_ann, FED_TARGET),
        "TCU":              ewm_z_score(combined["TCU"]),
    }

    # --- Warm-up gate ---
    # ewm(min_periods=12) yields no std until 12 observations exist, and the YoY signals
    # burn a further 12 months. Both z helpers end in .fillna(0), which turns that silence
    # into a confident "exactly at trend" — the origin of the flat-line rows at the start
    # of the chart. Publish only from the first month every component is genuinely defined.
    def first_defined(series: pd.Series) -> pd.Timestamp | None:
        return series.ewm(span=24, min_periods=12, ignore_na=True).std().first_valid_index()

    warmup_inputs = [cli_deviation, indpro_yoy, icsa_yoy, unrate_diff,
                     pce_yoy, ppi_yoy, pce_mom_ann, combined["T5YIE"], combined["TCU"]]
    starts = [d for d in (first_defined(s) for s in warmup_inputs) if d is not None]
    first_publishable = max(starts) if starts else None
    if first_publishable is not None:
        print(f"  warm-up complete at {first_publishable.date()} — "
              f"earlier months will not be written")

    # --- Composite scores ---
    growth_z = sum(z[k] * w for k, w in GROWTH_WEIGHTS.items())
    inflation_z = sum(z[k] * w for k, w in INFLATION_WEIGHTS.items())

    # --- Upsert to DB ---
    print(f"{Fore.CYAN}Upserting to database...{Style.RESET_ALL}")

    conn = get_db_connection()
    cursor = conn.cursor()

    # Z-scores, phase, and clock angle are frozen once a month closes.
    # Only the current calendar month's row gets updated composite values —
    # this prevents new FRED releases from retroactively shifting prior months
    # via EWM recalculation (the root cause of Stagflation→Overheat flips in the UI).
    # Raw indicator values are always updated to capture FRED revisions.
    #
    # backfill=True lifts the freeze and rewrites every month. Reserved for methodology
    # changes, where the stored history was produced by superseded maths and leaving it
    # in place would mean the chart splices two incompatible definitions together.
    if backfill:
        freeze = "EXCLUDED.{col}"
    else:
        freeze = (
            "CASE WHEN date_trunc('month', investment_clock_data.biz_date) "
            ">= date_trunc('month', CURRENT_DATE) "
            "THEN EXCLUDED.{col} ELSE investment_clock_data.{col} END"
        )

    upsert_sql = f"""
        INSERT INTO investment_clock_data (
            biz_date,
            growth_z_score, inflation_z_score,
            data_phase, clock_angle,
            gdp_value, cpi_value, indpro_value, tcu_value, unrate_value,
            cli_value, icsa_value, cpi_yoy, cpi_mom_ann,
            t5yie_value, ppi_yoy,
            pce_value, pce_yoy, pce_mom_ann
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        ON CONFLICT (biz_date) DO UPDATE SET
            growth_z_score    = {freeze.format(col='growth_z_score')},
            inflation_z_score = {freeze.format(col='inflation_z_score')},
            data_phase        = {freeze.format(col='data_phase')},
            clock_angle       = {freeze.format(col='clock_angle')},
            gdp_value         = EXCLUDED.gdp_value,
            cpi_value         = EXCLUDED.cpi_value,
            indpro_value      = EXCLUDED.indpro_value,
            tcu_value         = EXCLUDED.tcu_value,
            unrate_value      = EXCLUDED.unrate_value,
            cli_value         = EXCLUDED.cli_value,
            icsa_value        = EXCLUDED.icsa_value,
            cpi_yoy           = EXCLUDED.cpi_yoy,
            cpi_mom_ann       = EXCLUDED.cpi_mom_ann,
            t5yie_value       = EXCLUDED.t5yie_value,
            ppi_yoy           = EXCLUDED.ppi_yoy,
            pce_value         = EXCLUDED.pce_value,
            pce_yoy           = EXCLUDED.pce_yoy,
            pce_mom_ann       = EXCLUDED.pce_mom_ann
    """

    rows_upserted = 0
    common_index = growth_z.dropna().index.intersection(inflation_z.dropna().index)

    for date in common_index:
        if first_publishable is not None and date < first_publishable:
            continue  # still inside the EWM/YoY warm-up — z would be a fillna(0) artefact
        g = float(growth_z.get(date, float("nan")))
        i = float(inflation_z.get(date, float("nan")))
        if math.isnan(g) or math.isnan(i):
            continue

        phase = PHASE_MAP[(g > 0, i > 0)]
        angle = clock_angle_from_z_scores(g, i)

        cursor.execute(upsert_sql, (
            date.date(),
            round(g, 4),
            round(i, 4),
            phase,
            angle,
            safe_float(combined.get("GDPC1", pd.Series(dtype=float)), date),
            safe_float(combined.get("CPILFESL", pd.Series(dtype=float)), date),
            safe_float(combined.get("INDPRO", pd.Series(dtype=float)), date),
            safe_float(combined.get("TCU", pd.Series(dtype=float)), date),
            safe_float(combined.get("UNRATE", pd.Series(dtype=float)), date),
            safe_float(combined.get("USALOLITONOSTSAM", pd.Series(dtype=float)), date),
            safe_float(combined.get("ICSA", pd.Series(dtype=float)), date),
            safe_float(cpi_yoy, date),
            safe_float(cpi_mom_ann, date),
            safe_float(combined.get("T5YIE", pd.Series(dtype=float)), date),
            safe_float(ppi_yoy, date),
            safe_float(combined.get("PCEPILFE", pd.Series(dtype=float)), date),
            safe_float(pce_yoy, date),
            safe_float(pce_mom_ann, date),
        ))
        rows_upserted += 1

    conn.commit()
    cursor.close()
    conn.close()

    # Print latest reading
    latest_date = common_index[-1]
    g_latest = float(growth_z[latest_date])
    i_latest = float(inflation_z[latest_date])
    phase_latest = PHASE_MAP[(g_latest > 0, i_latest > 0)]
    angle_latest = clock_angle_from_z_scores(g_latest, i_latest)

    cli_val     = safe_float(combined["USALOLITONOSTSAM"], latest_date)
    icsa_val    = safe_float(combined["ICSA"], latest_date)
    t5yie_val   = safe_float(combined["T5YIE"], latest_date)
    cpi_yoy_val = safe_float(cpi_yoy, latest_date)
    cpi_mom_val = safe_float(cpi_mom_ann, latest_date)
    ppi_yoy_val = safe_float(ppi_yoy, latest_date)
    pce_yoy_val = safe_float(pce_yoy, latest_date)
    pce_mom_val = safe_float(pce_mom_ann, latest_date)

    print(f"\n{Fore.GREEN}Done! Upserted {rows_upserted} rows.{Style.RESET_ALL}")
    print(f"\nLatest reading ({latest_date.date()}):")
    print(f"  Growth Z-score:    {g_latest:+.4f}")
    print(f"  Inflation Z-score: {i_latest:+.4f}")
    print(f"  Phase:             {phase_latest}")
    print(f"  Clock angle:       {angle_latest}\u00b0")
    print(f"\n  OECD CLI:          {cli_val:.2f}" if cli_val else "  OECD CLI:          N/A")
    print(f"  Jobless Claims:    {icsa_val:.0f}" if icsa_val else "  Jobless Claims:    N/A")
    print(f"  5Y Breakeven:      {t5yie_val:.2f}%" if t5yie_val else "  5Y Breakeven:      N/A")
    print(f"  Core PCE YoY:      {pce_yoy_val:.2f}%  <- drives composite"
          if pce_yoy_val else "  Core PCE YoY:      N/A")
    print(f"  Core PCE MoM Ann:  {pce_mom_val:.2f}%" if pce_mom_val else "  Core PCE MoM Ann:  N/A")
    print(f"  Core CPI YoY:      {cpi_yoy_val:.2f}%  (display only)"
          if cpi_yoy_val else "  Core CPI YoY:      N/A")
    print(f"  PPI YoY:           {ppi_yoy_val:.2f}%" if ppi_yoy_val else "  PPI YoY:           N/A")
    print(f"  Core CPI MoM Ann:  {cpi_mom_val:.2f}%" if cpi_mom_val else "  Core CPI MoM Ann:  N/A")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Investment Clock ETL")
    parser.add_argument(
        "--backfill", action="store_true",
        help="Recompute and overwrite Z-scores/phase for ALL months, lifting the "
             "closed-month freeze. Use only after a methodology change.")
    run_etl(backfill=parser.parse_args().backfill)
