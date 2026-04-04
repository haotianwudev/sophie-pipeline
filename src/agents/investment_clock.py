"""
Investment Clock ETL Agent

Fetches FRED macroeconomic data, applies the Hodrick-Prescott filter to extract
cyclical components, computes composite Z-scores, and determines the current
Investment Clock phase (Reflation / Recovery / Overheat / Stagflation).

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
import json
import datetime
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from colorama import Fore, Style

load_dotenv()

# ---------------------------------------------------------------------------
# FRED series configuration
# lambda: HP filter smoothing parameter (1600 for quarterly, 14400 for monthly)
# weight_growth / weight_inflation: how much this series contributes to composite Z-score
# invert: multiply cycle by -1 (unemployment is inverse of growth)
# ---------------------------------------------------------------------------
FRED_SERIES = {
    "GDPC1":    {"lambda": 1600,  "freq": "quarterly", "weight_growth": 0.40, "weight_inflation": 0.0,  "invert": False},
    "INDPRO":   {"lambda": 14400, "freq": "monthly",   "weight_growth": 0.35, "weight_inflation": 0.0,  "invert": False},
    "UNRATE":   {"lambda": 14400, "freq": "monthly",   "weight_growth": 0.25, "weight_inflation": 0.0,  "invert": True},
    "CPILFESL": {"lambda": 14400, "freq": "monthly",   "weight_growth": 0.0,  "weight_inflation": 0.70, "invert": False},
    "TCU":      {"lambda": 14400, "freq": "monthly",   "weight_growth": 0.0,  "weight_inflation": 0.30, "invert": False},
}

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


def hp_filter(series: pd.Series, lamb: int) -> tuple[pd.Series, pd.Series]:
    """
    Apply the Hodrick-Prescott filter. Returns (cycle, trend).
    Uses statsmodels if available, otherwise falls back to scipy sparse matrix implementation.
    """
    try:
        from statsmodels.tsa.filters.hp_filter import hpfilter
        cycle, trend = hpfilter(series.dropna(), lamb=lamb)
        return cycle, trend
    except ImportError:
        # Fallback: scipy-based HP filter
        from scipy import sparse
        from scipy.sparse import linalg
        T = len(series.dropna())
        vals = series.dropna().values
        I = sparse.eye(T, format='csc')
        D = sparse.diags([1, -2, 1], [0, 1, 2], shape=(T - 2, T), format='csc')
        trend_vals = linalg.spsolve(I + lamb * D.T @ D, vals)
        trend = pd.Series(trend_vals, index=series.dropna().index)
        cycle = series.dropna() - trend
        return cycle, trend


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


def compute_z_scores(series: pd.Series) -> pd.Series:
    """Normalize a series to Z-scores using its full historical mean and std."""
    mean = series.mean()
    std = series.std()
    if std == 0:
        return series * 0
    return (series - mean) / std


def clock_angle_from_z_scores(growth_z: float, inflation_z: float) -> float:
    """
    Map (growth_z, inflation_z) Cartesian coordinates to clock angle degrees.

    Convention (Merrill Lynch clock, clockwise from 12 o'clock):
      12 o'clock (0°)   = top of Recovery quadrant (high growth, zero inflation)
      3 o'clock (90°)   = top of Overheat quadrant (high inflation, zero growth)
      6 o'clock (180°)  = bottom of Stagflation quadrant (low growth, low inflation border)
      9 o'clock (270°)  = top of Reflation quadrant (low inflation, low growth)

    We use atan2(inflation_z, growth_z) which gives the standard mathematical angle
    (counterclockwise from east), then convert to clockwise from north.
    """
    # Standard math angle: east=0, counterclockwise positive
    math_angle_rad = math.atan2(inflation_z, growth_z)
    math_angle_deg = math.degrees(math_angle_rad)
    # Convert: clockwise from north = 90 - math_angle_deg
    clock_angle = (90 - math_angle_deg) % 360
    return round(clock_angle, 4)


def run_etl():
    """
    Main ETL function. Fetches FRED data, applies HP filter, computes Z-scores,
    determines phases and clock angles, then upserts all monthly rows to DB.
    """
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        raise ValueError("FRED_API_KEY environment variable not set")

    # Fetch 12 years of data for HP filter warm-up
    start_date = (datetime.date.today() - datetime.timedelta(days=365 * 12)).strftime("%Y-%m-%d")

    print(f"{Fore.CYAN}Fetching FRED series...{Style.RESET_ALL}")

    raw_series = {}
    for series_id, config in FRED_SERIES.items():
        print(f"  Fetching {series_id} ({config['freq']})...")
        raw_series[series_id] = fetch_fred_series(series_id, start_date, api_key)

    # Resample everything to monthly frequency (end of month)
    # GDP is quarterly — forward-fill to monthly
    monthly = {}
    for series_id, series in raw_series.items():
        resampled = series.resample("ME").last().ffill()
        monthly[series_id] = resampled

    # Align all series to a common monthly index
    combined = pd.DataFrame(monthly)
    combined = combined.dropna(how="all").ffill()

    print(f"{Fore.CYAN}Applying HP filters...{Style.RESET_ALL}")

    cycles = {}
    for series_id, config in FRED_SERIES.items():
        if series_id not in combined.columns:
            continue
        series = combined[series_id].dropna()
        cycle, _ = hp_filter(series, lamb=config["lambda"])
        # Reindex to combined index
        cycle = cycle.reindex(combined.index)
        # Invert if needed (unemployment is inverse indicator of growth)
        if config["invert"]:
            cycle = -cycle
        cycles[series_id] = cycle

    # Build Z-score normalized cycles
    z_scores = {}
    for series_id, cycle in cycles.items():
        z_scores[series_id] = compute_z_scores(cycle)

    # Composite growth and inflation Z-scores
    growth_z = sum(
        z_scores[sid] * cfg["weight_growth"]
        for sid, cfg in FRED_SERIES.items()
        if sid in z_scores and cfg["weight_growth"] > 0
    )
    inflation_z = sum(
        z_scores[sid] * cfg["weight_inflation"]
        for sid, cfg in FRED_SERIES.items()
        if sid in z_scores and cfg["weight_inflation"] > 0
    )

    print(f"{Fore.CYAN}Upserting to database...{Style.RESET_ALL}")

    conn = get_db_connection()
    cursor = conn.cursor()

    upsert_sql = """
        INSERT INTO investment_clock_data (
            biz_date,
            gdp_cyclical, cpi_cyclical, indpro_cyclical, tcu_cyclical, unrate_cyclical,
            growth_z_score, inflation_z_score,
            data_phase, clock_angle,
            gdp_value, cpi_value, indpro_value, tcu_value, unrate_value
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (biz_date) DO UPDATE SET
            gdp_cyclical = EXCLUDED.gdp_cyclical,
            cpi_cyclical = EXCLUDED.cpi_cyclical,
            indpro_cyclical = EXCLUDED.indpro_cyclical,
            tcu_cyclical = EXCLUDED.tcu_cyclical,
            unrate_cyclical = EXCLUDED.unrate_cyclical,
            growth_z_score = EXCLUDED.growth_z_score,
            inflation_z_score = EXCLUDED.inflation_z_score,
            data_phase = EXCLUDED.data_phase,
            clock_angle = EXCLUDED.clock_angle,
            gdp_value = EXCLUDED.gdp_value,
            cpi_value = EXCLUDED.cpi_value,
            indpro_value = EXCLUDED.indpro_value,
            tcu_value = EXCLUDED.tcu_value,
            unrate_value = EXCLUDED.unrate_value
    """

    rows_upserted = 0
    common_index = growth_z.dropna().index.intersection(inflation_z.dropna().index)

    for date in common_index:
        g = float(growth_z.get(date, float("nan")))
        i = float(inflation_z.get(date, float("nan")))
        if math.isnan(g) or math.isnan(i):
            continue

        phase = PHASE_MAP[(g > 0, i > 0)]
        angle = clock_angle_from_z_scores(g, i)

        def safe_float(series, idx):
            val = series.get(idx)
            return float(val) if val is not None and not (isinstance(val, float) and math.isnan(val)) else None

        cursor.execute(upsert_sql, (
            date.date(),
            safe_float(cycles.get("GDPC1", pd.Series(dtype=float)), date),
            safe_float(cycles.get("CPILFESL", pd.Series(dtype=float)), date),
            safe_float(cycles.get("INDPRO", pd.Series(dtype=float)), date),
            safe_float(cycles.get("TCU", pd.Series(dtype=float)), date),
            safe_float(cycles.get("UNRATE", pd.Series(dtype=float)), date),
            round(g, 4),
            round(i, 4),
            phase,
            angle,
            safe_float(combined.get("GDPC1", pd.Series(dtype=float)), date),
            safe_float(combined.get("CPILFESL", pd.Series(dtype=float)), date),
            safe_float(combined.get("INDPRO", pd.Series(dtype=float)), date),
            safe_float(combined.get("TCU", pd.Series(dtype=float)), date),
            safe_float(combined.get("UNRATE", pd.Series(dtype=float)), date),
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

    print(f"\n{Fore.GREEN}Done! Upserted {rows_upserted} rows.{Style.RESET_ALL}")
    print(f"\nLatest reading ({latest_date.date()}):")
    print(f"  Growth Z-score:    {g_latest:+.4f}")
    print(f"  Inflation Z-score: {i_latest:+.4f}")
    print(f"  Phase:             {phase_latest}")
    print(f"  Clock angle:       {angle_latest}°")


if __name__ == "__main__":
    run_etl()
