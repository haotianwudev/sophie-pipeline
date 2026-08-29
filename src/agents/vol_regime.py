"""
Volatility Regime / Variance Risk Premium (VRP) ETL Agent

Precomputes the daily volatility-regime signal set from data already in the
`prices` table (SPX, VIX) plus FRED's 3-month VIX for term structure, and
upserts one row per trading day into `vol_regime_data`.

Why this exists: the options viewer needs to answer "is option premium rich
right now, and is it safe to sell?" — but the headline inputs (a 252-day VIX
percentile, an EWM z-score of the premium) are history-dependent and cannot be
derived from a single live quote in the browser. So they're precomputed here,
the same way the Investment Clock precomputes its macro Z-scores.

Methodology:
  realized_vol_20d = stdev(log returns, 20d) * sqrt(252) * 100   (annualized %, VIX-comparable)
  vrp              = VIX - realized_vol_20d                       (implied minus realized)
  vrp_z            = EWM z-score of vrp (span=126)                (rich vs its own recent history)
  vix_rank         = 252d percentile rank of VIX (0..1)
  term_slope       = VIX3M - VIX                                  (negative = backwardation = stress)

Regime taxonomy (two axes, mirroring the Investment Clock's quadrant logic):
              | calm (vix_rank <= 0.8 and not backwardated) | stressed
  premium rich| Harvest                                     | Stressed Premium
  premium thin| Thin                                        | Crisis

  Harvest          - implied richer than realized in a calm tape: best selling conditions
  Stressed Premium - premium is rich but vol is elevated/backwardated: size down
  Thin             - little/no compensation for short vol: stand aside
  Crisis           - implied below realized while stressed: selling is uncompensated

Unlike the Investment Clock, past rows are NOT frozen: every signal here is
causal (backward-looking EWM / rolling percentile) over index closes that don't
get revised, so a full recompute is deterministic and idempotent.

Run manually:
    python -m src.agents.vol_regime
"""

import io
import math
import os
import datetime

import numpy as np
import pandas as pd
import requests
from colorama import Fore, Style
from dotenv import load_dotenv

from src.tools.api_db import get_db_connection

load_dotenv()

TRADING_DAYS = 252
VRP_Z_SPAN = 126          # ~6 months, matches lab/features.py in sophie-option-research
VRP_Z_MIN_PERIODS = 63

# Stress thresholds
VIX_RANK_STRESS = 0.80    # VIX in the top 20% of its trailing year
FRED_VIX3M_SERIES = "VXVCLS"   # CBOE S&P 500 3-Month Volatility Index (Yahoo no longer serves ^VIX3M)

# Free public feed behind squeezemetrics.com/monitor/dix's own chart — no auth, updates daily.
# History starts 2011-05-02 (vs. this table's 2000 start for SPX/VIX), so dix/dix_gex are NULL
# before then.
DIX_CSV_URL = "https://squeezemetrics.com/monitor/static/DIX.csv"

REGIME_HARVEST = "Harvest"
REGIME_STRESSED = "Stressed Premium"
REGIME_THIN = "Thin"
REGIME_CRISIS = "Crisis"


def rolling_percentile(series: pd.Series, window: int = TRADING_DAYS) -> pd.Series:
    """Percentile rank of the latest value within the trailing window (0..1)."""
    return series.rolling(window, min_periods=window // 2).apply(
        lambda w: (w <= w[-1]).mean(), raw=True
    )


def ewm_z_score(series: pd.Series, span: int = VRP_Z_SPAN,
                min_periods: int = VRP_Z_MIN_PERIODS) -> pd.Series:
    """EWM z-score — same normalization pattern the other Sophie agents use."""
    ewm_mean = series.ewm(span=span, min_periods=min_periods, ignore_na=True).mean()
    ewm_std = series.ewm(span=span, min_periods=min_periods, ignore_na=True).std()
    return (series - ewm_mean) / ewm_std.replace(0, float("nan"))


def load_prices_from_db(tickers=("SPX", "VIX")) -> pd.DataFrame:
    """Wide daily close frame (one column per ticker) from the `prices` table."""
    conn = get_db_connection()
    try:
        df = pd.read_sql(
            "SELECT ticker, biz_date, close FROM prices "
            "WHERE ticker = ANY(%(tickers)s) ORDER BY biz_date",
            conn, params={"tickers": list(tickers)},
        )
    finally:
        conn.close()

    if df.empty:
        raise SystemExit(
            "No SPX/VIX rows in `prices`. Backfill first:\n"
            "  poetry run python src/upload/raw_data_table_uploader.py "
            "--tickers SPX,VIX --table prices --start-date 2000-01-01"
        )

    df["biz_date"] = pd.to_datetime(df["biz_date"])
    df["close"] = df["close"].astype(float)
    wide = df.pivot(index="biz_date", columns="ticker", values="close").sort_index()
    return wide


def fetch_vix3m_from_fred() -> pd.Series:
    """3-month VIX from FRED. Optional — term structure degrades to NULL without it."""
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        print(f"{Fore.YELLOW}FRED_API_KEY not set — term structure will be NULL{Style.RESET_ALL}")
        return pd.Series(dtype=float)
    try:
        from fredapi import Fred

        series = Fred(api_key=api_key).get_series(FRED_VIX3M_SERIES).dropna()
        series.index = pd.to_datetime(series.index)
        return series.astype(float)
    except Exception as e:
        print(f"{Fore.YELLOW}Could not fetch {FRED_VIX3M_SERIES} from FRED "
              f"({e}) — term structure will be NULL{Style.RESET_ALL}")
        return pd.Series(dtype=float)


def fetch_dix_gex() -> pd.DataFrame:
    """SqueezeMetrics DIX/GEX from their free public CSV. Optional -- both columns degrade to
    NULL without it, same treatment as fetch_vix3m_from_fred(), since it's a third-party feed
    this ETL doesn't control the uptime of."""
    try:
        resp = requests.get(DIX_CSV_URL, timeout=30, headers={"User-Agent": "sophie-pipeline"})
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text), usecols=["date", "dix", "gex"])
        df["date"] = pd.to_datetime(df["date"])
        return df.set_index("date")[["dix", "gex"]].astype(float)
    except Exception as e:
        print(f"{Fore.YELLOW}Could not fetch DIX/GEX from squeezemetrics.com "
              f"({e}) — dix/dix_gex will be NULL{Style.RESET_ALL}")
        return pd.DataFrame(columns=["dix", "gex"])


def classify_regime(vrp_z: float, vix_rank: float, term_slope: float | None) -> str:
    """Two-axis regime label: is premium rich, and is the tape stressed?"""
    premium_rich = vrp_z > 0
    backwardated = term_slope is not None and not math.isnan(term_slope) and term_slope < 0
    stressed = (not math.isnan(vix_rank) and vix_rank > VIX_RANK_STRESS) or backwardated

    if premium_rich:
        return REGIME_STRESSED if stressed else REGIME_HARVEST
    return REGIME_CRISIS if stressed else REGIME_THIN


def build_signals() -> pd.DataFrame:
    """Compute the full daily signal frame."""
    wide = load_prices_from_db()
    missing = {"SPX", "VIX"} - set(wide.columns)
    if missing:
        raise SystemExit(f"`prices` is missing required tickers: {sorted(missing)}")

    spx, vix = wide["SPX"], wide["VIX"]

    log_ret = np.log(spx / spx.shift(1))
    rv20 = log_ret.rolling(20).std() * np.sqrt(TRADING_DAYS) * 100
    rv10 = log_ret.rolling(10).std() * np.sqrt(TRADING_DAYS) * 100

    vrp = vix - rv20
    # Variance-point VRP: the vol-point spread above overstates the premium via
    # Jensen's inequality (E[sigma] != sqrt(E[sigma^2])) -- this is what a variance
    # swap actually pays. Scaled by /100 to keep magnitudes comparable to vol points.
    vrp_variance = (vix**2 - rv20**2) / 100.0

    # Downside/upside realized semivariance (Barndorff-Nielsen split): is the
    # trailing-20d variance actually coming from down days or up days?
    neg_ret = log_ret.where(log_ret < 0, 0.0)
    pos_ret = log_ret.where(log_ret > 0, 0.0)
    rs_dn = (neg_ret**2).rolling(20).sum()
    rs_up = (pos_ret**2).rolling(20).sum()
    downside_variance_share = rs_dn / (rs_dn + rs_up)

    # Forward-looking research fields: what a seller of today's implied vol actually
    # collected once the next 21 sessions realized. NULL for the ~21 most recent rows
    # by construction (the future hasn't happened yet) -- these are for backtest-style
    # analysis of whether today's signal predicts the earned premium, never for a
    # "live" reading of the current day.
    fwd_realized_vol_21d = log_ret.rolling(21).std().shift(-21) * np.sqrt(TRADING_DAYS) * 100
    fwd_earned_premium = vix - fwd_realized_vol_21d

    vix3m = fetch_vix3m_from_fred()
    # Reindex onto SPX/VIX trading days; ffill bridges FRED's occasional gaps
    vix3m_aligned = vix3m.reindex(spx.index).ffill(limit=5) if len(vix3m) else pd.Series(
        index=spx.index, dtype=float)

    dix_gex = fetch_dix_gex()
    # No ffill here (unlike vix3m): a missing DIX/GEX row means squeezemetrics didn't publish
    # that day, not a stale-data bridge worth papering over -- NULL is the honest answer.
    dix_aligned = dix_gex["dix"].reindex(spx.index) if len(dix_gex) else pd.Series(
        index=spx.index, dtype=float)
    dix_gex_aligned = dix_gex["gex"].reindex(spx.index) if len(dix_gex) else pd.Series(
        index=spx.index, dtype=float)

    out = pd.DataFrame({
        "spx_close": spx,
        "vix": vix,
        "vix3m": vix3m_aligned,
        "dix": dix_aligned,
        "dix_gex": dix_gex_aligned,
        "realized_vol_20d": rv20,
        "realized_vol_10d": rv10,
        "vrp": vrp,
        "vrp_z": ewm_z_score(vrp),
        "vrp_percentile": rolling_percentile(vrp),
        "vrp_variance": vrp_variance,
        "downside_variance_share": downside_variance_share,
        "fwd_realized_vol_21d": fwd_realized_vol_21d,
        "fwd_earned_premium": fwd_earned_premium,
        "vix_rank": rolling_percentile(vix),
        "term_slope": vix3m_aligned - vix,
    })

    out["term_structure"] = np.where(
        out["term_slope"].isna(), None,
        np.where(out["term_slope"] < 0, "Backwardation", "Contango"),
    )

    # regime_score: signed premium attractiveness — rich premium in a calm tape scores
    # highest, and backwardation penalizes regardless of how rich the premium looks.
    out["regime_score"] = (
        out["vrp_z"].fillna(0)
        - 2.0 * (out["vix_rank"].fillna(0) > VIX_RANK_STRESS).astype(float)
        - 1.0 * (out["term_slope"].fillna(0) < 0).astype(float)
    )

    # Need vrp_z before a regime is meaningful
    out = out[out["vrp_z"].notna()].copy()

    out["regime"] = [
        classify_regime(
            row.vrp_z,
            row.vix_rank if not pd.isna(row.vix_rank) else float("nan"),
            None if pd.isna(row.term_slope) else row.term_slope,
        )
        for row in out.itertuples()
    ]
    return out


UPSERT_SQL = """
    INSERT INTO vol_regime_data (
        biz_date, spx_close, vix, vix3m,
        realized_vol_20d, realized_vol_10d,
        vrp, vrp_z, vrp_percentile, vrp_variance, downside_variance_share,
        fwd_realized_vol_21d, fwd_earned_premium,
        vix_rank, term_slope, term_structure,
        dix, dix_gex,
        regime, regime_score
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (biz_date) DO UPDATE SET
        spx_close               = EXCLUDED.spx_close,
        vix                     = EXCLUDED.vix,
        vix3m                   = EXCLUDED.vix3m,
        realized_vol_20d        = EXCLUDED.realized_vol_20d,
        realized_vol_10d        = EXCLUDED.realized_vol_10d,
        vrp                     = EXCLUDED.vrp,
        vrp_z                   = EXCLUDED.vrp_z,
        vrp_percentile          = EXCLUDED.vrp_percentile,
        vrp_variance            = EXCLUDED.vrp_variance,
        downside_variance_share = EXCLUDED.downside_variance_share,
        fwd_realized_vol_21d    = EXCLUDED.fwd_realized_vol_21d,
        fwd_earned_premium      = EXCLUDED.fwd_earned_premium,
        vix_rank                = EXCLUDED.vix_rank,
        term_slope              = EXCLUDED.term_slope,
        term_structure          = EXCLUDED.term_structure,
        dix                     = EXCLUDED.dix,
        dix_gex                 = EXCLUDED.dix_gex,
        regime                  = EXCLUDED.regime,
        regime_score            = EXCLUDED.regime_score,
        updated_at              = CURRENT_TIMESTAMP
"""


def _f(val) -> float | None:
    """psycopg2-safe float (NaN -> NULL)."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if math.isnan(f) else round(f, 6)
    except (TypeError, ValueError):
        return None


def run_etl(lookback_days: int | None = None) -> int:
    """Compute signals and upsert them. Pass lookback_days to limit the write
    window (the daily job) instead of rewriting all history (the backfill)."""
    print(f"{Fore.CYAN}Loading SPX/VIX from prices table...{Style.RESET_ALL}")
    signals = build_signals()

    if lookback_days:
        cutoff = pd.Timestamp(datetime.date.today() - datetime.timedelta(days=lookback_days))
        signals = signals[signals.index >= cutoff]

    print(f"{Fore.CYAN}Upserting {len(signals)} rows to vol_regime_data...{Style.RESET_ALL}")
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        for date, row in signals.iterrows():
            cursor.execute(UPSERT_SQL, (
                date.date(),
                _f(row.spx_close), _f(row.vix), _f(row.vix3m),
                _f(row.realized_vol_20d), _f(row.realized_vol_10d),
                _f(row.vrp), _f(row.vrp_z), _f(row.vrp_percentile),
                _f(row.vrp_variance), _f(row.downside_variance_share),
                _f(row.fwd_realized_vol_21d), _f(row.fwd_earned_premium),
                _f(row.vix_rank), _f(row.term_slope), row.term_structure,
                _f(row.dix), _f(row.dix_gex),
                row.regime, _f(row.regime_score),
            ))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

    latest = signals.iloc[-1]
    latest_date = signals.index[-1].date()
    print(f"\n{Fore.GREEN}Done! Upserted {len(signals)} rows.{Style.RESET_ALL}")
    print(f"\nLatest reading ({latest_date}):")
    print(f"  Regime:            {latest.regime}")
    print(f"  SPX close:         {latest.spx_close:,.2f}")
    print(f"  VIX:               {latest.vix:.2f}")
    print(f"  Realized vol 20d:  {latest.realized_vol_20d:.2f}")
    print(f"  VRP (implied-real):{latest.vrp:+.2f}")
    print(f"  VRP z-score:       {latest.vrp_z:+.2f}")
    print(f"  VIX rank (1y):     {latest.vix_rank:.2%}"
          if not pd.isna(latest.vix_rank) else "  VIX rank (1y):     N/A")
    if not pd.isna(latest.term_slope):
        print(f"  Term slope:        {latest.term_slope:+.2f} ({latest.term_structure})")
    else:
        print("  Term slope:        N/A")
    if not pd.isna(latest.dix):
        print(f"  DIX:               {latest.dix:.2%}")
        print(f"  GEX (SqueezeM.):   {latest.dix_gex:,.0f}")
    else:
        print("  DIX / GEX:         N/A")
    return len(signals)


if __name__ == "__main__":
    run_etl()
