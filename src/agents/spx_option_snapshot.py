"""
SPX option-chain snapshot ETL.

Captures one point-in-time SPX chain per trading session and persists it three ways:

  1. spx_option_snapshot        -- daily surface/positioning summary, one row per session
  2. spx_option_chain_snapshot  -- per-strike slice (key expirations, +/-25% of spot)
  3. GCS Parquet archive        -- the FULL chain, every contract, ~0.59 MB/session

The reason this exists at all: an option chain snapshot is not reproducible. A FRED series can
be backfilled at any time, which is why investment_clock survives on a best-effort schedule. A
chain quoted at 16:00 ET on a given session is gone the moment the session ends. Every
time-comparative feature in the Options Viewer -- percentile ranks on skew, day-over-day open
interest deltas, skew-vs-price divergence -- reads this history, and a missed run is a
permanent hole rather than a delayed row.

The Parquet archive exists for the same reason at a different altitude: it costs a few cents a
year and removes the need to guess today which metrics matter later. Anything not precomputed
into Postgres can still be recovered by reprocessing the archive.

Run via spx-option-snapshot/run.py, or as a Cloud Run Job (see services/spx-snapshot-etl/).
"""

import os
import sys
import pathlib
import logging
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

ROOT = pathlib.Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))

from src.tools.api_cboe import fetch_cboe_spx_raw  # noqa: E402
from src.tools.api_db import get_db_connection  # noqa: E402

logger = logging.getLogger(__name__)

# Strikes further than this from spot are archived to Parquet but not stored per-strike in
# Postgres. Deep wings quote wide and thin, and nothing in the viewer reads them per-strike.
STRIKE_BAND = 0.25

# Surface metrics are measured on the cycle nearest this DTE rather than on a fixed expiration.
# A fixed cycle rolls down over its life, so a series built on one measures time decay rather
# than the market. 30 days matches the convention VIX itself uses.
REF_DTE_TARGET = 30

WING_DELTA = 0.25

# A cycle holding at least this share of the busiest cycle's open interest is stored per-strike
# regardless of where it falls on the calendar. Measured on live data, this is the knee of the
# curve: it lifts captured book OI from 60% to 82% for ~70 MB/yr, where dropping the floor to
# 10% buys only 3 more points for another 36 MB/yr.
OI_SHARE_FLOOR = 0.15


# ---------------------------------------------------------------------------
# Expiration selection
# ---------------------------------------------------------------------------

def select_key_expirations(
    expirations: Sequence[str],
    as_of: date,
    oi_by_expiration: Optional[Dict[str, float]] = None,
    carried: Optional[Sequence[str]] = None,
) -> List[str]:
    """The cycles worth storing per-strike: structural picks UNION measured-liquidity picks.

    SPX lists ~55 expirations and most are near-empty, so some filter is required. Two rules
    are needed because they capture different things:

    **Structural** -- near-term dailies and the next few weeklies/monthlies. These matter for
    flow and gamma even when their resting OI is modest; 0DTE in particular carries enormous
    volume against small open interest.

    **Measured** -- any cycle holding at least ``OI_SHARE_FLOOR`` of the busiest cycle's open
    interest. This exists because a purely structural filter systematically misses the far-dated
    quarterlies where institutional hedges actually sit. On live data the old date-proximity
    "LEAPS anchor" selected 2027-08-20 (54k OI, 1% of the busiest cycle) while skipping
    2027-12-17 (1.19M OI, 22%) -- a 22x liquidity miss caused by ranking on date proximity, the
    very proxy this module warns against elsewhere. Measuring instead of guessing fixes it, and
    makes the arbitrary one-year anchor unnecessary: liquid far-dated cycles now qualify on merit.

    **Carried** -- every cycle stored in the previous session that has not yet expired, passed in
    as ``carried``. Both rules above are *rolling*: the 0-2 DTE window turns over daily and
    ``weeklies[:4]`` turns over weekly, so without this the stored set churns by 1-4 cycles a
    session. That churn is what breaks the day-over-day reads. A cycle appearing for the first
    time has no prior row to difference against, and the flow queries cannot distinguish "this
    cycle was not stored yesterday" from "this cycle held no open interest yesterday" -- so its
    entire resting OI books as a same-day build, which is exactly what genuine conviction looks
    like. Holding a cycle until it expires makes entry a one-time event per cycle and exit an
    expiry, so any two consecutive sessions describe the same book.

    Membership is therefore monotone over a cycle's life: once in, it stays in. That is bounded
    -- cycles leave by expiring, never by drifting -- and costs a few extra cycles of storage in
    exchange for a table whose own history is self-consistent.

    Falls back to structural-only when open interest is unavailable, which is a safe degradation
    -- a smaller slice, never a wrong one.
    """
    parsed: List[Tuple[date, int]] = []
    for e in expirations:
        try:
            d = date.fromisoformat(e)
        except ValueError:
            continue
        parsed.append((d, (d - as_of).days))
    parsed = [p for p in parsed if p[1] >= 0]
    parsed.sort(key=lambda p: p[1])

    picked: List[date] = []

    def add(d: date) -> None:
        if d not in picked:
            picked.append(d)

    # --- Structural ---
    for d, dte in parsed:
        if dte <= 2:
            add(d)

    monthlies = [d for d, _ in parsed if d.weekday() == 4 and 15 <= d.day <= 21]
    weeklies = [d for d, _ in parsed if d.weekday() == 4 and not (15 <= d.day <= 21)]
    for d in weeklies[:4]:
        add(d)
    for d in monthlies[:4]:
        add(d)

    # --- Measured ---
    if oi_by_expiration:
        live_oi = {e: v for e, v in oi_by_expiration.items()
                   if v and date.fromisoformat(e) >= as_of}
        if live_oi:
            busiest = max(live_oi.values())
            for e, v in live_oi.items():
                if busiest > 0 and v / busiest >= OI_SHARE_FLOOR:
                    add(date.fromisoformat(e))

    # --- Carried ---
    # Only cycles the feed still lists. A carried cycle that has vanished from the chain cannot
    # be stored anyway, and one already past `as_of` is expired.
    listed = {d for d, _ in parsed}
    for e in carried or []:
        try:
            d = date.fromisoformat(e)
        except ValueError:
            continue
        if d >= as_of and d in listed:
            add(d)

    return sorted(d.isoformat() for d in picked)


# ---------------------------------------------------------------------------
# Surface metrics
# ---------------------------------------------------------------------------

def _atm_iv(contracts: Sequence[Dict[str, Any]], spot: float) -> Optional[float]:
    """IV at the strike nearest spot, averaging call and put when both quote."""
    quoted = [c for c in contracts if c.get("iv", 0) > 0]
    if not quoted:
        return None
    nearest = min(abs(c["strike"] - spot) for c in quoted)
    at_strike = [c for c in quoted if abs(c["strike"] - spot) == nearest]
    return sum(c["iv"] for c in at_strike) / len(at_strike) * 100.0


def _wing_iv(contracts: Sequence[Dict[str, Any]], opt_type: str) -> Optional[float]:
    """IV of the contract closest to +/-25 delta on the given side."""
    side = [
        c for c in contracts
        if c["type"] == opt_type and c.get("iv", 0) > 0 and c.get("delta") is not None
    ]
    if not side:
        return None
    best = min(side, key=lambda c: abs(abs(c["delta"]) - WING_DELTA))
    return best["iv"] * 100.0


def compute_atm_skew_slope(
    contracts: Sequence[Dict[str, Any]], spot: float, band: float = 0.05
) -> Optional[float]:
    """Local slope of the smile at the money: d(IV in vol points) / d(ln K/S).

    Least-squares fit over OTM quotes within ``band`` of spot. Three deliberate choices:

    - **Tight band.** The smile is curved, so a wide window measures curvature as much as slope,
      and the quantity wanted here is the derivative *at* the money.
    - **OTM only.** On each side the OTM contract is the tradeable one; ITM quotes are wider and
      carry the same information less reliably.
    - **ln-moneyness, not strike.** Makes the slope scale-free, so it stays comparable as the
      index level drifts over years, and dimensionally matches the d(ln S) in the SSR below.

    Negative for equity indices -- IV falls as strike rises.
    """
    import math

    pts: List[Tuple[float, float]] = []
    for c in contracts:
        iv = c.get("iv")
        if not iv or iv <= 0 or spot <= 0 or c["strike"] <= 0:
            continue
        log_m = math.log(c["strike"] / spot)
        if abs(log_m) > band:
            continue
        is_otm = ((c["type"] == "CALL" and c["strike"] >= spot)
                  or (c["type"] == "PUT" and c["strike"] <= spot))
        if is_otm:
            pts.append((log_m, iv * 100.0))

    if len(pts) < 5:
        return None
    n = len(pts)
    mean_x = sum(x for x, _ in pts) / n
    mean_y = sum(y for _, y in pts) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in pts)
    den = sum((x - mean_x) ** 2 for x, _ in pts)
    if den == 0:
        return None
    return num / den


def compute_ssr(
    spot_now: float, atm_iv_now: float,
    spot_prev: float, atm_iv_prev: float,
    slope_prev: float,
) -> Optional[float]:
    """Skew Stickiness Ratio -- how the surface actually moved against how its own skew said it would.

        SSR = (d(ATM IV) / d(ln S)) / (dIV/d(ln K/S))

    Both numerator and denominator are vol points per log-unit, so SSR is dimensionless.

        SSR ~ 1   Sticky strike -- the smile stayed pinned to strikes, so ATM vol slid along the
                  existing skew exactly as much as the skew implied.
        SSR ~ 0   Sticky delta -- the smile travelled with spot; ATM vol barely moved.
        SSR > 1   Vol moved further than the skew implied: the surface repriced, not just shifted.
        SSR < 0   ATM vol moved opposite to the skew's implication -- usually a vol-regime change
                  rather than a spot-driven move.

    This is worth measuring rather than assuming, because the gamma-flip solver elsewhere in this
    platform *hardcodes* sticky strike when repricing the book. SSR is the only thing that says
    whether that assumption held on a given day.

    NOTE ON CONVENTION: normalisations differ across the literature (some scale so that sticky
    delta reads 1 and sticky strike 2). The definition above is the one used throughout this
    platform; do not compare the number directly against a published SSR without checking theirs.

    ``slope_prev`` is the prior session's skew -- the skew in place when the move happened.
    """
    import math

    if not (spot_now > 0 and spot_prev > 0) or not slope_prev:
        return None
    d_log_s = math.log(spot_now / spot_prev)
    # On a flat day the ratio is 0/0 in spirit: a tiny denominator turns quote noise in ATM IV
    # into an enormous, meaningless SSR. Below ~0.1% of spot movement there is no signal to read.
    if abs(d_log_s) < 0.001:
        return None
    if atm_iv_now is None or atm_iv_prev is None:
        return None
    return ((atm_iv_now - atm_iv_prev) / d_log_s) / slope_prev


def compute_surface_metrics(
    contracts: Sequence[Dict[str, Any]], spot: float
) -> Dict[str, Optional[float]]:
    """ATM IV, 25-delta wings, risk reversal, butterfly and normalised skew for one cycle."""
    atm = _atm_iv(contracts, spot)
    put25 = _wing_iv(contracts, "PUT")
    call25 = _wing_iv(contracts, "CALL")

    rr = fly = norm = None
    if put25 is not None and call25 is not None:
        rr = put25 - call25
        if atm:
            fly = (put25 + call25) / 2.0 - atm
            # Raw vol points are level-dependent: 5 points against a 12% ATM is a heavy
            # hedging bid, the same 5 points against a 35% ATM is close to complacent.
            norm = rr / atm if atm > 0 else None

    return {
        "atm_iv": atm,
        "put25_iv": put25,
        "call25_iv": call25,
        "rr25": rr,
        "fly25": fly,
        "normalized_skew": norm,
    }


def compute_book_metrics(
    contracts: Sequence[Dict[str, Any]], spot: float
) -> Dict[str, Optional[float]]:
    """Whole-book positioning and gamma. Aggregates EVERY listed expiration.

    Scope matters enormously here: a single front-month cycle can print negative net GEX while
    the whole book prints strongly positive -- opposite regime calls from the same data. The
    published provider levels these are compared against are whole-book, so these must be too.
    """
    call_vol = sum(c["volume"] for c in contracts if c["type"] == "CALL")
    put_vol = sum(c["volume"] for c in contracts if c["type"] == "PUT")
    call_oi = sum(c.get("open_interest") or 0.0 for c in contracts if c["type"] == "CALL")
    put_oi = sum(c.get("open_interest") or 0.0 for c in contracts if c["type"] == "PUT")

    # Dollar gamma per 1% move. The spot^2 term is not a typo: gamma * OI * 100 * spot * 1%
    # gives shares to hedge, and multiplying by spot again converts shares to dollars.
    #
    # The call-positive / put-negative split is the standard "naive dealer" assumption shared
    # with SqueezeMetrics and SpotGamma. It is a modelling assumption, not an observed fact --
    # public OI cannot establish which side the dealer is on -- and it is the single largest
    # source of model risk in this number.
    gex_by_strike: Dict[float, float] = {}
    net_gex = 0.0
    for c in contracts:
        g = c.get("gamma") or 0.0
        oi = c.get("open_interest") or 0.0
        if not g or not oi:
            continue
        dollar_gex = g * oi * 100.0 * spot * (spot * 0.01) / 1_000_000.0
        signed = dollar_gex if c["type"] == "CALL" else -dollar_gex
        gex_by_strike[c["strike"]] = gex_by_strike.get(c["strike"], 0.0) + signed
        net_gex += signed

    # Walls are gamma-weighted, not raw OI -- a deep ITM/OTM strike can hold enormous legacy OI
    # while contributing almost no gamma. The directional constraint keeps them on their own
    # side of spot; without it both can collapse onto the same strike.
    above = {k: v for k, v in gex_by_strike.items() if k >= spot}
    below = {k: v for k, v in gex_by_strike.items() if k <= spot}
    call_wall = max(above, key=lambda k: abs(above[k])) if above else None
    put_wall = max(below, key=lambda k: abs(below[k])) if below else None

    return {
        "pcr_volume": (put_vol / call_vol) if call_vol else None,
        "pcr_oi": (put_oi / call_oi) if call_oi else None,
        "total_volume": int(call_vol + put_vol),
        "total_open_interest": int(call_oi + put_oi),
        "net_gex_m": net_gex,
        "call_wall": call_wall,
        "put_wall": put_wall,
    }


# ---------------------------------------------------------------------------
# Snapshot assembly
# ---------------------------------------------------------------------------

def build_snapshot(
    payload: Dict[str, Any],
    carried_expirations: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Turn a raw Cboe payload into the summary row and the per-strike slice.

    ``carried_expirations`` is the previous session's stored cycle list, read by run_etl(). It is
    passed in rather than queried here so this function stays pure and ``--dry-run`` stays a
    read-only operation; omitting it degrades to the pre-hysteresis selection.
    """
    spot = payload["spot_price"]
    contracts = payload["contracts"]
    if not contracts or spot <= 0:
        raise ValueError("Cboe payload contained no usable contracts or spot price")

    ts_raw = payload.get("cboe_timestamp") or ""
    biz_date = _resolve_biz_date(ts_raw)

    oi_by_exp: Dict[str, float] = {}
    for c in contracts:
        oi_by_exp[c["expiration"]] = oi_by_exp.get(c["expiration"], 0.0) + (c.get("open_interest") or 0.0)
    key_exps = select_key_expirations(
        payload["expirations"], biz_date, oi_by_exp, carried_expirations
    )

    # Live cycles only. When the feed is frozen (weekend, holiday) an already-expired contract
    # can still be labelled 0 DTE, and inverting IV on its frozen quotes is numerically
    # unstable enough to produce solved vols of 300%+ on strikes $5 apart.
    live = [c for c in contracts if date.fromisoformat(c["expiration"]) >= biz_date]
    if not live:
        raise ValueError(f"No live expirations remain as of {biz_date}")

    by_exp: Dict[str, List[Dict[str, Any]]] = {}
    for c in live:
        by_exp.setdefault(c["expiration"], []).append(c)

    # Reference cycle for the surface metrics -- nearest REF_DTE_TARGET, not a fixed date.
    ref_exp = min(
        by_exp,
        key=lambda e: abs((date.fromisoformat(e) - biz_date).days - REF_DTE_TARGET),
    )
    ref_dte = (date.fromisoformat(ref_exp) - biz_date).days
    surface = compute_surface_metrics(by_exp[ref_exp], spot)
    book = compute_book_metrics(live, spot)
    # Measured on the same reference cycle as the surface metrics, so the slope and the ATM IV
    # that SSR divides one by the other always describe the same smile.
    skew_slope = compute_atm_skew_slope(by_exp[ref_exp], spot)

    # Term structure across live cycles carrying a usable ATM quote
    atm_curve: List[Tuple[int, float]] = []
    for e, cs in by_exp.items():
        iv = _atm_iv(cs, spot)
        if iv:
            atm_curve.append(((date.fromisoformat(e) - biz_date).days, iv))
    atm_curve.sort()
    front_iv = atm_curve[0][1] if atm_curve else None
    back_iv = atm_curve[-1][1] if atm_curve else None

    summary = {
        "biz_date": biz_date,
        "quote_timestamp": _parse_ts(ts_raw),
        "spot": spot,
        "ref_expiration": date.fromisoformat(ref_exp),
        "ref_dte": ref_dte,
        **surface,
        "atm_skew_slope": skew_slope,
        # Filled in by run_etl(), which can read the prior session. build_snapshot() is kept
        # pure -- it never touches the database -- so --dry-run stays a read-only operation.
        "ssr": None,
        "front_atm_iv": front_iv,
        "back_atm_iv": back_iv,
        "term_slope": (back_iv - front_iv) if (front_iv and back_iv) else None,
        **book,
        "expiration_count": len(by_exp),
        "contract_count": len(live),
    }

    lo, hi = spot * (1 - STRIKE_BAND), spot * (1 + STRIKE_BAND)
    slice_rows = [
        c for c in live
        if c["expiration"] in key_exps and lo <= c["strike"] <= hi
    ]

    return {"summary": summary, "slice_rows": slice_rows, "all_rows": live, "biz_date": biz_date}


def today_eastern() -> date:
    """Current calendar date in the exchange's own timezone."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).date()
    except Exception:
        return datetime.now(timezone.utc).date()


def most_recent_session(d: date) -> date:
    """The latest NYSE session on or before `d`."""
    try:
        import pandas_market_calendars as mcal
        days = mcal.get_calendar("NYSE").valid_days(
            start_date=d - timedelta(days=10), end_date=d
        )
        if len(days) > 0:
            return days[-1].date()
    except Exception as exc:
        logger.warning("Market calendar unavailable (%s); falling back to weekday walk", exc)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _resolve_biz_date(ts_raw: str) -> date:
    """Session the quotes actually belong to.

    The Cboe timestamp is the feed's *publication* date, not the session's. Outside market
    hours -- weekends, holidays, overnight -- the feed keeps serving the last session's final
    quotes while stamping them with the current calendar date. Taking that date at face value
    would file Friday's closing chain under Saturday, which then reads as a phantom session in
    any day-over-day comparison. Snapping back to the most recent NYSE session labels the data
    by when it was actually quoted.
    """
    feed_date = None
    if len(ts_raw) >= 10:
        try:
            feed_date = date.fromisoformat(ts_raw[:10])
        except ValueError:
            pass
    return most_recent_session(feed_date or today_eastern())


def _parse_ts(ts_raw: str) -> Optional[datetime]:
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(ts_raw[:19], fmt)
        except (ValueError, TypeError):
            continue
    return None


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

SUMMARY_COLUMNS = [
    "biz_date", "quote_timestamp", "spot", "ref_expiration", "ref_dte",
    "atm_iv", "put25_iv", "call25_iv", "rr25", "fly25", "normalized_skew",
    "atm_skew_slope", "ssr",
    "front_atm_iv", "back_atm_iv", "term_slope",
    "pcr_volume", "pcr_oi", "total_volume", "total_open_interest",
    "net_gex_m", "call_wall", "put_wall",
    "expiration_count", "contract_count",
]


def upsert_summary(conn, summary: Dict[str, Any]) -> None:
    cols = ", ".join(SUMMARY_COLUMNS)
    placeholders = ", ".join(["%s"] * len(SUMMARY_COLUMNS))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in SUMMARY_COLUMNS if c != "biz_date")
    sql = (
        f"INSERT INTO spx_option_snapshot ({cols}) VALUES ({placeholders}) "
        f"ON CONFLICT (biz_date) DO UPDATE SET {updates}, updated_at = NOW()"
    )
    with conn.cursor() as cur:
        cur.execute(sql, [summary.get(c) for c in SUMMARY_COLUMNS])


def upsert_chain_slice(conn, biz_date: date, rows: Sequence[Dict[str, Any]]) -> int:
    """Bulk-upsert the per-strike slice. Idempotent, so a retried run is harmless."""
    if not rows:
        return 0
    from psycopg2.extras import execute_values

    values = [
        (
            biz_date, r["expiration"], r["root"],
            "C" if r["type"] == "CALL" else "P", r["strike"],
            r.get("bid"), r.get("ask"),
            round((r.get("bid", 0) + r.get("ask", 0)) / 2.0, 4),
            r.get("iv"), r.get("delta"), r.get("gamma"),
            r.get("theta"), r.get("vega"), r.get("rho"),
            int(r.get("volume") or 0), int(r.get("open_interest") or 0),
        )
        for r in rows
    ]
    sql = """
        INSERT INTO spx_option_chain_snapshot (
            biz_date, expiration, root, opt_type, strike,
            bid, ask, mid, iv, delta, gamma, theta, vega, rho, volume, open_interest
        ) VALUES %s
        ON CONFLICT (biz_date, expiration, root, opt_type, strike) DO UPDATE SET
            bid = EXCLUDED.bid, ask = EXCLUDED.ask, mid = EXCLUDED.mid,
            iv = EXCLUDED.iv, delta = EXCLUDED.delta, gamma = EXCLUDED.gamma,
            theta = EXCLUDED.theta, vega = EXCLUDED.vega, rho = EXCLUDED.rho,
            volume = EXCLUDED.volume, open_interest = EXCLUDED.open_interest
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=1000)
    return len(values)


def archive_to_gcs(biz_date: date, rows: Sequence[Dict[str, Any]], bucket: str) -> Optional[str]:
    """Write the FULL chain to GCS as Parquet. No-op when no bucket is configured.

    Imported lazily so the agent runs locally without google-cloud-storage installed.
    """
    if not bucket:
        return None
    import pandas as pd
    from google.cloud import storage

    df = pd.DataFrame(rows)
    for c in ("bid", "ask", "last", "iv", "delta", "gamma", "theta", "vega", "rho", "theo",
              "strike", "change", "percent_change", "prev_day_close"):
        if c in df:
            df[c] = df[c].astype("float32")
    for c in ("volume", "open_interest"):
        if c in df:
            df[c] = df[c].fillna(0).astype("int32")
    # Sorting before writing materially improves columnar compression.
    df = df.sort_values(["expiration", "type", "strike"]).reset_index(drop=True)

    # Hive-style partitioning so the archive stays queryable by date without a manifest.
    key = (f"spx/chain/year={biz_date.year:04d}/month={biz_date.month:02d}/"
           f"spx_chain_{biz_date.isoformat()}.parquet")
    local = f"/tmp/spx_chain_{biz_date.isoformat()}.parquet"
    df.to_parquet(local, compression="zstd", index=False)

    storage.Client().bucket(bucket).blob(key).upload_from_filename(local)
    os.remove(local)
    logger.info("Archived %d contracts to gs://%s/%s", len(df), bucket, key)
    return f"gs://{bucket}/{key}"


# ---------------------------------------------------------------------------
# Data-quality guards
# ---------------------------------------------------------------------------

def is_trading_session(d: date) -> bool:
    """NYSE calendar check. Cloud Scheduler fires on weekdays regardless of holidays, so
    without this the job writes a stale duplicate row every Thanksgiving and Good Friday."""
    try:
        import pandas_market_calendars as mcal
        sessions = mcal.get_calendar("NYSE").valid_days(start_date=d, end_date=d)
        return len(sessions) > 0
    except Exception as exc:
        logger.warning("Market calendar unavailable (%s); assuming weekday = session", exc)
        return d.weekday() < 5


def fetch_previous_session(conn, biz_date: date) -> Optional[Dict[str, Any]]:
    """Prior session's spot, ATM IV and skew slope -- the inputs SSR needs."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT spot, atm_iv, atm_skew_slope
            FROM spx_option_snapshot
            WHERE biz_date < %s AND spot IS NOT NULL
            ORDER BY biz_date DESC LIMIT 1
            """,
            (biz_date,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {"spot": row[0], "atm_iv": row[1], "atm_skew_slope": row[2]}


def fetch_stored_expirations(conn, biz_date: date) -> List[str]:
    """Cycles stored per-strike in the most recent session before `biz_date`.

    Feeds the hysteresis rule in select_key_expirations(). Reads the single latest prior session
    rather than a union over history, so a cycle that has genuinely left the table (because it
    expired) is not resurrected by an old row.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT expiration FROM spx_option_chain_snapshot
            WHERE biz_date = (
                SELECT MAX(biz_date) FROM spx_option_chain_snapshot WHERE biz_date < %s
            )
            """,
            (biz_date,),
        )
        return [r[0].isoformat() for r in cur.fetchall()]


def check_previous_session(conn, biz_date: date) -> Optional[str]:
    """Warn if the prior session is missing.

    Alerting only on execution failure misses the case that actually matters -- a run that
    succeeded while writing nothing useful, or a run that never fired at all. Checking that
    yesterday landed catches gaps with no extra infrastructure.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(biz_date) FROM spx_option_snapshot WHERE biz_date < %s", (biz_date,)
        )
        row = cur.fetchone()
    last = row[0] if row else None
    if last is None:
        return None  # first ever run
    gap = (biz_date - last).days
    if gap > 4:
        return f"GAP: no snapshot between {last} and {biz_date} ({gap} days)"
    return None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_etl(force: bool = False) -> Dict[str, Any]:
    """Fetch, compute and persist one session's snapshot. Safe to re-run: everything upserts."""
    # The guard is on the RUN date, not on biz_date. biz_date always snaps back to a real
    # session, so testing it would never be False and the holiday skip would never fire.
    run_date = today_eastern()
    if not force and not is_trading_session(run_date):
        logger.info("%s is not an NYSE trading session - skipping", run_date)
        return {"status": "skipped", "reason": "not a trading session", "run_date": str(run_date)}

    payload = fetch_cboe_spx_raw()

    # biz_date is resolved here as well as inside build_snapshot() so the carried-cycle read can
    # happen before the snapshot is built. Both calls are pure functions of the same timestamp,
    # so they cannot disagree.
    biz_date = _resolve_biz_date(payload.get("cboe_timestamp") or "")

    conn = get_db_connection()
    try:
        carried = fetch_stored_expirations(conn, biz_date)
        snap = build_snapshot(payload, carried)
        if snap["biz_date"] != biz_date:  # defensive: the two resolutions must agree
            raise RuntimeError(
                f"biz_date mismatch: {biz_date} vs {snap['biz_date']}"
            )

        warning = check_previous_session(conn, biz_date)
        if warning:
            logger.warning(warning)

        # SSR needs the session before this one, so it is filled here rather than in
        # build_snapshot(). Stays null on the first stored session, and on any day the index
        # barely moved -- see compute_ssr() for why a flat tape has no ratio to report.
        prev = fetch_previous_session(conn, biz_date)
        if prev:
            snap["summary"]["ssr"] = compute_ssr(
                spot_now=snap["summary"]["spot"],
                atm_iv_now=snap["summary"]["atm_iv"],
                spot_prev=prev["spot"],
                atm_iv_prev=prev["atm_iv"],
                slope_prev=prev["atm_skew_slope"],
            )

        upsert_summary(conn, snap["summary"])
        n_slice = upsert_chain_slice(conn, biz_date, snap["slice_rows"])
        conn.commit()
    finally:
        conn.close()

    archive_uri = None
    try:
        archive_uri = archive_to_gcs(biz_date, snap["all_rows"], os.environ.get("GCS_ARCHIVE_BUCKET", ""))
    except Exception as exc:
        # The archive is a nice-to-have; Postgres already has what the UI reads. Failing the
        # whole run here would turn a recoverable problem into a lost session.
        logger.error("GCS archive failed (Postgres write already committed): %s", exc)

    result = {
        "status": "ok",
        "biz_date": str(biz_date),
        "spot": snap["summary"]["spot"],
        "contracts_total": len(snap["all_rows"]),
        "contracts_stored": n_slice,
        # Surfaced because a drop here is the signal that the day-over-day reads have gone thin:
        # cycles carried but no longer selected on merit are exactly the ones holding the join up.
        "cycles_carried": len(carried),
        "cycles_stored": len({r["expiration"] for r in snap["slice_rows"]}),
        "archive": archive_uri,
        "warning": warning,
    }
    logger.info("Snapshot complete: %s", result)
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(run_etl(force="--force" in sys.argv))
