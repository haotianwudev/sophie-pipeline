"""
SPX Option Chain Gap Backfill (ThetaData -> Postgres & Local Parquet Archive).

Fetches historical EOD chains for SPX & SPXW from a locally-running ThetaData Terminal
(localhost:25503), solves Black-Scholes IV and Greeks locally, computes daily surface
and book metrics using src.agents.spx_option_snapshot, and persists:
  1. spx_option_snapshot (Postgres) -- daily surface/positioning summary
  2. Local Parquet archive (F:\workspace\sophie-pipeline\data\spx_chain_archive)

Usage:
  python spx-option-snapshot/backfill.py --download-only
  python spx-option-snapshot/backfill.py --process-only
  python spx-option-snapshot/backfill.py --dry-run
  python spx-option-snapshot/backfill.py
"""

import os
import sys
import io
import time
import math
import logging
import argparse
import pathlib
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple, Sequence

import requests
import numpy as np
import pandas as pd
from scipy.stats import norm
from scipy.optimize import brentq
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.agents.spx_option_snapshot import (
    build_snapshot,
    upsert_summary,
    compute_ssr,
    most_recent_session,
)
from src.tools.api_db import get_db_connection

logger = logging.getLogger("spx_backfill")

THETA_BASE_URL = "http://localhost:25503"
RAW_DATA_DIR = ROOT / "data" / "raw_theta"
ARCHIVE_DIR = ROOT / "data" / "spx_chain_archive"

DEFAULT_START = "2024-01-02"
DEFAULT_END = "2026-08-20"


# ---------------------------------------------------------------------------
# Black-Scholes Solver
# ---------------------------------------------------------------------------

def bs_price(S: float, K: float, T: float, r: float, sigma: float, option_type: str) -> float:
    if T <= 0 or sigma <= 0:
        return max(0.0, S - K) if option_type == "CALL" else max(0.0, K - S)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if option_type == "CALL":
        return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    else:
        return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


def solve_iv(price: float, S: float, K: float, T: float, r: float, option_type: str) -> float:
    intrinsic = max(0.0, S - K) if option_type == "CALL" else max(0.0, K - S)
    if price <= intrinsic or price <= 0.01 or S <= 0 or K <= 0 or T <= 0.0001:
        return 0.0
    try:
        return float(brentq(lambda sig: bs_price(S, K, T, r, sig, option_type) - price, 0.005, 4.0, xtol=1e-4))
    except Exception:
        return 0.0


def compute_greeks(S: float, K: float, T: float, r: float, sigma: float, option_type: str) -> Dict[str, float]:
    if T <= 0.0001 or sigma <= 0.001 or S <= 0 or K <= 0:
        delta = (1.0 if S > K else 0.0) if option_type == "CALL" else (-1.0 if K > S else 0.0)
        return {"delta": delta, "gamma": 0.0, "theta": 0.0, "vega": 0.0}

    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    nd1 = norm.pdf(d1)

    gamma = nd1 / (S * sigma * sqrt_T)
    vega = S * sqrt_T * nd1 * 0.01

    if option_type == "CALL":
        delta = norm.cdf(d1)
        theta = (- (S * nd1 * sigma) / (2.0 * sqrt_T) - r * K * math.exp(-r * T) * norm.cdf(d2)) / 365.0
    else:
        delta = norm.cdf(d1) - 1.0
        theta = (- (S * nd1 * sigma) / (2.0 * sqrt_T) + r * K * math.exp(-r * T) * norm.cdf(-d2)) / 365.0

    return {"delta": delta, "gamma": gamma, "theta": theta, "vega": vega}


# ---------------------------------------------------------------------------
# Market Data (Spot Closes)
# ---------------------------------------------------------------------------

def fetch_spx_spot_history(start_date: str, end_date: str) -> Dict[date, float]:
    """Fetch daily SPX close prices for the backfill window."""
    import yfinance as yf
    logger.info(f"Downloading SPX spot history from Yahoo Finance ({start_date} -> {end_date})...")
    s = (date.fromisoformat(start_date) - timedelta(days=5)).isoformat()
    e = (date.fromisoformat(end_date) + timedelta(days=5)).isoformat()
    df = yf.download("^GSPC", start=s, end=e, progress=False)
    
    spot_map: Dict[date, float] = {}
    if isinstance(df.columns, pd.MultiIndex):
        close_series = df["Close"]["^GSPC"]
    else:
        close_series = df["Close"]
        
    for ts, val in close_series.dropna().items():
        d = ts.date() if hasattr(ts, "date") else ts
        spot_map[d] = float(val)
    logger.info(f"Loaded {len(spot_map)} SPX daily closes.")
    return spot_map


# ---------------------------------------------------------------------------
# ThetaData Puller
# ---------------------------------------------------------------------------

def list_theta_expirations(symbol: str) -> List[str]:
    url = f"{THETA_BASE_URL}/v3/option/list/expirations?symbol={symbol}"
    # SPXW alone has 700+ expirations (established earlier this session); its listing
    # call has been observed timing out at 60s, unlike SPX/SPXQ/SPXPM's much smaller
    # lists which return in seconds. Same generous-timeout fix as the per-job fetch.
    res = requests.get(url, timeout=1800)
    res.raise_for_status()
    df = pd.read_csv(io.StringIO(res.text))
    return sorted(df["expiration"].astype(str).tolist())


def _chunk_ranges(range_start: date, range_end: date, max_days: int = 365) -> List[Tuple[date, date]]:
    """Split [range_start, range_end] into <=max_days pieces -- ThetaData rejects any
    single request spanning more than 365 days."""
    chunks: List[Tuple[date, date]] = []
    cur = range_start
    while cur <= range_end:
        chunk_end = min(cur + timedelta(days=max_days - 1), range_end)
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return chunks


def download_raw_chains(
    start_date: str = DEFAULT_START,
    end_date: str = DEFAULT_END,
    max_expiration: str = "2028-12-31",
    delay_sec: float = 1.0,
) -> None:
    """Download all EOD chain files for SPX and SPXW from ThetaTerminal into
    data/raw_theta/<root>/<expiration>.csv -- one subfolder per root so the
    cache is browsable instead of one flat directory of hundreds of files.
    Each expiration's own date range (start_date -> min(end_date, expiration),
    since a contract can't trade after it expires) is split into <=365-day
    chunks, since ThetaData rejects a single request spanning more than that."""
    global_start = date.fromisoformat(start_date)
    global_end = date.fromisoformat(end_date)

    # Consecutive-failure tracking across the WHOLE function (not per-symbol): a wedged
    # Terminal (its single FREE-tier concurrent slot stuck from an earlier killed
    # request) returns HTTP 429 instantly for every request, including cheap ones like
    # list-expirations -- so left unchecked, a wedge burns through the entire remaining
    # job list in seconds doing nothing, and the run looks "complete" with no exception
    # raised. Fail fast and loud instead: 3 in a row raises immediately rather than
    # silently no-oping through everything else.
    consecutive_failures = 0
    total_failures = 0
    MAX_CONSECUTIVE_FAILURES = 3

    # SPX fans out into these roots per ThetaData's own symbology (confirmed via the
    # Terminal's NOT_FOUND hint). SPXQ (quarterlies) and SPXPM (PM-settled monthlies)
    # were discontinued before this gap window (last listings 2015 and 2018-12-21
    # respectively) so they contribute zero files here -- included anyway for
    # correctness/completeness rather than assuming.
    for symbol in ("SPX", "SPXW", "SPXQ", "SPXPM"):
        symbol_dir = RAW_DATA_DIR / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Fetching expirations for {symbol}...")
        try:
            exps = list_theta_expirations(symbol)
            consecutive_failures = 0
        except Exception as exc:
            logger.error(f"Failed to list expirations for {symbol}: {exc}")
            total_failures += 1
            consecutive_failures += 1
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                raise RuntimeError(
                    f"{consecutive_failures} consecutive failures (last: listing {symbol}) -- "
                    "Terminal is likely wedged (stuck concurrent-request slot from an earlier "
                    "killed request). Restart the Terminal before retrying."
                )
            continue

        target_exps = [e for e in exps if start_date <= e <= max_expiration]
        logger.info(f"Found {len(target_exps)} expirations for {symbol} in window.")

        # Precompute (expiration, chunk_start, chunk_end) so progress logging counts
        # actual requests, not just expirations.
        jobs: List[Tuple[str, date, date, int]] = []
        for exp in target_exps:
            exp_date = date.fromisoformat(exp)
            range_end = min(global_end, exp_date)
            if global_start > range_end:
                continue
            chunks = _chunk_ranges(global_start, range_end)
            for chunk_start, chunk_end in chunks:
                jobs.append((exp, chunk_start, chunk_end, len(chunks)))

        for idx, (exp, chunk_start, chunk_end, n_chunks) in enumerate(jobs, 1):
            suffix = "" if n_chunks == 1 else f"__{chunk_start.isoformat()}_{chunk_end.isoformat()}"
            out_file = symbol_dir / f"{exp}{suffix}.csv"

            if out_file.exists() and out_file.stat().st_size > 100:
                continue

            url = (
                f"{THETA_BASE_URL}/v3/option/history/eod?"
                f"symbol={symbol}&expiration={exp.replace('-', '')}&"
                f"start_date={chunk_start.isoformat().replace('-', '')}&"
                f"end_date={chunk_end.isoformat().replace('-', '')}"
            )
            try:
                t0 = time.time()
                # Full-year chunks for liquid expirations have taken up to 1400s (23+ min)
                # to complete successfully -- a short timeout here doesn't just fail the
                # request, it abandons a still-processing server-side job, which then looks
                # like a wedged concurrent slot to every subsequent request. Generous timeout
                # is the actual fix, not faster failure.
                res = requests.get(url, timeout=1800)
                if res.status_code == 200 and len(res.content) > 50:
                    out_file.write_bytes(res.content)
                    dt = time.time() - t0
                    logger.info(f"[{idx}/{len(jobs)}] Saved {symbol} {exp} {suffix or ''} ({len(res.content):,} bytes in {dt:.2f}s)")
                    consecutive_failures = 0
                else:
                    logger.warning(f"[{idx}/{len(jobs)}] {symbol} {exp} {suffix or ''} returned status {res.status_code} ({len(res.content)} bytes)")
                    total_failures += 1
                    consecutive_failures += 1
            except Exception as exc:
                logger.error(f"Error fetching {symbol} {exp} {suffix or ''}: {exc}")
                total_failures += 1
                consecutive_failures += 1

            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                raise RuntimeError(
                    f"{consecutive_failures} consecutive failures (last: {symbol} {exp}) -- "
                    "Terminal is likely wedged (stuck concurrent-request slot from an earlier "
                    "killed request). Restart the Terminal before retrying."
                )

            time.sleep(delay_sec)

    if total_failures > 0:
        raise RuntimeError(
            f"download_raw_chains finished with {total_failures} failed request(s) -- "
            "not a clean run, do not treat this as complete. Check the log for details."
        )


# ---------------------------------------------------------------------------
# Data Reshaper & Local Parquet Archiver
# ---------------------------------------------------------------------------

def archive_chain_to_local(biz_date: date, contracts: Sequence[Dict[str, Any]]) -> pathlib.Path:
    """Write the full point-in-time chain to local Parquet archive, one folder
    per day (year=/month=/day=) so each session's output is self-contained and
    browsable, while staying Hive-partition compatible with the live ETL's GCS
    layout (year=/month=) if this archive is ever uploaded there."""
    dest_dir = (
        ARCHIVE_DIR
        / f"year={biz_date.year}"
        / f"month={biz_date.month:02d}"
        / f"day={biz_date.day:02d}"
    )
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_file = dest_dir / "chain.parquet"

    df = pd.DataFrame(contracts)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, dest_file, compression="snappy")
    return dest_file


def process_backfill(
    start_date: str = DEFAULT_START,
    end_date: str = DEFAULT_END,
    dry_run: bool = False,
    r_rate: float = 0.05,
) -> None:
    """Process all downloaded raw CSVs, solve IV/Greeks, and write to Postgres & Parquet."""
    raw_files = list(RAW_DATA_DIR.glob("*/*.csv"))
    if not raw_files:
        logger.error("No raw CSV files found in data/raw_theta/<root>/. Run with download first.")
        return

    logger.info(f"Scanning {len(raw_files)} CSV files in {RAW_DATA_DIR}...")
    spot_map = fetch_spx_spot_history(start_date, end_date)

    # 1. Group raw contract rows by quote session date
    daily_raw_records: Dict[str, List[Dict[str, Any]]] = {}

    for f in raw_files:
        try:
            df = pd.read_csv(f, dtype={"symbol": str, "expiration": str, "right": str})
            if df.empty or "created" not in df.columns:
                continue
            
            df["quote_date"] = df["created"].str[:10]
            df = df[(df["quote_date"] >= start_date) & (df["quote_date"] <= end_date)]
            if df.empty:
                continue

            records = df.to_dict(orient="records")
            for r in records:
                q_date = r["quote_date"]
                daily_raw_records.setdefault(q_date, []).append(r)
        except Exception as exc:
            logger.warning(f"Failed to read {f.name}: {exc}")

    sorted_dates = sorted(daily_raw_records.keys())
    logger.info(f"Found {len(sorted_dates)} trading sessions to process from {sorted_dates[0] if sorted_dates else 'N/A'} to {sorted_dates[-1] if sorted_dates else 'N/A'}")

    conn = None if dry_run else get_db_connection()
    prev_summary: Optional[Dict[str, Any]] = None

    for idx, q_date_str in enumerate(sorted_dates, 1):
        q_date = date.fromisoformat(q_date_str)
        spot = spot_map.get(q_date)
        if not spot or spot <= 0:
            logger.warning(f"Skipping {q_date_str}: no spot price available.")
            continue

        raw_rows = daily_raw_records[q_date_str]
        contracts: List[Dict[str, Any]] = []
        expirations_set = set()

        for r in raw_rows:
            exp_str = str(r["expiration"])
            try:
                exp_d = date.fromisoformat(exp_str)
            except ValueError:
                continue

            dte = (exp_d - q_date).days
            if dte < 0:
                continue

            strike = float(r["strike"])
            opt_type = "CALL" if str(r["right"]).upper() in ("CALL", "C") else "PUT"
            bid = float(r.get("bid") or 0.0)
            ask = float(r.get("ask") or 0.0)
            close_p = float(r.get("close") or 0.0)
            vol = int(r.get("volume") or 0)

            # Mid price
            if bid > 0 and ask > 0:
                mid = (bid + ask) / 2.0
            elif close_p > 0:
                mid = close_p
            else:
                mid = 0.0

            # Solve IV
            T = max(dte / 365.0, 1.0 / 365.0)
            sigma = solve_iv(mid, spot, strike, T, r_rate, opt_type) if mid > 0 else 0.0
            greeks = compute_greeks(spot, strike, T, r_rate, sigma, opt_type)

            expirations_set.add(exp_str)
            contracts.append({
                "option": f"{r['symbol']}{exp_str.replace('-', '')}{opt_type[0]}{int(strike):05d}",
                "symbol": r["symbol"],
                "expiration": exp_str,
                "strike": strike,
                "type": opt_type,
                "dte": dte,
                "bid": bid,
                "ask": ask,
                "close": close_p,
                "iv": sigma if sigma > 0 else 0.0,
                "delta": greeks["delta"],
                "gamma": greeks["gamma"],
                "theta": greeks["theta"],
                "vega": greeks["vega"],
                "volume": vol,
                "open_interest": None,
            })

        if not contracts:
            continue

        payload = {
            "spot_price": spot,
            "cboe_timestamp": f"{q_date_str}T16:00:00",
            "expirations": sorted(list(expirations_set)),
            "contracts": contracts,
        }

        try:
            snap = build_snapshot(payload)
            summary = snap["summary"]
            summary["biz_date"] = q_date

            if prev_summary and prev_summary.get("spot") and prev_summary.get("atm_skew_slope"):
                ssr = compute_ssr(
                    spot_now=spot,
                    atm_iv_now=summary["atm_iv"],
                    spot_prev=prev_summary["spot"],
                    atm_iv_prev=prev_summary["atm_iv"],
                    slope_prev=prev_summary["atm_skew_slope"],
                )
                summary["ssr"] = ssr

            prev_summary = summary

            if not dry_run and conn:
                upsert_summary(conn, summary)
                conn.commit()

            parquet_path = archive_chain_to_local(q_date, contracts)

            if idx % 20 == 0 or idx == len(sorted_dates):
                logger.info(
                    f"[{idx}/{len(sorted_dates)}] {q_date_str} | Spot: {spot:,.2f} | "
                    f"ATM IV: {summary['atm_iv']:.2f}% | Slope: {summary['atm_skew_slope']} | "
                    f"Contracts: {len(contracts):,} -> {parquet_path.name}"
                )
        except Exception as exc:
            logger.error(f"Error building snapshot for {q_date_str}: {exc}")

    if conn:
        conn.close()
    logger.info("Backfill processing completed successfully.")


# ---------------------------------------------------------------------------
# CLI Entrypoint
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="SPX Option Chain Gap Backfill")
    parser.add_argument("--start-date", default=DEFAULT_START, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=DEFAULT_END, help="End date (YYYY-MM-DD)")
    parser.add_argument("--download-only", action="store_true", help="Only download raw CSVs from ThetaData")
    parser.add_argument("--process-only", action="store_true", help="Only process existing downloaded CSVs")
    parser.add_argument("--dry-run", action="store_true", help="Process without writing to Postgres")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if not args.process_only:
        logger.info(f"Starting raw ThetaData download ({args.start_date} -> {args.end_date})...")
        download_raw_chains(start_date=args.start_date, end_date=args.end_date)

    if not args.download_only:
        logger.info(f"Starting processing ({args.start_date} -> {args.end_date})...")
        process_backfill(start_date=args.start_date, end_date=args.end_date, dry_run=args.dry_run)

    return 0


if __name__ == "__main__":
    sys.exit(main())
