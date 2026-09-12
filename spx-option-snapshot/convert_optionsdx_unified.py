"""Re-derive the OptionsDX 2010-2023 SPX chain history from the RAW .7z archives
(not the lossy data/processed/*.parquet) into the canonical unified schema,
with IV/greeks recomputed uniformly via the same Black-Scholes engine the
ThetaData backfill uses (src/tools/options_math.py) -- see the
spx-option-chain-unify skill, decision point 1.

Does NOT touch sophie-option-research/src/convert_optionsdx.py or its output;
writes a fresh parallel tree instead, one chain.parquet per trading day:
  data/spx_chain_unified/year=YYYY/month=MM/day=DD/chain.parquet

Root is written as the ROOT_SPX_UNSPLIT sentinel for every row -- confirmed
2026-09-10 that the raw files carry no SPX/SPXW indicator anywhere (33-column
header inspected directly), so this is permanent, not a placeholder pending
more digging.

Usage:
  poetry run python spx-option-snapshot/convert_optionsdx_unified.py
  (run from the sophie-option-research .venv, which already has py7zr+scipy+pyarrow:
   F:\\workspace\\sophie-option-research\\.venv\\Scripts\\python.exe spx-option-snapshot\\convert_optionsdx_unified.py)
"""

import io
import logging
import pathlib
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from typing import Any, Dict, List

import pandas as pd
import py7zr

ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.tools.options_math import solve_iv, compute_greeks  # noqa: E402
from src.tools.spx_chain_schema import (  # noqa: E402
    write_canonical_chain, ROOT_SPX_UNSPLIT, SOURCE_OPTIONSDX,
)

ARCHIVE_DIR = pathlib.Path(r"F:\workspace\sophie-option-research\spx option")
OUT_ROOT = ROOT / "data" / "spx_chain_unified"
CONTROL_DIR = OUT_ROOT / "_control"
LOG_FILE = ROOT / "data" / "convert_optionsdx_unified.log"

R_RATE = 0.05

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_FILE)],
)
logger = logging.getLogger("convert_optionsdx_unified")


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().strip("[]").strip() for c in df.columns]
    return df


def _num(v, default: float = 0.0) -> float:
    """OptionsDX raw fields are sometimes a literal blank/whitespace string
    (e.g. ' ') rather than truly empty -- plain float(x or default) still
    raises on those. Coerce defensively instead."""
    try:
        if v is None:
            return default
        f = float(v)
        return f if f == f else default  # filters NaN
    except (ValueError, TypeError):
        return default


def process_month_file(name: str, raw: pd.DataFrame) -> int:
    """Process one month's wide-format OptionsDX file, writing one
    chain.parquet per trading day found inside it. Returns total contracts written."""
    df = normalize_headers(raw)

    # Intraday products carry multiple snapshots/day -- keep only the last
    # (matches convert_optionsdx.py's existing handling for consistency).
    if "QUOTE_TIME_HOURS" in df.columns:
        last_time = df.groupby("QUOTE_DATE")["QUOTE_TIME_HOURS"].transform("max")
        df = df[df["QUOTE_TIME_HOURS"] == last_time]

    df["QUOTE_DATE"] = pd.to_datetime(df["QUOTE_DATE"].astype(str).str.strip()).dt.date
    df["EXPIRE_DATE"] = pd.to_datetime(df["EXPIRE_DATE"].astype(str).str.strip()).dt.date

    total_contracts = 0
    for q_date, day_df in df.groupby("QUOTE_DATE"):
        rows: List[Dict[str, Any]] = []
        for r in day_df.to_dict(orient="records"):
            S = _num(r.get("UNDERLYING_LAST"))
            K = _num(r.get("STRIKE"))
            exp_d = r["EXPIRE_DATE"]
            dte = max((exp_d - q_date).days, 0)
            T = max(dte / 365.0, 1.0 / 365.0)
            if S <= 0 or K <= 0:
                continue

            for prefix, opt_type in (("C", "CALL"), ("P", "PUT")):
                bid = _num(r.get(f"{prefix}_BID"))
                ask = _num(r.get(f"{prefix}_ASK"))
                last_px = _num(r.get(f"{prefix}_LAST"))
                vol = int(_num(r.get(f"{prefix}_VOLUME")))

                mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else (last_px if last_px > 0 else 0.0)
                sigma = solve_iv(mid, S, K, T, R_RATE, opt_type) if mid > 0 else 0.0
                greeks = compute_greeks(S, K, T, R_RATE, sigma, opt_type)

                rows.append({
                    "biz_date": q_date,
                    "root": ROOT_SPX_UNSPLIT,
                    "symbol": None,
                    "expiration": exp_d,
                    "strike": K,
                    "type": opt_type,
                    "dte": dte,
                    "bid": bid,
                    "ask": ask,
                    "last": last_px,
                    "iv": sigma,
                    "delta": greeks["delta"],
                    "gamma": greeks["gamma"],
                    "theta": greeks["theta"],
                    "vega": greeks["vega"],
                    "rho": None,
                    "volume": vol,
                    "open_interest": None,
                    "theo": None,
                    "change": None,
                    "percent_change": None,
                    "prev_day_close": None,
                    "last_trade_time": None,
                    "source": SOURCE_OPTIONSDX,
                })

        if rows:
            write_canonical_chain(q_date, rows, OUT_ROOT)
            total_contracts += len(rows)

    return total_contracts


def process_archive(archive_path_str: str) -> str:
    """Extract one .7z archive to a temp dir and process every month file
    inside it. Runs in a worker process -- must be a plain top-level function
    (picklable) for ProcessPoolExecutor. Writes its own .done marker on success
    so a killed/restarted run doesn't reprocess archives already finished."""
    archive_path = pathlib.Path(archive_path_str)
    marker = CONTROL_DIR / f"{archive_path.stem}.done"
    if marker.exists():
        return f"{archive_path.name}: SKIPPED (already done)"

    t0 = time.time()
    total = 0
    months_done = 0
    with tempfile.TemporaryDirectory() as tmp:
        with py7zr.SevenZipFile(archive_path) as zf:
            zf.extractall(tmp)
        for extracted in sorted(pathlib.Path(tmp).rglob("*")):
            if extracted.suffix.lower() not in (".csv", ".txt"):
                continue
            raw = pd.read_csv(extracted, low_memory=False)
            n = process_month_file(extracted.stem, raw)
            total += n
            months_done += 1

    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(f"contracts={total} months={months_done} elapsed_sec={time.time()-t0:.1f}\n")
    elapsed = time.time() - t0
    return f"{archive_path.name}: {months_done} months, {total:,} contracts in {elapsed:.0f}s"


def main():
    CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    archives = sorted(ARCHIVE_DIR.glob("*.7z"))
    logger.info(f"Found {len(archives)} archives in {ARCHIVE_DIR}")

    max_workers = 12  # leave headroom on a 24-core box for the rest of the system
    t0 = time.time()
    done, failed = 0, 0
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(process_archive, str(a)): a for a in archives}
        for fut in as_completed(futures):
            archive = futures[fut]
            try:
                msg = fut.result()
                done += 1
                logger.info(f"[{done+failed}/{len(archives)}] {msg}")
            except Exception as exc:
                failed += 1
                logger.error(f"[{done+failed}/{len(archives)}] {archive.name}: FAILED - {exc}")

    elapsed = time.time() - t0
    logger.info(f"CONVERT COMPLETE: {done} succeeded, {failed} failed, {elapsed/60:.1f} min total")
    if failed == 0:
        (OUT_ROOT / "OPTIONSDX_CONVERT_COMPLETE").write_text(f"done={done} elapsed_min={elapsed/60:.1f}\n")
        logger.info("CONVERT FULLY COMPLETE")


if __name__ == "__main__":
    main()
