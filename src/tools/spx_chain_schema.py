"""Canonical schema + writer for the unified SPX option chain archive
(data/spx_chain_unified/year=YYYY/month=MM/day=DD/chain.parquet), consolidating
OptionsDX (2010-2023), the ThetaData gap backfill (2024-2026-08-20), and the
live Cboe ETL (2026-08-21+). See the spx-option-chain-unify skill for the full
rationale behind each column and the methodology seams between sources.
"""

import pathlib
from datetime import date
from typing import Any, Dict, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Recommended superset schema (spx-option-chain-unify skill, decision point 4).
# Nulls where a given source genuinely can't provide the field -- never
# backfilled with a guess (e.g. symbol stays null pre-2026-08-21 rather than
# reusing the ThetaData backfill's non-OSI-format synthetic `option` field).
CANONICAL_COLUMNS = [
    "biz_date", "root", "symbol", "expiration", "strike", "type", "dte",
    "bid", "ask", "last", "iv", "delta", "gamma", "theta", "vega", "rho",
    "volume", "open_interest", "theo", "change", "percent_change",
    "prev_day_close", "last_trade_time", "source",
]

SOURCE_OPTIONSDX = "optionsdx"
SOURCE_THETADATA_BACKFILL = "thetadata_backfill"
SOURCE_CBOE_LIVE = "cboe_live"

# Root sentinel for the whole 2010-2023 OptionsDX range: confirmed (2026-09-10)
# that the raw files carry no SPX/SPXW indicator anywhere, so this is a
# permanent, not temporary, placeholder -- never conflate with a real 'SPX'.
ROOT_SPX_UNSPLIT = "SPX_UNSPLIT"


def write_canonical_chain(biz_date: date, rows: Sequence[Dict[str, Any]], out_root: pathlib.Path) -> pathlib.Path:
    """Write one day's canonical-schema rows to out_root/year=/month=/day=/chain.parquet."""
    dest_dir = out_root / f"year={biz_date.year}" / f"month={biz_date.month:02d}" / f"day={biz_date.day:02d}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_file = dest_dir / "chain.parquet"

    df = pd.DataFrame(rows)
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[CANONICAL_COLUMNS]

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, dest_file, compression="snappy")
    return dest_file
