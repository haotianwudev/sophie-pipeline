-- SPX option-chain snapshot tables.
--
-- Populated daily by src/agents/spx_option_snapshot.py. These exist because an option chain
-- snapshot is NOT reproducible: unlike FRED series, a chain quoted at 16:00 ET on a given
-- session is gone forever if the run misses it. Everything time-comparative in the Options
-- Viewer -- percentile ranks, day-over-day OI deltas, skew-vs-price divergence -- depends on
-- this history existing, and none of it can be backfilled after the fact.
--
-- Two grains, deliberately split:
--   spx_option_snapshot        one row per session   (~250 rows/yr) -- what percentile ranks read
--   spx_option_chain_snapshot  per-strike slice      (~4.1k rows/session, ~1.0M/yr)
--
-- The full chain (~18k contracts/session, ~2.5 MB/day in Postgres) is archived to GCS as
-- Parquet instead (~0.59 MB/day, a few cents a year), so the raw data survives without paying
-- Postgres prices for cold strikes nothing queries.

-- ---------------------------------------------------------------------------
-- 1. Daily summary
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS spx_option_snapshot (
    biz_date            DATE PRIMARY KEY,
    quote_timestamp     TIMESTAMPTZ,
    spot                REAL,

    -- Surface metrics, measured on a constant-maturity reference cycle (see ref_dte).
    -- A FIXED expiration would roll down over its life, so a series built on one would be
    -- measuring time decay rather than the market -- hence the ~30 DTE anchor, matching the
    -- convention VIX itself uses.
    ref_expiration      DATE,
    ref_dte             INTEGER,
    atm_iv              REAL,
    put25_iv            REAL,
    call25_iv           REAL,
    rr25                REAL,   -- put25_iv - call25_iv, vol points. NOTE: platform convention is
                                -- put-minus-call, the inverse of the FX/dealer convention.
    fly25               REAL,   -- (put25_iv + call25_iv)/2 - atm_iv
    normalized_skew     REAL,   -- rr25 / atm_iv -- level-independent, what the morphology bands cut on

    -- Surface dynamics. atm_skew_slope is d(IV vol pts)/d(ln K/S) at the money, measured on the
    -- reference cycle. ssr divides the realised move in ATM IV per unit log-spot by that slope:
    --   ~1 sticky strike, ~0 sticky delta, >1 the surface repriced rather than shifted.
    -- ssr is null on the first stored session and on days the index barely moved.
    atm_skew_slope      REAL,
    ssr                 REAL,

    -- Term structure
    front_atm_iv        REAL,
    back_atm_iv         REAL,
    term_slope          REAL,   -- back - front; positive = contango

    -- Positioning, whole book (every listed expiration)
    pcr_volume          REAL,
    pcr_oi              REAL,
    total_volume        BIGINT,
    total_open_interest BIGINT,

    -- Gamma, whole book. Gamma flip is deliberately NOT stored: it requires repricing every
    -- contract at candidate spots, and it is fully recomputable from spx_option_chain_snapshot
    -- after the fact -- so it is not one of the non-reproducible things this table exists for.
    net_gex_m           REAL,   -- $M of dealer hedging flow per 1% move
    call_wall           REAL,   -- gamma-weighted, at or above spot
    put_wall            REAL,   -- gamma-weighted, at or below spot

    -- Provenance
    expiration_count    INTEGER,
    contract_count      INTEGER,
    source              TEXT DEFAULT 'cboe-delayed',
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Additive migration for databases created before the surface-dynamics columns existed.
ALTER TABLE spx_option_snapshot ADD COLUMN IF NOT EXISTS atm_skew_slope REAL;
ALTER TABLE spx_option_snapshot ADD COLUMN IF NOT EXISTS ssr REAL;

COMMENT ON TABLE spx_option_snapshot IS
    'Daily SPX option-surface summary. One row per trading session; upserted on biz_date.';
COMMENT ON COLUMN spx_option_snapshot.rr25 IS
    '25-delta risk reversal as put IV minus call IV (positive = normal put skew). Inverse of the FX/dealer convention.';

-- ---------------------------------------------------------------------------
-- 2. Per-strike slice -- key expirations, strikes within +/-25% of spot
-- ---------------------------------------------------------------------------
-- SPX and SPXW are kept as SEPARATE rows rather than merged. They are genuinely different
-- products sharing a calendar date with their own OI pools, and the correct handling differs
-- by consumer: an IV surface must dedup to one contract per strike, while OI/volume/GEX must
-- sum across both. Storing both preserves that choice downstream; merging here would destroy it.
CREATE TABLE IF NOT EXISTS spx_option_chain_snapshot (
    biz_date        DATE     NOT NULL,
    expiration      DATE     NOT NULL,
    root            TEXT     NOT NULL,   -- 'SPX' (AM-settled) | 'SPXW' (PM-settled)
    opt_type        CHAR(1)  NOT NULL,   -- 'C' | 'P'
    strike          REAL     NOT NULL,

    bid             REAL,
    ask             REAL,
    mid             REAL,
    iv              REAL,
    delta           REAL,
    gamma           REAL,
    theta           REAL,
    vega            REAL,
    rho             REAL,
    volume          INTEGER,
    open_interest   INTEGER,

    PRIMARY KEY (biz_date, expiration, root, opt_type, strike)
);

-- No secondary index by design: the primary key already leads with biz_date, so date-range
-- scans and day-over-day joins are covered. Each extra index would add ~0.5 MB/day.

COMMENT ON TABLE spx_option_chain_snapshot IS
    'Per-strike SPX chain slice: key expirations only, strikes within +/-25% of spot. Feeds day-over-day open-interest deltas.';
