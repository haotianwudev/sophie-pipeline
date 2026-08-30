-- Adds Cboe DSPX (S&P 500 Dispersion Index) to vol_regime_data.
--
-- dspx : implied correlation proxy -- the spread between index-level implied vol (VIX) and the
--        dollar-weighted implied vol of the S&P 500's single-name constituents. Low DSPX means
--        the index is priced expecting names to move together (correlation risk); high DSPX means
--        dispersion/idiosyncratic risk dominates. Free, no-auth Cboe CDN feed:
--        https://cdn.cboe.com/api/global/us_indices/daily_prices/DSPX_History.csv
--
-- Shortest history of this table's three external feeds -- starts 2014-06-19 (vs. 2011-05-02 for
-- dix/dix_gex, 2000 for SPX/VIX). NULL before then.

ALTER TABLE vol_regime_data
    ADD COLUMN IF NOT EXISTS dspx NUMERIC(10, 4);

COMMENT ON COLUMN vol_regime_data.dspx IS
    'Cboe S&P 500 Dispersion Index (free public CDN feed) -- implied correlation proxy. NULL before 2014-06-19.';
