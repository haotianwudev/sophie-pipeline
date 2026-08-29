-- Adds SqueezeMetrics DIX/GEX to vol_regime_data.
--
-- dix     : Dark Index -- dollar-weighted Dark Pool Indicator of the S&P 500 components (0..1,
--           higher = more bullish dark-pool sentiment). Free public feed, no auth:
--           https://squeezemetrics.com/monitor/static/DIX.csv
-- dix_gex : Gamma Exposure in dollars, from the same feed -- option market-makers' aggregate
--           hedging obligation (high = dampened/low realized vol expected, low = choppier).
--           Prefixed `dix_` (not just `gex`) to keep it unambiguous from any future GEX computed
--           on this platform's own option-chain OI/gamma (e.g. spx_option_snapshot.net_gex_m) --
--           the two are different derivations and will disagree; that disagreement is itself a
--           signal, not a bug, so don't merge the columns later.
--
-- Source updates daily (no weekends/holidays) and only goes back to 2011-05-02, unlike the rest
-- of this table's SPX/VIX history to 2000 -- expect NULL before then.

ALTER TABLE vol_regime_data
    ADD COLUMN IF NOT EXISTS dix NUMERIC(10, 6),
    ADD COLUMN IF NOT EXISTS dix_gex NUMERIC(20, 4);

COMMENT ON COLUMN vol_regime_data.dix IS
    'Dark Index from SqueezeMetrics (free public feed) -- dollar-weighted dark-pool sentiment, 0..1. NULL before 2011-05-02.';
COMMENT ON COLUMN vol_regime_data.dix_gex IS
    'Gamma Exposure ($) from SqueezeMetrics DIX feed -- NOT derived from this platform''s own option-chain OI/gamma. NULL before 2011-05-02.';
