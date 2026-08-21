-- Adds VRP decomposition + forward-looking columns to vol_regime_data.
--
-- vrp_variance          : VIX^2 - realized_vol_20d^2, scaled to comparable magnitude
--                          (the vol-point VRP overstates the premium via Jensen's
--                          inequality; variance points are what a variance swap pays)
-- downside_variance_share: fraction of trailing-20d realized variance from down days
--                          (Barndorff-Nielsen semivariance split) -- is the "risk" in
--                          the risk premium actually downside risk?
-- fwd_realized_vol_21d   : realized vol over the NEXT 21 sessions (lookahead by
--                          construction -- NULL for the most recent ~21 rows, since
--                          the future hasn't happened yet)
-- fwd_earned_premium     : vix - fwd_realized_vol_21d, i.e. what a seller of today's
--                          implied vol actually collected once the future realized

ALTER TABLE vol_regime_data
    ADD COLUMN IF NOT EXISTS vrp_variance NUMERIC(10, 4),
    ADD COLUMN IF NOT EXISTS downside_variance_share NUMERIC(10, 6),
    ADD COLUMN IF NOT EXISTS fwd_realized_vol_21d NUMERIC(10, 4),
    ADD COLUMN IF NOT EXISTS fwd_earned_premium NUMERIC(10, 4);

COMMENT ON COLUMN vol_regime_data.fwd_earned_premium IS
    'Backward-looking research field only: what a seller of today''s implied vol actually earned once the next 21 sessions realized. NULL for the most recent ~21 rows.';
