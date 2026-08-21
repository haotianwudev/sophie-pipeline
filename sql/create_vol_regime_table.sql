-- Volatility Regime / Variance Risk Premium (VRP) precomputed daily signals.
--
-- Populated by src/agents/vol_regime.py from the `prices` table (SPX, VIX) plus
-- FRED VXVCLS (3-month VIX) for term structure. Mirrors investment_clock_data:
-- one row per business date, upserted, with a categorical regime + the raw
-- components behind it so the frontend can show the reasoning, not just a label.
--
-- Why precomputed: vix_rank is a 252-day percentile and vrp_z is an EWM z-score --
-- neither can be derived browser-side from a single live quote.

CREATE TABLE IF NOT EXISTS vol_regime_data (
    biz_date            DATE PRIMARY KEY,

    -- Raw inputs
    spx_close           NUMERIC(15, 4),
    vix                 NUMERIC(10, 4),
    vix3m               NUMERIC(10, 4),

    -- Volatility measures (annualized %, comparable to VIX)
    realized_vol_20d    NUMERIC(10, 4),
    realized_vol_10d    NUMERIC(10, 4),

    -- Variance risk premium: implied minus realized
    vrp                 NUMERIC(10, 4),
    vrp_z               NUMERIC(10, 4),   -- EWM z-score of vrp (is the premium rich vs its own history)
    vrp_percentile      NUMERIC(10, 6),   -- 252d percentile rank of vrp (0..1)
    vrp_variance        NUMERIC(10, 4),   -- VIX^2 - realized_vol_20d^2 (variance-point VRP, no convexity inflation)
    downside_variance_share NUMERIC(10, 6), -- share of trailing 20d realized variance from down days (0..1)

    -- Forward-looking research fields (lookahead by construction; NULL for most recent ~21 rows)
    fwd_realized_vol_21d NUMERIC(10, 4),  -- realized vol over the NEXT 21 sessions
    fwd_earned_premium  NUMERIC(10, 4),   -- vix - fwd_realized_vol_21d: what a seller actually earned

    -- Stress / regime context
    vix_rank            NUMERIC(10, 6),   -- 252d percentile rank of VIX (0..1)
    term_slope          NUMERIC(10, 4),   -- vix3m - vix; negative = backwardation = stress
    term_structure      VARCHAR(20),      -- 'Contango' | 'Backwardation' | NULL when vix3m missing

    -- Classification
    regime              VARCHAR(30) NOT NULL,  -- Harvest | Stressed Premium | Thin | Crisis
    regime_score        NUMERIC(10, 4),        -- signed premium-attractiveness score

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_vol_regime_biz_date ON vol_regime_data(biz_date DESC);
CREATE INDEX IF NOT EXISTS idx_vol_regime_regime ON vol_regime_data(regime);

COMMENT ON TABLE vol_regime_data IS
    'Daily volatility-regime / variance-risk-premium signals precomputed from SPX+VIX history';
COMMENT ON COLUMN vol_regime_data.vrp IS
    'VIX minus 20d realized vol. Positive = options implied richer than realized (premium sellers paid)';
COMMENT ON COLUMN vol_regime_data.regime IS
    'Harvest (rich premium, calm) | Stressed Premium (rich, high vol) | Thin (poor premium) | Crisis (implied below realized)';
