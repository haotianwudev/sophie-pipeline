-- Technicals Table
-- Stores all technical indicators and signals from technicals.py
CREATE TABLE technicals (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    biz_date DATE NOT NULL,
    
    -- Composite signal
    signal VARCHAR(10),
    confidence DECIMAL(5,2),
    
    -- Trend Following
    trend_signal VARCHAR(10),
    trend_confidence DECIMAL(5,2),
    trend_score DECIMAL(12,4),
    trend_adx_threshold DECIMAL(5,2) DEFAULT 25.0,
    trend_ema_crossover_threshold BOOLEAN DEFAULT TRUE,
    ema_8 DECIMAL(12,4),
    ema_21 DECIMAL(12,4),
    ema_55 DECIMAL(12,4),
    adx DECIMAL(12,4),
    di_plus DECIMAL(12,4),
    di_minus DECIMAL(12,4),
    
    -- Mean Reversion
    mr_signal VARCHAR(10),
    mr_confidence DECIMAL(5,2),
    mr_score DECIMAL(12,4),
    mr_z_score_threshold DECIMAL(5,2) DEFAULT 2.0,
    mr_rsi_low_threshold DECIMAL(5,2) DEFAULT 30.0,
    mr_rsi_high_threshold DECIMAL(5,2) DEFAULT 70.0,
    z_score DECIMAL(12,4),
    bb_upper DECIMAL(12,4),
    bb_lower DECIMAL(12,4),
    rsi_14 DECIMAL(12,4),
    rsi_28 DECIMAL(12,4),
    
    -- Momentum
    momentum_signal VARCHAR(10),
    momentum_confidence DECIMAL(5,2),
    momentum_score DECIMAL(12,4),
    momentum_min_strength DECIMAL(5,4) DEFAULT 0.05,
    momentum_volume_ratio_threshold DECIMAL(5,2) DEFAULT 1.0,
    mom_1m DECIMAL(12,4),
    mom_3m DECIMAL(12,4),
    mom_6m DECIMAL(12,4),
    volume_ratio DECIMAL(12,4),
    
    -- Volatility
    volatility_signal VARCHAR(10),
    volatility_confidence DECIMAL(5,2),
    volatility_score DECIMAL(12,4),
    volatility_low_regime DECIMAL(5,2) DEFAULT 0.8,
    volatility_high_regime DECIMAL(5,2) DEFAULT 1.2,
    volatility_z_threshold DECIMAL(5,2) DEFAULT 1.0,
    hist_vol_21d DECIMAL(12,4),
    vol_regime DECIMAL(12,4),
    vol_z_score DECIMAL(12,4),
    atr_ratio DECIMAL(12,4),
    
    -- Statistical Arbitrage
    stat_arb_signal VARCHAR(10),
    stat_arb_confidence DECIMAL(5,2),
    stat_arb_score DECIMAL(12,4),
    stat_arb_hurst_threshold DECIMAL(5,2) DEFAULT 0.4,
    stat_arb_skew_threshold DECIMAL(5,2) DEFAULT 1.0,
    hurst_exp DECIMAL(12,4),
    skewness DECIMAL(12,4),
    kurtosis DECIMAL(12,4),
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT uq_technicals UNIQUE (ticker, biz_date)
);

-- Indexes for performance
CREATE INDEX idx_technicals_ticker ON technicals (ticker);
CREATE INDEX idx_technicals_date ON technicals (biz_date);
CREATE INDEX idx_technicals_signal ON technicals (signal);

COMMENT ON TABLE technicals IS 'Stores technical analysis signals and metrics from technicals.py';
COMMENT ON COLUMN technicals.trend_adx_threshold IS 'ADX threshold for trend strength (default 25)';
COMMENT ON COLUMN technicals.mr_z_score_threshold IS 'Z-score threshold for mean reversion signals (default 2.0)';
COMMENT ON COLUMN technicals.momentum_min_strength IS 'Minimum momentum strength threshold (default 0.05)';
