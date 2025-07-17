CREATE TABLE IF NOT EXISTS valuation (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    valuation_method VARCHAR(50) NOT NULL,
    intrinsic_value NUMERIC(20, 4),
    market_cap NUMERIC(20, 4),
    gap NUMERIC(10, 4),
    signal VARCHAR(20),
    biz_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_valuation UNIQUE (ticker, valuation_method, biz_date)
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_valuation_ticker ON valuation(ticker);
CREATE INDEX IF NOT EXISTS idx_valuation_biz_date ON valuation(biz_date);
