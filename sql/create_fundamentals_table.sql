CREATE TABLE IF NOT EXISTS fundamentals (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    biz_date DATE NOT NULL,
    
    -- Overall results
    overall_signal VARCHAR(10) NOT NULL,
    confidence DECIMAL(5,2),
    
    -- Profitability metrics
    return_on_equity DECIMAL(10,4),
    roe_threshold DECIMAL(10,4) DEFAULT 0.15,
    net_margin DECIMAL(10,4),
    net_margin_threshold DECIMAL(10,4) DEFAULT 0.20,
    operating_margin DECIMAL(10,4),
    op_margin_threshold DECIMAL(10,4) DEFAULT 0.15,
    profitability_score INTEGER,
    profitability_signal VARCHAR(10),
    
    -- Growth metrics  
    revenue_growth DECIMAL(10,4),
    revenue_growth_threshold DECIMAL(10,4) DEFAULT 0.10,
    earnings_growth DECIMAL(10,4),
    earnings_growth_threshold DECIMAL(10,4) DEFAULT 0.10,
    book_value_growth DECIMAL(10,4),
    book_value_growth_threshold DECIMAL(10,4) DEFAULT 0.10,
    growth_score INTEGER,
    growth_signal VARCHAR(10),
    
    -- Financial health metrics
    current_ratio DECIMAL(10,4),
    current_ratio_threshold DECIMAL(10,4) DEFAULT 1.5,
    debt_to_equity DECIMAL(10,4),
    debt_to_equity_threshold DECIMAL(10,4) DEFAULT 0.5,
    free_cash_flow_per_share DECIMAL(10,4),
    earnings_per_share DECIMAL(10,4),
    fcf_conversion_threshold DECIMAL(10,4) DEFAULT 0.8,
    health_score INTEGER,
    health_signal VARCHAR(10),
    
    -- Valuation metrics
    pe_ratio DECIMAL(10,4),
    pe_threshold DECIMAL(10,4) DEFAULT 25,
    pb_ratio DECIMAL(10,4),
    pb_threshold DECIMAL(10,4) DEFAULT 3,
    ps_ratio DECIMAL(10,4),
    ps_threshold DECIMAL(10,4) DEFAULT 5,
    valuation_score INTEGER,
    valuation_signal VARCHAR(10),
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_fundamental UNIQUE (ticker, biz_date)
);

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_fundamentals_ticker ON fundamentals(ticker);
CREATE INDEX IF NOT EXISTS idx_fundamentals_biz_date ON fundamentals(biz_date);
