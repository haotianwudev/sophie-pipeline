CREATE TABLE IF NOT EXISTS sophie_analysis (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    biz_date DATE NOT NULL,
    signal VARCHAR(20) NOT NULL CHECK (signal IN ('bullish', 'bearish', 'neutral')),
    confidence NUMERIC(5, 2) CHECK (confidence BETWEEN 0 AND 100),
    overall_score NUMERIC(5, 2) CHECK (overall_score BETWEEN 1 AND 100),
    reasoning TEXT,
    
    -- Time horizon analysis
    short_term_outlook TEXT,
    medium_term_outlook TEXT,
    long_term_outlook TEXT,
    
    -- Bullish/bearish factors (stored as JSON arrays)
    bullish_factors JSONB,
    bearish_factors JSONB,
    risks JSONB,
    
    -- Metadata
    model_name VARCHAR(50),
    model_display_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_sophie_analysis UNIQUE (ticker, biz_date, model_display_name)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_sophie_ticker ON sophie_analysis(ticker);
CREATE INDEX IF NOT EXISTS idx_sophie_date ON sophie_analysis(biz_date);
CREATE INDEX IF NOT EXISTS idx_sophie_model ON sophie_analysis(model_name);

COMMENT ON TABLE sophie_analysis IS 'Stores composite analysis results from Sophie agent';
COMMENT ON COLUMN sophie_analysis.signal IS 'Overall signal: bullish, bearish, or neutral';
COMMENT ON COLUMN sophie_analysis.confidence IS 'Confidence score between 0-100';
COMMENT ON COLUMN sophie_analysis.overall_score IS 'Composite score between 1-100';
