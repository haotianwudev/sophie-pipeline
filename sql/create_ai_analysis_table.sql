CREATE TABLE IF NOT EXISTS ai_analysis (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    agent VARCHAR(50) NOT NULL,
    signal VARCHAR(20) NOT NULL,
    confidence NUMERIC(5, 2) CHECK (confidence BETWEEN 0 AND 100),
    reasoning TEXT,
    model_name VARCHAR(50),
    model_display_name VARCHAR(100),
    
    biz_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_ai_analysis UNIQUE (ticker, agent, biz_date, model_display_name)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_ai_analysis_ticker ON ai_analysis(ticker);
CREATE INDEX IF NOT EXISTS idx_ai_analysis_agent ON ai_analysis(agent);
CREATE INDEX IF NOT EXISTS idx_ai_analysis_date ON ai_analysis(biz_date);
CREATE INDEX IF NOT EXISTS idx_ai_analysis_model ON ai_analysis(model_name);
