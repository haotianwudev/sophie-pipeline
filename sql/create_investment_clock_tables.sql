-- Investment Clock: stores computed FRED data + HP filter results (runs daily/weekly)
CREATE TABLE IF NOT EXISTS investment_clock_data (
    id SERIAL PRIMARY KEY,
    biz_date DATE NOT NULL,

    -- HP filter cycle components (deviation from long-term trend)
    gdp_cyclical       NUMERIC(10, 6),
    cpi_cyclical       NUMERIC(10, 6),
    indpro_cyclical    NUMERIC(10, 6),
    tcu_cyclical       NUMERIC(10, 6),
    unrate_cyclical    NUMERIC(10, 6),

    -- Composite Z-score signals (used to place clock hand)
    growth_z_score     NUMERIC(8, 4),
    inflation_z_score  NUMERIC(8, 4),

    -- Algorithmically determined phase (pure data, no LLM)
    data_phase VARCHAR(20) NOT NULL
        CHECK (data_phase IN ('Reflation', 'Recovery', 'Overheat', 'Stagflation')),

    -- Clock hand angle in degrees 0-360 (0 = 12 o'clock, clockwise)
    clock_angle        NUMERIC(7, 4),

    -- Raw FRED values snapshot at time of run
    gdp_value          NUMERIC(12, 4),
    cpi_value          NUMERIC(10, 4),
    indpro_value       NUMERIC(10, 4),
    tcu_value          NUMERIC(10, 4),
    unrate_value       NUMERIC(8, 4),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_investment_clock_data_date UNIQUE (biz_date)
);

CREATE INDEX IF NOT EXISTS idx_investment_clock_data_date ON investment_clock_data(biz_date);

COMMENT ON TABLE investment_clock_data IS 'Stores FRED-derived HP filter cycle metrics for the Investment Clock (automated ETL)';
COMMENT ON COLUMN investment_clock_data.data_phase IS 'Phase determined purely by Z-score signs: Reflation/Recovery/Overheat/Stagflation';
COMMENT ON COLUMN investment_clock_data.clock_angle IS 'Degrees 0-360 clockwise from 12 o clock for D3 clock hand rendering';


-- Investment Clock: stores LLM evaluation after Gemini Deep Research (human-in-the-loop, weekly)
CREATE TABLE IF NOT EXISTS investment_clock_evaluation (
    id SERIAL PRIMARY KEY,
    biz_date DATE NOT NULL,

    -- Claude's final phase determination (may differ from data_phase after research)
    final_phase VARCHAR(20) NOT NULL
        CHECK (final_phase IN ('Reflation', 'Recovery', 'Overheat', 'Stagflation')),

    -- Confidence 0-100
    phase_confidence   NUMERIC(5, 2) CHECK (phase_confidence BETWEEN 0 AND 100),

    -- clockwise | counterclockwise | stable
    phase_direction    VARCHAR(20),

    -- Structured LLM output
    reasoning          TEXT,
    outlook            TEXT,
    key_indicators     JSONB,
    risks              JSONB,

    -- Asset allocation recommendation for current phase
    best_asset         VARCHAR(50),
    recommended_sectors JSONB,

    -- Summary of key findings from the Gemini Deep Research paper
    gemini_research_summary TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_investment_clock_evaluation_date UNIQUE (biz_date)
);

CREATE INDEX IF NOT EXISTS idx_investment_clock_eval_date ON investment_clock_evaluation(biz_date);

COMMENT ON TABLE investment_clock_evaluation IS 'Stores Claude LLM evaluation of Investment Clock phase after Gemini Deep Research (human-in-loop)';
COMMENT ON COLUMN investment_clock_evaluation.final_phase IS 'Claude final phase call, informed by quantitative data + Gemini research paper';
COMMENT ON COLUMN investment_clock_evaluation.gemini_research_summary IS 'Key findings extracted from the Gemini Deep Research paper';
