-- Create table for storing line items data
CREATE TABLE IF NOT EXISTS line_items (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    report_period DATE NOT NULL,
    period VARCHAR(10) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    cash_and_equivalents NUMERIC(25, 6),
    current_assets NUMERIC(25, 6),
    current_liabilities NUMERIC(25, 6),
    outstanding_shares NUMERIC(25, 6),
    total_assets NUMERIC(25, 6),
    shareholders_equity NUMERIC(25, 6),
    total_liabilities NUMERIC(25, 6),
    goodwill_and_intangible_assets NUMERIC(25, 6),
    total_debt NUMERIC(25, 6),
    free_cash_flow NUMERIC(25, 6),
    net_income NUMERIC(25, 6),
    dividends_and_other_cash_distributions NUMERIC(25, 6),
    depreciation_and_amortization NUMERIC(25, 6),
    capital_expenditure NUMERIC(25, 6),
    earnings_per_share NUMERIC(25, 6),
    research_and_development NUMERIC(25, 6),
    operating_income NUMERIC(25, 6),
    revenue NUMERIC(25, 6),
    working_capital NUMERIC(25, 6),
    operating_margin NUMERIC(25, 6),
    book_value_per_share NUMERIC(25, 6),
    gross_margin NUMERIC(25, 6),
    return_on_invested_capital NUMERIC(25, 6),
    ebitda NUMERIC(25, 6),
    ebit NUMERIC(25, 6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_line_items_ticker ON line_items(ticker);
CREATE INDEX IF NOT EXISTS idx_line_items_report_period ON line_items(report_period);
CREATE INDEX IF NOT EXISTS idx_line_items_ticker_report_period ON line_items(ticker, report_period);

-- Add comment to table
COMMENT ON TABLE line_items IS 'Stores financial line items data for companies with individual columns for each metric';

-- Check if a unique constraint already exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'line_items_ticker_report_period_period_key'
    ) THEN
        -- Add unique constraint
        ALTER TABLE line_items ADD CONSTRAINT line_items_ticker_report_period_period_key 
        UNIQUE (ticker, report_period, period);
    END IF;
EXCEPTION
    WHEN others THEN
        -- In case of error, just log and continue
        RAISE NOTICE 'Error creating unique constraint: %', SQLERRM;
END $$; 