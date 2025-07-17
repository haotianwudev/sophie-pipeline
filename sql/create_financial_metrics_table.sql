-- Create table for storing financial metrics data
CREATE TABLE IF NOT EXISTS financial_metrics (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    report_period DATE NOT NULL,
    period VARCHAR(10) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    market_cap NUMERIC(20, 2),
    enterprise_value NUMERIC(20, 2),
    price_to_earnings_ratio NUMERIC(15, 2),
    price_to_book_ratio NUMERIC(15, 2),
    price_to_sales_ratio NUMERIC(15, 2),
    enterprise_value_to_ebitda_ratio NUMERIC(15, 2),
    enterprise_value_to_revenue_ratio NUMERIC(15, 2),
    free_cash_flow_yield NUMERIC(10, 4),
    peg_ratio NUMERIC(10, 2),
    gross_margin NUMERIC(10, 4),
    operating_margin NUMERIC(10, 4),
    net_margin NUMERIC(10, 4),
    return_on_equity NUMERIC(10, 4),
    return_on_assets NUMERIC(10, 4),
    return_on_invested_capital NUMERIC(10, 4),
    asset_turnover NUMERIC(10, 2),
    inventory_turnover NUMERIC(10, 2),
    receivables_turnover NUMERIC(10, 2),
    days_sales_outstanding NUMERIC(10, 2),
    operating_cycle NUMERIC(10, 2),
    working_capital_turnover NUMERIC(10, 2),
    current_ratio NUMERIC(10, 2),
    quick_ratio NUMERIC(10, 2),
    cash_ratio NUMERIC(10, 2),
    operating_cash_flow_ratio NUMERIC(10, 2),
    debt_to_equity NUMERIC(10, 2),
    debt_to_assets NUMERIC(10, 4),
    interest_coverage NUMERIC(15, 2),
    revenue_growth NUMERIC(10, 4),
    earnings_growth NUMERIC(10, 4),
    book_value_growth NUMERIC(10, 4),
    earnings_per_share_growth NUMERIC(10, 4),
    free_cash_flow_growth NUMERIC(10, 4),
    operating_income_growth NUMERIC(10, 4),
    ebitda_growth NUMERIC(10, 4),
    payout_ratio NUMERIC(10, 4),
    earnings_per_share NUMERIC(15, 2),
    book_value_per_share NUMERIC(15, 2),
    free_cash_flow_per_share NUMERIC(15, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_financial_metrics_ticker ON financial_metrics(ticker);
CREATE INDEX IF NOT EXISTS idx_financial_metrics_report_period ON financial_metrics(report_period);
CREATE INDEX IF NOT EXISTS idx_financial_metrics_ticker_report_period ON financial_metrics(ticker, report_period);
CREATE INDEX IF NOT EXISTS idx_financial_metrics_ticker_period ON financial_metrics(ticker, period);

-- Add comment to table
COMMENT ON TABLE financial_metrics IS 'Stores financial metrics data for companies';

-- Check if a unique constraint already exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'financial_metrics_ticker_report_period_period_key'
    ) THEN
        -- Add unique constraint to prevent duplicate data for the same ticker, report_period and period
        ALTER TABLE financial_metrics ADD CONSTRAINT financial_metrics_ticker_report_period_period_key UNIQUE (ticker, report_period, period);
    END IF;
EXCEPTION
    WHEN others THEN
        -- In case of error, just log and continue
        RAISE NOTICE 'Error creating unique constraint: %', SQLERRM;
END $$; 