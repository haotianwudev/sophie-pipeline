-- Create table for storing price data
CREATE TABLE IF NOT EXISTS prices (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    time TIMESTAMP NOT NULL,
    biz_date DATE NOT NULL,
    open NUMERIC(15, 2) NOT NULL,
    close NUMERIC(15, 2) NOT NULL,
    high NUMERIC(15, 2) NOT NULL,
    low NUMERIC(15, 2) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_prices_ticker ON prices(ticker);
CREATE INDEX IF NOT EXISTS idx_prices_biz_date ON prices(biz_date);
CREATE INDEX IF NOT EXISTS idx_prices_ticker_biz_date ON prices(ticker, biz_date);

-- Add comment to table
COMMENT ON TABLE prices IS 'Stores historical price data for stocks';

-- Check if a unique constraint already exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'prices_ticker_biz_date_key'
    ) THEN
        -- Add unique constraint to prevent duplicate data for the same ticker and biz_date
        ALTER TABLE prices ADD CONSTRAINT prices_ticker_biz_date_key UNIQUE (ticker, biz_date);
    END IF;
EXCEPTION
    WHEN others THEN
        -- In case of error, just log and continue
        RAISE NOTICE 'Error creating unique constraint: %', SQLERRM;
END $$; 