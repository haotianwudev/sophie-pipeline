-- Create table for storing insider trades data
CREATE TABLE IF NOT EXISTS insider_trades (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    issuer VARCHAR(255),
    name VARCHAR(255),
    title VARCHAR(255),
    is_board_director BOOLEAN,
    transaction_date DATE,
    transaction_shares NUMERIC(20, 2),
    transaction_price_per_share NUMERIC(15, 2),
    transaction_value NUMERIC(20, 2),
    shares_owned_before_transaction NUMERIC(20, 2),
    shares_owned_after_transaction NUMERIC(20, 2),
    security_title VARCHAR(255),
    filing_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_insider_trades_ticker ON insider_trades(ticker);
CREATE INDEX IF NOT EXISTS idx_insider_trades_filing_date ON insider_trades(filing_date);
CREATE INDEX IF NOT EXISTS idx_insider_trades_transaction_date ON insider_trades(transaction_date);
CREATE INDEX IF NOT EXISTS idx_insider_trades_ticker_filing_date ON insider_trades(ticker, filing_date);
CREATE INDEX IF NOT EXISTS idx_insider_trades_name ON insider_trades(name);

-- Add comment to table
COMMENT ON TABLE insider_trades IS 'Stores insider trading data for companies';

-- Create a unique constraint on the group columns to prevent duplicates
-- This allows us to identify records uniquely and update them rather than
-- creating duplicates when reloading data
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'insider_trades_unique_constraint'
    ) THEN
        ALTER TABLE insider_trades ADD CONSTRAINT insider_trades_unique_constraint
        UNIQUE (ticker, issuer, name, title, is_board_director, transaction_date, security_title, filing_date);
    END IF;
EXCEPTION
    WHEN others THEN
        RAISE NOTICE 'Error creating unique constraint: %', SQLERRM;
END $$;

-- Create additional composite index for common queries
CREATE INDEX IF NOT EXISTS idx_insider_trades_ticker_name_transaction_date 
ON insider_trades(ticker, name, transaction_date); 