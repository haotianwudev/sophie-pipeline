-- Create table for storing company news data
CREATE TABLE IF NOT EXISTS company_news (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    title VARCHAR(500) NOT NULL,
    author VARCHAR(255),
    source VARCHAR(255) NOT NULL,
    date DATE NOT NULL,
    url VARCHAR(1024) NOT NULL,
    sentiment VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_company_news_ticker ON company_news(ticker);
CREATE INDEX IF NOT EXISTS idx_company_news_date ON company_news(date);
CREATE INDEX IF NOT EXISTS idx_company_news_ticker_date ON company_news(ticker, date);
CREATE INDEX IF NOT EXISTS idx_company_news_source ON company_news(source);
CREATE INDEX IF NOT EXISTS idx_company_news_sentiment ON company_news(sentiment);

-- Add comment to table
COMMENT ON TABLE company_news IS 'Stores news articles related to companies';

-- Check if a unique constraint already exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'company_news_ticker_url_key'
    ) THEN
        -- Add unique constraint - use URL as a unique identifier for a news article
        ALTER TABLE company_news ADD CONSTRAINT company_news_ticker_url_key 
        UNIQUE (ticker, url);
    END IF;
EXCEPTION
    WHEN others THEN
        -- In case of error, just log and continue
        RAISE NOTICE 'Error creating unique constraint: %', SQLERRM;
END $$; 