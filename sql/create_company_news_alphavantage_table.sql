-- Create table for storing Alpha Vantage news sentiment data
CREATE TABLE IF NOT EXISTS company_news_alphavantage (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    title VARCHAR(500) NOT NULL,
    url VARCHAR(1024) NOT NULL,
    time_published TIMESTAMP,
    date DATE,
    source VARCHAR(255),
    author VARCHAR(255),
    summary TEXT,
    overall_sentiment_score FLOAT,
    overall_sentiment_label VARCHAR(32),
    ticker_sentiment_score FLOAT,
    ticker_relevance_score FLOAT,
    sentiment VARCHAR(32),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_company_news_alphavantage_ticker ON company_news_alphavantage(ticker);
CREATE INDEX IF NOT EXISTS idx_company_news_alphavantage_date ON company_news_alphavantage(date);
CREATE INDEX IF NOT EXISTS idx_company_news_alphavantage_sentiment ON company_news_alphavantage(sentiment);
CREATE INDEX IF NOT EXISTS idx_company_news_alphavantage_time_published ON company_news_alphavantage(time_published);

-- Add comment to table
COMMENT ON TABLE company_news_alphavantage IS 'Stores news sentiment articles from Alpha Vantage API';

-- Add unique constraint to avoid duplicate articles for the same ticker and url
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'company_news_alphavantage_ticker_url_key'
    ) THEN
        ALTER TABLE company_news_alphavantage ADD CONSTRAINT company_news_alphavantage_ticker_url_key 
        UNIQUE (ticker, url);
    END IF;
EXCEPTION
    WHEN others THEN
        RAISE NOTICE 'Error creating unique constraint: %', SQLERRM;
END $$; 