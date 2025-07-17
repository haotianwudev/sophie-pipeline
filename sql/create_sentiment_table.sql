-- Sentiment Analysis Table Schema
-- Stores sentiment analysis results from insider trading and news analysis

CREATE TABLE sentiment (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    biz_date DATE NOT NULL,
    overall_signal VARCHAR(10) NOT NULL,  -- 'bullish', 'bearish', 'neutral'
    confidence DECIMAL(5,2) NOT NULL,     -- 0-100%
    
    -- Insider Trading Metrics
    insider_total INTEGER,                 -- Total insider transactions analyzed
    insider_bullish INTEGER,               -- Count of bullish insider trades
    insider_bearish INTEGER,               -- Count of bearish insider trades
    insider_value_total FLOAT,                 -- Total insider transaction_value
    insider_value_bullish FLOAT,               -- Sum of bullish insider trades transaction_value
    insider_value_bearish FLOAT,               -- Sum of bearish insider trades transaction_value
    insider_weight DECIMAL(3,2) DEFAULT 0.30,
    
    -- News Sentiment Metrics
    news_total INTEGER,                    -- Total news articles analyzed
    news_bullish INTEGER,                  -- Count of bullish news articles
    news_bearish INTEGER,                  -- Count of bearish news articles
    news_neutral INTEGER,                  -- Count of neutral news articles  
    news_weight DECIMAL(3,2) DEFAULT 0.70,
    
    -- Calculated Weighted Values
    weighted_bullish DECIMAL(10,2),        -- insider_bullish*0.3 + news_bullish*0.7
    weighted_bearish DECIMAL(10,2),        -- insider_bearish*0.3 + news_bearish*0.7
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_ticker_date UNIQUE (ticker, biz_date)
);

COMMENT ON TABLE sentiment IS 'Stores sentiment analysis results combining insider trading and news sentiment';
COMMENT ON COLUMN sentiment.overall_signal IS 'Final aggregated signal (bullish/bearish/neutral)';
COMMENT ON COLUMN sentiment.confidence IS 'Confidence level 0-100% based on signal strength';
COMMENT ON COLUMN sentiment.insider_weight IS 'Weight given to insider trading signals (default 30%)';
COMMENT ON COLUMN sentiment.news_weight IS 'Weight given to news sentiment signals (default 70%)';
