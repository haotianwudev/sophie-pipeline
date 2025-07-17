-- SQL file to create company_facts table
-- Create company_facts table
CREATE TABLE IF NOT EXISTS company_facts (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    name VARCHAR(255) NOT NULL,
    cik VARCHAR(20),
    industry VARCHAR(100),
    sector VARCHAR(100),
    category VARCHAR(100),
    exchange VARCHAR(20),
    is_active BOOLEAN,
    listing_date DATE,
    location VARCHAR(255),
    market_cap NUMERIC(20, 2),
    number_of_employees INTEGER,
    sec_filings_url VARCHAR(255),
    sic_code VARCHAR(10),
    sic_industry VARCHAR(100),
    sic_sector VARCHAR(100),
    website_url VARCHAR(255),
    weighted_average_shares BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT company_facts_ticker_unique UNIQUE (ticker)
);

-- Add index for faster lookups by ticker
CREATE INDEX IF NOT EXISTS idx_company_facts_ticker ON company_facts(ticker);

-- Add comment to the table
COMMENT ON TABLE company_facts IS 'Stores company facts and metadata information';

-- Sample insert for Apple based on mock data
INSERT INTO company_facts (
    ticker, name, cik, industry, sector, category, exchange, is_active, 
    listing_date, location, market_cap, number_of_employees, sec_filings_url,
    sic_code, sic_industry, sic_sector, website_url, weighted_average_shares
) VALUES (
    'AAPL', 
    'Apple Inc.', 
    '0000320193', 
    'Technology Hardware', 
    'Information Technology', 
    'Electronic Technology', 
    'NASDAQ', 
    TRUE, 
    '1980-12-12', 
    'Cupertino, California, United States', 
    2918000000000.00, 
    164000, 
    'https://www.sec.gov/cgi-bin/browse-edgar?CIK=0000320193', 
    '3571', 
    'Electronic Computers', 
    'Manufacturing', 
    'https://www.apple.com', 
    15520000000
) ON CONFLICT (ticker) DO UPDATE SET
    name = EXCLUDED.name,
    cik = EXCLUDED.cik,
    industry = EXCLUDED.industry,
    sector = EXCLUDED.sector,
    category = EXCLUDED.category,
    exchange = EXCLUDED.exchange,
    is_active = EXCLUDED.is_active,
    listing_date = EXCLUDED.listing_date,
    location = EXCLUDED.location,
    market_cap = EXCLUDED.market_cap,
    number_of_employees = EXCLUDED.number_of_employees,
    sec_filings_url = EXCLUDED.sec_filings_url,
    sic_code = EXCLUDED.sic_code,
    sic_industry = EXCLUDED.sic_industry,
    sic_sector = EXCLUDED.sic_sector,
    website_url = EXCLUDED.website_url,
    weighted_average_shares = EXCLUDED.weighted_average_shares,
    updated_at = CURRENT_TIMESTAMP; 