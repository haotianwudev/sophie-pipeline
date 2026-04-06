-- Quant Trending: Phase 1
-- Sources: arxiv, github, reddit, hackernews

CREATE TABLE IF NOT EXISTS quant_trending_items (
    id SERIAL PRIMARY KEY,
    source VARCHAR(20) NOT NULL,           -- 'arxiv' | 'github' | 'reddit' | 'hackernews'
    external_id VARCHAR(500) NOT NULL,     -- source-specific unique ID (arxiv ID, github full_name, reddit ID, HN objectID)
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    description TEXT,                      -- abstract / repo description / post excerpt
    author VARCHAR(300),
    heat_score FLOAT DEFAULT 0,            -- normalized 0-100 within source batch
    raw_score FLOAT DEFAULT 0,             -- original metric (stars/upvotes/points)
    tags JSONB DEFAULT '[]',               -- e.g. ["factor-model", "risk-parity"]
    published_at TIMESTAMPTZ,
    fetched_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source, external_id)
);

CREATE INDEX IF NOT EXISTS idx_quant_trending_source  ON quant_trending_items(source);
CREATE INDEX IF NOT EXISTS idx_quant_trending_heat    ON quant_trending_items(heat_score DESC);
CREATE INDEX IF NOT EXISTS idx_quant_trending_fetched ON quant_trending_items(fetched_at DESC);
