-- Schema for the local paper-index SQLite DB.
-- Rebuilt from scratch by build_index.py on every run (source of truth stays
-- the markdown in sophie-desk/papers/ -- this DB is a disposable, queryable copy).

CREATE TABLE papers (
    slug TEXT PRIMARY KEY,          -- filename stem, e.g. "bekaert-hoerova-2014-vix-variance-premium"
    title TEXT,
    authors TEXT,
    year INTEGER,
    link TEXT,
    area TEXT,                      -- frontmatter 'area' (e.g. vrp-measurement)
    category TEXT,                  -- folder under papers/ the note lives in (e.g. option-writing)
    relevance TEXT,                 -- High / Medium / Low
    has_pdf INTEGER,                -- 0/1
    has_detailed_summary INTEGER,   -- 0/1
    citations_surfaced INTEGER,
    file_path TEXT                  -- path relative to the workspace root, for opening in Obsidian
);

CREATE VIRTUAL TABLE papers_fts USING fts5(
    slug UNINDEXED,
    title,
    authors,
    body                             -- full markdown body incl. summary + detailed summary
);

CREATE TABLE candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT,                     -- candidates/<topic>.md filename stem
    title TEXT,
    authors_year TEXT,
    why TEXT,
    tags TEXT,                      -- comma-separated, freeform
    surfaced_by TEXT,                -- paper slug(s) or Sophie article link(s) that surfaced this
    doc_id_source TEXT,             -- Gemini Deep Research Drive doc_id(s), if any
    status TEXT                     -- e.g. "Selected -- librarian-round-6-vrp-core"
);

CREATE VIRTUAL TABLE candidates_fts USING fts5(
    id UNINDEXED,
    title,
    authors_year,
    why,
    tags
);
