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

-- The two tables below index sophie-desk/gdocs/ -- the user's personal Google
-- Drive research-session index and article-linking backlog. That directory
-- is gitignored (personal titles, not for the public repo); this DB is too,
-- so pulling it in here is safe. Both tables are skipped (no error) when
-- gdocs/ doesn't exist on the machine running build_index.py.

CREATE TABLE gdocs_index (
    doc_id TEXT PRIMARY KEY,        -- Google Drive doc id
    title TEXT,                     -- .gdoc stub filename minus extension
    resource_key TEXT,              -- often empty
    relpath TEXT,                   -- path relative to the Drive sync root
    mtime REAL                      -- epoch seconds
);

CREATE TABLE article_gdoc_matches (
    slug TEXT PRIMARY KEY,          -- Sophie article slug
    article_title TEXT,
    extracted_page_title TEXT,      -- title as found in the fetched Drive doc
    match_tier TEXT,                -- matched / no match (confirmed) / fetch failed
    matched_doc_id TEXT             -- references gdocs_index.doc_id; empty if no match
);

-- One row per tasks/*.md and tasks/done/*.md file -- the sophie-desk task
-- board. Parsed with a lenient line-based frontmatter reader, NOT real YAML
-- (see build_index.py's parse_flat_frontmatter for why: probe-error text in
-- `progress` routinely contains unquoted colons that break a real parser).

CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    title TEXT,
    lane TEXT,                      -- content / research / platform
    status TEXT,                    -- queued / active / blocked / gate / done
    assignee TEXT,                  -- claude / agy / either / none
    gate TEXT,                      -- g1 / g2 / g3 / empty
    repo TEXT,
    blocker TEXT,
    next TEXT,
    probe TEXT,
    progress TEXT,                  -- written by the supervisor -- not hand-edited
    probe_status TEXT,              -- OK / RUN / STALL / ERROR
    stall_flag TEXT,                -- non-empty when active with no recent commit
    outcome TEXT,
    artifacts TEXT,
    created TEXT,
    updated TEXT,
    in_done INTEGER,                -- 1 if the file lives in tasks/done/, else 0
    file_path TEXT
);

-- One row per notes/pipeline/*.md -- supervisor-written pipeline health
-- (fetched from Neon + Cloud Scheduler, survives the workstation being
-- asleep). Not implemented yet as of 2026-09 -- this table stays empty
-- until that folder exists; build_index.py skips it gracefully either way.

CREATE TABLE pipeline (
    table_name TEXT PRIMARY KEY,
    schedule TEXT,
    last_row TEXT,
    note TEXT
);
