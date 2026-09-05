# paper-index

A local SQLite index built from the markdown notes in `sophie-desk/papers/`, for
querying the paper library and the (1000+ row) follow-up candidate backlog
without going through Obsidian Dataview.

The markdown stays the source of truth. This is a **disposable, derived**
index -- rebuild it any time the papers change, don't edit `papers.db` by hand.

## Rebuild

```
python build_index.py
```

Writes `sophie-desk/papers/.paper-index/papers.db` (gitignored there) -- inside
the `sophie-desk` vault on purpose, not next to this script, so Obsidian's
SQLite plugins can open it (they only accept vault-relative paths, not an
arbitrary external file). Optional flags:

```
python build_index.py --papers-dir <path to sophie-desk/papers> --db <output .db path>
```

## Tables

- `papers` -- one row per note in `papers/<category>/*.md` (e.g. `option-writing/`),
  parsed from its YAML frontmatter: `slug`, `title`, `authors`, `year`, `link`,
  `area`, `category`, `relevance`, `has_pdf`, `has_detailed_summary`,
  `citations_surfaced`, `file_path`.
- `papers_fts` -- FTS5 full-text search over `slug`/`title`/`authors`/`body`
  (body = the whole note, so this also searches summaries and detailed notes).
- `candidates` -- one row per entry in the `papers/candidates/*.md` backlog
  tables: `topic` (from filename), `title`, `authors_year`, `why`, `tags`,
  `surfaced_by`, `doc_id_source`, `status`.
- `candidates_fts` -- FTS5 full-text search over `title`/`authors_year`/`why`/`tags`.

See `schema.sql` for exact column types.

## Querying

**Python** (works everywhere, including FTS5):

```python
import sqlite3
conn = sqlite3.connect(r"F:\workspace\sophie-desk\papers\.paper-index\papers.db")
conn.row_factory = sqlite3.Row

# plain filter
conn.execute("SELECT slug, year FROM papers WHERE relevance = 'High' ORDER BY year").fetchall()

# full-text search
conn.execute("SELECT title, authors_year FROM candidates_fts WHERE candidates_fts MATCH 'kelly'").fetchall()

# a hyphenated term needs to be quoted as a phrase, or FTS5 parses the "-" as NOT
conn.execute('SELECT slug FROM papers_fts WHERE papers_fts MATCH \'"HAR-RV"\'').fetchall()
```

**sqlite3 CLI**: works for plain SQL, but the `sqlite3.exe` on this machine
wasn't built with the FTS5 extension, so `candidates_fts`/`papers_fts` queries
will fail there with `no such module: fts5`. Use Python (above), or a GUI
client with FTS5 support (e.g. [DB Browser for SQLite](https://sqlitebrowser.org/)),
for full-text search. Plain `SELECT`/`WHERE`/`GROUP BY` over `papers` and
`candidates` works fine in the CLI:

```
sqlite3 sophie-desk/papers/.paper-index/papers.db "SELECT topic, count(*) FROM candidates GROUP BY topic ORDER BY count(*) DESC"
```

## Viewing it in Obsidian

Since the DB lives inside the `sophie-desk` vault, a SQLite-aware community
plugin can open it directly:

1. In Obsidian, **Settings -> Community plugins -> Browse**, search for
   **SQLite Explorer** ([repo](https://github.com/qf3l3k/obsidian-sqlite-explorer)),
   install and enable it. (Alternatives: **SQLite DB** for charts/CSV export,
   **SQL Viewer** for a lighter read-only browser.)
2. Open `papers/.paper-index/papers.db` from the file tree (it may need
   "detect all file extensions" or similar enabled in the plugin/vault settings
   to show `.db` files) to get a table/schema browser with a read-only SQL runner.
3. To embed a live query in a note instead, use a fenced code block:
   ````
   ```sqlite-query
   SELECT topic, count(*) AS n FROM candidates GROUP BY topic ORDER BY n DESC
   ```
   ````
   (exact fence syntax per the plugin's own docs -- check after installing,
   in case it's changed).

Query results are read-only and only refresh on rebuild + note refresh --
nothing here can accidentally corrupt the markdown or the DB.

## Notes

- `papers/option-writing/REVIEW-INDEX.md` is skipped (it's an index file, not
  a paper note -- no frontmatter).
- Only `papers/<category>/*.md` folders are scanned for paper notes (currently
  just `option-writing/`); `papers/candidates/` is parsed separately into the
  `candidates` table.
- `papers/FOLLOWUP-CANDIDATES.md`'s "Passed on" table isn't indexed yet -- add
  it to `build_index.py` if that list grows large enough to need querying too.
