"""Rebuild the local SQLite paper index from sophie-desk/papers markdown.

Source of truth stays the markdown in sophie-desk/papers/ -- this script parses
it into a queryable SQLite file. Safe to rerun any time; it fully replaces the
DB contents on each run rather than trying to diff/update.

The DB is written *inside* the sophie-desk vault (papers/paper-index/papers.db)
rather than next to this script, so an Obsidian SQLite plugin (e.g. SQLite
Explorer) can open it directly -- those plugins require vault-relative paths,
they can't point at an arbitrary file outside the vault.

Usage:
    python build_index.py [--papers-dir PATH] [--db PATH]
"""

import argparse
import json
import re
import sqlite3
from pathlib import Path

import yaml

DEFAULT_PAPERS_DIR = Path(__file__).resolve().parents[2] / "sophie-desk" / "papers"
DEFAULT_DB_PATH = DEFAULT_PAPERS_DIR / "paper-index" / "papers.db"
DEFAULT_GDOCS_DIR = DEFAULT_PAPERS_DIR.parent / "gdocs"
SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"

FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n?(.*)$", re.DOTALL)
CANDIDATE_HEADER_MARKERS = ("Title (best guess)", "Authors / Year")
ARTICLE_MATCH_HEADER_MARKERS = ("Slug", "Article Title")


def _relative_path(path: Path, workspace_root: Path) -> Path:
    try:
        return path.relative_to(workspace_root)
    except ValueError:
        return path


def parse_paper_note(path: Path, category: str, workspace_root: Path) -> dict | None:
    text = path.read_text(encoding="utf-8")
    match = FRONTMATTER_RE.match(text)
    if not match:
        return None
    meta = yaml.safe_load(match.group(1)) or {}
    body = match.group(2)
    return {
        "slug": path.stem,
        "title": meta.get("title"),
        "authors": meta.get("authors"),
        "year": meta.get("year"),
        "link": meta.get("link"),
        "area": meta.get("area"),
        "category": category,
        "relevance": meta.get("relevance"),
        "has_pdf": 1 if meta.get("has_pdf") else 0,
        "has_detailed_summary": 1 if meta.get("has_detailed_summary") else 0,
        "citations_surfaced": meta.get("citations_surfaced"),
        "file_path": str(_relative_path(path, workspace_root)).replace("\\", "/"),
        "body": body,
    }


CELL_SPLIT_RE = re.compile(r"(?<!\\)\|")


def split_row(line: str) -> list[str]:
    """Split one markdown table row into cells. A literal `|` inside a cell
    is written `\\|` per standard markdown table escaping -- splitting on
    every unescaped `|` (not a naive str.split('|')) is what makes that
    survive. Hit live: gdocs/article-exact-matches.md's 'theta.md \\| Theta
    Research' row misparsed into 6 cells instead of 5 before this fix,
    shifting Match Tier/Matched Doc ID by one column."""
    line = line.strip()
    if line.startswith("|"):
        line = line[1:]
    if line.endswith("|"):
        line = line[:-1]
    return [cell.strip().replace("\\|", "|") for cell in CELL_SPLIT_RE.split(line)]


def is_separator_row(cells: list[str]) -> bool:
    return all(re.fullmatch(r":?-+:?", c) for c in cells if c)


def parse_candidates_file(path: Path) -> list[dict]:
    topic = path.stem
    lines = path.read_text(encoding="utf-8").splitlines()
    rows: list[dict] = []
    i, n = 0, len(lines)
    while i < n:
        line = lines[i]
        if line.strip().startswith("|") and all(m in line for m in CANDIDATE_HEADER_MARKERS):
            if i + 1 < n and is_separator_row(split_row(lines[i + 1])):
                j = i + 2
                while j < n and lines[j].strip().startswith("|"):
                    cells = split_row(lines[j])
                    if len(cells) >= 7:
                        rows.append({
                            "topic": topic,
                            "title": cells[0],
                            "authors_year": cells[1],
                            "why": cells[2],
                            "tags": cells[3],
                            "surfaced_by": cells[4],
                            "doc_id_source": cells[5],
                            "status": cells[6],
                        })
                    j += 1
                i = j
                continue
        i += 1
    return rows


def parse_article_matches(path: Path) -> list[dict]:
    """Parse gdocs/article-exact-matches.md's single table: Slug, Article Title,
    Extracted Page Title, Match Tier, Matched Doc ID."""
    lines = path.read_text(encoding="utf-8").splitlines()
    rows: list[dict] = []
    i, n = 0, len(lines)
    while i < n:
        line = lines[i]
        if line.strip().startswith("|") and all(m in line for m in ARTICLE_MATCH_HEADER_MARKERS):
            if i + 1 < n and is_separator_row(split_row(lines[i + 1])):
                j = i + 2
                while j < n and lines[j].strip().startswith("|"):
                    cells = split_row(lines[j])
                    if len(cells) >= 5:
                        rows.append({
                            "slug": cells[0],
                            "article_title": cells[1],
                            "extracted_page_title": cells[2],
                            "match_tier": cells[3],
                            "matched_doc_id": cells[4],
                        })
                    j += 1
                i = j
                continue
        i += 1
    return rows


def collect_paper_notes(papers_dir: Path) -> list[Path]:
    notes = []
    for category_dir in sorted(papers_dir.iterdir()):
        if not category_dir.is_dir() or category_dir.name == "candidates":
            continue
        notes.extend(sorted(category_dir.glob("*.md")))
    return notes


def build(papers_dir: Path, db_path: Path, gdocs_dir: Path | None = None) -> None:
    workspace_root = papers_dir.parents[1]

    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))

    paper_count = 0
    for note_path in collect_paper_notes(papers_dir):
        category = note_path.parent.name
        paper = parse_paper_note(note_path, category, workspace_root)
        if paper is None:
            print(f"skip (no frontmatter): {note_path}")
            continue
        conn.execute(
            """INSERT INTO papers
               (slug, title, authors, year, link, area, category, relevance,
                has_pdf, has_detailed_summary, citations_surfaced, file_path)
               VALUES (:slug, :title, :authors, :year, :link, :area, :category,
                       :relevance, :has_pdf, :has_detailed_summary,
                       :citations_surfaced, :file_path)""",
            paper,
        )
        conn.execute(
            "INSERT INTO papers_fts (slug, title, authors, body) VALUES (?, ?, ?, ?)",
            (paper["slug"], paper["title"], paper["authors"], paper["body"]),
        )
        paper_count += 1

    candidate_count = 0
    candidates_dir = papers_dir / "candidates"
    for candidates_path in sorted(candidates_dir.glob("*.md")):
        for row in parse_candidates_file(candidates_path):
            cur = conn.execute(
                """INSERT INTO candidates
                   (topic, title, authors_year, why, tags, surfaced_by, doc_id_source, status)
                   VALUES (:topic, :title, :authors_year, :why, :tags, :surfaced_by,
                           :doc_id_source, :status)""",
                row,
            )
            conn.execute(
                "INSERT INTO candidates_fts (id, title, authors_year, why, tags) VALUES (?, ?, ?, ?, ?)",
                (cur.lastrowid, row["title"], row["authors_year"], row["why"], row["tags"]),
            )
            candidate_count += 1

    gdoc_index_count = 0
    article_match_count = 0
    if gdocs_dir is not None and gdocs_dir.is_dir():
        index_json_path = gdocs_dir / "index.json"
        if index_json_path.exists():
            entries = json.loads(index_json_path.read_text(encoding="utf-8"))
            for entry in entries:
                conn.execute(
                    """INSERT OR REPLACE INTO gdocs_index
                       (doc_id, title, resource_key, relpath, mtime)
                       VALUES (:doc_id, :title, :resource_key, :relpath, :mtime)""",
                    entry,
                )
                gdoc_index_count += 1
        else:
            print(f"skip (not found): {index_json_path}")

        matches_path = gdocs_dir / "article-exact-matches.md"
        if matches_path.exists():
            for row in parse_article_matches(matches_path):
                conn.execute(
                    """INSERT OR REPLACE INTO article_gdoc_matches
                       (slug, article_title, extracted_page_title, match_tier, matched_doc_id)
                       VALUES (:slug, :article_title, :extracted_page_title,
                               :match_tier, :matched_doc_id)""",
                    row,
                )
                article_match_count += 1
        else:
            print(f"skip (not found): {matches_path}")
    else:
        print(f"skip gdocs tables: {gdocs_dir} not found on this machine")

    conn.commit()
    conn.close()
    print(f"Indexed {paper_count} papers and {candidate_count} candidates -> {db_path}")
    print(f"Indexed {gdoc_index_count} gdocs_index rows and {article_match_count} article_gdoc_matches rows")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--papers-dir", type=Path, default=DEFAULT_PAPERS_DIR)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--gdocs-dir", type=Path, default=DEFAULT_GDOCS_DIR)
    args = parser.parse_args()
    build(args.papers_dir, args.db, args.gdocs_dir)
