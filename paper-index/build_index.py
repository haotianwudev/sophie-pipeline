"""Rebuild the local SQLite paper index from sophie-desk/papers markdown.

Source of truth stays the markdown in sophie-desk/papers/ -- this script parses
it into a queryable SQLite file. Safe to rerun any time; it fully replaces the
DB contents on each run rather than trying to diff/update.

The DB is written *inside* the sophie-desk vault (papers/.paper-index/papers.db)
rather than next to this script, so an Obsidian SQLite plugin (e.g. SQLite
Explorer) can open it directly -- those plugins require vault-relative paths,
they can't point at an arbitrary file outside the vault.

Usage:
    python build_index.py [--papers-dir PATH] [--db PATH]
"""

import argparse
import re
import sqlite3
from pathlib import Path

import yaml

DEFAULT_PAPERS_DIR = Path(__file__).resolve().parents[2] / "sophie-desk" / "papers"
DEFAULT_DB_PATH = DEFAULT_PAPERS_DIR / ".paper-index" / "papers.db"
SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"

FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n?(.*)$", re.DOTALL)
CANDIDATE_HEADER_MARKERS = ("Title (best guess)", "Authors / Year")


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


def split_row(line: str) -> list[str]:
    line = line.strip()
    if line.startswith("|"):
        line = line[1:]
    if line.endswith("|"):
        line = line[:-1]
    return [cell.strip() for cell in line.split("|")]


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


def collect_paper_notes(papers_dir: Path) -> list[Path]:
    notes = []
    for category_dir in sorted(papers_dir.iterdir()):
        if not category_dir.is_dir() or category_dir.name == "candidates":
            continue
        notes.extend(sorted(category_dir.glob("*.md")))
    return notes


def build(papers_dir: Path, db_path: Path) -> None:
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

    conn.commit()
    conn.close()
    print(f"Indexed {paper_count} papers and {candidate_count} candidates -> {db_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--papers-dir", type=Path, default=DEFAULT_PAPERS_DIR)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    build(args.papers_dir, args.db)
