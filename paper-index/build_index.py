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
from datetime import datetime, timezone
from pathlib import Path

import yaml

DEFAULT_PAPERS_DIR = Path(__file__).resolve().parents[2] / "sophie-desk" / "papers"
DEFAULT_DB_PATH = DEFAULT_PAPERS_DIR / "paper-index" / "papers.db"
DEFAULT_GDOCS_DIR = DEFAULT_PAPERS_DIR.parent / "gdocs"
DEFAULT_TASKS_DIR = DEFAULT_PAPERS_DIR.parent / "tasks"
DEFAULT_PIPELINE_DIR = DEFAULT_PAPERS_DIR.parent / "notes" / "pipeline"
SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"

FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n?(.*)$", re.DOTALL)
CANDIDATE_HEADER_MARKERS = ("Title (best guess)", "Authors / Year")
ARTICLE_MATCH_HEADER_MARKERS = ("Slug", "Article Title")
FLAT_LINE_RE = re.compile(r"^([A-Za-z_-]+):(.*)$")


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


def parse_flat_frontmatter(block: str) -> dict:
    """Line-based frontmatter parser for tasks/pipeline notes -- deliberately
    NOT yaml.safe_load. A task's `progress`/`blocker`/`outcome` fields often
    hold raw probe/error text (e.g. "ERROR: CreateProcessCommon:640: ...")
    with unquoted colons; a real YAML parser reads a mid-line ": " as a
    nested mapping and throws "mapping values are not allowed here". This
    mirrors supervisor/run.py's own parse_frontmatter for exactly that
    reason -- confirmed live: yaml.safe_load broke on 8 of 24 task files
    before this fix, every one of them a probe-error or long free-text
    value. Same one-line-per-key, no-colon-splitting contract as that
    parser: whatever follows the first ":" is the whole value, verbatim."""
    out: dict[str, str] = {}
    for line in block.splitlines():
        m = FLAT_LINE_RE.match(line)
        if not m:
            continue
        key, val = m.group(1).strip(), m.group(2).strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
            val = val[1:-1]
        out[key] = val
    return out


def parse_task_note(path: Path, in_done: bool, workspace_root: Path) -> dict | None:
    text = path.read_text(encoding="utf-8")
    match = FRONTMATTER_RE.match(text)
    if not match:
        return None
    meta = parse_flat_frontmatter(match.group(1))
    return {
        "id": meta.get("id", path.stem),
        "title": meta.get("title"),
        "lane": meta.get("lane"),
        "status": meta.get("status"),
        "assignee": meta.get("assignee"),
        "gate": meta.get("gate"),
        "repo": meta.get("repo"),
        "blocker": meta.get("blocker"),
        "next": meta.get("next"),
        "probe": meta.get("probe"),
        "progress": meta.get("progress"),
        "probe_status": meta.get("probe_status"),
        "stall_flag": meta.get("stall_flag"),
        "outcome": meta.get("outcome"),
        "artifacts": meta.get("artifacts"),
        "created": meta.get("created"),
        "updated": meta.get("updated"),
        "in_done": 1 if in_done else 0,
        "file_path": str(_relative_path(path, workspace_root)).replace("\\", "/"),
    }


def collect_task_notes(tasks_dir: Path) -> list[tuple[Path, bool]]:
    notes = [(p, False) for p in sorted(tasks_dir.glob("*.md"))]
    done_dir = tasks_dir / "done"
    if done_dir.is_dir():
        notes.extend((p, True) for p in sorted(done_dir.glob("*.md")))
    return notes


def parse_pipeline_note(path: Path) -> dict | None:
    """notes/pipeline/*.md -- supervisor-written pipeline health notes.
    Same flat-frontmatter contract as tasks (`note` may hold free text)."""
    text = path.read_text(encoding="utf-8")
    match = FRONTMATTER_RE.match(text)
    if not match:
        return None
    meta = parse_flat_frontmatter(match.group(1))
    return {
        "table_name": meta.get("table_name", path.stem),
        "schedule": meta.get("schedule"),
        "last_row": meta.get("last_row"),
        "note": meta.get("note"),
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


NON_CATEGORY_DIRS = {"candidates", "paper-index", "db-schema"}


def collect_paper_notes(papers_dir: Path) -> list[Path]:
    notes = []
    for category_dir in sorted(papers_dir.iterdir()):
        if not category_dir.is_dir() or category_dir.name in NON_CATEGORY_DIRS:
            continue
        notes.extend(sorted(category_dir.glob("*.md")))
    return notes


def build(
    papers_dir: Path,
    db_path: Path,
    gdocs_dir: Path | None = None,
    tasks_dir: Path | None = None,
    pipeline_dir: Path | None = None,
) -> None:
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

    task_count = 0
    if tasks_dir is not None and tasks_dir.is_dir():
        for note_path, in_done in collect_task_notes(tasks_dir):
            task = parse_task_note(note_path, in_done, workspace_root)
            if task is None:
                print(f"skip (no frontmatter): {note_path}")
                continue
            conn.execute(
                """INSERT INTO tasks
                   (id, title, lane, status, assignee, gate, repo, blocker, next,
                    probe, progress, probe_status, stall_flag, outcome, artifacts,
                    created, updated, in_done, file_path)
                   VALUES (:id, :title, :lane, :status, :assignee, :gate, :repo,
                           :blocker, :next, :probe, :progress, :probe_status,
                           :stall_flag, :outcome, :artifacts, :created, :updated,
                           :in_done, :file_path)""",
                task,
            )
            task_count += 1
    else:
        print(f"skip tasks table: {tasks_dir} not found")

    pipeline_count = 0
    if pipeline_dir is not None and pipeline_dir.is_dir():
        for note_path in sorted(pipeline_dir.glob("*.md")):
            row = parse_pipeline_note(note_path)
            if row is None:
                print(f"skip (no frontmatter): {note_path}")
                continue
            conn.execute(
                """INSERT INTO pipeline (table_name, schedule, last_row, note)
                   VALUES (:table_name, :schedule, :last_row, :note)""",
                row,
            )
            pipeline_count += 1
    else:
        # Not built yet as of 2026-09 -- Desk.md's Pipeline section documents this
        # as planned (supervisor writing from Neon + Cloud Scheduler), not live.
        # Skipped gracefully, same as gdocs/ on a machine that doesn't have it.
        print(f"skip pipeline table: {pipeline_dir} not found (not implemented yet)")

    conn.execute(
        "INSERT INTO meta (key, value) VALUES ('built_at', ?)",
        (datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z",),
    )

    conn.commit()
    conn.close()
    print(f"Indexed {paper_count} papers and {candidate_count} candidates -> {db_path}")
    print(f"Indexed {gdoc_index_count} gdocs_index rows and {article_match_count} article_gdoc_matches rows")
    print(f"Indexed {task_count} tasks rows and {pipeline_count} pipeline rows")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--papers-dir", type=Path, default=DEFAULT_PAPERS_DIR)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--gdocs-dir", type=Path, default=DEFAULT_GDOCS_DIR)
    parser.add_argument("--tasks-dir", type=Path, default=DEFAULT_TASKS_DIR)
    parser.add_argument("--pipeline-dir", type=Path, default=DEFAULT_PIPELINE_DIR)
    args = parser.parse_args()
    build(args.papers_dir, args.db, args.gdocs_dir, args.tasks_dir, args.pipeline_dir)
