"""Loads and indexes the 240 Sophie wiki markdown pages.

Frontmatter parser mirrors client/src/lib/wiki.ts (scalars + single-line bracketed arrays only —
that's all the wiki files actually use). The registry TS file at src/data/wiki/index.ts is
regex-parsed only to recover `summary`, the one field present there but not in frontmatter;
if that parse fails for any reason we degrade to frontmatter-only rather than hard-failing the
whole toolkit on a TS formatting change.

Retrieval is BM25-lite pure Python: tokenize, IDF-weight, score over title x3, labels x2,
summary x2, '##' headings x1.5, body x1. At 240 documents this needs no index build step, no
embedding key, no vector-store dependency.

Retrieval has two granularities: whole-page, and section (a '##' or '###' block). The section
layer exists because the methodology pages have grown past 350 lines with 30 headings — pulling
a whole page to answer "how is realized vol calculated" spends most of an agent's context on
unrelated sections. Sections are addressed by *heading text*, not by URL anchor: the client's
markdown renderer has no rehype-slug, so headings carry no `id` and '#fragment' links do not
resolve. Heading text is the stable address here.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from functools import cached_property
from pathlib import Path

_FRONTMATTER_RE = re.compile(r"^---\r?\n(.*?)\r?\n---\r?\n?", re.DOTALL)
_FM_LINE_RE = re.compile(r"^([a-zA-Z]+):\s*(.*)$")
_HEADING_RE = re.compile(r"^##\s+(.+)$", re.MULTILINE)
_TOKEN_RE = re.compile(r"[a-z0-9]+")

# Section splitting. Only '##' and '###' start a section; '#' is the page title and '####+' is
# treated as body text (no wiki page nests that deep for navigational purposes).
_SECTION_HEADING_RE = re.compile(r"^(#{2,3})\s+(.+?)\s*$", re.MULTILINE)

# A '```' fence toggles code context. Headings inside fenced blocks are code, not structure.
_FENCE_RE = re.compile(r"^\s*```")

_PREAMBLE_HEADING = "(intro)"

# form13f and form-13f are two separate on-disk categories that mean the same thing.
_CATEGORY_ALIASES = {"form-13f": "form13f"}


def _tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall(text.lower())


def _snippet(body: str, max_chars: int = 240) -> str:
    """First prose line of a section, for search results. Skips markdown table rows, math
    blocks and list bullets so the preview shows a sentence rather than '|---|---|'."""
    for line in body.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(("|", "$$", "```", ">", "-", "*", "#")):
            continue
        return stripped[:max_chars] + ("…" if len(stripped) > max_chars else "")
    collapsed = " ".join(body.split())
    return collapsed[:max_chars] + ("…" if len(collapsed) > max_chars else "")


@dataclass
class WikiSection:
    """One '##' or '###' block of a page — the third addressing level below category and page."""

    page_path: str
    page_title: str
    heading: str
    level: int  # 2 for '##', 3 for '###'
    parent_heading: str | None  # the enclosing '##' for a level-3 section
    body: str  # this section's own text, excluding nested subsections

    @property
    def word_count(self) -> int:
        return len(self.body.split())

    @property
    def breadcrumb(self) -> str:
        """Human/agent-readable address, e.g. 'option-strategy/x > Core Signals > Why 20 days?'."""
        parts = [self.page_path]
        if self.parent_heading:
            parts.append(self.parent_heading)
        parts.append(self.heading)
        return " > ".join(parts)


def _split_sections(content: str) -> list[tuple[str, int, str | None, str]]:
    """Split markdown into (heading, level, parent_heading, body) tuples.

    Headings inside fenced code blocks are ignored — a '## ' line in a shell example is not a
    section boundary. Any text before the first heading becomes a synthetic preamble section so
    a page's opening lines are never unreachable.
    """
    lines = content.splitlines()
    fenced = False
    # (heading, level, parent, [body lines])
    blocks: list[tuple[str, int, str | None, list[str]]] = []
    current: tuple[str, int, str | None, list[str]] = (_PREAMBLE_HEADING, 2, None, [])
    last_h2: str | None = None

    for line in lines:
        if _FENCE_RE.match(line):
            fenced = not fenced
        m = None if fenced else _SECTION_HEADING_RE.match(line)
        if m:
            # Close the open block; drop an empty synthetic preamble rather than emit a blank one.
            if current[0] != _PREAMBLE_HEADING or "".join(current[3]).strip():
                blocks.append(current)
            level = len(m.group(1))
            heading = m.group(2).strip()
            if level == 2:
                last_h2 = heading
                current = (heading, 2, None, [])
            else:
                current = (heading, 3, last_h2, [])
        else:
            current[3].append(line)

    if current[0] != _PREAMBLE_HEADING or "".join(current[3]).strip():
        blocks.append(current)

    return [(h, lvl, parent, "\n".join(body).strip()) for h, lvl, parent, body in blocks]


@dataclass
class WikiPage:
    path: str  # e.g. "option-strategy/gex"
    title: str
    category: str
    article_slug: str | None
    page_date: str | None
    labels: list[str]
    related: list[str]
    summary: str | None
    content: str
    file_path: Path

    @cached_property
    def headings(self) -> list[str]:
        return _HEADING_RE.findall(self.content)

    @cached_property
    def sections(self) -> list[WikiSection]:
        return [
            WikiSection(
                page_path=self.path,
                page_title=self.title,
                heading=heading,
                level=level,
                parent_heading=parent,
                body=body,
            )
            for heading, level, parent, body in _split_sections(self.content)
        ]

    def outline(self) -> list[dict]:
        """Cheap table of contents — lets an agent pick a section without reading the page."""
        return [
            {
                "heading": s.heading,
                "level": s.level,
                "parent": s.parent_heading,
                "words": s.word_count,
            }
            for s in self.sections
        ]

    def find_section(self, heading: str, include_subsections: bool = True) -> str | None:
        """Return one section's markdown, addressed by heading text (case-insensitive).

        For a level-2 heading, `include_subsections` folds in its level-3 children — asking for
        'Core Signals' should return the whole thing, not just the paragraph before its first
        subsection.
        """
        want = heading.strip().lower()
        sections = self.sections
        for i, s in enumerate(sections):
            if s.heading.strip().lower() != want:
                continue
            hashes = "#" * s.level
            parts = [f"{hashes} {s.heading}", s.body] if s.body else [f"{hashes} {s.heading}"]
            if include_subsections and s.level == 2:
                for nxt in sections[i + 1:]:
                    if nxt.level <= 2:
                        break
                    parts.append(f"{'#' * nxt.level} {nxt.heading}")
                    if nxt.body:
                        parts.append(nxt.body)
            return "\n\n".join(p for p in parts if p).strip()
        return None


def _parse_frontmatter(raw: str) -> tuple[dict, str]:
    match = _FRONTMATTER_RE.match(raw)
    if not match:
        return {}, raw
    block, content = match.group(1), raw[match.end():]
    fm: dict[str, str | list[str]] = {}
    for line in block.splitlines():
        m = _FM_LINE_RE.match(line)
        if not m:
            continue
        key, raw_value = m.group(1), m.group(2).strip()
        if raw_value.startswith("[") and raw_value.endswith("]"):
            items = raw_value[1:-1].split(",")
            fm[key] = [item.strip().strip("\"'") for item in items if item.strip()]
        else:
            fm[key] = raw_value.strip("\"'")
    return fm, content


def _parse_registry_summaries(registry_path: Path) -> dict[str, str]:
    """Best-effort regex extraction of {path, summary} pairs from the TS registry array."""
    try:
        text = registry_path.read_text(encoding="utf-8")
    except OSError:
        return {}
    summaries: dict[str, str] = {}
    try:
        # Split on the object-open boundary used by every entry in the array.
        for chunk in text.split("\n  {")[1:]:
            path_m = re.search(r'path:\s*"([^"]+)"', chunk)
            summary_m = re.search(r'summary:\s*\n?\s*"((?:[^"\\]|\\.)*)"', chunk)
            if path_m and summary_m:
                summaries[path_m.group(1)] = summary_m.group(1).replace('\\"', '"')
    except Exception:
        return {}
    return summaries


class WikiStore:
    def __init__(self, wiki_dir: Path, registry_path: Path) -> None:
        self._wiki_dir = wiki_dir
        self._registry_path = registry_path
        self._pages: dict[str, WikiPage] = {}
        self._index: dict[str, Counter[str]] | None = None
        self._doc_freq: Counter[str] | None = None
        self._section_entries: list[tuple[WikiSection, Counter[str]]] | None = None
        self._section_doc_freq: Counter[str] | None = None
        self._load()

    def _load(self) -> None:
        summaries = _parse_registry_summaries(self._registry_path)
        if not self._wiki_dir.exists():
            return
        for md_file in self._wiki_dir.rglob("*.md"):
            raw = md_file.read_text(encoding="utf-8")
            fm, content = _parse_frontmatter(raw)
            path = fm.get("path") or str(md_file.relative_to(self._wiki_dir).with_suffix("")).replace("\\", "/")
            category = path.split("/")[0] if "/" in path else "uncategorized"
            page = WikiPage(
                path=path,
                title=fm.get("title", md_file.stem),
                category=category,
                article_slug=fm.get("articleSlug"),
                page_date=fm.get("date"),
                labels=fm.get("labels", []) if isinstance(fm.get("labels"), list) else [],
                related=fm.get("related", []) if isinstance(fm.get("related"), list) else [],
                summary=summaries.get(path),
                content=content,
                file_path=md_file,
            )
            self._pages[path] = page

    # -- retrieval -----------------------------------------------------------------

    def _build_index(self) -> None:
        index: dict[str, Counter[str]] = {}
        doc_freq: Counter[str] = Counter()
        for path, page in self._pages.items():
            weighted = Counter()
            for term in _tokenize(page.title):
                weighted[term] += 3
            for label in page.labels:
                for term in _tokenize(label):
                    weighted[term] += 2
            if page.summary:
                for term in _tokenize(page.summary):
                    weighted[term] += 2
            for heading in page.headings:
                for term in _tokenize(heading):
                    weighted[term] += 1.5
            for term in _tokenize(page.content):
                weighted[term] += 1
            index[path] = weighted
            for term in set(weighted):
                doc_freq[term] += 1
        self._index = index
        self._doc_freq = doc_freq

    def _normalize_category(self, category: str) -> str:
        return _CATEGORY_ALIASES.get(category, category)

    def search(
        self,
        query: str,
        category: str | None = None,
        label: str | None = None,
        limit: int = 8,
        as_of: date | None = None,
    ) -> list[dict]:
        if self._index is None:
            self._build_index()
        assert self._index is not None and self._doc_freq is not None

        n_docs = max(len(self._pages), 1)
        query_terms = _tokenize(query)
        scores: list[tuple[float, WikiPage]] = []

        for path, weighted in self._index.items():
            page = self._pages[path]
            if category and self._normalize_category(page.category) != self._normalize_category(category):
                continue
            if label and label.lower() not in {l.lower() for l in page.labels}:
                continue
            if as_of and page.page_date and page.page_date > as_of.isoformat():
                continue
            score = 0.0
            for term in query_terms:
                tf = weighted.get(term, 0)
                if tf == 0:
                    continue
                idf = math.log((n_docs + 1) / (self._doc_freq.get(term, 0) + 1)) + 1
                score += tf * idf
            if score > 0:
                scores.append((score, page))

        scores.sort(key=lambda x: x[0], reverse=True)
        return [
            {
                "path": p.path,
                "title": p.title,
                "category": p.category,
                "summary": p.summary,
                "labels": p.labels,
                "score": round(s, 3),
            }
            for s, p in scores[:limit]
        ]

    def get_page(self, path: str) -> WikiPage | None:
        return self._pages.get(path)

    # -- section level -------------------------------------------------------------

    def _build_section_index(self) -> None:
        """Flat (section, weighted-terms) list. Built lazily and separately from the page index so
        callers that only ever search pages don't pay for it."""
        entries: list[tuple[WikiSection, Counter[str]]] = []
        doc_freq: Counter[str] = Counter()
        for page in self._pages.values():
            for section in page.sections:
                weighted: Counter[str] = Counter()
                # Heading terms dominate: a section is usually found by what it's called.
                for term in _tokenize(section.heading):
                    weighted[term] += 4
                if section.parent_heading:
                    for term in _tokenize(section.parent_heading):
                        weighted[term] += 2
                # Page title/labels carry context so a section stays findable by its topic.
                for term in _tokenize(page.title):
                    weighted[term] += 2
                for label in page.labels:
                    for term in _tokenize(label):
                        weighted[term] += 1
                for term in _tokenize(section.body):
                    weighted[term] += 1
                entries.append((section, weighted))
                for term in set(weighted):
                    doc_freq[term] += 1
        self._section_entries = entries
        self._section_doc_freq = doc_freq

    def search_sections(
        self,
        query: str,
        category: str | None = None,
        label: str | None = None,
        limit: int = 8,
        as_of: date | None = None,
    ) -> list[dict]:
        """Same scoring model as `search`, but each '##'/'###' block is its own document."""
        if self._section_entries is None:
            self._build_section_index()
        assert self._section_entries is not None and self._section_doc_freq is not None

        n_docs = max(len(self._section_entries), 1)
        query_terms = _tokenize(query)
        scores: list[tuple[float, WikiSection]] = []

        for section, weighted in self._section_entries:
            page = self._pages[section.page_path]
            if category and self._normalize_category(page.category) != self._normalize_category(category):
                continue
            if label and label.lower() not in {l.lower() for l in page.labels}:
                continue
            if as_of and page.page_date and page.page_date > as_of.isoformat():
                continue
            score = 0.0
            for term in query_terms:
                tf = weighted.get(term, 0)
                if tf == 0:
                    continue
                idf = math.log((n_docs + 1) / (self._section_doc_freq.get(term, 0) + 1)) + 1
                score += tf * idf
            if score > 0:
                scores.append((score, section))

        scores.sort(key=lambda x: x[0], reverse=True)
        return [
            {
                "path": s.page_path,
                "title": s.page_title,
                "heading": s.heading,
                "level": s.level,
                "parent": s.parent_heading,
                "breadcrumb": s.breadcrumb,
                "words": s.word_count,
                "snippet": _snippet(s.body),
                "score": round(sc, 3),
            }
            for sc, s in scores[:limit]
        ]

    def get_section(self, path: str, heading: str, include_subsections: bool = True) -> str | None:
        page = self._pages.get(path)
        if page is None:
            return None
        return page.find_section(heading, include_subsections=include_subsections)

    def get_outline(self, path: str) -> list[dict] | None:
        page = self._pages.get(path)
        return None if page is None else page.outline()

    def list_categories(self) -> list[str]:
        cats = {self._normalize_category(p.category) for p in self._pages.values()}
        return sorted(cats)

    def for_article(self, article_slug: str) -> list[WikiPage]:
        return [p for p in self._pages.values() if p.article_slug == article_slug]

    def __len__(self) -> int:
        return len(self._pages)
