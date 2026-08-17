"""Loads and indexes the 240 Sophie wiki markdown pages.

Frontmatter parser mirrors client/src/lib/wiki.ts (scalars + single-line bracketed arrays only —
that's all the wiki files actually use). The registry TS file at src/data/wiki/index.ts is
regex-parsed only to recover `summary`, the one field present there but not in frontmatter;
if that parse fails for any reason we degrade to frontmatter-only rather than hard-failing the
whole toolkit on a TS formatting change.

Retrieval is BM25-lite pure Python: tokenize, IDF-weight, score over title x3, labels x2,
summary x2, '##' headings x1.5, body x1. At 240 documents this needs no index build step, no
embedding key, no vector-store dependency.
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

# form13f and form-13f are two separate on-disk categories that mean the same thing.
_CATEGORY_ALIASES = {"form-13f": "form13f"}


def _tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall(text.lower())


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

    def list_categories(self) -> list[str]:
        cats = {self._normalize_category(p.category) for p in self._pages.values()}
        return sorted(cats)

    def for_article(self, article_slug: str) -> list[WikiPage]:
        return [p for p in self._pages.values() if p.article_slug == article_slug]

    def __len__(self) -> int:
        return len(self._pages)
