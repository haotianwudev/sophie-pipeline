"""WikiToolkit — search and read Sophie's 240 wiki markdown pages."""

from __future__ import annotations

from functools import lru_cache
from typing import ClassVar

from langchain_core.tools import BaseTool, tool

from ...context.wiki_store import WikiStore
from ...core.config import AgentConfig
from ..base import SophieToolkit
from ..ui_envelope import ui_envelope


@lru_cache(maxsize=4)
def _get_wiki_store(wiki_dir: str, registry_path: str) -> WikiStore:
    """Cached by path pair so every toolkit instance (including per-delegate spawns) shares one
    parsed corpus instead of re-reading 240 files each time an agent is constructed."""
    from pathlib import Path

    return WikiStore(Path(wiki_dir), Path(registry_path))


class WikiToolkit(SophieToolkit):
    toolkit_name: ClassVar[str] = "wiki"

    def _store(self) -> WikiStore:
        cfg: AgentConfig = self.config
        return _get_wiki_store(str(cfg.wiki_dir), str(cfg.wiki_registry_path))

    def get_tools(self) -> list[BaseTool]:
        wiki = self._store()
        run_ctx = self.run_ctx

        @tool
        def wiki_search(query: str, category: str | None = None, label: str | None = None, limit: int = 8) -> str:
            """Search Sophie's wiki (240 published articles' machine-readable knowledge-base
            pages) by keyword. Ranks by title/labels/summary/headings/body relevance. Use this
            first for any "what does our content say about X" question. `category` narrows to one
            of the wiki_list_categories() values; `label` filters by a topic label."""
            results = wiki.search(query, category=category, label=label, limit=limit, as_of=run_ctx.as_of)
            if not results:
                return "No wiki pages matched that query."
            lines = [
                f"- {r['path']} — {r['title']} (score={r['score']}, category={r['category']})"
                + (f"\n  {r['summary']}" if r["summary"] else "")
                for r in results
            ]
            text = "\n".join(lines)
            return ui_envelope(text, "wiki_citations", results=results)

        @tool
        def wiki_get_page(path: str) -> str:
            """Fetch the full markdown content of a wiki page by its exact path (e.g.
            'option-strategy/gex'), as returned by wiki_search. Math blocks ($$...$$) are
            preserved intact."""
            page = wiki.get_page(path)
            if page is None:
                return f"No wiki page found at path '{path}'. Use wiki_search to find the right path."
            text = f"# {page.title}\n\n{page.content}"
            return ui_envelope(text, "wiki_page", path=page.path, title=page.title, category=page.category)

        @tool
        def wiki_list_categories() -> str:
            """List every wiki category (form13f and form-13f are the same category, normalized)."""
            return ", ".join(wiki.list_categories())

        @tool
        def wiki_for_article(article_slug: str) -> str:
            """Find the wiki page(s) that are the machine-readable projection of a given article,
            by the article's URL slug."""
            pages = wiki.for_article(article_slug)
            if not pages:
                return f"No wiki page is linked to article slug '{article_slug}'."
            return "\n".join(f"- {p.path} — {p.title}" for p in pages)

        return [wiki_search, wiki_get_page, wiki_list_categories, wiki_for_article]

    def system_prompt_fragment(self) -> str:
        return (
            "WIKI TOOLKIT: 240 markdown pages under Sophie's public wiki, each the "
            "machine-readable projection of a published article (Overview / Key Concepts / "
            "Formulas / Key Takeaways sections). Use wiki_search first, then wiki_get_page for the "
            "full text. This is Sophie's own published material — cite the page `path` for any "
            "claim you attribute to it."
        )
