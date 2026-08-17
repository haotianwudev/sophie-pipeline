"""WikiToolkit — search and read Sophie's 240 wiki markdown pages."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Annotated, ClassVar

from langchain.tools import ToolRuntime
from langchain_core.tools import BaseTool, tool
from pydantic import Field

from ...context.wiki_store import WikiStore
from ..base import SophieToolkit
from ..ui_envelope import ui_envelope


@lru_cache(maxsize=4)
def _get_wiki_store(wiki_dir: str, registry_path: str) -> WikiStore:
    """Cached by path pair so the 240-file corpus is parsed once per process rather than per
    lookup. Toolkits are stateless now, so this is resolved on each tool call from the config on
    the live context — the cache is what keeps that cheap."""
    return WikiStore(Path(wiki_dir), Path(registry_path))


def _store_for(runtime: ToolRuntime) -> WikiStore:
    cfg = runtime.context.config
    return _get_wiki_store(str(cfg.wiki_dir), str(cfg.wiki_registry_path))


@tool
def wiki_search(
    runtime: ToolRuntime,
    query: Annotated[str, Field(description="Search keywords to find relevant wiki pages (e.g. 'gamma exposure', 'iron condor').")],
    category: Annotated[str | None, Field(description="Optional category filter from wiki_list_categories (e.g. 'option-strategy', 'form13f').")] = None,
    label: Annotated[str | None, Field(description="Optional topic label filter (e.g. 'GEX', 'delta', 'macro').")] = None,
    limit: Annotated[int, Field(ge=1, le=50, description="Maximum number of search results to return (1 to 50).")] = 8,
) -> str:
    """Search Sophie's wiki (240 published articles' machine-readable knowledge-base pages) by
    keyword. Ranks by title/labels/summary/headings/body relevance. Use this first for any "what
    does our content say about X" question. `category` narrows to one of the wiki_list_categories()
    values; `label` filters by a topic label."""
    wiki = _store_for(runtime)
    results = wiki.search(query, category=category, label=label, limit=limit, as_of=runtime.context.run_ctx.as_of)
    if not results:
        return "No wiki pages matched that query."
    lines = [
        f"- {r['path']} — {r['title']} (score={r['score']}, category={r['category']})"
        + (f"\n  {r['summary']}" if r["summary"] else "")
        for r in results
    ]
    return ui_envelope("\n".join(lines), "wiki_citations", results=results)


@tool
def wiki_get_page(
    runtime: ToolRuntime,
    path: Annotated[str, Field(description="Exact wiki path returned by wiki_search (e.g. 'option-strategy/gex', 'market/inflation').")],
) -> str:
    """Fetch the full markdown content of a wiki page by its exact path (e.g.
    'option-strategy/gex'), as returned by wiki_search. Math blocks ($$...$$) are preserved
    intact."""
    page = _store_for(runtime).get_page(path)
    if page is None:
        return f"No wiki page found at path '{path}'. Use wiki_search to find the right path."
    text = f"# {page.title}\n\n{page.content}"
    return ui_envelope(text, "wiki_page", path=page.path, title=page.title, category=page.category)


@tool
def wiki_list_categories(runtime: ToolRuntime) -> str:
    """List every wiki category (form13f and form-13f are the same category, normalized)."""
    return ", ".join(_store_for(runtime).list_categories())


@tool
def wiki_for_article(
    runtime: ToolRuntime,
    article_slug: Annotated[str, Field(description="URL slug of the published article (e.g. 'iron-condor-backtest', 'gex-explained').")],
) -> str:
    """Find the wiki page(s) that are the machine-readable projection of a given article, by the
    article's URL slug."""
    pages = _store_for(runtime).for_article(article_slug)
    if not pages:
        return f"No wiki page is linked to article slug '{article_slug}'."
    return "\n".join(f"- {p.path} — {p.title}" for p in pages)


class WikiToolkit(SophieToolkit):
    toolkit_name: ClassVar[str] = "wiki"

    def get_tools(self) -> list[BaseTool]:
        return [wiki_search, wiki_get_page, wiki_list_categories, wiki_for_article]

    def system_prompt_fragment(self) -> str:
        return (
            "WIKI TOOLKIT: 240 markdown pages under Sophie's public wiki, each the "
            "machine-readable projection of a published article (Overview / Key Concepts / "
            "Formulas / Key Takeaways sections). Use wiki_search first, then wiki_get_page for the "
            "full text. This is Sophie's own published material — cite the page `path` for any "
            "claim you attribute to it."
        )
