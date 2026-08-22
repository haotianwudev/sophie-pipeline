"""WikiToolkit — search and read Sophie's wiki markdown pages (~250 at time of writing)."""

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
    """Cached by path pair so the corpus is parsed once per process rather than per
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
    label: Annotated[str | None, Field(description="Optional broad domain filter (e.g. 'Options Trading', 'Quantitative Finance'). Coarse — usually a whole category shares one.")] = None,
    topic: Annotated[str | None, Field(description="Optional sub-topic filter within a category, from wiki_list_topics (e.g. 'Volatility & VRP', 'Spreads & Structures'). Narrower and more useful than `label`.")] = None,
    limit: Annotated[int, Field(ge=1, le=50, description="Maximum number of search results to return (1 to 50).")] = 8,
) -> str:
    """Search Sophie's wiki — the machine-readable knowledge-base pages behind the published
    articles — by keyword. Ranks by title/topics/labels/summary/headings/body relevance. Use this
    first for any "what does our content say about X" question. `category` narrows to one of the
    wiki_list_categories() values; `topic` narrows to a sub-topic within a category (see
    wiki_list_topics); `label` is a coarse domain tag and rarely discriminates on its own."""
    wiki = _store_for(runtime)
    results = wiki.search(query, category=category, label=label, topic=topic, limit=limit, as_of=runtime.context.run_ctx.as_of)
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


def _format_section_hits(results: list[dict], empty_msg: str) -> str:
    if not results:
        return empty_msg
    lines = [
        f"- {r['breadcrumb']} (score={r['score']}, {r['words']}w)\n  {r['snippet']}"
        for r in results
    ]
    return ui_envelope("\n".join(lines), "wiki_citations", results=results)


@tool
def wiki_search_sections(
    runtime: ToolRuntime,
    query: Annotated[str, Field(description="Search keywords to find the specific section that answers the question (e.g. 'how is realized vol calculated', 'gamma flip').")],
    category: Annotated[str | None, Field(description="Optional category filter from wiki_list_categories (e.g. 'option-strategy').")] = None,
    label: Annotated[str | None, Field(description="Optional broad domain filter (e.g. 'Options Trading'). Coarse — usually a whole category shares one.")] = None,
    topic: Annotated[str | None, Field(description="Optional sub-topic filter within a category, from wiki_list_topics (e.g. 'Volatility & VRP').")] = None,
    limit: Annotated[int, Field(ge=1, le=50, description="Maximum number of section results to return (1 to 50).")] = 8,
) -> str:
    """Search the wiki at SECTION level and return the specific '##'/'###' blocks that match,
    each with its page path, heading and a snippet. Prefer this over wiki_search when the question
    is narrow ("how is X calculated?"), because the pages are long — reading one section instead of
    a whole page typically costs ~85% less context. Pass a returned `path` + `heading` to
    wiki_get_section to read it."""
    wiki = _store_for(runtime)
    results = wiki.search_sections(
        query, category=category, label=label, topic=topic, limit=limit, as_of=runtime.context.run_ctx.as_of
    )
    return _format_section_hits(results, "No wiki sections matched that query.")


@tool
def wiki_outline(
    runtime: ToolRuntime,
    path: Annotated[str, Field(description="Exact wiki path returned by wiki_search (e.g. 'option-strategy/vol-regime-methodology').")],
) -> str:
    """List a wiki page's section headings with their nesting level and word counts, without
    fetching the page body. Use this to decide which section you actually need before spending
    context on wiki_get_page — the largest pages run past 5,000 words across 30 sections."""
    outline = _store_for(runtime).get_outline(path)
    if outline is None:
        return f"No wiki page found at path '{path}'. Use wiki_search to find the right path."
    if not outline:
        return f"'{path}' has no section headings."
    lines = [
        f"{'  ' if row['level'] == 3 else ''}- {row['heading']} ({row['words']}w)"
        for row in outline
    ]
    total = sum(row["words"] for row in outline)
    return f"{path} — {len(outline)} sections, {total} words total:\n" + "\n".join(lines)


@tool
def wiki_get_section(
    runtime: ToolRuntime,
    path: Annotated[str, Field(description="Exact wiki path (e.g. 'option-strategy/vol-regime-methodology').")],
    heading: Annotated[str, Field(description="Exact section heading text as shown by wiki_outline or wiki_search_sections (e.g. 'Core Signals'). Case-insensitive.")],
    include_subsections: Annotated[bool, Field(description="For a top-level ('##') heading, also include its nested '###' subsections. Default true.")] = True,
) -> str:
    """Fetch ONE section of a wiki page by its heading text, instead of the whole page. Math blocks
    ($$...$$) and tables are preserved intact. Sections are addressed by heading text, not by URL
    anchor. Cite the page `path` (and the heading) for any claim taken from it."""
    page = _store_for(runtime).get_page(path)
    if page is None:
        return f"No wiki page found at path '{path}'. Use wiki_search to find the right path."
    text = page.find_section(heading, include_subsections=include_subsections)
    if text is None:
        available = ", ".join(f"'{s.heading}'" for s in page.sections) or "(none)"
        return f"No section titled '{heading}' on '{path}'. Available sections: {available}"
    return ui_envelope(
        f"# {page.title}\n\n{text}",
        "wiki_page",
        path=page.path,
        title=page.title,
        category=page.category,
        heading=heading,
    )


@tool
def wiki_list_topics(
    runtime: ToolRuntime,
    category: Annotated[str | None, Field(description="Optional category to scope to (e.g. 'option-strategy'). Topics are per-category, so scoping is usually what you want.")] = None,
) -> str:
    """List the sub-topics in use, with page counts. These are the values accepted by the `topic`
    filter on wiki_search / wiki_search_sections, and are the dimension the site itself groups a
    category by — narrower than `label`, which a whole category tends to share. A page can carry
    more than one topic, so counts overlap."""
    topics = _store_for(runtime).list_topics(category)
    if not topics:
        scope = f" for category '{category}'" if category else ""
        return f"No sub-topics assigned{scope} yet."
    return "\n".join(f"- {name} ({count})" for name, count in topics)


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
        return [
            wiki_search,
            wiki_search_sections,
            wiki_outline,
            wiki_get_section,
            wiki_get_page,
            wiki_list_categories,
            wiki_list_topics,
            wiki_for_article,
        ]

    def system_prompt_fragment(self) -> str:
        return (
            "WIKI TOOLKIT: ~250 markdown pages under Sophie's public wiki, each the "
            "machine-readable projection of a published article (Overview / Key Concepts / "
            "Formulas / Key Takeaways sections). Content is addressable at three levels: "
            "category > page > section.\n"
            "Preferred flow for a narrow question ('how is X calculated?'): wiki_search_sections "
            "to find the exact section, then wiki_get_section to read just that block — the "
            "methodology pages run past 5,000 words, so this typically costs ~85% less context "
            "than the whole page. For a broad question, wiki_search to find the page, then "
            "wiki_outline to see its sections before deciding what to read. Reserve wiki_get_page "
            "for when you genuinely need the entire document.\n"
            "To narrow a large category, filter by `topic` (wiki_list_topics shows the values and "
            "counts) — these are the sub-topics the site itself groups by, e.g. 'Volatility & VRP' "
            "inside option-strategy. Prefer `topic` over `label`: a label is a broad domain tag "
            "that a whole category tends to share, so it rarely narrows anything. A page can "
            "carry several topics. "
            "This is Sophie's own published material — cite the page `path` for any claim you "
            "attribute to it."
        )


# --- option_strategist's scoped wiki access -------------------------------------------------
# A *separate* toolkit rather than a flag on WikiToolkit: wiki_search/wiki_get_page above are
# free functions shared with the full-access "wiki" toolkit (supervisor), so scoping them via a
# constructor arg would mean threading per-instance config through module-level @tool functions
# that only ever see `runtime.context`, not `self`. Two small dedicated functions that hardcode
# the category is simpler and can't leak into the shared ones by accident.
_OPTION_WIKI_CATEGORY = "option-strategy"


@tool
def option_wiki_search(
    runtime: ToolRuntime,
    query: Annotated[str, Field(description="Search keywords to find relevant option-strategy wiki pages (e.g. 'iron condor deltas', 'theta decay').")],
    label: Annotated[str | None, Field(description="Optional broad domain filter. Coarse — most option-strategy pages share one.")] = None,
    topic: Annotated[str | None, Field(description="Optional sub-topic filter, from option_wiki_list_topics (e.g. 'Volatility & VRP', 'Income & Writing').")] = None,
    limit: Annotated[int, Field(ge=1, le=50, description="Maximum number of search results to return (1 to 50).")] = 8,
) -> str:
    """Search Sophie's option-strategy wiki pages only (category='option-strategy') — the
    strategy/Greeks/mechanics reference material, not market/macro/13F content. Use this first for
    any "what does our content say about X strategy" question."""
    wiki = _store_for(runtime)
    results = wiki.search(query, category=_OPTION_WIKI_CATEGORY, label=label, topic=topic, limit=limit, as_of=runtime.context.run_ctx.as_of)
    if not results:
        return "No option-strategy wiki pages matched that query."
    lines = [
        f"- {r['path']} — {r['title']} (score={r['score']})"
        + (f"\n  {r['summary']}" if r["summary"] else "")
        for r in results
    ]
    return ui_envelope("\n".join(lines), "wiki_citations", results=results)


@tool
def option_wiki_get_page(
    runtime: ToolRuntime,
    path: Annotated[str, Field(description="Exact wiki path returned by option_wiki_search (e.g. 'option-strategy/gex').")],
) -> str:
    """Fetch the full markdown content of an option-strategy wiki page by its exact path, as
    returned by option_wiki_search. Refuses paths outside the option-strategy category."""
    page = _store_for(runtime).get_page(path)
    if page is None:
        return f"No wiki page found at path '{path}'. Use option_wiki_search to find the right path."
    if page.category != _OPTION_WIKI_CATEGORY:
        return f"'{path}' is outside the option-strategy wiki (category='{page.category}') — not available here."
    text = f"# {page.title}\n\n{page.content}"
    return ui_envelope(text, "wiki_page", path=page.path, title=page.title, category=page.category)


@tool
def option_wiki_search_sections(
    runtime: ToolRuntime,
    query: Annotated[str, Field(description="Search keywords to find the specific section that answers the question (e.g. 'how is realized vol calculated', 'gamma flip').")],
    label: Annotated[str | None, Field(description="Optional broad domain filter. Coarse — most option-strategy pages share one.")] = None,
    topic: Annotated[str | None, Field(description="Optional sub-topic filter, from option_wiki_list_topics (e.g. 'Spreads & Structures').")] = None,
    limit: Annotated[int, Field(ge=1, le=50, description="Maximum number of section results to return (1 to 50).")] = 8,
) -> str:
    """Search the option-strategy wiki at SECTION level, returning the specific '##'/'###' blocks
    that match. Prefer this over option_wiki_search for narrow questions ("how is X calculated?") —
    the methodology pages are long, and reading one section instead of the whole page typically
    costs ~85% less context. Pass a returned `path` + `heading` to option_wiki_get_section."""
    wiki = _store_for(runtime)
    results = wiki.search_sections(
        query,
        category=_OPTION_WIKI_CATEGORY,
        label=label,
        topic=topic,
        limit=limit,
        as_of=runtime.context.run_ctx.as_of,
    )
    return _format_section_hits(results, "No option-strategy wiki sections matched that query.")


@tool
def option_wiki_outline(
    runtime: ToolRuntime,
    path: Annotated[str, Field(description="Exact wiki path (e.g. 'option-strategy/vol-regime-methodology').")],
) -> str:
    """List an option-strategy wiki page's section headings with nesting level and word counts,
    without fetching the body. Use it to pick the section you need before spending context on the
    full page. Refuses paths outside the option-strategy category."""
    store = _store_for(runtime)
    page = store.get_page(path)
    if page is None:
        return f"No wiki page found at path '{path}'. Use option_wiki_search to find the right path."
    if page.category != _OPTION_WIKI_CATEGORY:
        return f"'{path}' is outside the option-strategy wiki (category='{page.category}') — not available here."
    outline = page.outline()
    if not outline:
        return f"'{path}' has no section headings."
    lines = [
        f"{'  ' if row['level'] == 3 else ''}- {row['heading']} ({row['words']}w)"
        for row in outline
    ]
    total = sum(row["words"] for row in outline)
    return f"{path} — {len(outline)} sections, {total} words total:\n" + "\n".join(lines)


@tool
def option_wiki_get_section(
    runtime: ToolRuntime,
    path: Annotated[str, Field(description="Exact wiki path (e.g. 'option-strategy/vol-regime-methodology').")],
    heading: Annotated[str, Field(description="Exact section heading text from option_wiki_outline or option_wiki_search_sections. Case-insensitive.")],
    include_subsections: Annotated[bool, Field(description="For a top-level ('##') heading, also include its nested '###' subsections. Default true.")] = True,
) -> str:
    """Fetch ONE section of an option-strategy wiki page by heading text instead of the whole page.
    Math blocks and tables are preserved intact. Refuses paths outside the option-strategy
    category. Cite the page `path` (and heading) for any claim taken from it."""
    page = _store_for(runtime).get_page(path)
    if page is None:
        return f"No wiki page found at path '{path}'. Use option_wiki_search to find the right path."
    if page.category != _OPTION_WIKI_CATEGORY:
        return f"'{path}' is outside the option-strategy wiki (category='{page.category}') — not available here."
    text = page.find_section(heading, include_subsections=include_subsections)
    if text is None:
        available = ", ".join(f"'{s.heading}'" for s in page.sections) or "(none)"
        return f"No section titled '{heading}' on '{path}'. Available sections: {available}"
    return ui_envelope(
        f"# {page.title}\n\n{text}",
        "wiki_page",
        path=page.path,
        title=page.title,
        category=page.category,
        heading=heading,
    )


@tool
def option_wiki_list_topics(runtime: ToolRuntime) -> str:
    """List the option-strategy wiki's sub-topics with page counts. These are the values accepted
    by the `topic` filter on option_wiki_search / option_wiki_search_sections. A page can carry
    more than one topic, so counts overlap."""
    topics = _store_for(runtime).list_topics(_OPTION_WIKI_CATEGORY)
    if not topics:
        return "No sub-topics assigned in the option-strategy wiki yet."
    return "\n".join(f"- {name} ({count})" for name, count in topics)


class OptionWikiToolkit(SophieToolkit):
    toolkit_name: ClassVar[str] = "wiki_options"

    def get_tools(self) -> list[BaseTool]:
        return [
            option_wiki_search,
            option_wiki_search_sections,
            option_wiki_outline,
            option_wiki_get_section,
            option_wiki_get_page,
            option_wiki_list_topics,
        ]

    def system_prompt_fragment(self) -> str:
        return (
            "OPTION WIKI TOOLKIT: Sophie's option-strategy wiki pages only — strategy mechanics, "
            "Greeks, and related reference material. Content is addressable at page and section "
            "level.\n"
            "For a narrow question ('how is X calculated?'): option_wiki_search_sections, then "
            "option_wiki_get_section to read just that block — much cheaper than the whole page. "
            "For a broad question: option_wiki_search, then option_wiki_outline to see the "
            "sections before choosing. Reserve option_wiki_get_page for when you need the entire "
            "document.\n"
            "The 44 pages here are grouped into sub-topics — Volatility & VRP, Income & Writing, "
            "Spreads & Structures, Platform Methodology, Greeks & Mechanics, Practice & Tax — "
            "usable as the `topic` filter and listed with counts by option_wiki_list_topics. "
            "Prefer it over `label`, which nearly every page here shares. A page can carry "
            "several topics. "
            "Cite the page `path` for any claim you attribute to it. Market/macro/13F "
            "wiki content is out of scope here."
        )
