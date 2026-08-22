"""Section-level wiki addressing (category > page > section) and sub-topic labels.

Covers the splitter's edge cases rather than the happy path only: fenced code blocks that
contain '##' lines, pages whose text starts before any heading, and level-2 headings whose
body lives entirely in their level-3 children.

The second half covers `topics`, which are regex-recovered from the TS registry rather than
frontmatter. That parse is the fragile part -- the field is injected directly after `path:`,
ahead of `summary:` -- so it is pinned here against a realistic registry fixture.
"""

from __future__ import annotations

import textwrap

import pytest

from sophie_agent.context.wiki_store import WikiStore

PAGE = textwrap.dedent(
    """\
    ---
    path: option-strategy/demo
    title: Demo Page
    labels: ["Options"]
    ---

    Opening line before any heading.

    ## Core Signals

    Intro text for core signals.

    ### How realized vol is calculated

    Sample stdev of log returns, annualized by sqrt(252).

    ### Why 20 trading days?

    VIX targets 30 calendar days.

    ## Shell Examples

    Run it like this:

    ```bash
    ## this is a comment, not a heading
    ### neither is this
    echo hi
    ```

    Done.

    ## Limitations

    Only daily closes.
    """
)


@pytest.fixture()
def store(tmp_path):
    wiki_dir = tmp_path / "wiki" / "option-strategy"
    wiki_dir.mkdir(parents=True)
    (wiki_dir / "demo.md").write_text(PAGE, encoding="utf-8")
    registry = tmp_path / "index.ts"
    registry.write_text("export const wikiEntries = [];", encoding="utf-8")
    return WikiStore(tmp_path / "wiki", registry)


def test_preamble_becomes_its_own_section(store):
    page = store.get_page("option-strategy/demo")
    assert page is not None
    first = page.sections[0]
    assert "Opening line before any heading." in first.body


def test_headings_inside_code_fences_are_not_sections(store):
    page = store.get_page("option-strategy/demo")
    headings = [s.heading for s in page.sections]
    assert "this is a comment, not a heading" not in headings
    assert "neither is this" not in headings
    # The fenced block stays with its own section body.
    shell = next(s for s in page.sections if s.heading == "Shell Examples")
    assert "echo hi" in shell.body


def test_levels_and_parentage(store):
    page = store.get_page("option-strategy/demo")
    by_heading = {s.heading: s for s in page.sections}
    assert by_heading["Core Signals"].level == 2
    assert by_heading["Core Signals"].parent_heading is None
    assert by_heading["Why 20 trading days?"].level == 3
    assert by_heading["Why 20 trading days?"].parent_heading == "Core Signals"


def test_get_section_folds_in_subsections_by_default(store):
    text = store.get_section("option-strategy/demo", "Core Signals")
    assert "Intro text for core signals." in text
    assert "How realized vol is calculated" in text
    assert "Why 20 trading days?" in text
    # Must not bleed into the next level-2 section.
    assert "Only daily closes." not in text


def test_get_section_can_exclude_subsections(store):
    text = store.get_section("option-strategy/demo", "Core Signals", include_subsections=False)
    assert "Intro text for core signals." in text
    assert "Why 20 trading days?" not in text


def test_level_3_heading_is_directly_addressable(store):
    text = store.get_section("option-strategy/demo", "Why 20 trading days?")
    assert text.startswith("### Why 20 trading days?")
    assert "VIX targets 30 calendar days." in text
    assert "Sample stdev" not in text


def test_heading_lookup_is_case_insensitive(store):
    assert store.get_section("option-strategy/demo", "core SIGNALS") is not None


def test_unknown_section_returns_none(store):
    assert store.get_section("option-strategy/demo", "Nope") is None
    assert store.get_section("option-strategy/missing", "Core Signals") is None


def test_outline_reports_structure_without_body(store):
    outline = store.get_outline("option-strategy/demo")
    headings = [(r["heading"], r["level"]) for r in outline]
    assert ("Core Signals", 2) in headings
    assert ("Why 20 trading days?", 3) in headings
    assert all("words" in r for r in outline)


def test_section_search_ranks_the_matching_section_first(store):
    hits = store.search_sections("how is realized vol calculated")
    assert hits, "expected at least one section hit"
    assert hits[0]["heading"] == "How realized vol is calculated"
    assert hits[0]["path"] == "option-strategy/demo"
    assert "Core Signals" in hits[0]["breadcrumb"]


def test_section_search_respects_category_filter(store):
    assert store.search_sections("realized vol", category="option-strategy")
    assert store.search_sections("realized vol", category="macro") == []


def test_section_search_is_cheaper_than_the_whole_page(store):
    page = store.get_page("option-strategy/demo")
    section = store.get_section("option-strategy/demo", "Limitations")
    assert len(section.split()) < len(page.content.split())


# ---------------------------------------------------------------------------------------
# Sub-topics (parsed from the TS registry, not frontmatter)
# ---------------------------------------------------------------------------------------

REGISTRY_TS = """import { ArticleLabel } from "@/data/articles/types";

export const wikiEntries: WikiEntry[] = [
  {
    path: "option-strategy/demo",
    topics: ["Volatility & VRP", "Income & Writing"],
    title: "Demo Page",
    articleSlug: "",
    date: "2026-08-22",
    labels: [ArticleLabel.OPTIONS],
    summary:
      "A demo page used by the tests.",
  },
  {
    path: "option-strategy/other",
    topics: ["Income & Writing"],
    title: "Other Page",
    articleSlug: "",
    date: "2026-08-22",
    labels: [ArticleLabel.OPTIONS],
    summary: "Another demo page.",
  },
  {
    path: "option-strategy/untagged",
    title: "Untagged Page",
    articleSlug: "",
    date: "2026-08-22",
    labels: [ArticleLabel.OPTIONS],
    summary: "Has no topics.",
  },
];
"""

OTHER_PAGE = """\
---
path: option-strategy/other
title: Other Page
---

## Writing Premium

Selling covered calls for income.
"""

UNTAGGED_PAGE = """\
---
path: option-strategy/untagged
title: Untagged Page
---

## Something

Body text.
"""


@pytest.fixture()
def topic_store(tmp_path):
    wiki_dir = tmp_path / "wiki" / "option-strategy"
    wiki_dir.mkdir(parents=True)
    (wiki_dir / "demo.md").write_text(PAGE, encoding="utf-8")
    (wiki_dir / "other.md").write_text(OTHER_PAGE, encoding="utf-8")
    (wiki_dir / "untagged.md").write_text(UNTAGGED_PAGE, encoding="utf-8")
    registry = tmp_path / "index.ts"
    registry.write_text(REGISTRY_TS, encoding="utf-8")
    return WikiStore(tmp_path / "wiki", registry)


def test_topics_are_parsed_from_the_registry(topic_store):
    page = topic_store.get_page("option-strategy/demo")
    assert page.topics == ["Volatility & VRP", "Income & Writing"]


def test_topics_default_to_empty_when_absent(topic_store):
    assert topic_store.get_page("option-strategy/untagged").topics == []


def test_topics_do_not_break_summary_parsing(topic_store):
    # The topics field is injected directly after `path:`, ahead of `summary:` -- a regression
    # here would silently strip every summary from the 44 entries that carry topics.
    assert topic_store.get_page("option-strategy/demo").summary == "A demo page used by the tests."
    assert topic_store.get_page("option-strategy/untagged").summary == "Has no topics."


def test_list_topics_counts_and_orders_by_use(topic_store):
    assert topic_store.list_topics() == [("Income & Writing", 2), ("Volatility & VRP", 1)]


def test_list_topics_can_scope_to_a_category(topic_store):
    assert topic_store.list_topics("option-strategy")
    assert topic_store.list_topics("macro") == []


def test_topic_filter_narrows_page_search(topic_store):
    hits = topic_store.search("page", topic="Volatility & VRP", limit=50)
    assert [h["path"] for h in hits] == ["option-strategy/demo"]


def test_multi_topic_page_is_reachable_from_each_of_its_topics(topic_store):
    for t in ("Volatility & VRP", "Income & Writing"):
        paths = [h["path"] for h in topic_store.search("page", topic=t, limit=50)]
        assert "option-strategy/demo" in paths


def test_unknown_topic_matches_nothing(topic_store):
    assert topic_store.search("page", topic="Nope", limit=50) == []


def test_topic_filter_applies_to_section_search(topic_store):
    hits = topic_store.search_sections("premium income", topic="Income & Writing", limit=50)
    assert hits
    assert {h["path"] for h in hits} <= {"option-strategy/demo", "option-strategy/other"}
    assert all("option-strategy/untagged" != h["path"] for h in hits)


def test_page_search_results_expose_topics(topic_store):
    hits = topic_store.search("demo", limit=5)
    assert hits
    assert "topics" in hits[0]
