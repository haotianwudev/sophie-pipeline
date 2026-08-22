"""Section-level wiki addressing (category > page > section).

Covers the splitter's edge cases rather than the happy path only: fenced code blocks that
contain '##' lines, pages whose text starts before any heading, and level-2 headings whose
body lives entirely in their level-3 children.
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
