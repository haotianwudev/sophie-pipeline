"""
Quant Trending ETL — Phase 1
Fetches trending quant finance content from: ArXiv, GitHub, Reddit, Hacker News.
No API keys required (GitHub token optional for higher rate limits).
"""

import os
import sys
import json
import math
import datetime
import pathlib
import xml.etree.ElementTree as ET

import requests
import psycopg2

ROOT = pathlib.Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))
from src.tools.api_db import get_db_connection

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ARXIV_CATEGORIES = ["q-fin.PM", "q-fin.RM", "q-fin.ST", "q-fin.TR", "q-fin.CP"]
REDDIT_SUBREDDITS = ["quant", "algotrading", "investing"]
HN_KEYWORDS = ["quant finance", "portfolio optimization", "factor model", "risk management", "algorithmic trading"]
GITHUB_QUERY = "quant finance"

USER_AGENT = "SophieDaddy-Quant-Bot/1.0 (https://sophiedaddy.com)"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def age_hours(published_at: datetime.datetime) -> float:
    """Hours since published_at (UTC). Minimum 0.1 to avoid division by zero."""
    if published_at is None:
        return 24.0
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=datetime.timezone.utc)
    return max((now - published_at).total_seconds() / 3600, 0.1)


def normalize_to_100(items: list[dict], score_key: str = "raw_score") -> list[dict]:
    """Normalize raw scores within a batch to 0–100."""
    if not items:
        return items
    scores = [item[score_key] for item in items]
    min_s, max_s = min(scores), max(scores)
    rng = max_s - min_s if max_s > min_s else 1.0
    for item in items:
        item["heat_score"] = round((item[score_key] - min_s) / rng * 100, 2)
    return items


def safe_get(url: str, params: dict = None, headers: dict = None, timeout: int = 15):
    """GET with error handling; returns response or None."""
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp
    except Exception as e:
        print(f"  [WARN] Request failed for {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# Fetchers
# ---------------------------------------------------------------------------

def fetch_arxiv(days: int = 3, max_results: int = 30) -> list[dict]:
    """Fetch recent q-fin papers from ArXiv Atom API."""
    print(f"[ArXiv] Fetching last {days} days of q-fin papers...")
    category_query = " OR ".join(f"cat:{c}" for c in ARXIV_CATEGORIES)
    params = {
        "search_query": category_query,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
        "max_results": max_results,
    }
    resp = safe_get("http://export.arxiv.org/api/query", params=params)
    if resp is None:
        return []

    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "arxiv": "http://arxiv.org/schemas/atom",
    }
    root = ET.fromstring(resp.text)
    cutoff = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=days)

    items = []
    for entry in root.findall("atom:entry", ns):
        published_str = (entry.findtext("atom:published", "", ns) or "").strip()
        try:
            published_at = datetime.datetime.fromisoformat(published_str.replace("Z", "+00:00"))
        except ValueError:
            continue

        if published_at < cutoff:
            continue

        arxiv_id_url = (entry.findtext("atom:id", "", ns) or "").strip()
        arxiv_id = arxiv_id_url.split("/abs/")[-1] if "/abs/" in arxiv_id_url else arxiv_id_url
        title = (entry.findtext("atom:title", "", ns) or "").strip().replace("\n", " ")
        abstract = (entry.findtext("atom:summary", "", ns) or "").strip().replace("\n", " ")
        abstract = abstract[:500] + "..." if len(abstract) > 500 else abstract

        authors = [
            (a.findtext("atom:name", "", ns) or "").strip()
            for a in entry.findall("atom:author", ns)
        ]
        author = ", ".join(authors[:3]) + (" et al." if len(authors) > 3 else "")

        categories = [
            c.get("term", "")
            for c in entry.findall("atom:category", ns)
            if c.get("term", "").startswith("q-fin")
        ]

        # Recency score: 100 / (1 + age_hours)
        raw_score = 100.0 / (1.0 + age_hours(published_at))

        items.append({
            "source": "arxiv",
            "external_id": arxiv_id,
            "title": title,
            "url": arxiv_id_url,
            "description": abstract,
            "author": author,
            "raw_score": raw_score,
            "tags": categories,
            "published_at": published_at,
        })

    print(f"  ->{len(items)} papers found")
    return normalize_to_100(items)


def fetch_github(query: str = GITHUB_QUERY, max_results: int = 20) -> list[dict]:
    """Fetch trending quant finance repos from GitHub Search API."""
    print(f"[GitHub] Searching repos for '{query}'...")
    headers = {"Accept": "application/vnd.github+json", "User-Agent": USER_AGENT}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    # Search repos pushed in last 30 days, sorted by stars
    since_date = (datetime.datetime.utcnow() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
    params = {
        "q": f"{query} pushed:>{since_date}",
        "sort": "stars",
        "order": "desc",
        "per_page": max_results,
    }
    resp = safe_get("https://api.github.com/search/repositories", params=params, headers=headers)
    if resp is None:
        return []

    data = resp.json()
    repos = data.get("items", [])
    items = []
    for repo in repos:
        pushed_str = repo.get("pushed_at") or repo.get("created_at")
        try:
            pushed_at = datetime.datetime.fromisoformat(pushed_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pushed_at = None

        stars = repo.get("stargazers_count", 0)
        # Stars with recency decay: newer activity = higher score
        age_d = age_hours(pushed_at) / 24.0
        raw_score = stars * (0.9 ** (age_d / 7.0))

        description = (repo.get("description") or "")[:400]
        topics = repo.get("topics", [])

        items.append({
            "source": "github",
            "external_id": repo["full_name"],
            "title": repo["full_name"],
            "url": repo["html_url"],
            "description": description,
            "author": repo.get("owner", {}).get("login", ""),
            "raw_score": raw_score,
            "tags": topics[:8],
            "published_at": pushed_at,
        })

    print(f"  ->{len(items)} repos found")
    return normalize_to_100(items)


def fetch_reddit(subreddits: list[str] = None, limit: int = 25) -> list[dict]:
    """Fetch hot posts from quant-related subreddits via public JSON API."""
    if subreddits is None:
        subreddits = REDDIT_SUBREDDITS
    headers = {"User-Agent": USER_AGENT}
    items = []

    for sub in subreddits:
        print(f"[Reddit] Fetching r/{sub}/hot...")
        resp = safe_get(
            f"https://www.reddit.com/r/{sub}/hot.json",
            params={"limit": limit},
            headers=headers,
        )
        if resp is None:
            continue

        posts = resp.json().get("data", {}).get("children", [])
        for post in posts:
            d = post.get("data", {})
            if d.get("stickied") or d.get("is_self") is False and not d.get("url"):
                continue

            created_utc = d.get("created_utc")
            published_at = (
                datetime.datetime.fromtimestamp(created_utc, tz=datetime.timezone.utc)
                if created_utc else None
            )
            upvotes = d.get("ups", 0)
            num_comments = d.get("num_comments", 0)

            # HN-style gravity score
            raw_score = (upvotes + 1) / (age_hours(published_at) + 2) ** 1.5

            url = d.get("url") or f"https://reddit.com{d.get('permalink', '')}"
            selftext = (d.get("selftext") or "")[:400].replace("\n", " ")
            flair = d.get("link_flair_text") or ""
            tags = [f"r/{sub}"] + ([flair] if flair else [])

            items.append({
                "source": "reddit",
                "external_id": d.get("id", ""),
                "title": d.get("title", ""),
                "url": url,
                "description": selftext or None,
                "author": d.get("author", ""),
                "raw_score": raw_score,
                "tags": tags,
                "published_at": published_at,
            })

    print(f"  ->{len(items)} posts found")
    return normalize_to_100(items)


def fetch_hackernews(keywords: list[str] = None, limit: int = 25) -> list[dict]:
    """Fetch recent HN stories matching quant finance keywords via Algolia API."""
    if keywords is None:
        keywords = HN_KEYWORDS
    items = []
    seen_ids = set()

    cutoff_ts = int(
        (datetime.datetime.utcnow() - datetime.timedelta(days=7)).timestamp()
    )

    for keyword in keywords:
        print(f"[HN] Searching '{keyword}'...")
        resp = safe_get(
            "https://hn.algolia.com/api/v1/search",
            params={
                "query": keyword,
                "tags": "story",
                "numericFilters": f"created_at_i>{cutoff_ts}",
                "hitsPerPage": limit,
            },
        )
        if resp is None:
            continue

        for hit in resp.json().get("hits", []):
            obj_id = hit.get("objectID", "")
            if obj_id in seen_ids:
                continue
            seen_ids.add(obj_id)

            created_ts = hit.get("created_at_i")
            published_at = (
                datetime.datetime.fromtimestamp(created_ts, tz=datetime.timezone.utc)
                if created_ts else None
            )
            points = hit.get("points") or 0
            num_comments = hit.get("num_comments") or 0
            raw_score = (points + num_comments * 0.5) / (age_hours(published_at) + 2) ** 1.5

            url = hit.get("url") or f"https://news.ycombinator.com/item?id={obj_id}"
            items.append({
                "source": "hackernews",
                "external_id": obj_id,
                "title": hit.get("title", ""),
                "url": url,
                "description": None,
                "author": hit.get("author", ""),
                "raw_score": raw_score,
                "tags": ["hackernews"],
                "published_at": published_at,
            })

    print(f"  ->{len(items)} HN stories found")
    return normalize_to_100(items)


# ---------------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------------

def upsert_items(items: list[dict]):
    """Upsert trending items into quant_trending_items table."""
    if not items:
        return
    conn = get_db_connection()
    cursor = conn.cursor()
    count = 0
    for item in items:
        try:
            cursor.execute(
                """
                INSERT INTO quant_trending_items
                    (source, external_id, title, url, description, author,
                     heat_score, raw_score, tags, published_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, external_id) DO UPDATE SET
                    heat_score  = EXCLUDED.heat_score,
                    raw_score   = EXCLUDED.raw_score,
                    title       = EXCLUDED.title,
                    description = EXCLUDED.description,
                    fetched_at  = CURRENT_TIMESTAMP
                """,
                (
                    item["source"],
                    item["external_id"],
                    item["title"],
                    item["url"],
                    item.get("description"),
                    item.get("author"),
                    item.get("heat_score", 0),
                    item.get("raw_score", 0),
                    json.dumps(item.get("tags", [])),
                    item.get("published_at"),
                ),
            )
            count += 1
        except Exception as e:
            print(f"  [WARN] Upsert failed for {item.get('external_id')}: {e}")
            conn.rollback()
    conn.commit()
    cursor.close()
    conn.close()
    print(f"  [OK] Upserted {count} items")


# ---------------------------------------------------------------------------
# Main ETL
# ---------------------------------------------------------------------------

def run_etl():
    print("=" * 60)
    print("Quant Trending ETL - Phase 1")
    print("=" * 60)

    all_items = []

    arxiv_items = fetch_arxiv(days=3, max_results=30)
    all_items.extend(arxiv_items)
    upsert_items(arxiv_items)

    github_items = fetch_github(max_results=20)
    all_items.extend(github_items)
    upsert_items(github_items)

    reddit_items = fetch_reddit(limit=25)
    all_items.extend(reddit_items)
    upsert_items(reddit_items)

    hn_items = fetch_hackernews(limit=25)
    all_items.extend(hn_items)
    upsert_items(hn_items)

    print("-" * 60)
    print(f"ETL complete. Total items processed: {len(all_items)}")
    by_source = {}
    for item in all_items:
        by_source[item["source"]] = by_source.get(item["source"], 0) + 1
    for src, cnt in by_source.items():
        print(f"  {src}: {cnt}")


if __name__ == "__main__":
    run_etl()
