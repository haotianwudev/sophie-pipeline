"""
Quant Trending ETL — Phase 1
Fetches trending quant finance content from: ArXiv, GitHub, Reddit, Hacker News, and Google News.
No API keys required (GitHub token optional for higher rate limits).
"""

import os
import sys
import json
import math
import re
import time
import datetime
import pathlib
import email.utils
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

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"


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
    if headers is None:
        headers = {"User-Agent": USER_AGENT}
    elif "User-Agent" not in headers:
        headers["User-Agent"] = USER_AGENT

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
    """Fetch recent q-fin papers from ArXiv Atom API, with RSS fallback."""
    print(f"[ArXiv] Fetching last {days} days of q-fin papers...")
    category_query = " OR ".join(f"cat:{c}" for c in ARXIV_CATEGORIES)
    params = {
        "search_query": category_query,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
        "max_results": max_results,
    }
    resp = safe_get("https://export.arxiv.org/api/query", params=params, timeout=10)

    items = []
    cutoff = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=days)

    if resp is not None and resp.status_code == 200:
        ns = {
            "atom": "http://www.w3.org/2005/Atom",
            "arxiv": "http://arxiv.org/schemas/atom",
        }
        try:
            root = ET.fromstring(resp.text)
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
        except Exception as e:
            print(f"  [WARN] ArXiv API parse failed: {e}")

    # Fallback to arXiv RSS feed if API failed or returned empty
    if not items:
        print("  [ArXiv] Fallback to arXiv q-fin RSS feed...")
        rss_resp = safe_get("https://rss.arxiv.org/rss/q-fin", headers={"User-Agent": USER_AGENT}, timeout=10)
        if rss_resp is not None and rss_resp.status_code == 200:
            try:
                root = ET.fromstring(rss_resp.text)
                for item_node in root.findall(".//item"):
                    title = (item_node.findtext("title", "") or "").strip().replace("\n", " ")
                    link = (item_node.findtext("link", "") or "").strip()
                    description = (item_node.findtext("description", "") or "").strip().replace("\n", " ")
                    description = description[:500] + "..." if len(description) > 500 else description
                    guid = (item_node.findtext("guid", "") or link).split("/")[-1]
                    pub_str = (item_node.findtext("pubDate", "") or "").strip()

                    published_at = None
                    if pub_str:
                        try:
                            published_at = email.utils.parsedate_to_datetime(pub_str)
                        except Exception:
                            published_at = None

                    raw_score = 100.0 / (1.0 + age_hours(published_at))

                    items.append({
                        "source": "arxiv",
                        "external_id": guid,
                        "title": title,
                        "url": link,
                        "description": description,
                        "author": "arXiv q-fin",
                        "raw_score": raw_score,
                        "tags": ["q-fin"],
                        "published_at": published_at,
                    })
            except Exception as e:
                print(f"  [WARN] ArXiv RSS parse failed: {e}")

    print(f"  ->{len(items)} papers found")
    return normalize_to_100(items)


def fetch_github(query: str = GITHUB_QUERY, max_results: int = 20) -> list[dict]:
    """Fetch trending quant finance repos from GitHub Search API, with topic fallback."""
    print(f"[GitHub] Searching repos for '{query}'...")
    headers = {"Accept": "application/vnd.github+json", "User-Agent": USER_AGENT}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    search_query = "quant finance OR algorithmic trading OR backtesting"
    params = {
        "q": search_query,
        "sort": "updated",
        "order": "desc",
        "per_page": max_results,
    }
    resp = safe_get("https://api.github.com/search/repositories", params=params, headers=headers, timeout=10)

    repos = []
    if resp is not None and resp.status_code == 200:
        repos = resp.json().get("items", [])
    else:
        print("  [WARN] GitHub search rate limited or failed, falling back to topic search...")
        params_fb = {"q": "topic:quant-finance OR topic:algotrading", "sort": "stars", "per_page": max_results}
        resp_fb = safe_get("https://api.github.com/search/repositories", params=params_fb, headers=headers, timeout=10)
        if resp_fb is not None and resp_fb.status_code == 200:
            repos = resp_fb.json().get("items", [])

    items = []
    for repo in repos:
        pushed_str = repo.get("pushed_at") or repo.get("updated_at") or repo.get("created_at")
        pushed_at = None
        if pushed_str:
            try:
                pushed_at = datetime.datetime.fromisoformat(pushed_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                pushed_at = None

        stars = repo.get("stargazers_count", 0)
        age_d = age_hours(pushed_at) / 24.0
        raw_score = (stars + 1) * (0.9 ** (age_d / 7.0))

        description = (repo.get("description") or "")[:400]
        topics = repo.get("topics", [])

        items.append({
            "source": "github",
            "external_id": repo.get("full_name", ""),
            "title": repo.get("full_name", ""),
            "url": repo.get("html_url", ""),
            "description": description,
            "author": repo.get("owner", {}).get("login", ""),
            "raw_score": raw_score,
            "tags": topics[:8],
            "published_at": pushed_at,
        })

    print(f"  ->{len(items)} repos found")
    return normalize_to_100(items)


def fetch_reddit(subreddits: list[str] = None, limit: int = 25) -> list[dict]:
    """Fetch hot posts from quant-related subreddits via public RSS feed."""
    if subreddits is None:
        subreddits = REDDIT_SUBREDDITS
    headers = {"User-Agent": USER_AGENT}
    items = []

    for sub in subreddits:
        print(f"[Reddit] Fetching r/{sub}/hot.rss...")
        time.sleep(2.0)
        resp = safe_get(
            f"https://www.reddit.com/r/{sub}/hot.rss",
            params={"limit": limit},
            headers=headers,
            timeout=10,
        )
        if resp is None or resp.status_code != 200:
            # Retry with old.reddit.com if main domain rate limited
            time.sleep(2.0)
            resp = safe_get(
                f"https://old.reddit.com/r/{sub}/hot.rss",
                params={"limit": limit},
                headers=headers,
                timeout=10,
            )
            if resp is None or resp.status_code != 200:
                continue

        raw_xml = resp.text
        clean_xml = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', raw_xml)

        parsed_entries = []
        try:
            root = ET.fromstring(clean_xml)
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            for entry in root.findall('atom:entry', ns):
                title = (entry.findtext('atom:title', '', ns) or '').strip()
                link_elem = entry.find('atom:link', ns)
                url = link_elem.get('href') if link_elem is not None else f"https://reddit.com/r/{sub}"
                author_elem = entry.find('atom:author/atom:name', ns)
                author = (author_elem.text if author_elem is not None else '').strip()
                entry_id = (entry.findtext('atom:id', '', ns) or '').strip().split('/')[-1]
                published_str = (entry.findtext('atom:updated', '', ns) or '').strip()

                published_at = None
                if published_str:
                    try:
                        published_at = datetime.datetime.fromisoformat(published_str.replace("Z", "+00:00"))
                    except Exception:
                        published_at = None

                parsed_entries.append({
                    "id": entry_id or url,
                    "title": title,
                    "url": url,
                    "author": author,
                    "published_at": published_at
                })
        except Exception:
            entries = re.findall(r'<entry>(.*?)</entry>', clean_xml, re.DOTALL)
            for entry_str in entries:
                title_m = re.search(r'<title>(.*?)</title>', entry_str)
                link_m = re.search(r'<link href="(.*?)"', entry_str)
                id_m = re.search(r'<id>(.*?)</id>', entry_str)
                updated_m = re.search(r'<updated>(.*?)</updated>', entry_str)
                author_m = re.search(r'<name>(.*?)</name>', entry_str)
                if title_m and link_m:
                    pub_at = None
                    if updated_m:
                        try:
                            pub_at = datetime.datetime.fromisoformat(updated_m.group(1).replace("Z", "+00:00"))
                        except Exception:
                            pub_at = None
                    parsed_entries.append({
                        "id": id_m.group(1).split('/')[-1] if id_m else link_m.group(1),
                        "title": title_m.group(1),
                        "url": link_m.group(1),
                        "author": author_m.group(1) if author_m else "",
                        "published_at": pub_at
                    })

        for p in parsed_entries:
            published_at = p["published_at"]
            raw_score = 100.0 / (1.0 + age_hours(published_at))
            tags = [f"r/{sub}"]

            items.append({
                "source": "reddit",
                "external_id": p["id"][:500],
                "title": p["title"],
                "url": p["url"],
                "description": None,
                "author": p["author"],
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
                "external_id": obj_id[:500],
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


def fetch_google_news(query: str = "quant finance", limit: int = 25) -> list[dict]:
    """Fetch trending news articles for quant finance via Google News RSS."""
    print(f"[News] Fetching Google News RSS for '{query}'...")
    headers = {"User-Agent": USER_AGENT}
    url = f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=en-US&gl=US&ceid=US:en"

    resp = safe_get(url, headers=headers, timeout=10)
    if resp is None or resp.status_code != 200:
        return []

    items = []
    try:
        root = ET.fromstring(resp.text)
        for item_node in root.findall(".//item")[:limit]:
            title = (item_node.findtext("title", "") or "").strip()
            link = (item_node.findtext("link", "") or "").strip()
            guid = (item_node.findtext("guid", "") or link).strip()
            pub_str = (item_node.findtext("pubDate", "") or "").strip()

            author = "Google News"
            if " - " in title:
                parts = title.rsplit(" - ", 1)
                title_clean = parts[0]
                author = parts[1]
            else:
                title_clean = title

            published_at = None
            if pub_str:
                try:
                    published_at = email.utils.parsedate_to_datetime(pub_str)
                except Exception:
                    published_at = None

            raw_score = 100.0 / (1.0 + age_hours(published_at))

            items.append({
                "source": "news",
                "external_id": guid[:500],
                "title": title_clean,
                "url": link,
                "description": title,
                "author": author,
                "raw_score": raw_score,
                "tags": ["quant-finance", "news"],
                "published_at": published_at,
            })
    except Exception as e:
        print(f"  [WARN] Google News RSS parse failed: {e}")

    print(f"  ->{len(items)} news stories found")
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

    news_items = fetch_google_news(limit=25)
    all_items.extend(news_items)
    upsert_items(news_items)

    print("-" * 60)
    print(f"ETL complete. Total items processed: {len(all_items)}")
    by_source = {}
    for item in all_items:
        by_source[item["source"]] = by_source.get(item["source"], 0) + 1
    for src, cnt in by_source.items():
        print(f"  {src}: {cnt}")


if __name__ == "__main__":
    run_etl()
