"""
Builds/extends urls.txt with Korea Times article URLs matching a search
query, by paging through Korea Times' own search API directly -- far more
reliable than automating clicks through the rendered search page (which
requires JavaScript and doesn't reflect the search term or page number in
its URL).

Confirmed live API (2026):
    GET https://goatway.koreatimes.co.kr/api/home/ai/search/ARTICLE
        ?query=<term>&sort=RELEVANCE&page=<0-based>&size=10&scopes=TITLE,KEYWORD

Response shape:
    data.page.totalPages / totalElements -- pagination info
    data.contents[] -- one dict per article, each with a "slugUrl" like
        "/opinion/columns/deskcolumns/20260722/hallyu-meets-heritage"
    which becomes a full article URL once prefixed with the site domain --
    exactly the URL format scrape_korea_times.py expects.

Usage: edit QUERY below (or pass it as a command-line arg), then run.
Appends new, not-already-present URLs to urls.txt -- safe to run multiple
times with different search terms to build up one combined urls.txt; it
won't duplicate URLs already in the file or across search terms.
"""

import os
import sys
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

QUERY = "hallyu"  # change this, or pass as: python fetch_koreatimes_search_urls.py "your term"

API_URL = "https://goatway.koreatimes.co.kr/api/home/ai/search/ARTICLE"
SITE_DOMAIN = "https://www.koreatimes.co.kr"
PAGE_SIZE = 10
OUTPUT_FILE = "urls.txt"
REQUEST_DELAY_SECONDS = 0.5
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def build_session():
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    retry = Retry(
        total=MAX_RETRIES, connect=MAX_RETRIES, read=MAX_RETRIES, status=MAX_RETRIES,
        backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]), raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


def fetch_page(session, query, page):
    params = {
        "query": query, "sort": "RELEVANCE", "page": page,
        "size": PAGE_SIZE, "scopes": "TITLE,KEYWORD",
    }
    resp = session.get(API_URL, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("result") != "success":
        raise RuntimeError(f"API returned non-success: {payload.get('message')}")
    return payload["data"]


def fetch_all_urls_for_query(session, query):
    first_page = fetch_page(session, query, 0)
    total_pages = first_page["page"]["totalPages"]
    total_elements = first_page["page"]["totalElements"]
    print(f"Query {query!r}: {total_elements} articles across {total_pages} pages")

    urls = [SITE_DOMAIN + item["slugUrl"] for item in first_page["contents"]]
    print(f"  page 1/{total_pages}: {len(first_page['contents'])} articles (running total {len(urls)})")

    for page in range(1, total_pages):
        time.sleep(REQUEST_DELAY_SECONDS)
        data = fetch_page(session, query, page)
        contents = data["contents"]
        urls.extend(SITE_DOMAIN + item["slugUrl"] for item in contents)
        print(f"  page {page + 1}/{total_pages}: {len(contents)} articles (running total {len(urls)})")

    return urls


def main():
    query = sys.argv[1] if len(sys.argv) > 1 else QUERY

    session = build_session()
    urls = fetch_all_urls_for_query(session, query)

    seen = set()
    deduped = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            deduped.append(u)
    n_dupes = len(urls) - len(deduped)
    if n_dupes:
        print(f"Removed {n_dupes} duplicate URLs within this search's own results.")

    existing = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            existing = {line.strip() for line in f if line.strip()}

    new_urls = [u for u in deduped if u not in existing]
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        for u in new_urls:
            f.write(u + "\n")

    print()
    print(f"Added {len(new_urls)} new URLs to {OUTPUT_FILE} "
          f"({len(deduped) - len(new_urls)} were already present, skipped).")
    print(f"{OUTPUT_FILE} now has {len(existing) + len(new_urls)} total URLs.")


if __name__ == "__main__":
    main()
