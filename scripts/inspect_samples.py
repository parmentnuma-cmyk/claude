"""
Diagnostic script — run this FIRST, in Colab (where you have real internet
access), before trusting scrape_korea_herald.py on your full 1,600-URL list.

Why this exists: the environment used to *write* this script could not reach
koreaherald.com (network policy blocks it), so the selectors in the main
script were not verified against a live page. Rather than guess and hope,
the main script leans on JSON-LD (schema.org NewsArticle) and OpenGraph/meta
tags first — these are a standard, template-agnostic way modern news CMSs
expose title/author/date/section, so they tend to survive template changes
better than hand-picked CSS classes. BeautifulSoup selector guesses are only
the last-resort fallback.

This script fetches a handful of URLs (ideally including at least one
pre-2018 article, since older templates sometimes differ) and prints every
candidate field it can find, so you can eyeball real output and tell me
(or just yourself) whether the assumptions hold before running the full batch.

Usage:
    python inspect_samples.py https://www.koreaherald.com/article/XXXXXXX \
                              https://www.koreaherald.com/view.php?ud=20161012000545 \
                              ...

If no URLs are given on the command line, edit SAMPLE_URLS below.
"""

import json
import sys

import requests
from bs4 import BeautifulSoup

try:
    import trafilatura
except ImportError:
    trafilatura = None

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Fill in a few real URLs from your urls.txt if you're not passing them
# on the command line. Include at least one pre-2018 article.
SAMPLE_URLS = [
    # "https://www.koreaherald.com/article/3283510",
    # "https://www.koreaherald.com/view.php?ud=20161012000545",
]

META_KEYS_OF_INTEREST = [
    "og:title", "og:type", "og:section", "og:article:section",
    "article:published_time", "article:modified_time",
    "article:author", "article:section", "author",
    "twitter:title", "description",
]


def line(char="-", n=70):
    print(char * n)


def inspect_one(url, session):
    print()
    line("=")
    print(f"URL: {url}")
    line("=")

    try:
        resp = session.get(url, timeout=20)
    except requests.exceptions.RequestException as e:
        print(f"  REQUEST FAILED: {type(e).__name__}: {e}")
        return

    print(f"  HTTP status: {resp.status_code}")
    print(f"  Final URL (after redirects): {resp.url}")
    print(f"  Content-Length: {len(resp.content)} bytes")

    if resp.status_code != 200:
        print("  Non-200 response, skipping parse.")
        return

    soup = BeautifulSoup(resp.text, "html.parser")

    # 1. <title>
    line()
    print("1. <title> tag:")
    print(" ", soup.title.get_text(strip=True) if soup.title else "(none)")

    # 2. Meta tags of interest
    line()
    print("2. Meta tags found:")
    found_any = False
    for tag in soup.find_all("meta"):
        key = tag.get("property") or tag.get("name")
        if key and key.lower() in [k.lower() for k in META_KEYS_OF_INTEREST]:
            found_any = True
            print(f"   {key} = {tag.get('content')!r}")
    if not found_any:
        print("   (none of the expected keys found — dump ALL meta property/name below)")
        for tag in soup.find_all("meta"):
            key = tag.get("property") or tag.get("name")
            if key:
                print(f"   {key} = {tag.get('content')!r}")

    # 3. JSON-LD blocks
    line()
    print("3. JSON-LD <script type='application/ld+json'> blocks:")
    ld_blocks = soup.find_all("script", type="application/ld+json")
    if not ld_blocks:
        print("   (none found)")
    for i, block in enumerate(ld_blocks):
        print(f"   --- block {i} ---")
        try:
            data = json.loads(block.string or "{}")
            print(f"   @type: {data.get('@type')}")
            for k in ("headline", "datePublished", "dateModified", "author",
                      "articleSection", "publisher"):
                if k in data:
                    print(f"   {k}: {data[k]}")
        except (json.JSONDecodeError, TypeError) as e:
            print(f"   (could not parse as JSON: {e})")
            print(f"   raw (first 300 chars): {(block.string or '')[:300]}")

    # 4. Visible "Published" / "Updated" text near top of article
    #    (many Korean news templates show this as plain text, e.g.
    #    "Published : Nov 8, 2023 - 10:32" / "Updated : Nov 8, 2023 - 15:10")
    line()
    print("4. Visible text containing 'Publish' or 'Updat' (first 5 hits):")
    hits = 0
    for el in soup.find_all(string=lambda s: s and ("publish" in s.lower() or "updat" in s.lower())):
        text = el.strip()
        if text and len(text) < 200:
            print(f"   [{el.parent.name}.{el.parent.get('class')}] {text!r}")
            hits += 1
        if hits >= 5:
            break
    if hits == 0:
        print("   (no matches)")

    # 5. Breadcrumb / nav elements (possible section source)
    line()
    print("5. Candidate breadcrumb/section elements:")
    for sel in ["nav", ".breadcrumb", ".location", ".category", ".section"]:
        found = soup.select(sel)
        if found:
            for f in found[:2]:
                txt = f.get_text(" > ", strip=True)
                if txt:
                    print(f"   [{sel}] {txt[:150]}")

    # 6. Article body candidates: <article>, <main>, biggest <div> by <p> count
    line()
    print("6. Body container candidates (tag, selector, #<p>, first 150 chars):")
    candidates = []
    for tag_name in ["article", "main"]:
        el = soup.find(tag_name)
        if el:
            candidates.append((tag_name, el))
    for el in soup.find_all("div", id=True):
        if len(el.find_all("p")) >= 3:
            candidates.append((f"div#{el['id']}", el))
    for el in soup.find_all("div", class_=True):
        if len(el.find_all("p")) >= 3:
            candidates.append((f"div.{'.'.join(el['class'])}", el))

    # de-dup and rank by paragraph count, show top 5
    seen = set()
    ranked = []
    for label, el in candidates:
        key = id(el)
        if key in seen:
            continue
        seen.add(key)
        p_count = len(el.find_all("p"))
        ranked.append((p_count, label, el))
    ranked.sort(reverse=True, key=lambda x: x[0])
    for p_count, label, el in ranked[:5]:
        text = " ".join(p.get_text(" ", strip=True) for p in el.find_all("p"))
        print(f"   {label}: {p_count} <p> tags, {len(text)} chars")
        print(f"      preview: {text[:150]!r}")

    # 7. trafilatura's own view
    line()
    print("7. trafilatura.extract() with metadata:")
    if trafilatura is None:
        print("   trafilatura not installed in this environment")
    else:
        extracted = trafilatura.extract(
            resp.text, url=url, with_metadata=True,
            output_format="json", include_comments=False,
        )
        if extracted:
            data = json.loads(extracted)
            for k in ("title", "author", "date", "sitename", "categories", "tags"):
                print(f"   {k}: {data.get(k)}")
            body = data.get("text") or ""
            print(f"   body length: {len(body)} chars")
            print(f"   body preview: {body[:150]!r}")
        else:
            print("   trafilatura returned nothing")


def main():
    urls = sys.argv[1:] or SAMPLE_URLS
    if not urls:
        print("No URLs given. Pass URLs as command-line args, or edit SAMPLE_URLS.")
        sys.exit(1)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    for url in urls:
        inspect_one(url, session)

    line("=")
    print("Done. Compare the output above against what scrape_korea_herald.py")
    print("assumes (see the EXTRACTION NOTES comment near the top of that file).")
    print("If JSON-LD/meta tags were present and populated, no changes needed.")
    print("If section 6's ranked candidates point at a specific div/class that")
    print("looks like the real article body, add it to BODY_SELECTOR_CANDIDATES")
    print("near the top of scrape_korea_herald.py, in priority order.")


if __name__ == "__main__":
    main()
