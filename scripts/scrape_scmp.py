"""
Scrapes a list of South China Morning Post (SCMP) article URLs into a
structured CSV corpus for manual content-coding.

INPUT:  urls.txt          (one URL per line, working directory)
OUTPUT: scmp_corpus.csv    one row per unique input URL (success or failed)
        scmp_failures.csv  one row per failed URL, with error detail
        raw_html_scmp/<article_id>.html   raw HTML of every successfully fetched page

--------------------------------------------------------------------------
EXTRACTION NOTES (read this before running on your full list)
--------------------------------------------------------------------------
Verified against 5 live articles spanning 2021-2026. Same overall
JSON-LD-first / meta-second / trafilatura-third pipeline, adapted for
these confirmed SCMP-specific findings:

    - Priority flipped vs. both Korean sites: JSON-LD headline is the
      full, accurate title here, while og:title is consistently a
      DIFFERENT, shorter, reworded "social share" headline (not just the
      same text with a suffix) -- e.g. JSON-LD "China's gold jewellery
      market shines as culturally resonant designs draw young buyers to
      safe-haven metal" vs og:title "...as cultural designs draw young
      buyers". Using og:title as primary would silently give a different,
      less complete title than the article's real headline. clean_title()
      still strips a " | South China Morning Post" suffix as a defensive
      no-op fallback (seen on the <title> tag, not on JSON-LD/og:title).
    - Author is reliable via JSON-LD across all 5 samples, including one
      showing "SCMP Reporter" -- confirmed NOT a junk site-name leak like
      the Korean sites: SCMP has an explicit no-bylines editorial policy
      (a noBylinesPolicy link is present in the JSON-LD publisher block),
      so "SCMP Reporter" is an intentional, legitimate attribution. No
      author-junk-filtering or byline-fallback needed for this site.
    - Confirmed bug, now fixed: body_text was leaking non-body content at
      the very start in 4 of 5 samples -- the headline repeated verbatim
      (SCMP's <article> starts with an <h1> matching the headline, which
      trafilatura/BS4 both include as if it were a paragraph), sometimes
      followed by a "- <sentence>" line that exactly matches the meta
      description (SCMP's "deck" summary bullet UI), and in one sample
      even a literal "Advertisement" placeholder line before the headline
      repeat. strip_leading_boilerplate() removes these known non-body
      lines from the start of body_text (matched against the
      already-extracted title/description, or an exact "Advertisement"
      line) -- run repeatedly since more than one can stack at the start.
    - dateModified is always present (never an empty string) and often
      differs from datePublished by only minutes on the same calendar
      day -- same date-equality suppression as Korea Times applies
      (updated_date only populated when genuinely a different day).
    - No related-articles/recirculation widget contamination observed in
      any of the 5 samples (all body-container candidates in
      inspect_samples.py matched exactly, via <article> tag and its
      Next.js CSS-module-wrapped parents) -- NOISE_CONTAINER_CLASSES
      starts empty, same as Korea Times.
    - JSON-LD's articleSection (e.g. "Economic Indicators", "China
      Business", "Fashion & Beauty") is clean and matches the URL path
      segments, unlike Korea Herald where it was junk -- available if a
      section column is wanted later, not included per prior instruction
      to drop that column.

NOT yet verified:
    - What a paywalled article's page structure looks like -- none of the
      5 samples appeared paywalled (all had complete-looking 900-2000
      char bodies). If you hit one in your real run, send me the URL/
      inspect_samples.py output so I can add explicit paywall detection
      rather than silently saving a truncated page as if it succeeded.
    - URL patterns for non-article pages (video/photo galleries, topic
      index pages) -- NON_ARTICLE_URL_PATTERNS below are generic guesses
      plus an "/author/" pattern inferred from JSON-LD's sameAs author
      profile links, unconfirmed against real SCMP non-article URLs.
"""

import csv
import html
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import trafilatura
except ImportError:
    print("trafilatura is required: pip install trafilatura")
    raise

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
INPUT_FILE = "urls.txt"
OUTPUT_CSV = "scmp_corpus.csv"
FAILURES_CSV = "scmp_failures.csv"
RAW_HTML_DIR = "raw_html_scmp"

REQUEST_DELAY_SECONDS = 1.5
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
BACKOFF_FACTOR = 2  # -> retry sleeps of ~2s, 4s, 8s on transient errors
MIN_BODY_CHARS = 100  # below this, trafilatura output is considered a miss

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# URL patterns that are never articles -- skipped without a network request.
# /author/ confirmed to exist (seen in JSON-LD sameAs profile links); the
# rest are generic unverified guesses carried over from the Korean sites.
NON_ARTICLE_URL_PATTERNS = [
    r"/photo/", r"/video/", r"/section/", r"/tag/", r"/gallery/",
    r"/author/", r"\.jpg$", r"\.png$", r"/search\b",
]

# No site-specific body selector guesses -- Korea Times' class names are
# Next.js CSS-module build hashes (e.g. "Wrapper_wrap__YfhIp"), not stable
# across deploys, so hardcoding one would be fragile. Body extraction
# relies entirely on trafilatura, then the tag-agnostic generic BS4
# fallback (<article>/<main>/largest <p>-bearing div) in extract_body_bs4.
BODY_SELECTOR_CANDIDATES = []

# Not yet needed: no related-articles/recirculation widget contamination
# observed in the 2 confirmed samples (trafilatura's extracted length
# closely tracked the full container length in both). Add entries here,
# same mechanism as the Korea Herald scraper, if inspect_samples.py ever
# shows a widget being mistaken for body content.
NOISE_CONTAINER_CLASSES = set()
NOISE_CONTAINER_ID_PREFIXES = ()

# Defensive safety net only -- seen on the <title> tag, not on JSON-LD
# headline or og:title, both of which are already clean/reworded on their
# own. Kept in case a rare page ever needs title falling back to <title>.
TITLE_SUFFIX_RE = re.compile(r"\s*\|\s*South China Morning Post\s*$", re.IGNORECASE)

# Confirmed live: a literal "Advertisement" placeholder line can appear as
# the very first line of extracted body_text (seen on 1 of 5 samples).
LEADING_NOISE_LINES = {"advertisement"}

CSV_FIELDS = [
    "article_id", "url", "title", "publication_date", "updated_date",
    "date_flag", "author", "body_text",
    "extraction_method", "scrape_status", "scrape_timestamp",
]

FAILURE_FIELDS = ["article_id", "url", "error_type", "http_status_code", "error_message"]


# --------------------------------------------------------------------------
# HTTP session with retry/backoff on transient errors
# --------------------------------------------------------------------------
def build_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    retry = Retry(
        total=MAX_RETRIES,
        connect=MAX_RETRIES,
        read=MAX_RETRIES,
        status=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# --------------------------------------------------------------------------
# URL loading / dedup
# --------------------------------------------------------------------------
def load_and_dedup_urls(path):
    with open(path, "r", encoding="utf-8") as f:
        raw = [line.strip() for line in f]
    raw = [u for u in raw if u]

    seen = set()
    deduped = []
    for u in raw:
        if u not in seen:
            seen.add(u)
            deduped.append(u)

    n_duplicates = len(raw) - len(deduped)
    print(f"Loaded {len(raw)} URLs, {n_duplicates} duplicates removed, "
          f"{len(deduped)} unique URLs to process.")
    return deduped


def is_non_article_url(url):
    return any(re.search(pat, url, re.IGNORECASE) for pat in NON_ARTICLE_URL_PATTERNS)


# --------------------------------------------------------------------------
# Resumability: figure out which URLs are already recorded
# --------------------------------------------------------------------------
def load_already_done(output_csv):
    done = set()
    if os.path.exists(output_csv):
        with open(output_csv, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                done.add(row["url"])
    return done


def ensure_csv_header(path, fields):
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()


def append_row(path, fields, row):
    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writerow(row)


# --------------------------------------------------------------------------
# Date parsing
# --------------------------------------------------------------------------
def parse_iso_date(raw):
    """Parse an ISO-8601-ish datetime string into a date() or None."""
    if not raw:
        return None
    raw = raw.strip()
    try:
        cleaned = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        return dt.date()
    except ValueError:
        pass
    # common non-ISO fallbacks seen on news sites
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
                "%Y.%m.%d %H:%M", "%Y.%m.%d", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


# --------------------------------------------------------------------------
# Metadata extraction: JSON-LD -> meta tags -> trafilatura
# --------------------------------------------------------------------------
def extract_jsonld_metadata(soup):
    """Return dict with title/author/section/published/updated from the
    first NewsArticle/Article JSON-LD block found, or {} if none usable."""
    for block in soup.find_all("script", type="application/ld+json"):
        raw = block.string
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        # Some sites wrap the article in a list or a @graph array
        candidates = []
        if isinstance(data, list):
            candidates.extend(data)
        elif isinstance(data, dict):
            candidates.append(data)
            if "@graph" in data and isinstance(data["@graph"], list):
                candidates.extend(data["@graph"])

        for item in candidates:
            if not isinstance(item, dict):
                continue
            item_type = item.get("@type", "")
            type_str = " ".join(item_type) if isinstance(item_type, list) else str(item_type)
            if "Article" not in type_str and "NewsArticle" not in type_str:
                continue

            author = ""
            author_field = item.get("author")
            if isinstance(author_field, dict):
                author = author_field.get("name", "") or ""
            elif isinstance(author_field, list):
                names = [a.get("name", "") for a in author_field if isinstance(a, dict)]
                author = ", ".join(n for n in names if n)
            elif isinstance(author_field, str):
                author = author_field

            return {
                "title": item.get("headline", "") or "",
                "author": author,
                "published_raw": item.get("datePublished", ""),
                "updated_raw": item.get("dateModified", ""),
            }
    return {}


def extract_meta_tag_metadata(soup):
    def meta_content(key):
        tag = soup.find("meta", attrs={"property": key}) or soup.find("meta", attrs={"name": key})
        return tag.get("content", "").strip() if tag else ""

    return {
        "title": meta_content("og:title") or meta_content("twitter:title"),
        "author": meta_content("author") or meta_content("article:author"),
        "published_raw": meta_content("article:published_time"),
        "updated_raw": meta_content("article:modified_time") or meta_content("og:updated_time"),
        "description": meta_content("description"),
    }


def _is_noise_div(div):
    classes = set(div.get("class") or [])
    if classes & NOISE_CONTAINER_CLASSES:
        return True
    div_id = div.get("id") or ""
    return any(div_id.startswith(p) for p in NOISE_CONTAINER_ID_PREFIXES)


def strip_known_noise(soup):
    """Remove known recirculation-widget/nav-chrome containers from the
    soup IN PLACE, before any extraction (trafilatura or BS4) sees the
    HTML. Confirmed live: trafilatura can otherwise mistake one of these
    widgets for the real article body, especially on short articles
    surrounded by relatively text-heavy "related articles" modules.
    Also strips <nav>/<header>/<footer>/<aside> as cheap extra insurance
    (trafilatura already excludes these by tag semantics, but the BS4
    fallback path doesn't, so this keeps both paths consistent)."""
    for div in soup.find_all("div"):
        # decomposing a parent div also invalidates its descendants, which
        # may still be sitting in this pre-computed find_all() list
        if div.parent is None:
            continue
        if _is_noise_div(div):
            div.decompose()
    for tag_name in ("nav", "header", "footer", "aside"):
        for el in soup.find_all(tag_name):
            el.decompose()
    return soup


def extract_body_bs4(soup):
    """Last-resort BeautifulSoup body extraction. Returns (text, method) or (None, None)."""
    for attr, value in BODY_SELECTOR_CANDIDATES:
        el = soup.find(class_=value) if attr == "class" else soup.find(id=value)
        if el:
            paragraphs = [p.get_text(" ", strip=True) for p in el.find_all("p")]
            paragraphs = [p for p in paragraphs if p]
            text = "\n".join(paragraphs)
            if len(text) >= MIN_BODY_CHARS:
                return text, f"bs4_selector:{attr}={value}"

    # generic fallback: <article> or <main>, else largest <p>-bearing div
    # (never the known related-articles/nav widgets)
    for tag_name in ("article", "main"):
        el = soup.find(tag_name)
        if el:
            paragraphs = [p.get_text(" ", strip=True) for p in el.find_all("p")]
            paragraphs = [p for p in paragraphs if p]
            text = "\n".join(paragraphs)
            if len(text) >= MIN_BODY_CHARS:
                return text, f"bs4_generic:{tag_name}"

    best_text, best_len = None, 0
    for div in soup.find_all("div"):
        if _is_noise_div(div):
            continue
        paragraphs = [p.get_text(" ", strip=True) for p in div.find_all("p", recursive=False)]
        paragraphs = [p for p in paragraphs if p]
        text = "\n".join(paragraphs)
        if len(text) > best_len:
            best_text, best_len = text, len(text)
    if best_text and best_len >= MIN_BODY_CHARS:
        return best_text, "bs4_generic:largest_div"

    return None, None


def clean_title(title):
    """Un-escape HTML entities and strip a trailing
    ' | South China Morning Post' suffix (defensive -- not seen on
    JSON-LD headline or og:title in practice, only on <title>)."""
    if not title:
        return ""
    title = html.unescape(title)
    title = TITLE_SUFFIX_RE.sub("", title)
    return title.strip()


def strip_leading_boilerplate(body_text, title, description):
    """Confirmed live: body_text can start with a literal "Advertisement"
    placeholder line, then the article's own headline repeated verbatim
    (the <article> element's <h1> gets extracted as if it were a body
    paragraph), then sometimes a "- <sentence>" line that's an exact
    match for the meta description (SCMP's "deck" summary bullet UI).
    Strips any of these found at the very start, repeatedly, since more
    than one can stack (e.g. Advertisement + headline repeat together)."""
    if not body_text:
        return body_text
    lines = body_text.split("\n")
    changed = True
    while changed:
        changed = False
        while lines and not lines[0].strip():
            lines.pop(0)
            changed = True
        if not lines:
            break
        first = lines[0].strip()
        if first.casefold() in LEADING_NOISE_LINES:
            lines.pop(0)
            changed = True
            continue
        if title and first.casefold() == title.strip().casefold():
            lines.pop(0)
            changed = True
            continue
        if description and first.startswith("-"):
            remainder = first.lstrip("-").strip()
            if remainder.casefold() == description.strip().casefold():
                lines.pop(0)
                changed = True
                continue
    return "\n".join(lines).lstrip("\n")


def extract_article(html_text, url):
    """
    Returns a dict with title, author, body_text,
    publication_date, updated_date, date_flag, extraction_method.
    """
    soup = BeautifulSoup(html_text, "html.parser")

    jsonld = extract_jsonld_metadata(soup)
    meta = extract_meta_tag_metadata(soup)

    # Remove known recirculation-widget/nav chrome BEFORE any body
    # extraction runs, so neither trafilatura nor the BS4 fallback can
    # mistake it for article content (see NOISE_CONTAINER_CLASSES above).
    strip_known_noise(soup)
    cleaned_html = str(soup)

    # trafilatura: primary body extractor + secondary metadata source
    traf_json = trafilatura.extract(
        cleaned_html, url=url, with_metadata=True, output_format="json",
        include_comments=False, favor_precision=True,
    )
    traf_data = json.loads(traf_json) if traf_json else {}
    traf_body = (traf_data.get("text") or "").strip()

    body_text, extraction_method = None, None
    if len(traf_body) >= MIN_BODY_CHARS:
        body_text, extraction_method = traf_body, "trafilatura"
    else:
        body_text, extraction_method = extract_body_bs4(soup)

    if not body_text:
        body_text, extraction_method = "", "failed"
    else:
        # Strip the confirmed-live headline-repeat / deck-bullet /
        # "Advertisement" noise that can lead body_text (see docstring).
        body_text = strip_leading_boilerplate(
            body_text,
            jsonld.get("title") or meta.get("title") or "",
            meta.get("description") or "",
        )

    # Field priority: confirmed live against real pages. JSON-LD headline
    # is the full, accurate title; og:title is a different, shorter,
    # reworded "social share" variant here (not just a suffixed dupe), so
    # it's only a fallback, not preferred.
    title = clean_title(jsonld.get("title") or meta.get("title") or traf_data.get("title") or "")
    # Author is reliable via JSON-LD/meta on this site -- including the
    # intentional "SCMP Reporter" no-byline-policy value, which is NOT
    # junk here (unlike the Korean sites' site-name leaks), so no
    # junk-filtering or byline-fallback needed.
    author = jsonld.get("author") or meta.get("author") or traf_data.get("author") or ""

    published_raw = jsonld.get("published_raw") or meta.get("published_raw") or ""
    updated_raw = jsonld.get("updated_raw") or meta.get("updated_raw") or ""

    publication_date = parse_iso_date(published_raw)
    updated_date = parse_iso_date(updated_raw)

    # dateModified equals datePublished exactly when an article was never
    # actually revised (confirmed live) -- don't record a false "update".
    if updated_date == publication_date:
        updated_date = None

    date_flag = ""
    if not publication_date:
        if traf_data.get("date"):
            # trafilatura's own heuristic date, used only as a last resort
            # and explicitly flagged as lower-confidence.
            fallback = parse_iso_date(traf_data["date"])
            if fallback:
                publication_date = fallback
                date_flag = "low_confidence_trafilatura_date"
        if not publication_date:
            date_flag = "unparsed_date"

    return {
        "title": title.strip(),
        "author": author.strip(),
        "body_text": body_text,
        "publication_date": publication_date.isoformat() if publication_date else "",
        "updated_date": updated_date.isoformat() if updated_date else "",
        "date_flag": date_flag,
        "extraction_method": extraction_method,
    }


# --------------------------------------------------------------------------
# Main scrape loop
# --------------------------------------------------------------------------
def scrape_one(session, article_id, url):
    """Returns (corpus_row_dict, failure_row_dict_or_None)."""
    now = datetime.now(timezone.utc).isoformat()

    if is_non_article_url(url):
        corpus_row = {
            "article_id": article_id, "url": url, "title": "", "publication_date": "",
            "updated_date": "", "date_flag": "", "author": "",
            "body_text": "", "extraction_method": "",
            "scrape_status": "failed", "scrape_timestamp": now,
        }
        failure_row = {
            "article_id": article_id, "url": url, "error_type": "non_article_url",
            "http_status_code": "", "error_message": "URL matched a non-article pattern; skipped",
        }
        return corpus_row, failure_row

    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
    except requests.exceptions.Timeout as e:
        return _failure(article_id, url, now, "timeout", "", str(e))
    except requests.exceptions.ConnectionError as e:
        return _failure(article_id, url, now, "connection_error", "", str(e))
    except requests.exceptions.RequestException as e:
        return _failure(article_id, url, now, "request_error", "", str(e))

    if resp.status_code != 200:
        return _failure(article_id, url, now, "http_error", resp.status_code,
                         f"Non-200 response: {resp.status_code}")

    try:
        with open(os.path.join(RAW_HTML_DIR, f"{article_id}.html"), "w", encoding="utf-8") as f:
            f.write(resp.text)
    except OSError as e:
        return _failure(article_id, url, now, "disk_write_error", resp.status_code, str(e))

    try:
        extracted = extract_article(resp.text, url)
    except Exception as e:  # noqa: BLE001 - never let one bad page crash the run
        return _failure(article_id, url, now, "parse_error", resp.status_code, str(e))

    if extracted["extraction_method"] == "failed":
        corpus_row = {
            "article_id": article_id, "url": url, "title": extracted["title"],
            "publication_date": extracted["publication_date"],
            "updated_date": extracted["updated_date"], "date_flag": extracted["date_flag"],
            "author": extracted["author"],
            "body_text": "", "extraction_method": "failed",
            "scrape_status": "failed", "scrape_timestamp": now,
        }
        failure_row = {
            "article_id": article_id, "url": url, "error_type": "extraction_failed",
            "http_status_code": resp.status_code,
            "error_message": "Fetched OK but no usable body text found (trafilatura + bs4 fallback both empty/short)",
        }
        return corpus_row, failure_row

    corpus_row = {
        "article_id": article_id, "url": url, "title": extracted["title"],
        "publication_date": extracted["publication_date"],
        "updated_date": extracted["updated_date"], "date_flag": extracted["date_flag"],
        "author": extracted["author"],
        "body_text": extracted["body_text"],
        "extraction_method": extracted["extraction_method"],
        "scrape_status": "success", "scrape_timestamp": now,
    }
    return corpus_row, None


def _failure(article_id, url, now, error_type, status_code, message):
    corpus_row = {
        "article_id": article_id, "url": url, "title": "", "publication_date": "",
        "updated_date": "", "date_flag": "", "author": "",
        "body_text": "", "extraction_method": "",
        "scrape_status": "failed", "scrape_timestamp": now,
    }
    failure_row = {
        "article_id": article_id, "url": url, "error_type": error_type,
        "http_status_code": status_code, "error_message": message,
    }
    return corpus_row, failure_row


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file '{INPUT_FILE}' not found in the working directory.")
        sys.exit(1)

    os.makedirs(RAW_HTML_DIR, exist_ok=True)
    ensure_csv_header(OUTPUT_CSV, CSV_FIELDS)
    ensure_csv_header(FAILURES_CSV, FAILURE_FIELDS)

    urls = load_and_dedup_urls(INPUT_FILE)
    already_done = load_already_done(OUTPUT_CSV)
    if already_done:
        print(f"Resuming: {len(already_done)} URLs already recorded in {OUTPUT_CSV}, will be skipped.")

    session = build_session()
    total = len(urls)
    n_success = 0
    n_failed = 0
    n_skipped_resume = 0

    for i, url in enumerate(urls, start=1):
        article_id = i  # stable, based on position in the deduped list

        if url in already_done:
            n_skipped_resume += 1
            continue

        try:
            corpus_row, failure_row = scrape_one(session, article_id, url)
        except Exception as e:  # noqa: BLE001 - absolute last-resort guard
            now = datetime.now(timezone.utc).isoformat()
            corpus_row, failure_row = _failure(
                article_id, url, now, "unexpected_error", "", str(e)
            )

        append_row(OUTPUT_CSV, CSV_FIELDS, corpus_row)
        if failure_row is not None:
            append_row(FAILURES_CSV, FAILURE_FIELDS, failure_row)

        if corpus_row["scrape_status"] == "success":
            n_success += 1
            status_label = "OK"
        else:
            n_failed += 1
            status_label = "FAIL"

        print(f"[{i}/{total}] {status_label:4s} success={n_success} failed={n_failed} :: {url}")

        # Be polite -- but no need to sleep after non-article URLs we never fetched.
        if not is_non_article_url(url):
            time.sleep(REQUEST_DELAY_SECONDS)

    print()
    print("=" * 70)
    print(f"Done. {total} unique URLs processed this run "
          f"({n_skipped_resume} skipped as already-done, "
          f"{n_success} succeeded, {n_failed} failed).")
    print(f"Corpus:   {OUTPUT_CSV}")
    print(f"Failures: {FAILURES_CSV}")
    print(f"Raw HTML: {RAW_HTML_DIR}/")


if __name__ == "__main__":
    main()
