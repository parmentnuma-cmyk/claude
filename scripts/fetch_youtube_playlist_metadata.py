"""
Extracts metadata for every video in a YouTube playlist via the official
YouTube Data API v3, into a structured CSV.

INPUT:  a playlist ID (from the playlist URL's ?list=... parameter) and
        an API key (read from the YOUTUBE_API_KEY environment variable --
        see the Colab cell that sets this via getpass, so the key is
        never written into this script file or saved to disk in plain
        text).
OUTPUT: youtube_playlist_metadata.csv

Two-step API flow:
    1. playlistItems.list (paginated, 50 per page) -- enumerates every
       video ID in the playlist, in playlist order. This also flags
       entries whose title is literally "Private video" or "Deleted
       video" (YouTube's own placeholder for videos the API can't
       return full metadata for), which is checked directly rather than
       guessed at, since the playlist item itself still carries a status.
    2. videos.list (batched, up to 50 IDs per call) -- fetches the real
       metadata for each of those IDs: title, description, publish date,
       channel, duration, view/like/comment counts, tags.

Quota cost: 1 unit per playlistItems.list call + 1 unit per videos.list
call, regardless of how many items/IDs are in that call. ~19 pages of
playlist items + ~19 batches of video details for 902 videos is ~38
units total -- trivial against the default 10,000/day quota.
"""

import csv
import os
import re
import sys
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_BASE = "https://www.googleapis.com/youtube/v3"
OUTPUT_CSV = "youtube_playlist_metadata.csv"
PAGE_SIZE = 50  # max allowed by the API for both endpoints used here
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3

CSV_FIELDS = [
    "playlist_position", "video_id", "url", "title", "description",
    "published_at", "channel_title", "channel_id", "duration_seconds",
    "duration_iso8601", "view_count", "like_count", "comment_count",
    "tags", "availability_status",
]

ISO8601_DURATION_RE = re.compile(
    r"^PT(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?$"
)


def build_session():
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES, connect=MAX_RETRIES, read=MAX_RETRIES, status=MAX_RETRIES,
        backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]), raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


def parse_iso8601_duration(raw):
    """Converts YouTube's ISO 8601 duration ("PT4M13S") to whole seconds."""
    if not raw:
        return None
    m = ISO8601_DURATION_RE.match(raw)
    if not m:
        return None
    hours = int(m.group("hours") or 0)
    minutes = int(m.group("minutes") or 0)
    seconds = int(m.group("seconds") or 0)
    return hours * 3600 + minutes * 60 + seconds


def get_playlist_id_from_url_or_id(value):
    """Accepts either a bare playlist ID or a full playlist URL."""
    m = re.search(r"[?&]list=([\w-]+)", value)
    return m.group(1) if m else value


def api_get(session, api_key, endpoint, params):
    params = dict(params)
    params["key"] = api_key
    resp = session.get(f"{API_BASE}/{endpoint}", params=params, timeout=REQUEST_TIMEOUT)
    if resp.status_code != 200:
        raise RuntimeError(
            f"YouTube API returned {resp.status_code} for {endpoint}: {resp.text[:500]}"
        )
    return resp.json()


def fetch_playlist_items(session, api_key, playlist_id):
    """Returns a list of dicts: {position, video_id, title_hint} in
    playlist order. title_hint lets us flag "Private video"/"Deleted
    video" placeholders directly from the API's own response, rather
    than guessing from a later missing videos.list result."""
    items = []
    page_token = None
    page_num = 0

    while True:
        page_num += 1
        params = {
            "part": "snippet,contentDetails",
            "playlistId": playlist_id,
            "maxResults": PAGE_SIZE,
        }
        if page_token:
            params["pageToken"] = page_token

        data = api_get(session, api_key, "playlistItems", params)

        for entry in data.get("items", []):
            snippet = entry.get("snippet", {})
            content_details = entry.get("contentDetails", {})
            items.append({
                "position": snippet.get("position"),
                "video_id": content_details.get("videoId"),
                "title_hint": snippet.get("title", ""),
            })

        total = data.get("pageInfo", {}).get("totalResults", "?")
        print(f"  playlistItems page {page_num}: {len(data.get('items', []))} items "
              f"(running total {len(items)}/{total})")

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return items


def fetch_video_details(session, api_key, video_ids):
    """Batches video IDs 50 at a time and returns {video_id: details_dict}."""
    details = {}
    for i in range(0, len(video_ids), PAGE_SIZE):
        batch = video_ids[i:i + PAGE_SIZE]
        params = {
            "part": "snippet,contentDetails,statistics",
            "id": ",".join(batch),
        }
        data = api_get(session, api_key, "videos", params)
        for item in data.get("items", []):
            details[item["id"]] = item
        print(f"  videos.list batch {i // PAGE_SIZE + 1}: "
              f"{len(data.get('items', []))}/{len(batch)} IDs returned details")
    return details


def build_rows(playlist_items, video_details):
    rows = []
    for item in playlist_items:
        video_id = item["video_id"]
        title_hint = item["title_hint"]

        if title_hint in ("Private video", "Deleted video"):
            rows.append({
                "playlist_position": item["position"], "video_id": video_id or "",
                "url": f"https://www.youtube.com/watch?v={video_id}" if video_id else "",
                "title": "", "description": "", "published_at": "",
                "channel_title": "", "channel_id": "", "duration_seconds": "",
                "duration_iso8601": "", "view_count": "", "like_count": "",
                "comment_count": "", "tags": "",
                "availability_status": title_hint.lower().replace(" ", "_"),
            })
            continue

        detail = video_details.get(video_id)
        if detail is None:
            # In the playlist but videos.list returned nothing for it --
            # e.g. region-restricted or removed after the playlist page
            # was fetched. Recorded rather than silently dropped.
            rows.append({
                "playlist_position": item["position"], "video_id": video_id or "",
                "url": f"https://www.youtube.com/watch?v={video_id}" if video_id else "",
                "title": "", "description": "", "published_at": "",
                "channel_title": "", "channel_id": "", "duration_seconds": "",
                "duration_iso8601": "", "view_count": "", "like_count": "",
                "comment_count": "", "tags": "",
                "availability_status": "unavailable",
            })
            continue

        snippet = detail.get("snippet", {})
        content_details = detail.get("contentDetails", {})
        statistics = detail.get("statistics", {})
        duration_iso = content_details.get("duration", "")

        rows.append({
            "playlist_position": item["position"],
            "video_id": video_id,
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "title": snippet.get("title", ""),
            "description": snippet.get("description", ""),
            "published_at": snippet.get("publishedAt", ""),
            "channel_title": snippet.get("channelTitle", ""),
            "channel_id": snippet.get("channelId", ""),
            "duration_seconds": parse_iso8601_duration(duration_iso),
            "duration_iso8601": duration_iso,
            # view/like/comment counts absent from statistics entirely
            # when the uploader has disabled that count -- left blank
            # rather than guessed as 0, which would be a false claim of
            # "zero views" instead of "count not public".
            "view_count": statistics.get("viewCount", ""),
            "like_count": statistics.get("likeCount", ""),
            "comment_count": statistics.get("commentCount", ""),
            "tags": "; ".join(snippet.get("tags", [])),
            "availability_status": "available",
        })

    return rows


def main():
    if len(sys.argv) < 2:
        print("Usage: python fetch_youtube_playlist_metadata.py <playlist_url_or_id>")
        sys.exit(1)

    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        print("YOUTUBE_API_KEY environment variable not set. "
              "Run the getpass cell in Colab first.")
        sys.exit(1)

    playlist_id = get_playlist_id_from_url_or_id(sys.argv[1])
    print(f"Playlist ID: {playlist_id}")

    session = build_session()

    print("Fetching playlist items...")
    playlist_items = fetch_playlist_items(session, api_key, playlist_id)
    print(f"Total playlist items: {len(playlist_items)}")

    video_ids = [it["video_id"] for it in playlist_items
                 if it["video_id"] and it["title_hint"] not in ("Private video", "Deleted video")]

    print(f"Fetching full details for {len(video_ids)} videos...")
    video_details = fetch_video_details(session, api_key, video_ids)

    rows = build_rows(playlist_items, video_details)

    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    n_available = sum(1 for r in rows if r["availability_status"] == "available")
    n_unavailable = len(rows) - n_available
    print()
    print("=" * 70)
    print(f"Done. {len(rows)} playlist entries written to {OUTPUT_CSV}")
    print(f"  available:   {n_available}")
    print(f"  unavailable: {n_unavailable} "
          f"(private/deleted/otherwise inaccessible -- see availability_status column)")


if __name__ == "__main__":
    main()
