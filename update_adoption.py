#!/usr/bin/env python3
"""Update data/adoption.json from the MoltX API (global feed).

Requires env:
- MOLTX_API_KEY (Bearer token)
Optional:
- LIMIT (default 120)

Writes: data/adoption.json
"""

import json
import os
import re
from datetime import datetime, timezone
from urllib.request import Request, urlopen

LIMIT = int(os.environ.get("LIMIT", "100"))
API_KEY = os.environ.get("MOLTX_API_KEY")

# How many endpoints to query per run.
# MoltX appears to cap feed results at 100 items per request.
TOP_HASHTAGS = int(os.environ.get("TOP_HASHTAGS", "12"))

if not API_KEY:
    raise SystemExit("MOLTX_API_KEY is required")

BOLT11_RE = re.compile(r"\bln(?:bc|tb|bcrt)[0-9a-z]+\b", re.IGNORECASE)
LNURL_RE = re.compile(r"\blnurl[0-9a-z]+\b", re.IGNORECASE)
KW_LIGHTNING = re.compile(r"\blightning\b", re.IGNORECASE)
KW_PHOENIXD = re.compile(r"\bphoenixd\b|\bphoenix-cli\b", re.IGNORECASE)
KW_TIPJAR = re.compile(r"/\.well-known/lightning\.json", re.IGNORECASE)


def fetch_json(url: str) -> dict:
    req = Request(
        url,
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "User-Agent": "lightning-adoption-dashboard/0.2",
            "Accept": "application/json",
        },
    )
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get_posts(j: dict):
    return (j.get("data") or {}).get("posts") or []


def scan_posts(posts, counts, highlights, reason: str):
    for p in posts:
        pid = p.get("id")
        t = p.get("content") or ""
        if not t:
            continue

        counts["posts_scanned"] += 1

        hit = False
        if KW_LIGHTNING.search(t):
            counts["lightning_mentions"] += 1
        if BOLT11_RE.search(t):
            counts["bolt11_mentions"] += 1
            hit = True
        if LNURL_RE.search(t):
            counts["lnurl_mentions"] += 1
        if KW_PHOENIXD.search(t):
            counts["phoenixd_mentions"] += 1
            hit = True
        if KW_TIPJAR.search(t):
            counts["tipjar_wellknown_mentions"] += 1
            hit = True

        if hit and pid:
            highlights.append({"url": f"https://moltx.io/post/{pid}", "reason": reason})


def main():
    # Collect from multiple endpoints to increase coverage.
    # MoltX feed endpoints appear to cap at 100 results per request.
    endpoints = []

    endpoints.append(
        ("global", f"https://moltx.io/v1/feed/global?type=post,quote&limit={LIMIT}")
    )

    # Targeted hashtags
    for tag in [
        "lightning",
        "bitcoin",
        "lnurl",
        "phoenixd",
        "tips",
        "tipjar",
    ]:
        endpoints.append(
            (f"hashtag:{tag}", f"https://moltx.io/v1/feed/global?hashtag={tag}&type=post,quote&limit={LIMIT}")
        )

    # Trending hashtags: take the top N and scan them too.
    try:
        trending = fetch_json(f"https://moltx.io/v1/hashtags/trending?limit={TOP_HASHTAGS}")
        tags = (trending.get("data") or {}).get("hashtags") or []
        for t in tags[:TOP_HASHTAGS]:
            name = t.get("name")
            if not name:
                continue
            endpoints.append(
                (
                    f"trending:{name}",
                    f"https://moltx.io/v1/feed/global?hashtag={name}&type=post,quote&limit={LIMIT}",
                )
            )
    except Exception:
        # Best-effort: trending fetch may fail; still proceed.
        pass

    # Search queries: small but high-signal.
    for q in ["lightning", "bolt11", "lnbc", "lnurl", "phoenixd", "phoenix-cli", "lightning.json"]:
        endpoints.append((f"search:{q}", f"https://moltx.io/v1/search/posts?q={q}&limit={LIMIT}"))

    # De-duplicate by post id across endpoints.
    seen = set()

    counts = {
        "posts_scanned": 0,
        "lightning_mentions": 0,
        "bolt11_mentions": 0,
        "lnurl_mentions": 0,
        "phoenixd_mentions": 0,
        "tipjar_wellknown_mentions": 0,
    }

    meta = {
        "endpoints_queried": 0,
        "unique_posts": 0,
    }

    highlights = []

    for label, url in endpoints:
        try:
            j = fetch_json(url)
        except Exception:
            continue

        meta["endpoints_queried"] += 1
        posts = get_posts(j)

        unique = []
        for p in posts:
            pid = p.get("id")
            if not pid or pid in seen:
                continue
            seen.add(pid)
            unique.append(p)

        meta["unique_posts"] += len(unique)
        scan_posts(unique, counts, highlights, reason=f"marker match in {label}")

    out = {
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "sources": {
            "moltx": {
                "mode": "api_multi_endpoints",
                "meta": meta,
                "counts": counts,
                "highlights": highlights[:20],
            }
        },
    }

    os.makedirs("data", exist_ok=True)
    with open("data/adoption.json", "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    main()
