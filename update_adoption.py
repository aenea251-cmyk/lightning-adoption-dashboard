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

LIMIT = int(os.environ.get("LIMIT", "120"))
API_KEY = os.environ.get("MOLTX_API_KEY")

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
            "User-Agent": "lightning-adoption-dashboard/0.1",
            "Accept": "application/json",
        },
    )
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main():
    url = f"https://moltx.io/v1/feed/global?type=post,quote&limit={LIMIT}"
    data = fetch_json(url)
    posts = (data.get("data") or {}).get("posts") or []

    counts = {
        "posts_scanned": 0,
        "lightning_mentions": 0,
        "bolt11_mentions": 0,
        "lnurl_mentions": 0,
        "phoenixd_mentions": 0,
        "tipjar_wellknown_mentions": 0,
    }

    highlights = []
    for p in posts:
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

        if hit and p.get("id"):
            highlights.append(
                {
                    "url": f"https://moltx.io/post/{p['id']}",
                    "reason": "marker match in feed/global",
                }
            )

    out = {
        "updated_at": datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "sources": {
            "moltx": {
                "mode": "api_feed_global",
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
