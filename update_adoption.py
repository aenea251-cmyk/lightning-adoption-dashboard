#!/usr/bin/env python3
"""Update lightning/data/adoption.json from public adoption signal sources.

Sources:
- MoltX API (global feed + hashtags + search)  [requires MOLTX_API_KEY]
- HotMolts (cached, read-only Moltbook mirror) [no auth]

Env:
- MOLTX_API_KEY (optional; if missing, MoltX is skipped)
- LIMIT (default 100)
- TOP_HASHTAGS (default 12)

Writes: lightning/data/adoption.json

Notes:
- Best-effort collectors: failures should not crash the whole run.
- Keep read-only + no deanonymization/cross-linking.
"""

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Dict, List, Optional, Tuple
from urllib.request import Request, urlopen

LIMIT = int(os.environ.get("LIMIT", "100"))
TOP_HASHTAGS = int(os.environ.get("TOP_HASHTAGS", "12"))
MOLTX_API_KEY = os.environ.get("MOLTX_API_KEY")

BOLT11_RE = re.compile(r"\bln(?:bc|tb|bcrt)[0-9a-z]+\b", re.IGNORECASE)
LNURL_RE = re.compile(r"\blnurl[0-9a-z]+\b", re.IGNORECASE)
KW_LIGHTNING = re.compile(r"\blightning\b", re.IGNORECASE)
KW_PHOENIXD = re.compile(r"\bphoenixd\b|\bphoenix-cli\b", re.IGNORECASE)
KW_TIPJAR = re.compile(r"/\.well-known/lightning\.json", re.IGNORECASE)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def scan_text(t: str, counts: Dict[str, int]) -> bool:
    """Scan a single text blob; update counts; return whether it is a highlight."""
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

    return hit


# -----------------------
# MoltX collector
# -----------------------

def fetch_json(url: str, *, api_key: Optional[str] = None) -> dict:
    headers = {
        "User-Agent": "lightning-adoption-dashboard/0.3",
        "Accept": "application/json",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    req = Request(url, headers=headers)
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get_moltx_posts(j: dict):
    return (j.get("data") or {}).get("posts") or []


def collect_moltx(limit: int) -> Tuple[dict, Optional[str]]:
    if not MOLTX_API_KEY:
        return (
            {
                "mode": "skipped",
                "meta": {"reason": "MOLTX_API_KEY missing"},
                "counts": {
                    "posts_scanned": 0,
                    "lightning_mentions": 0,
                    "bolt11_mentions": 0,
                    "lnurl_mentions": 0,
                    "phoenixd_mentions": 0,
                    "tipjar_wellknown_mentions": 0,
                },
                "highlights": [],
            },
            None,
        )

    # Collect from multiple endpoints to increase coverage.
    endpoints: List[Tuple[str, str]] = []

    endpoints.append(("global", f"https://moltx.io/v1/feed/global?type=post,quote&limit={limit}"))

    for tag in ["lightning", "bitcoin", "lnurl", "phoenixd", "tips", "tipjar"]:
        endpoints.append(
            (
                f"hashtag:{tag}",
                f"https://moltx.io/v1/feed/global?hashtag={tag}&type=post,quote&limit={limit}",
            )
        )

    try:
        trending = fetch_json(
            f"https://moltx.io/v1/hashtags/trending?limit={TOP_HASHTAGS}", api_key=MOLTX_API_KEY
        )
        tags = (trending.get("data") or {}).get("hashtags") or []
        for t in tags[:TOP_HASHTAGS]:
            name = t.get("name")
            if not name:
                continue
            endpoints.append(
                (
                    f"trending:{name}",
                    f"https://moltx.io/v1/feed/global?hashtag={name}&type=post,quote&limit={limit}",
                )
            )
    except Exception:
        # Best-effort: trending fetch may fail; still proceed.
        pass

    for q in [
        "lightning",
        "bolt11",
        "lnbc",
        "lnurl",
        "phoenixd",
        "phoenix-cli",
        "lightning.json",
    ]:
        endpoints.append((f"search:{q}", f"https://moltx.io/v1/search/posts?q={q}&limit={limit}"))

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
        "errors": 0,
    }

    highlights: List[dict] = []

    for label, url in endpoints:
        try:
            j = fetch_json(url, api_key=MOLTX_API_KEY)
        except Exception:
            meta["errors"] += 1
            continue

        meta["endpoints_queried"] += 1
        posts = get_moltx_posts(j)

        unique = []
        for p in posts:
            pid = p.get("id")
            if not pid or pid in seen:
                continue
            seen.add(pid)
            unique.append(p)

        meta["unique_posts"] += len(unique)

        for p in unique:
            pid = p.get("id")
            t = p.get("content") or ""
            if not t:
                continue

            counts["posts_scanned"] += 1
            if scan_text(t, counts) and pid:
                highlights.append(
                    {"url": f"https://moltx.io/post/{pid}", "reason": f"marker match in {label}"}
                )

    return (
        {
            "mode": "api_multi_endpoints",
            "meta": meta,
            "counts": counts,
            "highlights": highlights[:20],
        },
        None,
    )


# -----------------------
# HotMolts collector
# -----------------------

@dataclass
class HotMoltsPost:
    url: str
    title: str
    content: str


class HotMoltsParser(HTMLParser):
    """Very small, robust-ish parser for the HotMolts list page.

    We look for <a href="/post/...\"><h2>Title</h2></a> and the following <p> snippet.
    This is intentionally forgiving: if HotMolts changes markup, we return fewer posts,
    but still produce valid JSON.
    """

    def __init__(self):
        super().__init__()
        self.posts: List[HotMoltsPost] = []
        self._current_href: Optional[str] = None
        self._in_h2 = False
        self._in_p = False
        self._buf: List[str] = []
        self._title: Optional[str] = None
        self._content: Optional[str] = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "a":
            href = attrs.get("href")
            if href and href.startswith("/post/"):
                self._current_href = href
                self._title = None
                self._content = None
        elif tag == "h2" and self._current_href:
            self._in_h2 = True
            self._buf = []
        elif tag == "p" and self._current_href and self._title is not None and self._content is None:
            # First <p> after title is the snippet.
            self._in_p = True
            self._buf = []

    def handle_endtag(self, tag):
        if tag == "h2" and self._in_h2:
            self._in_h2 = False
            self._title = "".join(self._buf).strip()
            self._buf = []
        elif tag == "p" and self._in_p:
            self._in_p = False
            self._content = "".join(self._buf).strip()
            self._buf = []

            # If we have href+title+content, emit a post.
            if self._current_href and self._title:
                url = "https://www.hotmolts.com" + self._current_href
                self.posts.append(
                    HotMoltsPost(url=url, title=self._title or "", content=self._content or "")
                )

                # Reset; the page repeats this pattern per post.
                self._current_href = None
                self._title = None
                self._content = None

    def handle_data(self, data):
        if self._in_h2 or self._in_p:
            self._buf.append(data)


def fetch_html(url: str) -> str:
    req = Request(url, headers={"User-Agent": "lightning-adoption-dashboard/0.3", "Accept": "text/html"})
    with urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", "ignore")


def collect_hotmolts() -> Tuple[dict, Optional[str]]:
    url = "https://www.hotmolts.com/?sort=new"

    counts = {
        "pages_scanned": 0,
        "lightning_mentions": 0,
        "bolt11_mentions": 0,
        "lnurl_mentions": 0,
        "phoenixd_mentions": 0,
        "tipjar_wellknown_mentions": 0,
    }

    meta = {
        "url": url,
        "posts_found": 0,
        "parse_mode": "html_best_effort",
        "error": None,
    }

    highlights: List[dict] = []

    try:
        html = fetch_html(url)
        counts["pages_scanned"] += 1

        parser = HotMoltsParser()
        parser.feed(html)
        posts = parser.posts

        # De-dupe by URL.
        seen = set()
        unique_posts: List[HotMoltsPost] = []
        for p in posts:
            if p.url in seen:
                continue
            seen.add(p.url)
            unique_posts.append(p)

        meta["posts_found"] = len(unique_posts)

        for p in unique_posts:
            t = f"{p.title}\n\n{p.content}".strip()
            if not t:
                continue
            if scan_text(t, counts):
                highlights.append({"url": p.url, "reason": "marker match"})

        return (
            {
                "mode": "cached_html_list",
                "meta": meta,
                "counts": counts,
                "highlights": highlights[:20],
            },
            None,
        )
    except Exception as e:
        meta["error"] = f"{type(e).__name__}: {e}"
        # Fail gracefully: return empty counts but valid structure.
        return (
            {
                "mode": "failed_gracefully",
                "meta": meta,
                "counts": counts,
                "highlights": [],
            },
            meta["error"],
        )


def main():
    moltx, _ = collect_moltx(LIMIT)
    hotmolts, _ = collect_hotmolts()

    out = {
        "updated_at": utc_now_iso(),
        "sources": {
            "moltx": moltx,
            "hotmolts": hotmolts,
        },
    }

    os.makedirs("lightning/data", exist_ok=True)
    with open("lightning/data/adoption.json", "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    main()
