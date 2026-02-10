#!/usr/bin/env python3
"""Update lightning/data/adoption.json from public adoption signal sources.

Sources:
- MoltX API (global feed + hashtags + search)  [requires MOLTX_API_KEY]
- Moltbook API (new posts pagination)          [requires MOLTBOOK_API_KEY]
- HotMolts (cached, read-only Moltbook mirror) [no auth]

Env:
- MOLTX_API_KEY (optional; if missing, MoltX is skipped)
- MOLTBOOK_API_KEY (optional; if missing, Moltbook is skipped)
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
import time
from typing import Dict, List, Optional, Tuple
from urllib.request import Request, urlopen
from concurrent.futures import ThreadPoolExecutor, as_completed

# Per-request limit. MoltX supports pagination via `offset`.
LIMIT = int(os.environ.get("LIMIT", "200"))
TOP_HASHTAGS = int(os.environ.get("TOP_HASHTAGS", "12"))

def _read_secret_file(path: str) -> Optional[str]:
    """Read a secret from a file without ever printing it.

    Supports both:
    - paths relative to current working directory
    - paths relative to this script's directory (so cron can run from anywhere)
    """
    candidates = [
        path,
        os.path.join(os.path.dirname(__file__), path),
        # When this repo is under workspace/projects/, secrets usually live at workspace/secrets/.
        os.path.join(os.path.dirname(__file__), "..", "..", path),
    ]

    for p in candidates:
        try:
            with open(p, "r", encoding="utf-8") as f:
                v = f.read().strip()
                if v:
                    return v
        except FileNotFoundError:
            continue

    return None

# Prefer explicit env vars, but allow local workspace secrets for cron/agent runs.
MOLTX_API_KEY = os.environ.get("MOLTX_API_KEY") or _read_secret_file("secrets/moltx_api_key.txt")
# Moltbook: prefer env, then the claimed-key file used elsewhere in this workspace.
MOLTBOOK_API_KEY = os.environ.get("MOLTBOOK_API_KEY") or _read_secret_file("secrets/moltbook_api_key_old.txt")

# Target number of unique MoltX posts to scan per run.
# NOTE: scaled up via env in GitHub Actions.
MOLTX_TARGET_UNIQUE_POSTS = int(os.environ.get("MOLTX_TARGET_UNIQUE_POSTS", os.environ.get("TARGET_UNIQUE_POSTS", "5000")))

# Target number of Moltbook posts to scan per run.
MOLTBOOK_TARGET_POSTS = int(os.environ.get("MOLTBOOK_TARGET_POSTS", "2000"))

# Target number of HotMolts posts to scan per run (via sitemap).
HOTMOLTS_TARGET_POSTS = int(os.environ.get("HOTMOLTS_TARGET_POSTS", "500"))
# Soft time budget (seconds) for HotMolts scanning so the hourly workflow finishes reliably.
HOTMOLTS_TIME_BUDGET_SEC = float(os.environ.get("HOTMOLTS_TIME_BUDGET_SEC", "25"))

BOLT11_RE = re.compile(r"\bln(?:bc|tb|bcrt)[0-9a-z]+\b", re.IGNORECASE)
LNURL_RE = re.compile(r"\blnurl[0-9a-z]+\b", re.IGNORECASE)
KW_LIGHTNING = re.compile(r"\blightning\b", re.IGNORECASE)
KW_PHOENIXD = re.compile(r"\bphoenixd\b|\bphoenix-cli\b", re.IGNORECASE)
KW_TIPJAR = re.compile(r"/\.well-known/lightning\.json", re.IGNORECASE)

# Simple “rail mention” rules for a lightweight comparison panel.
# Count at most once per post/page.
RAIL_RULES = [
    ("btc-lightning", "BTC on Lightning", re.compile(r"\b(lightning|bolt11|lnurl|lnbc|lntb|lnbcrt)\b", re.IGNORECASE)),
    ("usdc-ethereum-erc20", "USDC on Ethereum (ERC20)", re.compile(r"\busdc\b.*\b(ethereum|erc20)\b|\b(ethereum|erc20)\b.*\busdc\b", re.IGNORECASE | re.DOTALL)),
    ("usdt-tron-trc20", "USDT on Tron (TRC20)", re.compile(r"\busdt\b.*\b(tron|trc20)\b|\b(tron|trc20)\b.*\busdt\b", re.IGNORECASE | re.DOTALL)),
    ("usdc-solana-spl", "USDC on Solana (SPL)", re.compile(r"\busdc\b.*\b(solana|spl)\b|\b(solana|spl)\b.*\busdc\b", re.IGNORECASE | re.DOTALL)),
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def scan_rails_once(t: str, rail_counts: Dict[str, int]) -> None:
    """Increment per-rail mention counters (at most once per rail per text blob)."""
    for rail_id, _label, rx in RAIL_RULES:
        try:
            if rx.search(t):
                rail_counts[rail_id] = rail_counts.get(rail_id, 0) + 1
        except Exception:
            # Never let rail comparison break the core adoption scan.
            continue


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
        "User-Agent": "lightning-adoption-dashboard/0.5",
        "Accept": "application/json",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    req = Request(url, headers=headers)
    with urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get_moltx_posts(j: dict):
    return (j.get("data") or {}).get("posts") or []


def iter_paginated(url_base: str, *, api_key: str, per_page: int, max_items: int):
    """Iterate posts from a MoltX endpoint that supports limit+offset.

    Best-effort: retries and stops this endpoint if it stays unhealthy.
    """
    offset = 0
    emitted = 0
    while emitted < max_items:
        url = f"{url_base}&limit={per_page}&offset={offset}"

        j = None
        for attempt in range(5):
            try:
                j = fetch_json(url, api_key=api_key)
                break
            except Exception:
                time.sleep(0.6 * (attempt + 1))

        if j is None:
            break

        posts = get_moltx_posts(j)
        if not posts:
            break

        for p in posts:
            yield p
            emitted += 1
            if emitted >= max_items:
                break

        offset += per_page
        time.sleep(0.25)


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

    # For raw scale, use a small set of high-yield endpoints and paginate them.
    endpoints: List[Tuple[str, str]] = [
        ("global", "https://moltx.io/v1/feed/global?type=post,quote"),
        ("search:lightning", "https://moltx.io/v1/search/posts?q=lightning"),
        ("search:lnurl", "https://moltx.io/v1/search/posts?q=lnurl"),
        ("search:bolt11", "https://moltx.io/v1/search/posts?q=bolt11"),
        ("search:phoenixd", "https://moltx.io/v1/search/posts?q=phoenixd"),
        ("search:lightning.json", "https://moltx.io/v1/search/posts?q=lightning.json"),
    ]

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
    rail_counts: Dict[str, int] = {}

    for label, url in endpoints:
        try:
            if label == "global":
                posts = list(
                    iter_paginated(
                        url,
                        api_key=MOLTX_API_KEY,
                        per_page=limit,
                        max_items=MOLTX_TARGET_UNIQUE_POSTS,
                    )
                )
            else:
                j = fetch_json(url, api_key=MOLTX_API_KEY)
                posts = get_moltx_posts(j)
        except Exception:
            meta["errors"] += 1
            continue

        meta["endpoints_queried"] += 1

        unique = []
        for p in posts:
            pid = p.get("id")
            if not pid or pid in seen:
                continue
            seen.add(pid)
            unique.append(p)

            # Stop early once we hit our unique-post budget.
            if label == "global" and len(seen) >= MOLTX_TARGET_UNIQUE_POSTS:
                break

        meta["unique_posts"] += len(unique)

        for p in unique:
            pid = p.get("id")
            t = p.get("content") or ""
            if not t:
                continue

            counts["posts_scanned"] += 1
            scan_rails_once(t, rail_counts)
            if scan_text(t, counts) and pid:
                highlights.append(
                    {"url": f"https://moltx.io/post/{pid}", "reason": f"marker match in {label}"}
                )

    return (
        {
            "mode": "api_multi_endpoints",
            "meta": meta,
            "counts": counts,
            "rails": rail_counts,
            "highlights": highlights[:20],
        },
        None,
    )


# -----------------------
# Moltbook collector (API)
# -----------------------

def get_moltbook_posts(j: dict):
    # Moltbook returns {success:true, posts:[...]}
    return j.get("posts") or []


def _load_state() -> dict:
    try:
        with open("lightning/data/state.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"moltbook_offset": 0, "moltbook_last_run_at": None}


def _save_state(state: dict) -> None:
    os.makedirs("lightning/data", exist_ok=True)
    with open("lightning/data/state.json", "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)


def collect_moltbook(target_posts: int = None, per_page: int = 50) -> Tuple[dict, Optional[str]]:
    if target_posts is None:
        target_posts = MOLTBOOK_TARGET_POSTS

    if not MOLTBOOK_API_KEY:
        return (
            {
                "mode": "skipped",
                "meta": {"reason": "MOLTBOOK_API_KEY missing"},
                "counts": {
                    "posts_scanned": 0,
                    "lightning_mentions": 0,
                    "bolt11_mentions": 0,
                    "lnurl_mentions": 0,
                    "phoenixd_mentions": 0,
                    "tipjar_wellknown_mentions": 0,
                },
                "rails": {},
                "highlights": [],
            },
            None,
        )

    counts = {
        "posts_scanned": 0,
        "lightning_mentions": 0,
        "bolt11_mentions": 0,
        "lnurl_mentions": 0,
        "phoenixd_mentions": 0,
        "tipjar_wellknown_mentions": 0,
    }

    meta = {
        "endpoint": "https://www.moltbook.com/api/v1/posts",
        "per_page": per_page,
        "errors": 0,
        "unique_posts": 0,
    }

    highlights: List[dict] = []
    rail_counts: Dict[str, int] = {}
    seen = set()

    # Best-effort offset pagination with persistent cursor.
    state = _load_state()
    offset = int(state.get("moltbook_offset") or 0)
    while counts["posts_scanned"] < target_posts:
        url = f"https://www.moltbook.com/api/v1/posts?sort=new&limit={per_page}&offset={offset}"

        j = None
        for attempt in range(5):
            try:
                j = fetch_json(url, api_key=MOLTBOOK_API_KEY)
                break
            except Exception:
                time.sleep(0.8 * (attempt + 1))

        if j is None:
            meta["errors"] += 1
            break

        posts = get_moltbook_posts(j)
        if not posts:
            break

        for p in posts:
            pid = p.get("id") or p.get("url") or None
            if pid and pid in seen:
                continue
            if pid:
                seen.add(pid)

            title = p.get("title") or ""
            content = p.get("content") or ""
            t = f"{title}\n\n{content}".strip()
            if not t:
                continue

            counts["posts_scanned"] += 1
            scan_rails_once(t, rail_counts)
            if scan_text(t, counts):
                # Moltbook provides url sometimes; fall back to none.
                highlights.append({"url": p.get("url") or "https://www.moltbook.com/"})

            if counts["posts_scanned"] >= target_posts:
                break

        offset += per_page
        meta["unique_posts"] = len(seen)

        # Persist cursor every page so long runs can resume.
        state["moltbook_offset"] = offset
        state["moltbook_last_run_at"] = utc_now_iso()
        _save_state(state)

        time.sleep(0.2)

    # Final cursor write.
    state["moltbook_offset"] = offset
    state["moltbook_last_run_at"] = utc_now_iso()
    _save_state(state)

    return (
        {
            "mode": "api_paginated",
            "meta": meta,
            "counts": counts,
            "rails": rail_counts,
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
    req = Request(
        url,
        headers={
            "User-Agent": "lightning-adoption-dashboard/0.4",
            "Accept": "text/html,application/xml",
        },
    )
    with urlopen(req, timeout=12) as resp:
        return resp.read().decode("utf-8", "ignore")


def _extract_sitemap_locs(xml: str) -> List[str]:
    # Minimal XML parsing without dependencies.
    locs = re.findall(r"<loc>([^<]+)</loc>", xml)
    return [l.strip() for l in locs if l.strip()]


def collect_hotmolts() -> Tuple[dict, Optional[str]]:
    """Collect Moltbook signals via HotMolts cached mirror.

    Key change for scaling: use the HotMolts sitemap to get deep history quickly.
    We then fetch each post page (read-only) and scan the HTML for markers.
    """

    list_url = "https://hotmolts.com/?sort=new"
    sitemap_url = "https://hotmolts.com/sitemap.xml"

    counts = {
        "posts_scanned": 0,
        "lightning_mentions": 0,
        "bolt11_mentions": 0,
        "lnurl_mentions": 0,
        "phoenixd_mentions": 0,
        "tipjar_wellknown_mentions": 0,
    }

    meta = {
        "list_url": list_url,
        "sitemap_url": sitemap_url,
        "posts_found": 0,
        "errors": 0,
        "parse_mode": "sitemap+html",
        "error": None,
    }

    highlights: List[dict] = []
    rail_counts: Dict[str, int] = {}

    try:
        sm = fetch_html(sitemap_url)
        locs = _extract_sitemap_locs(sm)
        post_urls = [u for u in locs if "/post/" in u]

        meta["posts_found"] = len(post_urls)

        # Deterministic slice to keep runs stable.
        to_scan = post_urls[: max(0, HOTMOLTS_TARGET_POSTS)]

        started = time.monotonic()

        def work(u: str):
            try:
                return u, fetch_html(u), None
            except Exception as e:
                return u, None, e

        # Parallel fetch to scale to 1,000s quickly within the time budget.
        # Keep workers modest to avoid hammering HotMolts.
        max_workers = int(os.environ.get("HOTMOLTS_WORKERS", "16"))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(work, u) for u in to_scan]
            for fut in as_completed(futs):
                if (time.monotonic() - started) > HOTMOLTS_TIME_BUDGET_SEC:
                    break
                u, html, err = fut.result()
                if err is not None or not html:
                    meta["errors"] += 1
                    continue

                counts["posts_scanned"] += 1
                scan_rails_once(html, rail_counts)
                if scan_text(html, counts):
                    highlights.append({"url": u, "reason": "marker match"})

        return (
            {
                "mode": "sitemap_posts",
                "meta": meta,
                "counts": counts,
                "rails": rail_counts,
                "highlights": highlights[:20],
            },
            None,
        )
    except Exception as e:
        meta["error"] = f"{type(e).__name__}: {e}"
        return (
            {
                "mode": "failed_gracefully",
                "meta": meta,
                "counts": counts,
                "rails": rail_counts,
                "highlights": [],
            },
            meta["error"],
        )


def main():
    moltx, _ = collect_moltx(LIMIT)
    moltbook, _ = collect_moltbook()
    hotmolts, _ = collect_hotmolts()

    # Total per-rail counts (summed across sources).
    rails_total: Dict[str, int] = {}
    for src in (moltx, moltbook, hotmolts):
        for k, v in (src.get("rails") or {}).items():
            try:
                rails_total[k] = rails_total.get(k, 0) + int(v)
            except Exception:
                continue

    out = {
        "updated_at": utc_now_iso(),
        "rails_total": rails_total,
        "sources": {
            "moltx": moltx,
            "moltbook": moltbook,
            "hotmolts": hotmolts,
        },
    }

    os.makedirs("lightning/data", exist_ok=True)
    with open("lightning/data/adoption.json", "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    main()
