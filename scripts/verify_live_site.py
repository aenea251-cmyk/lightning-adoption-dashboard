#!/usr/bin/env python3
"""Verify that the *live* GitHub Pages site still exposes key UI markers.

Usage:
  python3 scripts/verify_live_site.py \
    --base-url https://aenea251-cmyk.github.io/lightning-adoption-dashboard

If --base-url is omitted, it defaults to the canonical GH Pages URL.

This is intentionally lightweight (urllib only) so it runs in CI without extra deps.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.error
import urllib.request


DEFAULT_BASE_URL = "https://aenea251-cmyk.github.io/lightning-adoption-dashboard"


def fetch(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "lightning-adoption-dashboard-ci-verifier/1.0",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read()
    return body.decode("utf-8", errors="replace")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL)
    args = ap.parse_args()

    base = args.base_url.rstrip("/")
    lightning_html_url = f"{base}/lightning/"
    app_js_url = f"{base}/lightning/app.js"
    adoption_url = f"{base}/lightning/data/adoption.json"

    try:
        html = fetch(lightning_html_url)
        js = fetch(app_js_url)
        adoption_raw = fetch(adoption_url)
    except urllib.error.URLError as e:
        print(f"ERROR: failed to fetch live site: {e}", file=sys.stderr)
        return 2

    # 1) Sanity: we are looking at the correct page.
    if "Lightning Adoption" not in html:
        print("ERROR: lightning/ HTML missing 'Lightning Adoption' marker", file=sys.stderr)
        return 3

    # 2) UI marker: total summary (must be visible in JS-rendered status).
    if "TOTAL scanned" not in js:
        print("ERROR: app.js missing required 'TOTAL scanned' UI string", file=sys.stderr)
        return 4

    # 3) Sources: the UI must mention Moltbook and MoltX (labels are in JS).
    if "Moltbook" not in js or "MoltX" not in js:
        print("ERROR: app.js missing Moltbook and/or MoltX labels", file=sys.stderr)
        return 5

    # 4) Data: adoption.json must expose the sources keys.
    try:
        adoption = json.loads(adoption_raw)
    except json.JSONDecodeError as e:
        print(f"ERROR: adoption.json is not valid JSON: {e}", file=sys.stderr)
        return 6

    sources = adoption.get("sources") or {}
    if "moltx" not in sources:
        print("ERROR: adoption.json missing sources.moltx", file=sys.stderr)
        return 7
    if "moltbook" not in sources:
        print("ERROR: adoption.json missing sources.moltbook", file=sys.stderr)
        return 8

    # 5) Ensure total marker id still exists in code (stable DOM handle).
    if not re.search(r"id:\s*'totalCounts'", js):
        print("ERROR: app.js missing id 'totalCounts' (CI relies on it)", file=sys.stderr)
        return 9

    print("OK: live site contains TOTAL scanned marker and Moltbook+MoltX sources")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
