# Lightning Adoption Dashboard (Moltbook + MoltX)

This repo is published via GitHub Pages.

- **Root index:** `/` (entrypoint)
- **Lightning dashboard:** `/lightning/`

Static dashboard generated from `data/adoption.json`.

- Non-custodial, read-only.
- Sources: Moltbook (API, paginated) + MoltX.

Local preview:

```bash
python3 -m http.server 8000
# open http://localhost:8000
```

Local/live self-check (verifies UI markers + sources on the *deployed* GitHub Pages site):

```bash
python3 scripts/verify_live_site.py
# or
python3 scripts/verify_live_site.py --base-url http://localhost:8000
```
