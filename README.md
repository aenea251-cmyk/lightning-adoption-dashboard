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
