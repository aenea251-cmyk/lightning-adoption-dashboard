async function main() {
  const statusEl = document.getElementById('status');
  const kpisEl = document.getElementById('kpis');
  const highlightsEl = document.getElementById('highlights');

  let data;
  try {
    const res = await fetch('./data/adoption.json', { cache: 'no-store' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    data = await res.json();
  } catch (e) {
    statusEl.textContent = `Failed to load data/adoption.json (${e}).`;
    return;
  }

  const updated = data.updated_at || 'unknown';
  const src = data.sources && data.sources.moltx;
  const counts = (src && src.counts) || {};
  const mode = (src && src.mode) || 'unknown';

  statusEl.innerHTML = `<div><b>Updated:</b> <code>${updated}</code></div>`
    + `<div class="muted">Source: MoltX (${mode})</div>`;

  const scanned = (counts.posts_scanned ?? counts.pages_scanned ?? 0);
  const scannedLabel = (counts.posts_scanned != null) ? 'Posts scanned' : 'Pages scanned';

  const rows = [
    [scannedLabel, scanned],
    ['Lightning mentions', counts.lightning_mentions ?? 0],
    ['BOLT11 mentions', counts.bolt11_mentions ?? 0],
    ['LNURL mentions', counts.lnurl_mentions ?? 0],
    ['phoenixd mentions', counts.phoenixd_mentions ?? 0],
    ['TipJar well-known mentions', counts.tipjar_wellknown_mentions ?? 0],
  ];

  kpisEl.innerHTML = '';
  for (const [label, value] of rows) {
    const d = document.createElement('div');
    d.className = 'card';
    d.innerHTML = `<div class="kpi">${value}</div><div class="muted">${label}</div>`;
    kpisEl.appendChild(d);
  }

  const highlights = (src && src.highlights) || [];
  highlightsEl.innerHTML = '';
  if (!highlights.length) {
    const li = document.createElement('li');
    li.className = 'muted';
    li.textContent = 'No highlights yet.';
    highlightsEl.appendChild(li);
  } else {
    for (const h of highlights) {
      const li = document.createElement('li');
      const a = document.createElement('a');
      a.href = h.url;
      a.textContent = h.url;
      a.rel = 'noreferrer noopener';
      a.target = '_blank';
      li.appendChild(a);
      if (h.reason) {
        const s = document.createElement('span');
        s.className = 'muted';
        s.textContent = ` — ${h.reason}`;
        li.appendChild(s);
      }
      highlightsEl.appendChild(li);
    }
  }
}

main();
