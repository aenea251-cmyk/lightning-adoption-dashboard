async function main() {
  const statusEl = document.getElementById('status');
  const kpisEl = document.getElementById('kpis');
  const highlightsEl = document.getElementById('highlights');
  const paymentRailsEl = document.getElementById('paymentRails');

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
  const meta = (src && src.meta) || {};

  const metaBits = [];
  if (meta.endpoints_queried != null) metaBits.push(`endpoints: <code>${meta.endpoints_queried}</code>`);
  if (meta.unique_posts != null) metaBits.push(`unique posts: <code>${meta.unique_posts}</code>`);

  statusEl.innerHTML = `<div><b>Updated:</b> <code>${updated}</code></div>`
    + `<div class="muted">Source: MoltX (${mode})${metaBits.length ? ' · ' + metaBits.join(' · ') : ''}</div>`;

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

  // Curated payment rails list (static JSON).
  if (paymentRailsEl) {
    try {
      const res = await fetch('./data/payment_rails.json', { cache: 'no-store' });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const pr = await res.json();
      const rails = pr.rails || [];

      paymentRailsEl.innerHTML = '';
      if (!rails.length) {
        const li = document.createElement('li');
        li.className = 'muted';
        li.textContent = 'No rails tracked yet.';
        paymentRailsEl.appendChild(li);
      } else {
        for (const r of rails) {
          const li = document.createElement('li');
          const standards = (r.standards && r.standards.length) ? ` (${r.standards.join(', ')})` : '';
          const unit = r.unit ? ` — ${r.unit}` : '';
          li.innerHTML = `<b>${r.currency}</b> on <b>${r.technology}</b>${standards}${unit}`;
          if (r.why) {
            const s = document.createElement('span');
            s.className = 'muted';
            s.textContent = ` — ${r.why}`;
            li.appendChild(s);
          }
          paymentRailsEl.appendChild(li);
        }
      }
    } catch (e) {
      paymentRailsEl.innerHTML = '';
      const li = document.createElement('li');
      li.className = 'muted';
      li.textContent = `Failed to load data/payment_rails.json (${e}).`;
      paymentRailsEl.appendChild(li);
    }
  }
}

main();
