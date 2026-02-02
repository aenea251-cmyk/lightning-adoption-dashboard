function el(tag, attrs = {}, children = []) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === 'class') e.className = v;
    else if (k === 'html') e.innerHTML = v;
    else e.setAttribute(k, v);
  }
  for (const c of children) e.appendChild(c);
  return e;
}

function normCounts(counts = {}) {
  return {
    scanned: counts.posts_scanned ?? counts.pages_scanned ?? 0,
    scannedLabel: (counts.posts_scanned != null) ? 'Posts scanned' : 'Pages scanned',
    lightning: counts.lightning_mentions ?? 0,
    bolt11: counts.bolt11_mentions ?? 0,
    lnurl: counts.lnurl_mentions ?? 0,
    phoenixd: counts.phoenixd_mentions ?? 0,
    tipjar: counts.tipjar_wellknown_mentions ?? 0,
  };
}

function sourceLabel(key) {
  if (key === 'moltx') return 'MoltX';
  if (key === 'moltbook') return 'Moltbook (API)';
  if (key === 'hotmolts') return 'HotMolts (cached Moltbook)';
  return key;
}

function renderKpis(kpisEl, counts) {
  const c = normCounts(counts);
  const rows = [
    [c.scannedLabel, c.scanned],
    ['Lightning mentions', c.lightning],
    ['BOLT11 mentions', c.bolt11],
    ['LNURL mentions', c.lnurl],
    ['phoenixd mentions', c.phoenixd],
    ['TipJar well-known mentions', c.tipjar],
  ];

  kpisEl.innerHTML = '';
  for (const [label, value] of rows) {
    const d = document.createElement('div');
    d.className = 'card';
    d.innerHTML = `<div class="kpi">${value}</div><div class="muted">${label}</div>`;
    kpisEl.appendChild(d);
  }
}

function renderHighlights(highlightsEl, highlights) {
  highlightsEl.innerHTML = '';
  if (!highlights || !highlights.length) {
    const li = document.createElement('li');
    li.className = 'muted';
    li.textContent = 'No highlights yet.';
    highlightsEl.appendChild(li);
    return;
  }

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
  const sources = (data.sources || {});

  // --- Status + source selector + side-by-side summary ---
  const order = ['moltbook', 'moltx', 'hotmolts'].filter(k => sources[k]);
  const selectedKey = order[0] || Object.keys(sources)[0];

  const selector = el('select', { id: 'sourceSelect' });
  for (const k of order) {
    selector.appendChild(el('option', { value: k, html: sourceLabel(k) }));
  }
  selector.value = selectedKey;

  const summary = el('div', { class: 'grid', id: 'sourceSummary' });
  for (const k of order) {
    const src = sources[k] || {};
    const mode = src.mode || 'unknown';
    const meta = src.meta || {};
    const c = normCounts(src.counts || {});

    const bits = [];
    if (meta.unique_posts != null) bits.push(`unique posts: <code>${meta.unique_posts}</code>`);
    if (meta.posts_found != null) bits.push(`posts found: <code>${meta.posts_found}</code>`);
    if (meta.endpoints_queried != null) bits.push(`endpoints: <code>${meta.endpoints_queried}</code>`);
    if (meta.errors != null && meta.errors) bits.push(`errors: <code>${meta.errors}</code>`);
    if (meta.error) bits.push(`<span class="muted">${meta.error}</span>`);

    const card = el('div', { class: 'card' });
    card.innerHTML = `
      <div style="display:flex; align-items:baseline; justify-content:space-between; gap:12px; flex-wrap:wrap">
        <div><b>${sourceLabel(k)}</b></div>
        <div class="muted"><code>${mode}</code></div>
      </div>
      <div class="muted" style="margin-top:6px">
        ${c.scannedLabel}: <code>${c.scanned}</code>
        &nbsp;·&nbsp; lightning: <code>${c.lightning}</code>
        &nbsp;·&nbsp; BOLT11: <code>${c.bolt11}</code>
        &nbsp;·&nbsp; LNURL: <code>${c.lnurl}</code>
        &nbsp;·&nbsp; phoenixd: <code>${c.phoenixd}</code>
        &nbsp;·&nbsp; tipjar: <code>${c.tipjar}</code>
      </div>
      ${bits.length ? `<div class="muted" style="margin-top:6px">${bits.join(' · ')}</div>` : ''}
    `;
    summary.appendChild(card);
  }

  // Compute totals (so the default view can't mislead).
  const total = Object.values(sources).reduce((acc, src) => {
    const c = normCounts((src || {}).counts || {});
    acc.scanned += c.scanned;
    acc.lightning += c.lightning;
    acc.bolt11 += c.bolt11;
    acc.lnurl += c.lnurl;
    acc.phoenixd += c.phoenixd;
    acc.tipjar += c.tipjar;
    return acc;
  }, { scanned: 0, lightning: 0, bolt11: 0, lnurl: 0, phoenixd: 0, tipjar: 0 });

  statusEl.innerHTML = '';
  statusEl.appendChild(el('div', { html: `<div><b>Updated:</b> <code>${updated}</code></div>` }));
  // DOM marker used by CI/local verification.
  statusEl.appendChild(el('div', {
    class: 'muted',
    id: 'totalCounts',
    html: `TOTAL scanned (all sources): posts <code>${total.scanned}</code> · lightning <code>${total.lightning}</code> · BOLT11 <code>${total.bolt11}</code> · LNURL <code>${total.lnurl}</code> · phoenixd <code>${total.phoenixd}</code> · tipjar <code>${total.tipjar}</code>`,
  }));

  const line = el('div', { class: 'muted' });
  line.appendChild(document.createTextNode('View: '));
  line.appendChild(selector);
  statusEl.appendChild(line);

  const hint = el('div', { class: 'muted', html: 'Moltbook (API) is the main scaling source; MoltX is the live network feed; HotMolts is a cached, read-only mirror (when available).' });
  hint.style.marginTop = '6px';
  statusEl.appendChild(hint);

  const hr = el('div');
  hr.style.height = '12px';
  statusEl.appendChild(hr);
  statusEl.appendChild(summary);

  function rerender() {
    const key = selector.value;
    const src = sources[key] || {};
    renderKpis(kpisEl, src.counts || {});
    renderHighlights(highlightsEl, src.highlights || []);
  }

  selector.addEventListener('change', rerender);
  rerender();

  // --- Curated payment rails list (static JSON). ---
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
