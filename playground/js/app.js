import { initGoConverter, convertToOTLP } from './goconvert.js'
import { isAutoExplainMessage, otlpToRenderTree } from './spans.js'
import { splitStatements } from './split.js'
import { createFlameGraph, fmtDur, fmtNum, escapeHTML } from './flame.js'

const PGLITE_VERSION = '0.5.4'
const CDN_BASE = `https://cdn.jsdelivr.net/npm/@electric-sql/pglite@${PGLITE_VERSION}/dist`

const CATEGORY_LABELS = {
  scan: 'Scan',
  join: 'Join',
  agg: 'Aggregate',
  sort: 'Sort / Hash / Materialize',
  setop: 'Append / Set ops',
  modify: 'Modify',
  parallel: 'Parallel',
  other: 'Other',
  chrome: 'Query / execution',
}

const SEED_SQL = `
CREATE TABLE customers (id int PRIMARY KEY, name text, country text);
INSERT INTO customers
SELECT i, 'customer_' || i, (ARRAY['SE','NO','DK','FI','DE'])[1 + i % 5]
FROM generate_series(1, 2000) i;

CREATE TABLE orders (
  id int PRIMARY KEY,
  customer_id int REFERENCES customers(id),
  created_at timestamptz,
  amount numeric
);
INSERT INTO orders
SELECT i, 1 + (i * 7) % 2000,
       now() - (i % 365) * interval '1 day',
       round((random() * 500)::numeric, 2)
FROM generate_series(1, 20000) i;
CREATE INDEX orders_customer_id_idx ON orders (customer_id);
CREATE INDEX orders_created_at_idx ON orders (created_at);

CREATE TABLE order_items (order_id int, product text, qty int, price numeric);
INSERT INTO order_items
SELECT 1 + i % 20000, 'product_' || (i % 100), 1 + i % 5,
       round((random() * 100)::numeric, 2)
FROM generate_series(1, 60000) i;
CREATE INDEX order_items_order_id_idx ON order_items (order_id);

ANALYZE;
`

const EXAMPLES = [
  {
    label: 'Join + aggregate',
    sql: `SELECT c.country, count(*) AS orders, sum(o.amount) AS revenue
FROM orders o
JOIN customers c ON c.id = o.customer_id
WHERE o.created_at > now() - interval '90 days'
GROUP BY c.country
ORDER BY revenue DESC;`,
  },
  {
    label: 'Index scan vs seq scan',
    sql: `SELECT * FROM orders WHERE customer_id = 42;

SELECT count(*) FROM orders WHERE amount > 250;`,
  },
  {
    label: 'Materialized CTE',
    sql: `WITH monthly AS MATERIALIZED (
  SELECT date_trunc('month', created_at) AS month, sum(amount) AS total
  FROM orders GROUP BY 1
)
SELECT * FROM monthly
WHERE total > (SELECT avg(total) FROM monthly)
ORDER BY month;`,
  },
  {
    label: 'Correlated subquery (nested loop)',
    sql: `SELECT c.name,
       (SELECT max(o.amount) FROM orders o WHERE o.customer_id = c.id) AS biggest_order
FROM customers c
WHERE c.country = 'SE'
ORDER BY biggest_order DESC NULLS LAST
LIMIT 10;`,
  },
  {
    label: 'Three-way join with sort',
    sql: `SELECT c.name, o.id AS order_id, sum(oi.qty * oi.price) AS item_total
FROM customers c
JOIN orders o ON o.customer_id = c.id
JOIN order_items oi ON oi.order_id = o.id
WHERE c.country = 'DE'
GROUP BY c.name, o.id
ORDER BY item_total DESC
LIMIT 20;`,
  },
  {
    label: 'UPDATE',
    sql: `UPDATE orders SET amount = amount * 1.1 WHERE customer_id = 42;`,
  },
  {
    label: 'PL/pgSQL function (nested statements)',
    sql: `CREATE OR REPLACE FUNCTION top_customer(country_code text) RETURNS text AS $$
DECLARE result text;
BEGIN
  SELECT c.name INTO result
  FROM customers c JOIN orders o ON o.customer_id = c.id
  WHERE c.country = country_code
  GROUP BY c.name
  ORDER BY sum(o.amount) DESC
  LIMIT 1;
  RETURN result;
END $$ LANGUAGE plpgsql;

SELECT top_customer('SE');`,
  },
]

const AUTO_EXPLAIN_SETUP = `
LOAD 'auto_explain';
SET auto_explain.log_min_duration = 0;
SET auto_explain.log_analyze = on;
SET auto_explain.log_timing = on;
SET auto_explain.log_buffers = on;
SET auto_explain.log_nested_statements = on;
SET auto_explain.log_format = 'json';
SET auto_explain.log_level = 'notice';
`

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

const ui = {
  status: document.getElementById('status'),
  editor: document.getElementById('editor'),
  output: document.getElementById('output'),
  run: document.getElementById('run'),
  reset: document.getElementById('reset'),
  examples: document.getElementById('examples'),
  layout: document.getElementById('layout'),
  pasteInput: document.getElementById('paste-input'),
  pasteRender: document.getElementById('paste-render'),
}

let db = null
let PGliteMod = null
let runCounter = 0
const planCards = [] // { rebuild }
window.__planCards = planCards // introspection hook for browser tests

function setStatus(text, kind = '') {
  ui.status.textContent = text
  ui.status.className = `status${kind ? ` is-${kind}` : ''}`
}

function setBusy(busy) {
  ui.run.disabled = busy
  ui.reset.disabled = busy
}

// ---------------------------------------------------------------------------
// PGlite loading: prefer a local vendored copy (see README), fall back to CDN.
// ---------------------------------------------------------------------------

async function importPGlite() {
  try {
    const [core, ae] = await Promise.all([
      import('../vendor/pglite/index.js'),
      import('../vendor/pglite/contrib/auto_explain.js'),
    ])
    return { PGlite: core.PGlite, auto_explain: ae.auto_explain, source: 'vendored' }
  } catch {
    const [core, ae] = await Promise.all([
      import(`${CDN_BASE}/index.js`),
      import(`${CDN_BASE}/contrib/auto_explain.js`),
    ])
    return { PGlite: core.PGlite, auto_explain: ae.auto_explain, source: 'CDN' }
  }
}

async function initDB() {
  setBusy(true)
  setStatus('Loading PGlite…')
  if (!PGliteMod) PGliteMod = await importPGlite()

  if (db) {
    await db.close().catch(() => {})
    db = null
  }
  setStatus('Starting Postgres…')
  db = new PGliteMod.PGlite({ extensions: { auto_explain: PGliteMod.auto_explain } })

  // Seed with auto_explain silenced, then enable it for user queries.
  await db.exec('SELECT 1')
  await db.exec(SEED_SQL)
  await db.exec(AUTO_EXPLAIN_SETUP)

  const v = await db.query('SELECT version()')
  const version = String(v.rows[0]?.version ?? '').match(/^PostgreSQL \S+/)?.[0] ?? 'PostgreSQL'
  setStatus(`Ready — ${version} via PGlite ${PGLITE_VERSION} (${PGliteMod.source}), auto_explain loaded; converter: otlppgplan Go→WASM. Demo schema: customers, orders, order_items.`, 'ready')
  setBusy(false)
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

function downloadJSON(filename, jsonText) {
  const blob = new Blob([jsonText], { type: 'application/json' })
  const a = document.createElement('a')
  a.href = URL.createObjectURL(blob)
  a.download = filename
  a.click()
  URL.revokeObjectURL(a.href)
}

function collectCategories(span, set = new Set()) {
  set.add(span.category)
  for (const c of span.children) collectCategories(c, set)
  return set
}

function renderDetails(panel, span) {
  if (!span) {
    panel.innerHTML = '<span class="details-hint">Click a span to see its OTLP attributes.</span>'
    return
  }
  const rows = Object.entries(span.attrs)
    .map(([k, v]) => `<tr><td>${escapeHTML(k)}</td><td>${escapeHTML(typeof v === 'number' ? fmtNum(v) : v)}</td></tr>`)
    .join('')
  panel.innerHTML = `<table><tbody>
    <tr><td>span</td><td>${escapeHTML(span.name)} — ${fmtDur(span.end - span.start)}</td></tr>
    ${rows}</tbody></table>`
}

// Renders one captured plan as a card: the input text is converted by the Go
// library (compiled to WASM) into OTLP/JSON, which is rendered as a flame
// graph and downloadable verbatim.
function addPlanCard(parentEl, inputText, { baseTimeMS }) {
  const state = { inputText, baseTimeMS, otlp: null, tree: null, flame: null }

  // Convert before creating any DOM so a bad input doesn't leave an empty card.
  state.otlp = convertToOTLP(inputText, { layout: ui.layout.value, baseTimeMS })
  state.tree = otlpToRenderTree(state.otlp)

  const card = document.createElement('div')
  card.className = 'card'
  parentEl.appendChild(card)

  const idx = planCards.length + 1
  const head = document.createElement('div')
  head.className = 'card-head'
  head.innerHTML = `
    <div class="query">${escapeHTML(state.tree.queryText || '(no query text)')}</div>
    <div class="dur">${fmtDur(state.tree.executionTimeMS)}</div>
    <div class="card-actions">
      <button type="button" data-act="otlp" title="Download this trace exactly as the Go library marshals it (OTLP/JSON)">OTLP JSON</button>
      <button type="button" data-act="raw" title="Show the raw auto_explain output">Plan JSON</button>
    </div>`
  card.appendChild(head)

  const legend = document.createElement('div')
  legend.className = 'legend'
  card.appendChild(legend)

  const flameWrap = document.createElement('div')
  flameWrap.className = 'flame-wrap'
  card.appendChild(flameWrap)

  const details = document.createElement('div')
  details.className = 'details'
  card.appendChild(details)

  const rawHolder = document.createElement('div')
  rawHolder.className = 'raw-json'
  rawHolder.hidden = true
  rawHolder.innerHTML = `<pre>${escapeHTML(inputText)}</pre>`
  card.appendChild(rawHolder)

  function rebuild() {
    state.flame?.destroy()
    state.otlp = convertToOTLP(inputText, { layout: ui.layout.value, baseTimeMS })
    state.tree = otlpToRenderTree(state.otlp)
    legend.innerHTML = [...collectCategories(state.tree.root)]
      .sort((a, b) => Object.keys(CATEGORY_LABELS).indexOf(a) - Object.keys(CATEGORY_LABELS).indexOf(b))
      .map((cat) => `<span class="chip"><span class="swatch" style="background:var(--cat-${cat})"></span>${CATEGORY_LABELS[cat]}</span>`)
      .join('')
    renderDetails(details, null)
    state.flame = createFlameGraph(flameWrap, state.tree, {
      onSelect: (span) => renderDetails(details, span),
    })
  }
  state.rebuild = rebuild
  rebuild()
  planCards.push(state)

  head.querySelector('[data-act=otlp]').addEventListener('click', () => {
    downloadJSON(`trace-${idx}.json`, JSON.stringify(JSON.parse(state.otlp), null, 2))
  })
  head.querySelector('[data-act=raw]').addEventListener('click', () => {
    rawHolder.hidden = !rawHolder.hidden
  })
}

function addErrorCard(parentEl, message) {
  const card = document.createElement('div')
  card.className = 'card error'
  card.innerHTML = `<div class="msg">${escapeHTML(message)}</div>`
  parentEl.appendChild(card)
}

function addResults(parentEl, results) {
  const withRows = results.filter((r) => (r.fields?.length ?? 0) > 0)
  if (withRows.length === 0) return
  for (const r of withRows) {
    const card = document.createElement('div')
    card.className = 'card results'
    const limit = 50
    const rows = r.rows.slice(0, limit)
    const header = r.fields.map((f) => `<th>${escapeHTML(f.name)}</th>`).join('')
    const body = rows
      .map((row) => `<tr>${r.fields.map((f) => `<td>${escapeHTML(formatCell(row[f.name]))}</td>`).join('')}</tr>`)
      .join('')
    const truncated = r.rows.length > limit
      ? `<div class="truncated">Showing ${limit} of ${fmtNum(r.rows.length)} rows</div>` : ''
    card.innerHTML = `<details open><summary>Result — ${fmtNum(r.rows.length)} row${r.rows.length === 1 ? '' : 's'}</summary>
      <table><thead><tr>${header}</tr></thead><tbody>${body}</tbody></table>${truncated}</details>`
    parentEl.appendChild(card)
  }
}

function formatCell(v) {
  if (v == null) return 'NULL'
  if (v instanceof Date) return v.toISOString()
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------

async function runSQL() {
  const sql = ui.editor.value.trim()
  if (!sql || !db) return
  setBusy(true)

  runCounter++
  const group = document.createElement('section')
  group.className = 'run-group'
  group.innerHTML = `<div class="run-title">Run #${runCounter} · ${new Date().toLocaleTimeString()}</div>`
  ui.output.prepend(group)

  // Run statements one at a time: auto_explain's "Query Text" is the entire
  // source text of an exec() call, so batching a script would stamp every
  // captured plan with the whole script instead of its own statement.
  const statements = splitStatements(sql)
  const baseTimeMS = Date.now()
  for (let s = 0; s < statements.length; s++) {
    const stmt = statements[s]
    const notices = []
    try {
      const results = await db.exec(stmt, { onNotice: (n) => notices.push(n) })
      handleNotices(group, notices, baseTimeMS)
      addResults(group, results)
    } catch (err) {
      handleNotices(group, notices, baseTimeMS)
      const remaining = statements.length - s - 1
      addErrorCard(group, `${String(err?.message ?? err)}\n\nin statement:\n${stmt}${remaining > 0 ? `\n\n(${remaining} following statement${remaining === 1 ? '' : 's'} not run)` : ''}`)
      break
    }
  }
  if (group.children.length === 1) {
    const note = document.createElement('div')
    note.className = 'messages'
    note.textContent = 'Statements executed; no plans were captured (auto_explain only logs plannable statements).'
    group.appendChild(note)
  }
  setBusy(false)
}

function handleNotices(group, notices, baseTimeMS) {
  const other = []
  for (const n of notices) {
    const text = n.message ?? ''
    if (isAutoExplainMessage(text)) {
      try {
        addPlanCard(group, text, { baseTimeMS })
      } catch (err) {
        addErrorCard(group, `Failed to convert auto_explain output: ${err.message}`)
      }
    } else if (text) {
      other.push(`${n.severity ?? 'NOTICE'}: ${text}`)
    }
  }
  if (other.length > 0) {
    const el = document.createElement('div')
    el.className = 'messages'
    el.textContent = other.join('\n')
    group.appendChild(el)
  }
}

function renderPasted() {
  const text = ui.pasteInput.value.trim()
  if (!text) return
  const group = document.createElement('section')
  group.className = 'run-group'
  group.innerHTML = `<div class="run-title">Pasted plan · ${new Date().toLocaleTimeString()}</div>`
  ui.output.prepend(group)
  try {
    addPlanCard(group, text, { baseTimeMS: Date.now() })
  } catch (err) {
    addErrorCard(group, `Could not convert input: ${err.message}`)
  }
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

for (const ex of EXAMPLES) {
  const opt = document.createElement('option')
  opt.value = ex.sql
  opt.textContent = ex.label
  ui.examples.appendChild(opt)
}
ui.examples.addEventListener('change', () => {
  if (ui.examples.value) {
    ui.editor.value = ui.examples.value
    ui.examples.selectedIndex = 0
    ui.editor.focus()
  }
})

ui.layout.addEventListener('change', () => {
  for (const c of planCards) c.rebuild()
})

ui.run.addEventListener('click', runSQL)
ui.editor.addEventListener('keydown', (ev) => {
  if ((ev.ctrlKey || ev.metaKey) && ev.key === 'Enter') {
    ev.preventDefault()
    runSQL()
  }
})
ui.reset.addEventListener('click', async () => {
  try {
    await initDB()
  } catch (err) {
    setStatus(`Failed to reset: ${err.message}`, 'error')
    setBusy(false)
  }
})
ui.pasteRender.addEventListener('click', renderPasted)

ui.editor.value = EXAMPLES[0].sql

async function init() {
  setStatus('Loading otlppgplan converter (Go → WASM)…')
  await initGoConverter()
  await initDB()
}

init().catch((err) => {
  console.error(err)
  setStatus(`Failed to start: ${err.message}. Run playground/build.sh to build convert.wasm; see playground/README.md.`, 'error')
})
