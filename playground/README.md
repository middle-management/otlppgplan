# otlppgplan playground

An in-browser playground for exploring PostgreSQL query plans as flame graphs,
powered by [PGlite](https://pglite.dev) (Postgres compiled to WASM) with the
`auto_explain` contrib module — and by this repository's Go converter,
compiled to WebAssembly.

Every statement you run is captured by `auto_explain` (as JSON, with ANALYZE,
timing and buffer stats, including nested statements from PL/pgSQL). Each
captured plan is fed to the actual Go library (`cmd/wasm`, built from
`convert.go`) running in the browser, which produces OTLP/JSON exactly as it
would for a collector. The playground then renders those spans as an
interactive flame graph. There is no reimplementation of the conversion in
JavaScript — what you see is what the library emits, and each trace can be
downloaded verbatim.

A deployed copy lives on GitHub Pages (built by
`.github/workflows/pages.yml` on every push to `main`).

## How it works

PGlite bundles `auto_explain`, and its plans can be captured entirely
client-side:

```js
import { PGlite } from '@electric-sql/pglite'
import { auto_explain } from '@electric-sql/pglite/contrib/auto_explain'

const db = new PGlite({ extensions: { auto_explain } })
await db.exec(`
  LOAD 'auto_explain';
  SET auto_explain.log_min_duration = 0;
  SET auto_explain.log_analyze = on;
  SET auto_explain.log_format = 'json';
  SET auto_explain.log_nested_statements = on;
  SET auto_explain.log_level = 'notice';  -- deliver via the wire protocol
`)
await db.exec('SELECT ...', { onNotice: (n) => console.log(n.message) })
// n.message: "duration: 1.509 ms  plan:\n{ \"Query Text\": ..., \"Plan\": {...} }"
```

`auto_explain` normally writes to the server log, which a browser can't see —
setting `auto_explain.log_level = 'notice'` sends the plan to the client as a
wire-protocol notice instead, where PGlite's `onNotice` callback picks it up.

The notice text is passed as-is to `otlppgplanConvert()` (the Go converter
compiled to WASM), which returns OTLP/JSON. `js/spans.js` only reshapes those
spans for drawing; all plan semantics — loop scaling, parallel participant
division, CTE de-duplication, child boost, flame/waterfall layouts — run in
the Go code.

## Running locally

Build the WASM converter once (produces `convert.wasm` + `wasm_exec.js`,
both gitignored):

```bash
./playground/build.sh
```

Then serve the directory (ES modules need HTTP, not file://):

```bash
cd playground
python3 -m http.server 8000
# or: npx serve .
```

Open <http://localhost:8000>. PGlite (~13 MB of WASM) is loaded from the
jsDelivr CDN on first visit.

### Offline / pinned local copy of PGlite

To avoid the CDN, vendor PGlite locally — the app prefers `vendor/pglite/`
when it exists (the Pages deployment does this):

```bash
npm install --prefix /tmp/pglite-vendor @electric-sql/pglite@0.5.4
cp -r /tmp/pglite-vendor/node_modules/@electric-sql/pglite/dist playground/vendor/pglite
```

## Features

- **SQL editor** with a pre-seeded demo schema (`customers`, `orders`,
  `order_items`) and an examples dropdown (joins, CTEs, correlated subqueries,
  updates, PL/pgSQL nested statements). Ctrl+Enter runs.
- **Flame layout** (default): pg_flame-style — children packed end-to-end
  inside their parent, bar width = inclusive time, parent overhang = exclusive
  time. The library's `LayoutFlame`.
- **Waterfall layout**: each node placed at its Actual Startup Time — shows
  when rows flowed. The library's `LayoutWaterfall`.
- **Zoom & inspect**: click a span to zoom into its time window and see all of
  its `db.postgresql.*` attributes; click the background or "Reset zoom" to go
  back. Hover for timing, row counts (actual vs estimated), and buffers.
- **OTLP/JSON export** per trace — byte-for-byte the library's marshaled
  output, ready to POST to an OTLP collector's `/v1/traces`.
- **Paste mode**: paste `EXPLAIN (ANALYZE, FORMAT JSON)` output, an
  auto_explain JSON entry, or a full PostgreSQL log line and render it without
  running anything.
- Traceparent SQL comments (`/*traceparent='00-…-…-01'*/`) are honored by the
  library, so exported traces link to the parent trace.
