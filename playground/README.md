# otlppgplan playground

An in-browser playground for exploring PostgreSQL query plans as flame graphs,
powered by [PGlite](https://pglite.dev) (Postgres compiled to WASM) with the
`auto_explain` contrib module.

Every statement you run is captured by `auto_explain` (as JSON, with ANALYZE,
timing and buffer stats, including nested statements from PL/pgSQL). Each
captured plan is converted to a span tree using the same derivation logic as
the Go library in this repository (`convert.go`) — loop scaling, parallel
participant division, CTE de-duplication, child boost — and rendered as an
interactive flame graph. Spans carry the same `db.postgresql.*` attributes the
library emits, and each trace can be downloaded as OTLP/JSON.

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

## Running

The playground is a static page (no build step), but it uses ES modules, so it
needs to be served over HTTP:

```bash
cd playground
python3 -m http.server 8000
# or: npx serve .
```

Then open <http://localhost:8000>. PGlite (~13 MB of WASM) is loaded from the
jsDelivr CDN on first visit.

### Offline / pinned local copy

To avoid the CDN, vendor PGlite locally — the app prefers `vendor/pglite/` when
it exists:

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
  time. Matches the library's `LayoutFlame`.
- **Waterfall layout**: each node placed at its Actual Startup Time — shows
  when rows flowed. Matches the library's `LayoutWaterfall`.
- **Zoom & inspect**: click a span to zoom into its time window and see all of
  its `db.postgresql.*` attributes; click the background or "Reset zoom" to go
  back. Hover for timing, row counts (actual vs estimated), and buffers.
- **OTLP/JSON export** per trace, shaped like the library's output
  (`service.name` resource, plan-node spans with the same attribute names) —
  ready to POST to an OTLP collector's `/v1/traces`.
- **Paste mode**: paste `EXPLAIN (ANALYZE, FORMAT JSON)` output, an
  auto_explain JSON entry, or a full PostgreSQL log line and render it without
  running anything.
- Traceparent SQL comments (`/*traceparent='00-…-…-01'*/`) are honored in the
  OTLP export, same as the library.
