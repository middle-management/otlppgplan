// Turns OTLP/JSON (as emitted by the Go converter) back into a span tree the
// flame renderer can draw. This file is rendering-side only — all plan
// semantics live in the Go code.

const LOG_PREFIX = /^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ \w+ \[\d+\] LOG:\s+)?duration:\s+[\d.]+\s+ms\s+plan:\s*/

export function isAutoExplainMessage(text) {
  return LOG_PREFIX.test((text ?? '').trimStart())
}

export function nodeCategory(nodeType) {
  if (!nodeType) return 'other'
  if (['Nested Loop', 'Hash Join', 'Merge Join'].includes(nodeType)) return 'join'
  if (['Aggregate', 'GroupAggregate', 'HashAggregate', 'MixedAggregate', 'WindowAgg', 'Group'].includes(nodeType)) return 'agg'
  if (['Sort', 'Incremental Sort', 'Materialize', 'Memoize', 'Hash'].includes(nodeType)) return 'sort'
  if (['Append', 'Merge Append', 'Recursive Union', 'SetOp', 'HashSetOp', 'Unique'].includes(nodeType)) return 'setop'
  if (['ModifyTable', 'Insert', 'Update', 'Delete', 'Merge'].includes(nodeType)) return 'modify'
  if (['Gather', 'Gather Merge'].includes(nodeType)) return 'parallel'
  if (nodeType.includes('Scan') || nodeType === 'BitmapAnd' || nodeType === 'BitmapOr') return 'scan'
  return 'other'
}

function anyValueToJS(v) {
  if (v == null) return null
  if ('stringValue' in v) return v.stringValue
  if ('doubleValue' in v) return v.doubleValue
  if ('intValue' in v) return Number(v.intValue)
  if ('boolValue' in v) return v.boolValue
  if ('arrayValue' in v) return (v.arrayValue.values ?? []).map(anyValueToJS)
  return JSON.stringify(v)
}

function flattenAttributes(list) {
  const out = {}
  for (const { key, value } of list ?? []) out[key] = anyValueToJS(value)
  return out
}

// otlpToRenderTree parses an OTLP/JSON string into
// { root, durationMS, queryText, executionTimeMS } for the flame renderer.
// Span times become ms offsets from the trace's earliest start.
export function otlpToRenderTree(otlpJSON) {
  const doc = typeof otlpJSON === 'string' ? JSON.parse(otlpJSON) : otlpJSON
  const spans = (doc.resourceSpans ?? []).flatMap((rs) =>
    (rs.scopeSpans ?? []).flatMap((ss) => ss.spans ?? []))
  if (spans.length === 0) throw new Error('OTLP document contains no spans')

  let base = null
  for (const s of spans) {
    const t = BigInt(s.startTimeUnixNano)
    if (base === null || t < base) base = t
  }
  const toMS = (nanoStr) => Number(BigInt(nanoStr) - base) / 1e6

  const byId = new Map()
  const nodes = spans.map((s) => {
    const attrs = flattenAttributes(s.attributes)
    const node = {
      spanId: s.spanId,
      parentSpanId: s.parentSpanId ?? null,
      name: s.name,
      start: toMS(s.startTimeUnixNano),
      end: toMS(s.endTimeUnixNano),
      attrs,
      kind: 'node',
      category: attrs['db.postgresql.node_type'] ? nodeCategory(attrs['db.postgresql.node_type']) : 'chrome',
      children: [],
    }
    byId.set(node.spanId, node)
    return node
  })

  let root = null
  for (const n of nodes) {
    const parent = n.parentSpanId ? byId.get(n.parentSpanId) : null
    if (parent) parent.children.push(n)
    else if (!root) root = n // first orphan = root (parent may be an external traceparent)
  }
  if (!root) root = nodes[0]

  if (root.attrs['db.statement'] != null) root.kind = 'query'
  for (const n of nodes) {
    if (n.name === 'DB PLAN') n.kind = 'planning'
    else if (n.name === 'DB EXECUTION') n.kind = 'execution'
  }

  return {
    root,
    durationMS: Math.max(root.end - root.start, ...nodes.map((n) => n.end - root.start)),
    queryText: root.attrs['db.statement'] ?? '',
    executionTimeMS: root.attrs['db.postgresql.execution_time_ms'] ?? root.end - root.start,
  }
}
