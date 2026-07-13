// OTLP/JSON export: serializes a span tree from convert.js into an
// ExportTraceServiceRequest-shaped object, matching the attribute layout the
// Go library emits (service.name resource, otlppgplan scope, db.* attributes).

function randomHex(bytes) {
  const buf = new Uint8Array(bytes)
  crypto.getRandomValues(buf)
  return Array.from(buf, (b) => b.toString(16).padStart(2, '0')).join('')
}

function toAnyValue(v) {
  if (typeof v === 'string') return { stringValue: v }
  if (typeof v === 'boolean') return { boolValue: v }
  if (Number.isInteger(v)) return { intValue: String(v) }
  return { doubleValue: v }
}

function toAttributes(attrs) {
  return Object.entries(attrs ?? {}).map(([key, value]) => ({ key, value: toAnyValue(value) }))
}

const SPAN_KIND_INTERNAL = 1
const SPAN_KIND_CLIENT = 3

// trace: { root, durationMS } from buildTrace. baseTimeMS: wall-clock epoch ms
// for trace start. traceparent: optional { traceId, spanId } linking the trace
// to an external parent (extracted from a SQL comment).
export function toOTLP(trace, { baseTimeMS = Date.now(), serviceName = 'pglite-playground', traceparent = null } = {}) {
  const traceId = traceparent?.traceId ?? randomHex(16)
  const spans = []

  const walk = (span, parentSpanId) => {
    const spanId = randomHex(8)
    spans.push({
      traceId,
      spanId,
      ...(parentSpanId ? { parentSpanId } : {}),
      name: span.name,
      kind: span.kind === 'query' ? SPAN_KIND_CLIENT : SPAN_KIND_INTERNAL,
      startTimeUnixNano: String(Math.round((baseTimeMS + span.start) * 1e6)),
      endTimeUnixNano: String(Math.round((baseTimeMS + span.end) * 1e6)),
      attributes: toAttributes(span.attrs),
    })
    for (const child of span.children) walk(child, spanId)
  }
  walk(trace.root, traceparent?.spanId ?? null)

  return {
    resourceSpans: [
      {
        resource: { attributes: toAttributes({ 'service.name': serviceName }) },
        scopeSpans: [{ scope: { name: 'otlppgplan-playground' }, spans }],
      },
    ],
  }
}
