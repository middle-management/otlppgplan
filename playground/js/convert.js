// Conversion of PostgreSQL auto_explain / EXPLAIN JSON output into span trees.
//
// This is a JavaScript port of the span-derivation logic in convert.go so the
// playground renders the same shapes the Go library exports as OTLP traces:
// loop-scaled derived totals, parallel participant division, CTE
// de-duplication, child boost, and the two layouts (waterfall / flame).

// PostgreSQL log prefix: "2025-12-18 08:20:34.162 UTC [3476] LOG:  duration: 2.166 ms  plan:"
// PGlite notices arrive without the timestamp part: "duration: 2.166 ms  plan:\n{...}"
const LOG_PREFIX = /^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ \w+ \[(\d+)\] LOG:\s+)?duration:\s+([\d.]+)\s+ms\s+plan:\s*/

// Matches convert.go's traceparentPattern.
const TRACEPARENT = /\/\*\s*traceparent\s*=?\s*'([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})'\s*\*\//

export function isAutoExplainMessage(text) {
  return LOG_PREFIX.test(text.trimStart())
}

// parseExplainText accepts any of the formats the Go library accepts:
//  - auto_explain notice/log text: "duration: X ms plan: {…}" (optional PG log prefix)
//  - bare auto_explain JSON: { "Query Text": …, "Plan": {…} }
//  - EXPLAIN (FORMAT JSON) output: [ { "Plan": {…}, "Execution Time": … } ]
// Returns { queryText, plan, planningTime, executionTime, durationMS, pid, raw }.
export function parseExplainText(text) {
  let input = text.trim()
  let durationMS = 0
  let pid = 0

  const m = LOG_PREFIX.exec(input)
  if (m) {
    if (m[2]) pid = parseInt(m[2], 10)
    durationMS = parseFloat(m[3])
    input = input.slice(m[0].length).trim()
  }

  const doc = JSON.parse(input)
  let root
  let queryText = ''
  if (Array.isArray(doc)) {
    if (doc.length === 0) throw new Error('empty explain document')
    root = doc[0]
    queryText = root['Query Text'] ?? ''
  } else {
    root = doc
    queryText = doc['Query Text'] ?? ''
  }
  if (!root['Plan']) throw new Error('no "Plan" key found in input')

  let executionTime = root['Execution Time'] ?? null
  if (executionTime == null && durationMS > 0) executionTime = durationMS

  return {
    queryText,
    plan: root['Plan'],
    planningTime: root['Planning Time'] ?? null,
    executionTime,
    durationMS,
    pid,
    traceparent: extractTraceparent(queryText),
    raw: input,
  }
}

export function extractTraceparent(queryText) {
  const m = TRACEPARENT.exec(queryText || '')
  if (!m) return null
  return { traceId: m[2], spanId: m[3] }
}

// ---------------------------------------------------------------------------
// Derived timing (ports of processPlanDerived and friends)
// ---------------------------------------------------------------------------

// Wall-clock duration per node. Serial nodes: ActualTotalTime × ActualLoops.
// Under Gather/Gather Merge, loops are divided by the participant count
// (leader + launched workers) because PG reports loops summed across
// participants while total time is the per-worker average.
function computeDerivedTotals(node, participants) {
  let childParticipants = participants
  if (node['Node Type'] === 'Gather' || node['Node Type'] === 'Gather Merge') {
    let p = 1
    if (node['Workers Launched'] > 0) p += node['Workers Launched']
    else if (node['Workers Planned'] > 0) p += node['Workers Planned']
    childParticipants = p
  }
  for (const child of node['Plans'] ?? []) computeDerivedTotals(child, childParticipants)

  const total = node['Actual Total Time'] ?? 0
  let loops = node['Actual Loops'] ?? 1
  if (participants > 1) loops = loops / participants
  node.__derivedTotal = total * loops
}

function collectCTENodes(node, inits, scans) {
  if (node['Parent Relationship'] === 'InitPlan' && typeof node['Subplan Name'] === 'string' && node['Subplan Name'].startsWith('CTE ')) {
    inits.set(node['Subplan Name'].slice(4), node)
  }
  if (node['Node Type'] === 'CTE Scan' && node['CTE Name']) {
    const name = node['CTE Name']
    if (!scans.has(name)) scans.set(name, [])
    scans.get(name).push(node)
  }
  for (const child of node['Plans'] ?? []) collectCTENodes(child, inits, scans)
}

// CTE scans include the init plan's runtime; shave it off proportionally so
// the same milliseconds aren't drawn twice.
function adjustCTETotals(inits, scans) {
  for (const [name, scanNodes] of scans) {
    const initNode = inits.get(name)
    if (!initNode) continue
    const initTotal = initNode.__derivedTotal
    if (!initTotal) continue
    let scanSum = 0
    for (const s of scanNodes) scanSum += s.__derivedTotal
    if (!scanSum) continue
    for (const s of scanNodes) {
      s.__derivedTotal = Math.max(0, s.__derivedTotal * (1 - initTotal / scanSum))
    }
  }
}

function applyChildBoost(node) {
  let childSum = 0
  for (const child of node['Plans'] ?? []) {
    applyChildBoost(child)
    childSum += child.__derivedTotal
  }
  if (node.__derivedTotal === 0 && childSum > 0) node.__derivedTotal = childSum
}

function processPlanDerived(root) {
  computeDerivedTotals(root, 1)
  const inits = new Map()
  const scans = new Map()
  collectCTENodes(root, inits, scans)
  adjustCTETotals(inits, scans)
  applyChildBoost(root)
}

function nodeDurationMS(node) {
  if (node.__derivedTotal > 0) return node.__derivedTotal
  return node['Actual Total Time'] ?? 0
}

function exclusiveFromActuals(node) {
  const plans = node['Plans'] ?? []
  if (node['Actual Total Time'] == null || plans.length === 0) return 0
  let childSum = 0
  for (const c of plans) childSum += c['Actual Total Time'] ?? 0
  const excl = node['Actual Total Time'] - childSum
  return excl > 0 ? Math.round(excl * 1e6) / 1e6 : 0
}

// ---------------------------------------------------------------------------
// Span tree construction (port of convertFromRoot / emitPlanNodeSpans)
// ---------------------------------------------------------------------------

export function formatNodeSpanName(node) {
  let base = node['Node Type'] ?? 'Unknown'
  if (node['Relation Name']) base += ` ${node['Relation Name']}`
  if (node['Index Name']) base += ` (${node['Index Name']})`
  const isJoin = ['Nested Loop', 'Hash Join', 'Merge Join'].includes(node['Node Type'])
  if (isJoin && node['Join Type']) base += ` [${node['Join Type']}]`
  return base
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

// Attribute names mirror the OTLP attributes emitted by convert.go.
function nodeAttributes(node) {
  const a = { 'db.system': 'postgresql', 'db.postgresql.node_type': node['Node Type'] }
  const map = {
    'db.postgresql.relation': 'Relation Name',
    'db.postgresql.schema': 'Schema',
    'db.postgresql.alias': 'Alias',
    'db.postgresql.index': 'Index Name',
    'db.postgresql.join_type': 'Join Type',
    'db.postgresql.parent_relationship': 'Parent Relationship',
    'db.postgresql.startup_cost': 'Startup Cost',
    'db.postgresql.total_cost': 'Total Cost',
    'db.postgresql.plan_rows': 'Plan Rows',
    'db.postgresql.plan_width': 'Plan Width',
    'db.postgresql.actual_startup_time_ms': 'Actual Startup Time',
    'db.postgresql.actual_total_time_ms': 'Actual Total Time',
    'db.postgresql.actual_rows': 'Actual Rows',
    'db.postgresql.actual_loops': 'Actual Loops',
    'db.postgresql.rows_removed_by_filter': 'Rows Removed by Filter',
    'db.postgresql.rows_removed_by_join_filter': 'Rows Removed by Join Filter',
    'db.postgresql.rows_removed_by_index_recheck': 'Rows Removed by Index Recheck',
    'db.postgresql.shared_hit_blocks': 'Shared Hit Blocks',
    'db.postgresql.shared_read_blocks': 'Shared Read Blocks',
    'db.postgresql.shared_dirtied_blocks': 'Shared Dirtied Blocks',
    'db.postgresql.shared_written_blocks': 'Shared Written Blocks',
    'db.postgresql.local_hit_blocks': 'Local Hit Blocks',
    'db.postgresql.local_read_blocks': 'Local Read Blocks',
    'db.postgresql.local_dirtied_blocks': 'Local Dirtied Blocks',
    'db.postgresql.local_written_blocks': 'Local Written Blocks',
    'db.postgresql.temp_read_blocks': 'Temp Read Blocks',
    'db.postgresql.temp_written_blocks': 'Temp Written Blocks',
    'db.postgresql.io_read_time_ms': 'I/O Read Time',
    'db.postgresql.io_write_time_ms': 'I/O Write Time',
    'db.postgresql.sort_method': 'Sort Method',
    'db.postgresql.sort_space_used_kb': 'Sort Space Used',
    'db.postgresql.sort_space_type': 'Sort Space Type',
  }
  for (const [key, field] of Object.entries(map)) {
    const v = node[field]
    if (v !== undefined && v !== null && v !== '') a[key] = v
  }
  a['db.postgresql.derived_total_time_ms'] = node.__derivedTotal ?? 0
  const excl = exclusiveFromActuals(node)
  if (excl > 0) a['db.postgresql.exclusive_time_ms'] = excl
  if (node['Filter']) a['db.postgresql.filter'] = node['Filter']
  if (node['Index Cond']) a['db.postgresql.index_cond'] = node['Index Cond']
  if (node['Hash Cond']) a['db.postgresql.hash_cond'] = node['Hash Cond']
  const workers = node['Workers'] ?? []
  if (workers.length > 0) a['db.postgresql.workers_count'] = workers.length
  return a
}

// buildTrace turns a parsed explain entry into a span tree, in milliseconds
// relative to the trace start. layout: 'flame' | 'waterfall' (see convert.go).
export function buildTrace(entry, { layout = 'flame' } = {}) {
  const plan = structuredClone(entry.plan)
  processPlanDerived(plan)

  const planningMS = entry.planningTime ?? 0
  const executionMS = entry.executionTime ?? plan['Actual Total Time'] ?? 0
  const execStart = planningMS > 0 ? planningMS : 0
  const execEnd = execStart + executionMS

  const KNOWN_OPS = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'WITH', 'CALL', 'EXECUTE', 'DECLARE', 'CREATE', 'REFRESH']
  const firstWord = (entry.queryText || '').replace(/^(\s|--[^\n]*\n|\/\*[\s\S]*?\*\/)*/, '').split(/\s+/)[0]?.toUpperCase() ?? ''
  const operation = KNOWN_OPS.includes(firstWord) ? firstWord : ''
  const root = {
    name: operation ? `DB ${operation}` : 'DB QUERY',
    kind: 'query',
    category: 'chrome',
    start: 0,
    end: execEnd,
    attrs: {
      'db.system': 'postgresql',
      ...(entry.queryText ? { 'db.statement': entry.queryText } : {}),
      ...(operation ? { 'db.operation': operation } : {}),
      ...(entry.planningTime != null ? { 'db.postgresql.planning_time_ms': entry.planningTime } : {}),
      ...(entry.executionTime != null ? { 'db.postgresql.execution_time_ms': entry.executionTime } : {}),
    },
    children: [],
  }

  if (planningMS > 0) {
    root.children.push({
      name: 'DB PLAN', kind: 'planning', category: 'chrome',
      start: 0, end: planningMS,
      attrs: { 'db.system': 'postgresql', 'db.postgresql.planning_time_ms': planningMS },
      children: [],
    })
  }

  let planParent = root
  if (executionMS > 0) {
    const exec = {
      name: 'DB EXECUTION', kind: 'execution', category: 'chrome',
      start: execStart, end: execEnd,
      attrs: { 'db.system': 'postgresql', 'db.postgresql.execution_time_ms': executionMS },
      children: [],
    }
    root.children.push(exec)
    planParent = exec
  }

  emitPlanNode(planParent, plan, layout, execStart, execStart, execEnd)
  return { root, durationMS: execEnd, layout }
}

function emitPlanNode(parent, node, layout, execBase, packStart, packEnd) {
  let start, end
  if (layout === 'flame') {
    // pg_flame-style: width is inclusive DerivedTotalTime, children packed
    // end-to-end inside the parent's window, clamped so spans nest strictly.
    start = packStart
    end = Math.min(start + nodeDurationMS(node), packEnd || Infinity)
    if (end < start) end = start
  } else {
    // Waterfall: [execBase + Actual Startup Time, execBase + duration] —
    // children typically start before parents (they emit rows first).
    start = execBase + (node['Actual Startup Time'] ?? 0)
    end = execBase + nodeDurationMS(node)
    if (end < start) end = start
  }

  const span = {
    name: formatNodeSpanName(node),
    kind: 'node',
    category: nodeCategory(node['Node Type']),
    start, end,
    attrs: nodeAttributes(node),
    node,
    children: [],
  }
  parent.children.push(span)

  let cursor = start
  for (const child of node['Plans'] ?? []) {
    emitPlanNode(span, child, layout, execBase, cursor, end)
    cursor = Math.min(cursor + nodeDurationMS(child), end)
  }
  return span
}
