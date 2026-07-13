// SVG flame-graph / waterfall renderer for span trees built by convert.js.
// Rows are span depth, x is time. Hovering shows a tooltip, clicking a span
// zooms the time window to it and selects it for the details panel.

const ROW_H = 22
const ROW_GAP = 2
const AXIS_H = 22
const FONT = 11
const CHAR_W = 6.4 // approximation for truncating labels
const SVG_NS = 'http://www.w3.org/2000/svg'

export function fmtDur(ms) {
  if (ms == null || Number.isNaN(ms)) return '—'
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)} s`
  if (ms >= 1) return `${ms.toFixed(2)} ms`
  return `${(ms * 1000).toFixed(0)} µs`
}

export function fmtNum(n) {
  if (n == null) return '—'
  return Number.isInteger(n) ? n.toLocaleString('en-US') : n.toLocaleString('en-US', { maximumFractionDigits: 2 })
}

function niceStep(raw) {
  const pow = Math.pow(10, Math.floor(Math.log10(raw)))
  const frac = raw / pow
  const nice = frac <= 1 ? 1 : frac <= 2 ? 2 : frac <= 5 ? 5 : 10
  return nice * pow
}

function el(name, attrs = {}) {
  const node = document.createElementNS(SVG_NS, name)
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v)
  return node
}

// Flattens the span tree into [{span, depth}] and computes max depth.
function flatten(root) {
  const out = []
  let maxDepth = 0
  const walk = (span, depth) => {
    out.push({ span, depth })
    if (depth > maxDepth) maxDepth = depth
    for (const c of span.children) walk(c, depth + 1)
  }
  walk(root, 0)
  return { rows: out, maxDepth }
}

export function createFlameGraph(container, trace, { onSelect } = {}) {
  let focus = null // span whose window is zoomed to, or null for full trace
  let selected = null
  const { rows, maxDepth } = flatten(trace.root)
  const height = AXIS_H + (maxDepth + 1) * (ROW_H + ROW_GAP)

  const tooltip = document.createElement('div')
  tooltip.className = 'flame-tooltip'
  tooltip.hidden = true
  document.body.appendChild(tooltip)

  const resetBtn = document.createElement('button')
  resetBtn.type = 'button'
  resetBtn.className = 'flame-reset'
  resetBtn.textContent = 'Reset zoom'
  resetBtn.hidden = true
  resetBtn.addEventListener('click', () => { focus = null; render() })
  container.appendChild(resetBtn)

  const svgHolder = document.createElement('div')
  svgHolder.className = 'flame-svg-holder'
  container.appendChild(svgHolder)

  function windowRange() {
    if (focus) {
      const span = focus
      if (span.end > span.start) return [span.start, span.end]
    }
    return [0, Math.max(trace.durationMS, 1e-6)]
  }

  function tooltipHTML(span) {
    const total = Math.max(trace.durationMS, 1e-9)
    const dur = span.end - span.start
    const a = span.attrs ?? {}
    const lines = [`<div class="tt-name">${escapeHTML(span.name)}</div>`]
    lines.push(row('Time', `${fmtDur(dur)} · ${((dur / total) * 100).toFixed(1)}%`))
    if (a['db.postgresql.actual_rows'] != null) {
      let rowsTxt = fmtNum(a['db.postgresql.actual_rows'])
      if (a['db.postgresql.plan_rows'] != null) rowsTxt += ` (est ${fmtNum(a['db.postgresql.plan_rows'])})`
      lines.push(row('Rows', rowsTxt))
    }
    if ((a['db.postgresql.actual_loops'] ?? 1) > 1) lines.push(row('Loops', fmtNum(a['db.postgresql.actual_loops'])))
    if (a['db.postgresql.total_cost'] != null) lines.push(row('Cost', fmtNum(a['db.postgresql.total_cost'])))
    const hit = a['db.postgresql.shared_hit_blocks']
    const read = a['db.postgresql.shared_read_blocks']
    if (hit || read) lines.push(row('Buffers', `${fmtNum(hit ?? 0)} hit / ${fmtNum(read ?? 0)} read`))
    if (a['db.postgresql.filter']) lines.push(row('Filter', escapeHTML(a['db.postgresql.filter'])))
    if (a['db.postgresql.index_cond']) lines.push(row('Index cond', escapeHTML(a['db.postgresql.index_cond'])))
    lines.push('<div class="tt-hint">click to zoom &amp; inspect</div>')
    return lines.join('')
    function row(k, v) { return `<div class="tt-row"><span>${k}</span><span>${v}</span></div>` }
  }

  function render() {
    svgHolder.textContent = ''
    const width = Math.max(svgHolder.clientWidth || container.clientWidth || 600, 200)
    const [w0, w1] = windowRange()
    const scale = width / (w1 - w0)
    const x = (t) => (t - w0) * scale

    resetBtn.hidden = !focus

    const svg = el('svg', { width, height, viewBox: `0 0 ${width} ${height}`, class: 'flame-svg' })

    // Time axis
    const axis = el('g', { class: 'flame-axis' })
    const step = niceStep((w1 - w0) / 6)
    for (let t = Math.ceil(w0 / step) * step; t <= w1 + 1e-9; t += step) {
      const px = x(t)
      if (px < 0 || px > width) continue
      axis.appendChild(el('line', { x1: px, y1: AXIS_H - 4, x2: px, y2: height, class: 'flame-grid' }))
      const label = el('text', { x: px + 3, y: AXIS_H - 8, class: 'flame-tick' })
      label.textContent = fmtDur(t)
      axis.appendChild(label)
    }
    svg.appendChild(axis)

    for (const { span, depth } of rows) {
      // Strict comparison so zero-duration spans (common under coarse browser
      // timers, where fast nodes measure 0.000 ms) still render as ticks.
      if (span.end < w0 || span.start > w1) continue
      let x0 = Math.max(x(span.start), 0)
      const x1 = Math.min(x(span.end), width)
      let w = Math.max(x1 - x0, 2)
      if (x0 + w > width) x0 = Math.max(width - w, 0)
      // 2px gap between adjacent fills when there's room for it
      const gap = w > 6 ? 1 : 0
      const y = AXIS_H + depth * (ROW_H + ROW_GAP)

      const g = el('g', { class: 'flame-span' })
      const rect = el('rect', {
        x: x0 + gap, y, width: Math.max(w - gap * 2, 0.5), height: ROW_H,
        rx: 3, class: `flame-rect cat-${span.category}${span === selected ? ' is-selected' : ''}`,
      })
      g.appendChild(rect)

      const labelW = w - 10
      if (labelW > 3 * CHAR_W) {
        const maxChars = Math.floor(labelW / CHAR_W)
        let text = span.name
        if (text.length > maxChars) text = text.slice(0, Math.max(maxChars - 1, 1)) + '…'
        const t = el('text', { x: x0 + gap + 5, y: y + ROW_H / 2 + FONT / 2 - 1.5, class: 'flame-label' })
        t.textContent = text
        g.appendChild(t)
      }

      g.addEventListener('pointermove', (ev) => {
        tooltip.innerHTML = tooltipHTML(span)
        tooltip.hidden = false
        positionTooltip(tooltip, ev.clientX, ev.clientY)
      })
      g.addEventListener('pointerleave', () => { tooltip.hidden = true })
      g.addEventListener('click', (ev) => {
        ev.stopPropagation()
        selected = span
        focus = span === trace.root ? null : span
        render()
        onSelect?.(span)
      })
      svg.appendChild(g)
    }

    svg.addEventListener('click', () => { focus = null; selected = null; render(); onSelect?.(null) })
    svgHolder.appendChild(svg)
  }

  const ro = new ResizeObserver(() => render())
  ro.observe(svgHolder)
  render()

  return {
    render,
    destroy() {
      ro.disconnect()
      tooltip.remove()
      resetBtn.remove()
      svgHolder.remove()
    },
  }
}

function positionTooltip(tooltip, cx, cy) {
  const pad = 12
  const rect = tooltip.getBoundingClientRect()
  let left = cx + pad
  let top = cy + pad
  if (left + rect.width > window.innerWidth - 8) left = cx - rect.width - pad
  if (top + rect.height > window.innerHeight - 8) top = cy - rect.height - pad
  tooltip.style.left = `${Math.max(left, 4)}px`
  tooltip.style.top = `${Math.max(top, 4)}px`
}

export function escapeHTML(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]))
}
