// Splits a SQL script into individual statements so each can be executed on
// its own. This matters for auto_explain: PostgreSQL reports "Query Text" as
// the *entire* source text of a simple-protocol query, so running a whole
// script in one exec() would stamp every captured plan with the full script.
//
// Handles line comments, nested block comments, 'single' (with '' doubling),
// E'escaped' (backslash escapes), "identifiers", and $tag$ dollar quoting.
export function splitStatements(sql) {
  const stmts = []
  const n = sql.length
  let start = 0
  let i = 0

  const push = (end) => {
    const s = sql.slice(start, end).trim()
    if (s && !/^;+$/.test(s)) stmts.push(s)
  }

  while (i < n) {
    const c = sql[i]

    if (c === '-' && sql[i + 1] === '-') {
      const nl = sql.indexOf('\n', i)
      i = nl === -1 ? n : nl + 1
      continue
    }

    if (c === '/' && sql[i + 1] === '*') {
      let depth = 1
      i += 2
      while (i < n && depth > 0) {
        if (sql[i] === '/' && sql[i + 1] === '*') { depth++; i += 2 }
        else if (sql[i] === '*' && sql[i + 1] === '/') { depth--; i += 2 }
        else i++
      }
      continue
    }

    if (c === "'") {
      // E'…' allows backslash escapes; plain '…' only doubles quotes.
      const prev = sql[i - 1] ?? ''
      const prevprev = sql[i - 2] ?? ''
      const isEscapeString = /[eE]/.test(prev) && !/[A-Za-z0-9_$]/.test(prevprev)
      i++
      while (i < n) {
        if (isEscapeString && sql[i] === '\\') { i += 2; continue }
        if (sql[i] === "'") {
          if (sql[i + 1] === "'") { i += 2; continue }
          i++
          break
        }
        i++
      }
      continue
    }

    if (c === '"') {
      i++
      while (i < n) {
        if (sql[i] === '"') {
          if (sql[i + 1] === '"') { i += 2; continue }
          i++
          break
        }
        i++
      }
      continue
    }

    if (c === '$') {
      const m = /^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/.exec(sql.slice(i, i + 64))
      if (m) {
        const tag = m[0]
        const close = sql.indexOf(tag, i + tag.length)
        i = close === -1 ? n : close + tag.length
        continue
      }
      i++
      continue
    }

    if (c === ';') {
      push(i + 1)
      i++
      start = i
      continue
    }

    i++
  }

  push(n)
  return stmts
}
