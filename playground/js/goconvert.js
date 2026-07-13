// Loads the Go library compiled to WebAssembly (cmd/wasm via build.sh) and
// exposes the conversion. The conversion itself — auto_explain parsing, span
// derivation, layouts, OTLP attributes — is the actual Go code from this
// repository, not a JS port.

let convertFn = null

function loadScript(src) {
  return new Promise((resolve, reject) => {
    const s = document.createElement('script')
    s.src = src
    s.onload = resolve
    s.onerror = () => reject(new Error(`failed to load ${src}`))
    document.head.appendChild(s)
  })
}

export async function initGoConverter() {
  if (convertFn) return
  await loadScript('wasm_exec.js')
  const go = new globalThis.Go()

  let instance
  try {
    const result = await WebAssembly.instantiateStreaming(fetch('convert.wasm'), go.importObject)
    instance = result.instance
  } catch {
    // Server may not send application/wasm; fall back to ArrayBuffer.
    const buf = await (await fetch('convert.wasm')).arrayBuffer()
    const result = await WebAssembly.instantiate(buf, go.importObject)
    instance = result.instance
  }
  go.run(instance) // main() blocks forever; don't await

  // Registration happens at the top of main(); give the scheduler a moment.
  for (let i = 0; i < 100 && !globalThis.otlppgplanConvert; i++) {
    await new Promise((r) => setTimeout(r, 10))
  }
  convertFn = globalThis.otlppgplanConvert
  if (!convertFn) throw new Error('otlppgplanConvert was not registered by convert.wasm')
}

// convertToOTLP runs the Go converter. input: auto_explain notice text /
// auto_explain JSON / EXPLAIN (FORMAT JSON). Returns the OTLP/JSON string
// exactly as the library marshals it.
export function convertToOTLP(input, { layout = 'flame', baseTimeMS = Date.now(), serviceName = 'pglite-playground', dbName = '' } = {}) {
  if (!convertFn) throw new Error('Go converter not initialized')
  const res = convertFn(input, JSON.stringify({ layout, baseTimeMs: baseTimeMS, serviceName, dbName }))
  if (res.error) throw new Error(res.error)
  return res.otlp
}
