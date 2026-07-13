// Service worker that injects COOP/COEP headers so the page becomes
// cross-origin isolated on hosts that can't set response headers (GitHub
// Pages). Isolation matters here because browsers coarsen performance.now()
// to ~100µs on non-isolated pages, and that clock is what PGlite's Postgres
// uses for EXPLAIN ANALYZE timings — isolated pages get ~5µs instead.
//
// COEP "credentialless" (rather than "require-corp") keeps the jsDelivr CDN
// fallback for PGlite working; browsers that don't support it simply stay
// non-isolated and the playground still works with coarser timings.
self.addEventListener('install', () => self.skipWaiting())
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()))

self.addEventListener('fetch', (event) => {
  const req = event.request
  if (req.cache === 'only-if-cached' && req.mode !== 'same-origin') return
  event.respondWith(
    (async () => {
      const res = await fetch(req)
      if (res.status === 0 || res.type === 'opaque' || res.type === 'opaqueredirect') return res
      const headers = new Headers(res.headers)
      headers.set('Cross-Origin-Opener-Policy', 'same-origin')
      headers.set('Cross-Origin-Embedder-Policy', 'credentialless')
      return new Response(res.body, { status: res.status, statusText: res.statusText, headers })
    })(),
  )
})
