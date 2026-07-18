import { createReadStream, existsSync, statSync } from 'node:fs'
import { createServer } from 'node:http'
import { extname, join, normalize, resolve, sep } from 'node:path'
import { fileURLToPath } from 'node:url'
import { sessionDetails, sessionListResponse } from './fixtures.mjs'

const APP_ROOT = resolve(fileURLToPath(new URL('..', import.meta.url)))
const DIST_ROOT = join(APP_ROOT, 'dist')
const HOST = '127.0.0.1'
const PORT = 4210

const contentTypes = new Map([
  ['.css', 'text/css; charset=utf-8'],
  ['.html', 'text/html; charset=utf-8'],
  ['.js', 'text/javascript; charset=utf-8'],
  ['.json', 'application/json; charset=utf-8'],
  ['.map', 'application/json; charset=utf-8'],
  ['.svg', 'image/svg+xml'],
  ['.woff2', 'font/woff2'],
])

function writeHeaders(response, status, contentType) {
  response.writeHead(status, {
    'Content-Type': contentType,
    'Cache-Control': 'no-store',
    'Content-Security-Policy': "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'",
    'Referrer-Policy': 'no-referrer',
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
  })
}

function writeJson(response, status, value) {
  writeHeaders(response, status, 'application/json; charset=utf-8')
  response.end(JSON.stringify(value))
}

function staticFileFor(pathname) {
  const requested = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '')
  const normalized = normalize(requested)
  const candidate = resolve(DIST_ROOT, normalized)
  if (candidate !== DIST_ROOT && !candidate.startsWith(`${DIST_ROOT}${sep}`)) {
    return null
  }
  if (existsSync(candidate) && statSync(candidate).isFile()) {
    return candidate
  }
  return join(DIST_ROOT, 'index.html')
}

const server = createServer((request, response) => {
  if (request.method !== 'GET' || request.url === undefined) {
    writeJson(response, 405, { code: 'method_not_allowed', message: 'Only read operations are available.' })
    return
  }

  const url = new URL(request.url, `http://${HOST}:${PORT}`)
  if (url.pathname === '/health') {
    writeJson(response, 200, { status: 'ready', surface: 'experience_contract' })
    return
  }
  if (url.pathname === '/v1/experience/sessions') {
    if (request.headers.cookie?.includes('contract_violation=1') === true) {
      writeJson(response, 200, {
        sessions: sessionListResponse.sessions.map((session, index) =>
          index === 0 ? { ...session, key: 'internal-graph-identity' } : session,
        ),
      })
    } else {
      writeJson(response, 200, sessionListResponse)
    }
    return
  }
  const detailMatch = /^\/v1\/experience\/sessions\/([A-Za-z0-9._:-]+)$/.exec(url.pathname)
  if (detailMatch !== null) {
    const sessionId = detailMatch[1]
    const detail = sessionId === undefined ? undefined : sessionDetails.get(sessionId)
    if (detail === undefined) {
      writeJson(response, 404, { code: 'session_not_found', message: 'The requested session is unavailable.' })
    } else {
      writeJson(response, 200, detail)
    }
    return
  }
  if (url.pathname.startsWith('/v1/')) {
    writeJson(response, 404, { code: 'route_not_found', message: 'The requested API route is unavailable.' })
    return
  }

  const file = staticFileFor(url.pathname)
  if (file === null || !existsSync(file)) {
    writeJson(response, 404, { code: 'asset_not_found', message: 'The requested asset is unavailable.' })
    return
  }
  if (url.searchParams.has('contract_violation')) {
    response.setHeader('Set-Cookie', 'contract_violation=1; Path=/; HttpOnly; SameSite=Strict')
  }
  writeHeaders(response, 200, contentTypes.get(extname(file)) ?? 'application/octet-stream')
  createReadStream(file).pipe(response)
})

server.listen(PORT, HOST)

function closeServer() {
  server.close((error) => {
    process.exitCode = error === undefined ? 0 : 1
  })
}

process.on('SIGINT', closeServer)
process.on('SIGTERM', closeServer)
