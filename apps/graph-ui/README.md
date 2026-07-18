# HarnessGraph experience explorer

This React surface reads citation-aware session projections from the HarnessGraph
API. It displays a completed Mistral enrichment when one is selected and falls
back to the authoritative deterministic activity view otherwise.

The browser never receives or renders Neo4j internal `key` properties, raw
transcript text, source file paths, provider credentials, or local secrets. Zod
decoders reject unknown fields, unresolved citations, invalid relation/entity
references, mismatched enrichment/display states, and obvious secret or local
path patterns before a response can enter UI state.

## API contract

```text
GET /v1/experience/sessions
GET /v1/experience/sessions/{session_id}
```

The list response is `{ sessions: SessionSummary[] }`. A session detail contains
typed deterministic activities, a completed-enrichment or unavailable-enrichment
coproduct, and content-free `source_anchors`. Completed enrichment includes:

```text
provider = mistral
model
prompt_version
disclosure_scope
authorization_policy_digest
prompt_digest
schema_version
episodes
entities
claims with confidence, epistemic_status, and citations
relations with confidence, epistemic_status, and citations
```

The list contract is:

```json
{
  "sessions": [
    {
      "session_id": "019d2a40-7324-77a2-832c-f5f9f84473b0",
      "display": {
        "source": "deterministic_fallback",
        "title": "...",
        "summary": "..."
      },
      "outcome": "inconclusive",
      "activity_count": 0,
      "enrichment": { "state": "unavailable", "reason": "no_completed_run" }
    }
  ]
}
```

Detail responses add deterministic `activities`, the completed enrichment
collections or a typed unavailable reason, and `source_anchors`. Invalid session
IDs return `400`, missing verified sessions return `404`, and graph-read failures
return `503`; errors contain only a closed `code` and source-safe `message`.

Each citation contains only `anchor_id`. Its corresponding source anchor contains
only a display label, closed source kind, record sequence, and content digest.
It contains no transcript field path, local path, or excerpt.

## Development

Start the Rust API against the configured Neo4j graph. To use a non-default API
port, override its bind address explicitly:

```bash
HARNESS_GRAPH_BIND_ADDRESS=127.0.0.1:3200 \
  cargo run -p harness-graph-cli -- serve
```

Then start the UI from this directory with the same credential-free loopback
origin:

```bash
npm install
VITE_API_PROXY_TARGET=http://127.0.0.1:3200 npm run dev
```

The Vite development server proxies `/v1` and `/health` to the CLI/API default
`http://127.0.0.1:3000` when `VITE_API_PROXY_TARGET` is absent. The UI itself is
served on `http://127.0.0.1:4173`. For the default API port, use:

```bash
npm run dev
```

## Validation

```bash
npm run typecheck
npm run lint
npm run build
npx playwright install chromium
npm run test:e2e
```

The browser E2E starts a real Node HTTP server serving the production build and
the exact source-safe API contract. It validates the enriched view, deterministic
fallback, provenance, resolvable citation navigation, forbidden-field absence,
keyboard access, and responsive mobile layout. The server is contract-faithful;
it does not substitute a fake provider or database behind production code.

### Live Rust API and Neo4j browser E2E

Keep the contract suite above as the fast deterministic boundary test. After a
real Rust API is running on `127.0.0.1:3000` with its real Neo4j connection, run:

```bash
npm run test:e2e:live
```

If the Rust API is intentionally bound to another loopback port, use the same
credential-free local target consumed by Vite and the live preflight:

```bash
VITE_API_PROXY_TARGET=http://127.0.0.1:3001 npm run test:e2e:live
```

The live Playwright configuration starts only the Vite UI. It does not start the
Node contract server, intercept requests, inject browser fixtures, or replace
Neo4j. It exercises the real session list/detail routes, a deterministic fallback
session, and—when present—a completed enrichment with Mistral provenance and a
resolvable citation. An unavailable API, empty graph, missing fallback, contract
violation, leaked private field, or unresolved completed-run citation fails with
a specific diagnostic.

The contract-browser suite and live deterministic API/Neo4j path are proven.
The composed live browser proof against a paid transcript-enriched graph,
including the planned graph and assurance panels, remains an open gate in the
root [`plan.md`](../../plan.md).
