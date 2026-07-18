# HarnessGraph agent guide

## Goal

Build and maintain a production Rust system that converts coding-agent
execution exports into a typed Neo4j experience graph and retrieves
evidence-backed paths for future work.

## Architecture boundaries

```text
interfaces -> application -> domain
infrastructure -> application/domain
protocol/adapters -> validated domain observations
domain -> no infrastructure, environment, Neo4j, HTTP, Rig, or provider types
```

The category-theory framing is:

```text
SessionBundle -> NativeRecord -> CanonicalObservation -> GraphCommand -> deterministic graph
VerifiedSessionBundle -> PreparedTranscript -> ValidatedChunkKnowledge
                      -> EnrichmentGraphCommand -> versioned overlay
```

Those morphisms must preserve ordering, identity, correlation, partial state,
and evidence provenance. Re-importing an unchanged source is the identity
operation. Reapplying an exact selected enrichment fingerprint is also an
identity operation with zero newly submitted chunks and zero new cost. The
overlay morphism is additive only: it cannot rewrite deterministic nodes,
edges, outcomes, risks, assurance, or ingestion receipts. Alternative states
and errors are typed coproducts, not strings.

## Domain rules

- Raw primitives do not cross domain, application, graph, provider, or public
  interfaces when they carry domain meaning.
- Use private-field newtypes, smart constructors, semantic enums, typestate,
  and typed errors with `thiserror`.
- No `unwrap`, `expect`, `panic`, `todo`, or `unimplemented` in production.
- External DTOs and Neo4j records never enter domain logic directly.
- Mistral is the only foundation-model provider. Rig/provider DTOs remain in
  the infrastructure adapter.
- Transcript enrichment uses only canonical `MISTRAL_API_KEY` from the exact
  repository `.env`, `mistral-small-2603`, and `https://api.eu.mistral.ai`.
- A paid chunk requires a database-enforced owner lease; successful projection
  and its checkpoint commit atomically before the run can be selected.

## Testing

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo nextest run --all-features
cargo test --test e2e --all-features
```

- No mocks, fake clients, fake repositories, fake clocks, or fake success paths.
- Use pure tests for pure logic, real temporary files, captured source-safe
  fixtures, a real local protocol server when needed, and real Neo4j for graph
  boundary tests.
- Every implementation milestone must include an end-to-end test and pass all
  focused checks before commit and push.
- Do not mark the composed real Mistral + real Neo4j transcript E2E, paid pilot,
  full backfill/rerun, or live graph-plus-assurance browser proof complete until
  that exact external workflow has run successfully.

## Secrets and source data

- Never commit `.env`, credentials, raw session exports, transcripts,
  instruction bodies, images, or production payloads.
- Never print secret values. Secret-bearing types require redacted `Debug`.
- Keep `HARNESS_GRAPH_REDACTION_HMAC_KEY` stable, at least 32 bytes, and
  distinct from Mistral/Neo4j credentials; inject it from 1Password at runtime.
- Set `HARNESS_GRAPH_MISTRAL_PRIVACY_CONTROL=training_opt_out_verified` only
  after an operator verifies the account control. It is an attestation, not an
  automatic privacy check.
- Never fetch remote asset references automatically.
- Use `.env.example` for names and non-secret defaults only.

## Build and lint

```bash
cargo build --all-features
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
```

## Deployment and recovery

- Apply Neo4j constraints before importing.
- Deterministic projection is idempotent and checkpointed only after committed
  batches.
- Enrichment resumes from committed per-chunk token receipts. A live owner-bound
  lease prevents duplicate paid calls; failed owners release their lease and
  expired leases are recoverable.
- Only fully checkpointed enrichment runs may become selected. Partial or failed
  runs must leave the deterministic graph and previous selected overlay intact.
- Preserve ingestion receipts and source digests for replay.
- Never delete or rewrite source exports during import or recovery.
