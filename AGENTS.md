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
SessionBundle -> NativeRecord -> CanonicalObservation -> GraphCommand
```

Those morphisms must preserve ordering, identity, correlation, partial state,
and evidence provenance. Re-importing an unchanged source is the identity
operation. Alternative states and errors are typed coproducts, not strings.

## Domain rules

- Raw primitives do not cross domain, application, graph, provider, or public
  interfaces when they carry domain meaning.
- Use private-field newtypes, smart constructors, semantic enums, typestate,
  and typed errors with `thiserror`.
- No `unwrap`, `expect`, `panic`, `todo`, or `unimplemented` in production.
- External DTOs and Neo4j records never enter domain logic directly.
- Mistral is the only foundation-model provider. Rig/provider DTOs remain in
  the infrastructure adapter.

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

## Secrets and source data

- Never commit `.env`, credentials, raw session exports, transcripts,
  instruction bodies, images, or production payloads.
- Never print secret values. Secret-bearing types require redacted `Debug`.
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
- Projection is idempotent and checkpointed only after committed batches.
- Preserve ingestion receipts and source digests for replay.
- Never delete or rewrite source exports during import or recovery.
