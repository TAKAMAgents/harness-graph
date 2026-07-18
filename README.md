# HarnessGraph

HarnessGraph converts sensitive coding-agent execution exports into a typed,
evidence-backed Neo4j experience graph. It validates exporter provenance,
streams canonical JSONL records through explicit Rust domain types, preserves
partial execution state, computes deterministic assurance and risk findings,
and uses Mistral only when semantic interpretation is genuinely ambiguous.

The implementation contract is maintained in [`plan.md`](plan.md).

## Safety contract

- Raw Codex rollouts, transcripts, instruction bodies, images, credentials, and
  absolute local paths are never committed or copied into Neo4j.
- Historical imports use `raw/rollout.jsonl` only after metadata and checksum
  validation.
- Unknown native variants are quarantined with typed provenance rather than
  silently dropped.
- Mistral is the only supported foundation-model provider.
- Tests use real filesystem, process, HTTP, and Neo4j boundaries; no mocks or
  fake repositories/providers are used.

## Configuration

Copy `.env.example` to `.env` and provide the required values. Canonical names
are documented in that file. The runtime also accepts the existing misspelled
local aliases without logging their values. When a project `.env` exists, its
canonical names and aliases are resolved before inherited process variables;
this prevents unrelated workstation-wide Neo4j settings from silently taking
precedence. Run commands from a neutral working directory to use process-only
configuration.

## Historical import

Inspect a verified bundle without touching Neo4j:

```bash
cargo run -p harness-graph-cli -- inspect --session-id <uuid>
```

Derive correlations, semantic activities, evidence assurance, risks, and a
content-addressed path without mutating Neo4j:

```bash
cargo run -p harness-graph-cli -- analyze --session-id <uuid>
```

Project it into Neo4j:

```bash
cargo run -p harness-graph-cli -- import --session-id <uuid>
```

The importer validates the complete checksum manifest first, streams canonical
records in bounded transactions, creates idempotent uniqueness constraints,
and writes a completion receipt only after the streamed count matches verified
metadata. Repeating the same import preserves one observation per source
digest and sequence. `HARNESS_GRAPH_NAMESPACE` isolates graph populations and
`HARNESS_GRAPH_BATCH_SIZE` controls the validated transaction bound.

The current projection stores source/session provenance, observations,
quarantined variants, content-addressed contexts, turns, native-ID-correlated
tool calls, tools, compressed semantic activities, evidence-derived outcomes,
risk findings, normalized execution paths, and ingestion receipts. Every
derived finding retains source-record evidence links. Visualization, aggregate
path profiles, and Mistral-backed planning remain subsequent vertical slices
tracked in [`plan.md`](plan.md).

The verified 621-record golden snapshot currently derives 89 completed tool
calls and 56 deterministic semantic episodes. These are evidence-preserving
episodes, not the later 15–25-item Mistral narrative summary; both layers have
separate contracts in the plan.

## Development commands

```bash
just fmt
just lint
just test
just e2e
just check
```

The live Neo4j contract test is opt-in and uses the configured local instance:

```bash
cargo test -p harness-graph-neo4j-adapter \
  live_neo4j_projection_is_idempotent -- --ignored
```

Detailed architecture, commands, migration procedures, observability, and
recovery instructions will remain synchronized here as each validated vertical
slice is implemented.
