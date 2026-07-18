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
local aliases without logging their values.

## Development commands

```bash
just fmt
just lint
just test
just e2e
just check
```

Detailed architecture, commands, migration procedures, observability, and
recovery instructions will remain synchronized here as each validated vertical
slice is implemented.
