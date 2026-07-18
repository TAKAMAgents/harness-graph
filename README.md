<p align="center">
  <img src="assets/harness.png" alt="HarnessGraph logo" width="720">
</p>

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
- Mistral output is an additive, versioned overlay. It cannot replace or mutate
  deterministic observations, activities, outcomes, risks, assurance,
  ingestion receipts, or evidence edges.
- Tests use real filesystem, process, HTTP, and Neo4j boundaries; no mocks or
  fake repositories/providers are used.

## Configuration

Copy `.env.example` to the repository-root `.env` and provide the required
values. The CLI resolves that exact file from its compiled project location,
independent of the current working directory. Neo4j credentials prefer canonical
or backward-compatible alias values from that project file before inherited
process values, preventing unrelated workstation settings from selecting another
database. Other non-account optional settings may still be overridden by a
non-empty process value. Configuration values are never logged.

`MISTRAL_API_KEY` is the canonical and required foundation-model credential.
Cost-bearing transcript enrichment is stricter than the other Mistral commands:
it reads only the non-empty canonical `MISTRAL_API_KEY` in this project's
`.env`. An inherited process value or the historical `MISTARL_API_KEY` typo
cannot select a different account for transcript disclosure.
`MISTRAL_MODEL` defaults to `mistral-small-latest` and rejects model names
outside Mistral-hosted families for source-safe interpretation commands.
Transcript enrichment independently pins `MISTRAL_TRANSCRIPT_MODEL` to
`mistral-small-2603` and the stateless EU endpoint
`https://api.eu.mistral.ai`; apply fails closed if the configured model differs.
The credential has a redacted debug representation and is never included in
command output.
`MISTRAL_MAX_CONCURRENCY` defaults to `2` and is constrained to `1..=4`.

`HARNESS_GRAPH_JOURNAL_PATH` selects the append-only live event journal and
defaults to `data/live-events.jsonl`. The `data/` directory and journal remain
local and Git-ignored.

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

Project every published session with bounded concurrency:

```bash
cargo run --release -p harness-graph-cli -- \
  import-all --scope all --concurrency 4
```

`import-all` shares one Neo4j connection pool, verifies session checksums on a
bounded blocking-worker set, and settles independent session imports
concurrently while preserving record order inside each session. Mutation
transactions alone pass through a shared adapter gate because otherwise
concurrent sessions can contend on namespace-scoped nodes such as `HGTool`;
checksum verification, decoding, and analysis remain concurrent. The command
emits one source-safe JSON settlement to stderr as each session finishes and a
sorted summary to stdout. Session-to-source provenance is always materialized
before exact source snapshots with a consistent completed receipt are reported
as `already_complete`, so distinct sessions that share identical source bytes
remain visible without replaying the observations. Individual failures do not
cancel unrelated imports; the final summary is `completed_with_failures` and
the process exits nonzero when any session fails, so the same command can be
rerun safely after repair.

The importer validates the complete checksum manifest first, streams canonical
records in bounded transactions, creates idempotent uniqueness constraints,
and writes a completion receipt only after the streamed count matches verified
metadata. Repeating the same import preserves one observation per source
digest and sequence, and a trustworthy receipt avoids replaying an already
complete snapshot. `HARNESS_GRAPH_NAMESPACE` isolates graph populations and
`HARNESS_GRAPH_BATCH_SIZE` controls the validated transaction bound.

Metadata-only sessions are valid raw snapshots but cannot support an outcome or
execution path. They are imported and receipted without synthetic semantic
nodes, and their import result reports
`analysis.status = "insufficient_semantic_evidence"`. Actual correlation,
classification, assurance, path, and projection errors still fail that session.

The current projection stores source/session provenance, observations,
quarantined variants, content-addressed contexts, turns, native-ID-correlated
tool calls, tools, compressed semantic activities, evidence-derived outcomes,
risk findings, normalized execution paths, and ingestion receipts. Every
derived finding retains source-record evidence links. Visualization and
aggregate path profiles remain subsequent vertical slices tracked in
[`plan.md`](plan.md).

The verified 621-record golden snapshot currently derives 89 completed tool
calls and 56 deterministic semantic episodes. These are evidence-preserving
episodes, not the later 15–25-item Mistral narrative summary; both layers have
separate contracts in the plan.

## Additive transcript knowledge enrichment

Transcript enrichment composes two separate graph layers:

```text
checksum-verified raw/rollout.jsonl -> deterministic import -> authoritative base graph
                                  \-> local scan/redact/chunk
                                      -> Mistral EU structured extraction
                                      -> citation validation -> versioned overlay
```

The local boundary excludes instruction bodies, hidden reasoning, assets, and
binary content. It replaces recognized secrets and PII with stable keyed
pseudonyms before provider transfer. Each bounded chunk asks Mistral to jointly
classify knowledge kinds and extract cited episodes, entities, claims, and
relations. Chunk calls run concurrently behind one shared provider semaphore;
session aggregation is deterministic and makes no reducer call.

### Dry run

Inventory one session or the complete verified catalog without Mistral calls or
Neo4j writes:

```bash
cargo run --release -p harness-graph-cli -- \
  enrich-transcripts --session-id <uuid> \
  --authorization <source-safe-operator-id> --dry-run

cargo run --release -p harness-graph-cli -- \
  enrich-all-transcripts --scope all \
  --authorization <source-safe-operator-id> --dry-run
```

The complete release dry run on 2026-07-18 produced this source-safe inventory:

| Measure | Observed value |
| --- | ---: |
| Discovered / eligible / metadata-only / blocked sessions | 1,369 / 867 / 10 / 492 |
| Verified records | 2,410,017 |
| Projected / sanitized fragments | 1,220,262 / 482,558 |
| Sanitized bytes | 691,299,618 |
| Expected chunks and API calls | 9,419 |
| Estimated input / output tokens | 60,236,474 / 9,645,056 |
| Estimated cost | 16,305,624 micro-USD (about $16.31) |
| Actual external provider calls / Neo4j writes | 0 / 0 |

All 492 blocks were scanner decisions: 327 non-text control-data, 26
asset/binary, and 139 suspicious encoded-blob cases. The scan recorded 927
known-secret, 165 private-key, 1,846 authentication-material, 3,580
credential-URL, 351 provider-token, 2,682 high-entropy-assignment, 26,586 email,
399 phone, 20,610 IP-address, and 485,580 home-path redactions. These are
inventory estimates, not paid-run usage.

The estimator is pinned to the repository's regional-EU pricing snapshot:
$0.165 per million input tokens and $0.66 per million output tokens, represented
as 165,000 and 660,000 micro-USD per million. Reconfirm provider pricing before
any paid backfill.

### Paid apply prerequisites

Apply remains fail-closed until all of these are true:

- `HARNESS_GRAPH_TRANSCRIPT_ENRICHMENT_MODE=enabled`.
- An operator has verified that the Mistral account's API training/data-sharing
  control is disabled and set
  `HARNESS_GRAPH_MISTRAL_PRIVACY_CONTROL=training_opt_out_verified`. This value
  is an operator attestation; the CLI does not infer account privacy state.
- The exact project `.env` contains canonical `MISTRAL_API_KEY`, and the resolved
  transcript model is the pinned `mistral-small-2603` (also the default).
- `HARNESS_GRAPH_REDACTION_HMAC_KEY` is one stable secret of at least 32 bytes,
  distinct from every Mistral and Neo4j credential.
- Neo4j is healthy and its deterministic and enrichment schemas can be applied.

Keep the stable HMAC in a distinct 1Password item or field. One safe local
pattern is an ignored `.env.enrichment-op` file containing only a secret
reference:

```dotenv
HARNESS_GRAPH_TRANSCRIPT_ENRICHMENT_MODE=enabled
HARNESS_GRAPH_MISTRAL_PRIVACY_CONTROL=training_opt_out_verified
HARNESS_GRAPH_REDACTION_HMAC_KEY=op://VAULT/ITEM/FIELD
```

Then inject it without printing or copying the value:

```bash
op run --env-file=.env.enrichment-op -- \
  cargo run --release -p harness-graph-cli -- \
  enrich-transcripts --session-id <uuid> \
  --authorization <source-safe-operator-id> --apply
```

Do not rotate this key during a resumable run or identical rerun: changing it
changes stable pseudonyms and therefore the enrichment fingerprint.

### Bounded rollout and rerun

The paid rollout commands below are implemented but have not yet been executed
against the raw archive. Run them in order only after reviewing the dry run and
the privacy attestation:

```bash
# One eligible session.
op run --env-file=.env.enrichment-op -- \
  cargo run --release -p harness-graph-cli -- \
  enrich-all-transcripts --scope all --authorization <source-safe-operator-id> \
  --apply --concurrency 2 --limit 1

# Ten eligible sessions.
op run --env-file=.env.enrichment-op -- \
  cargo run --release -p harness-graph-cli -- \
  enrich-all-transcripts --scope all --authorization <source-safe-operator-id> \
  --apply --concurrency 2 --limit 10

# Fifty eligible sessions.
op run --env-file=.env.enrichment-op -- \
  cargo run --release -p harness-graph-cli -- \
  enrich-all-transcripts --scope all --authorization <source-safe-operator-id> \
  --apply --concurrency 2 --limit 50

# Full eligible catalog. Omit --limit.
op run --env-file=.env.enrichment-op -- \
  cargo run --release -p harness-graph-cli -- \
  enrich-all-transcripts --scope all --authorization <source-safe-operator-id> \
  --apply --concurrency 2

# Identical full rerun: repeat the preceding command unchanged.
```

`--limit` selects the first 1–50 eligible sessions in stable catalog order;
metadata-only and blocked sessions may be scanned before the selected limit is
reached. Session concurrency is bounded to `1..=8`, while every session shares
the stricter `MISTRAL_MAX_CONCURRENCY` provider gate. Each verified session's
deterministic base import completes before provider work. Bulk apply settles all
selected sessions, sorts its source-safe results, and exits nonzero if any
session blocks or fails. Metadata-only sessions are successful no-ops.

Before a missing chunk reaches Mistral, Neo4j atomically grants an owner-bound
15-minute paid-call lease. Another live invocation sees `lease_busy` and makes
no provider call. A successful projection and its token checkpoint commit
together; failed work releases only its owner's lease, and an expired lease is
recoverable. Final checkpoint reconciliation handles ambiguous commit results,
and only a fully checkpointed run can atomically become the selected overlay.
An exact selected fingerprint reports `submitted_chunks=0` and
`new_cost_microusd=0`.

Completed output reports `submitted_chunks`, `resumed_chunks`,
`run_input_tokens`, `run_output_tokens`, `run_cost_microusd`, and
`completion_disposition`. Bulk totals use
`accounting_scope=completed_run_checkpoints`, so they include every checkpoint
in the completed run, including resumed chunks. They are not counts of raw HTTP
attempts or invocation-only token usage.

## Mistral interpretation and Pathfinder

Verify the configured Mistral credential against the real model catalog:

```bash
cargo run -p harness-graph-cli -- mistral-health
```

Create a 15–25-item narrative layer over deterministic activities:

```bash
cargo run -p harness-graph-cli -- summarize --session-id <uuid>
```

Classify a source-safe task and extract that session's narrative concurrently:

```bash
cargo run -p harness-graph-cli -- interpret \
  --session-id <uuid> \
  --task "Investigate and improve an agent workflow with incomplete verification evidence."
```

The `interpret` command starts two independent Mistral structured-output calls
and joins both results with `tokio::join!`. The shared semaphore permits two
in-flight calls by default, both branches settle even if one fails, and no
partial synchronized result is emitted. Classification and extraction retain
separate provider usage. Each call is limited to one model turn and a
90-second wall-clock request bound.

Rust partitions and owns every activity citation. Mistral supplies bounded
native JSON-schema labels only. If Mistral omits or duplicates a requested
group, the adapter retains coverage with a deterministic kind-only label and
reports its origin as `deterministic_fallback`; it never invents missing
evidence or silently treats the model response as complete.

Retrieve verified-success paths through the typed graph port and ask Mistral
for a citation-validated plan:

```bash
cargo run -p harness-graph-cli -- pathfinder \
  --task "Repair and verify a deprecated configuration under restricted sandboxing" \
  --precedents 1
```

Mistral never receives raw Cypher or raw rollout payloads. Candidate session
and activity citations are rejected unless they belong to the retrieved typed
precedents.

## Live ingestion and replay

Start the Axum API and server-sent event stream:

```bash
cargo run -p harness-graph-cli -- serve
```

The production surface is intentionally small:

```text
GET  /health
POST /v1/live/events
GET  /v1/live/events?after=<sequence>
GET  /v1/live/events/stream?after=<sequence>
GET  /v1/experience/sessions
GET  /v1/experience/sessions/{session_id}
```

The experience routes return a deterministic fallback or the selected completed
overlay with Mistral/model/prompt/schema and authorization-policy provenance.
Citations resolve through content-free source anchors; raw transcript text,
local paths, provider credentials, and Neo4j internal keys are excluded. See
[`apps/graph-ui/README.md`](apps/graph-ui/README.md) for the response contract
and UI startup with an API port override.

Live adapters submit only source-safe typed events. For example:

```json
{
  "event_id": "019d2a40-7324-77a2-832c-f5f9f84473b0",
  "session_id": "ses_example",
  "occurred_at": "2026-07-18T12:00:00Z",
  "payload": {
    "type": "activity_observed",
    "kind": "verify",
    "status": "succeeded"
  }
}
```

An append is acknowledged only after the JSONL record is flushed and
`sync_data` succeeds. Retrying the same event ID and content is an identity
operation; reusing an event ID with different content is rejected. Startup
replay verifies contiguous sequences, typed JSON, content digests, duplicate
IDs, and the final newline so torn writes fail closed. SSE first replays the
durable suffix and then follows new entries.

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
  --all-features -- --ignored --nocapture
```

The live Mistral contract test reads the exact project `.env` and requires its
canonical `MISTRAL_API_KEY`:

```bash
cargo test -p harness-graph-mistral-adapter \
  --all-features -- --ignored --nocapture
```

The real full-process classification-plus-extraction E2E uses the same
canonical credential and verified local archive:

```bash
cargo test -p harness-graph-cli --test live_mistral \
  live_interpret_classifies_and_extracts_concurrently -- --ignored --nocapture

cargo test -p harness-graph-cli --test live_mistral \
  live_pathfinder_preserves_typed_session_and_activity_citations -- --ignored --nocapture
```

The composed transcript apply test uses real Neo4j and the canonical project
Mistral credential, and is intentionally ignored until the privacy/HMAC gates
are explicitly satisfied:

```bash
op run --env-file=.env.enrichment-op -- \
  cargo test -p harness-graph-cli --test e2e \
  transcript_apply_projects_additively_and_identical_rerun_submits_no_chunks \
  -- --ignored --nocapture
```

That paid composed E2E, the 1/10/50/full archive backfill and identical rerun,
and the live browser proof of a completed enriched graph plus assurance remain
open validation gates in [`plan.md`](plan.md).

Detailed architecture, commands, migration procedures, observability, and
recovery instructions will remain synchronized here as each validated vertical
slice is implemented.
