# Unified implementation plan

## Project

# **HarnessGraph**

### Operational memory, observability, and path optimization for coding agents

HarnessGraph watches coding agents such as Codex, OpenCode, Pi, and Mistral Vibe, converts their low-level events into a typed execution graph, evaluates outcomes and risks, learns which paths work under which conditions, and helps future agents choose better paths.

The core loop:

```text
Observe
  → Validate
  → Classify
  → Project
  → Visualize
  → Evaluate
  → Learn
  → Retrieve
  → Plan
  → Execute again
```

The governing principle:

> **Harnesses report facts. Rust enforces meaning. Neo4j stores experience. Mistral interprets ambiguity. Evidence decides success.**

The authoritative-layer invariant is stricter:

> **Deterministic extraction is the immutable base graph. Mistral enrichment is
> additive, versioned, provenance-marked, and evidence-linked. It never replaces,
> rewrites, deletes, or upgrades a deterministic fact.**

If enrichment is unavailable, invalid, incomplete, or later removed, the verified
deterministic graph must remain complete and queryable exactly as before.

---

# 1. Actual Codex export contract

The input is an exporter archive, not two independent files. The archive root is
resolved from `CODEX_SESSION_RAW_DATA_PATH`; no local absolute path belongs in
source code, tests, or committed configuration.

```text
export root
├── active/<date>/<session-id>/
├── archived/<date>/<session-id>/
├── .staging/                         # never import
└── exporter-level indexes/reports    # discovery metadata only
```

Each published session bundle contains:

```text
metadata.json              provenance and exporter counts
checksums.sha256            integrity manifest
raw/rollout.jsonl           byte-identical canonical source snapshot
records/*.jsonl             the same records split by top-level type
timeline.jsonl              lossy human-oriented projection
timeline.html               rendered view
transcript.md               sensitive rendered transcript
assets/                     sensitive embedded/local/remote references
related/                    optional history and session-index records
```

## Canonical-source rule

The historical importer must use `raw/rollout.jsonl` as its only semantic source.
It must validate `metadata.json` and `checksums.sha256` before parsing.

The other representations have narrower roles:

* `records/*.jsonl` may be used for parity diagnostics, but is not a second
  source of truth;
* `timeline.jsonl` may be used as a sequence/count oracle, but cannot drive the
  graph because it omits native payloads;
* `transcript.md`, HTML, and assets must not be imported by default;
* opt-in transcript enrichment reconstructs typed textual fragments from the
  checksum-verified `raw/rollout.jsonl`; it does not treat `transcript.md` as a
  second semantic source or copy that rendered file into the graph;
* remote asset references must never be fetched automatically.

## Discovery snapshot

The inspected archive snapshot contained:

```text
1,369 session bundles
2,410,017 raw records
6.1 GB of canonical raw JSONL
approximately 21 GB including derived artifacts and assets
1 to 105,813 records per session
```

These counts are observations, not invariants. Runtime discovery and metadata
validation must determine the current population.

## Native record families

The canonical stream currently includes these top-level record families:

```text
session_meta
turn_context
event_msg
response_item
compacted
world_state
inter_agent_communication_metadata
```

Payload variants include ordinary messages and tool calls as well as patch,
command, web, MCP, goal, settings, compaction, rollback, turn-abort, sub-agent,
and inter-agent events. Exporter schema version `1` does not imply that the
native Codex payload schema is frozen: old and new sessions have different
fields and variant sets.

Some bundles omit record families entirely. A session may contain multiple
turns, repeated session metadata, aborted turns, incomplete calls, compaction,
and concurrent sub-agent activity. Absence and partiality are domain states,
not parse failures.

The faithful pipeline is:

```text
Verified session bundle
  → ordered native records
  → typed native variants
  → canonical observations
  → semantic activities
  → idempotent graph commands
```

---

# 2. Final architecture

```text
┌─────────────────────────────────────────────────────────┐
│ Harnesses                                               │
│                                                         │
│ Codex JSONL   OpenCode plugin   Pi extension   Vibe hooks│
└────────┬──────────────┬──────────────┬──────────────┬────┘
         └──────────────┴──────────────┴──────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────┐
│ Rust ingestion boundary                                 │
│ Parse → validate → redact → correlate → deduplicate     │
└──────────────────────────┬──────────────────────────────┘
                           │
             ┌─────────────┴──────────────┐
             ▼                            ▼
 Verified historical source      Append-only live journal
 reference + ingestion receipt       JSONL + payload blobs
             └─────────────┬──────────────┘
                           ▼
                   Typed observations
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│ Semantic kernel                                         │
│ Deterministic rules → Mistral classifier when ambiguous │
└──────────────────────────┬──────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────┐
│ Neo4j experience graph                                  │
│ Execution + context + assurance + risk + learning       │
└───────────────┬──────────────────────────┬──────────────┘
                ▼                          ▼
      Live visualization             Pathfinder
      Timeline + graph               Rig + Mistral
                │                          │
                └────────────┬─────────────┘
                             ▼
                   Better future execution
```

The experience graph has two compositionally separate layers:

```text
verified canonical records ── deterministic Rust morphisms ── authoritative graph

verified transcript text ── local disclosure/redaction gate ── Mistral
                         ── citation validator ── versioned enrichment subgraph
```

Only the second path may contain model interpretation. No command produced by
that path can mutate an authoritative node, relationship, key, status, outcome,
risk, assurance result, ingestion receipt, or evidence edge.

---

# 3. Rust workspace

```text
harness-graph/
├── Cargo.toml
├── crates/
│   ├── domain/
│   ├── protocol/
│   ├── ingestion/
│   ├── correlation/
│   ├── classification/
│   ├── transcript-enrichment/
│   ├── assurance/
│   ├── risk/
│   ├── path-analysis/
│   ├── optimization/
│   ├── planning/
│   ├── graph-port/
│   ├── neo4j-adapter/
│   ├── mistral-adapter/
│   ├── event-journal/
│   ├── api/
│   └── cli/
│
├── adapters/
│   ├── codex-jsonl/
│   ├── codex-timeline/
│   ├── opencode-plugin/
│   ├── pi-extension/
│   └── vibe-hooks/
│
├── apps/
│   └── graph-ui/
│
├── fixtures/
│   ├── codex/
│   ├── opencode/
│   ├── pi/
│   └── vibe/
│
└── deploy/
    └── compose.yaml
```

Dependency direction:

```text
domain
  ↑
application services
  ↑
ports
  ↑
external adapters
```

The `domain` crate must not know about:

```text
serde_json
Neo4j
Rig
Mistral HTTP
Axum
OpenCode
Pi
Codex
Vibe
```

---

# 4. Raw-data quarantine

Raw data cannot be entirely forbidden because external systems communicate using JSON, strings, paths, and numbers.

Instead:

```text
Untrusted external data
        ↓
protocol and adapter crates
        ↓ validate
strong domain types
```

Raw values are allowed only inside:

```text
protocol
ingestion adapters
raw journal
```

Historical imports must not duplicate the existing multi-gigabyte raw archive.
Store a typed `SourceSnapshotRef`, its verified digest, and an ingestion receipt.
The append-only journal is required for live adapters whose events do not yet
have an exporter-owned canonical snapshot.

The archive is sensitive local data:

* never copy raw rollouts, transcripts, instruction bodies, images, or asset
  payloads into this repository;
* never commit absolute source paths;
* never log raw payloads;
* project allowlisted, redacted properties into Neo4j;
* represent payload evidence with a digest and typed source reference;
* use source-safe golden fixtures captured from real records for tests.

Raw-transcript enrichment is an explicit, opt-in external-processing boundary,
not an exception to quarantine. The source archive remains read-only. Raw text
may exist only transiently inside the typed transcript reader and local scanner;
the provider input must be a bounded `LocallySanitizedFragment`. Raw or sanitized
prompt bodies must never be written to this repository, Neo4j, the event journal,
temporary files, stdout, stderr, tracing, telemetry, or error bodies.

The default disclosure scope is `ConversationAndExecution`: user and agent
messages plus textual tool requests/results, commands, patches, errors,
verification output, and completion summaries. System/developer instruction
bodies, hidden reasoning, assets, binary/base64 bodies, credentials, session
tokens, and suspicious high-entropy values are excluded. Instruction-bearing
context requires a separate future opt-in; remote assets remain forbidden.

Mandatory local scanning removes secrets even when raw-transcript processing is
authorized. It detects loaded credential values without logging them, private
keys, bearer/JWT/cookie material, credential URLs, common provider-token formats,
high-entropy assignments, emails, phone numbers, user names, customer identifiers,
IP addresses, and absolute home paths. Stable pseudonyms use a local keyed digest;
scanner uncertainty fails the session closed.

The raw transcript itself is never edited. If the exporter publishes different
canonical bytes, the changed source digest creates a new `SourceSnapshot` and a
new enrichment run. Knowledge can therefore be refreshed without rewriting
historical evidence.

They are forbidden inside:

```text
domain
assurance
risk
optimization
planning
```

Instead of:

```rust
struct Event {
    kind: String,
    payload: serde_json::Value,
}
```

Use:

```rust
pub enum ObservationKind {
    SessionMetadataAsserted,
    ContextAsserted,
    TaskStarted,
    TurnStarted,
    TurnAborted,
    TurnCompleted,
    UserMessageReceived,
    AgentMessageReceived,
    ToolRequested,
    ToolCompleted,
    CommandCompleted,
    PatchApplied,
    TokenUsageObserved,
    ThreadSettingsApplied,
    GoalUpdated,
    ContextCompacted,
    ThreadRolledBack,
    WorldStateAsserted,
    SubAgentActivityObserved,
    InterAgentMessageObserved,
    ErrorObserved,
    VerificationCompleted,
    TaskCompleted,
}
```

Native decoding must use a typed coproduct and preserve forward compatibility at
the adapter boundary:

```rust
pub enum DecodedNativeRecord {
    Known(KnownNativeRecord),
    Unsupported(UnsupportedNativeRecord),
}

pub struct UnsupportedNativeRecord {
    source: SourceRecordRef,
    native_kind: NativeRecordKind,
    payload_digest: PayloadDigest,
}
```

Unsupported records are quarantined and counted. They are not silently dropped,
projected as generic maps, or allowed to leak `serde_json::Value` into the
domain.

And:

```rust
pub struct RunId(Ulid);
pub struct TurnId(Ulid);
pub struct ActivityId(Ulid);
pub struct ContextDigest(Sha256Digest);
pub struct MoneyMicrousd(u64);
pub struct Milliseconds(u64);
pub struct BasisPoints(u16);
```

---

# 5. Typestate ingestion pipeline

```rust
pub struct Received;
pub struct Validated;
pub struct Classified;
pub struct Projected;

pub struct Observation<State, Content> {
    id: ObservationId,
    occurred_at: OccurredAt,
    content: Content,
    state: PhantomData<State>,
}
```

Transitions:

```rust
fn validate<C>(
    observation: Observation<Received, C>,
) -> Result<Observation<Validated, C>, ValidationError>;

async fn classify(
    observation: Observation<Validated, NativeActivity>,
) -> Result<Observation<Classified, SemanticActivity>, ClassificationError>;

async fn project(
    observation: Observation<Classified, SemanticActivity>,
) -> Result<Observation<Projected, SemanticActivity>, ProjectionError>;
```

This makes it impossible to store an unvalidated observation in Neo4j through normal application code.

---

# 6. Category-theory structure

Use category theory as the structural foundation.

## Harness adapters as structure-preserving mappings

```text
CodexEvent    ─┐
OpenCodeEvent ─┼──▶ CanonicalObservation
PiEvent       ─┤
VibeEvent     ─┘
```

Each adapter preserves:

* ordering;
* session identity;
* turn identity;
* call/result correlation;
* parent-child relationships;
* execution status.

## Pipeline as effectful composition

```text
NativeEvent
  → Result<Observation, Error>
  → Future<Result<Activity, Error>>
  → Result<GraphCommands, Error>
```

## Execution histories as a monoid

```text
identity    = empty history
composition = append activity history
```

This supports:

* incremental ingestion;
* replay;
* partial reconstruction;
* streaming projection.

## Graph projection as a homomorphism

```text
project(A followed by B)
=
project(A) followed by project(B)
```

Reprocessing the same journal must reconstruct the same logical graph.

---

# 7. Canonical semantic vocabulary

## Observation

Something directly reported by a harness or operating system.

## Activity

A meaningful operation reconstructed from one or more observations.

```rust
pub enum ActivityKind {
    Inspect,
    Search,
    Read,
    Plan,
    Modify,
    Execute,
    Verify,
    Diagnose,
    Repair,
    RequestPermission,
    Approve,
    Reject,
    Delegate,
    Complete,
}
```

## Execution path

An ordered and causal sequence of activities.

## Outcome

The evidence-backed result of a run.

## Path pattern

A normalized recurring execution strategy.

## Candidate plan

A proposed future path derived from historical evidence.

---

# 8. Core Neo4j nodes

## Execution

```text
Task
Run
Session
Turn
Activity
Message
Agent
Harness
AgentThread
```

## Context

```text
ContextSnapshot
ModelConfiguration
SandboxPolicy
WritableScope
ApprovalPolicy
InstructionSet
SkillVersion
TruncationPolicy
Environment
```

## Operations and artifacts

```text
SourceSnapshot
IngestionReceipt
Tool
ToolCall
ToolResult
Command
Process
Repository
Branch
Commit
File
FileVersion
Diff
Symbol
Compaction
```

## Assurance

```text
Verification
VerificationGate
Evidence
Failure
Finding
Policy
Control
PermissionRequest
PermissionDecision
ResidualConcern
Outcome
```

## Risk

```text
Hazard
RiskExposure
Incident
RiskAssessment
```

## Learning and optimization

```text
TaskClass
PathPattern
ExecutionContext
PerformanceProfile
OptimizationPolicy
CandidatePlan
PlanStep
Cluster
```

## Versioned semantic enrichment

These nodes are a non-authoritative overlay. Their implementation labels use the
existing `HG` prefix, but they remain separate from deterministic graph commands:

```text
EnrichmentRun
TranscriptSpan
NarrativeEpisode
KnowledgeEntity
KnowledgeClaim
KnowledgeRelation
EnrichmentView
```

`TranscriptSpan` stores only a source digest, record-sequence/field-path anchor,
role/kind, byte/token counts, and content digest. It never stores raw transcript
text. `EnrichmentView` is the only mutable enrichment-only selector; it may point
to the latest fully completed run without changing any base node.

---

# 9. Core relationships

## Structure

```text
Task-[:EXECUTED_AS]->Run
Run-[:HAS_SESSION]->Session
Session-[:IMPORTED_FROM]->SourceSnapshot
IngestionReceipt-[:VERIFIED]->SourceSnapshot
SourceSnapshot-[:CONTAINS]->Observation
Session-[:HAS_TURN]->Turn
Session-[:HAS_AGENT_THREAD]->AgentThread
Turn-[:HAS_ACTIVITY]->Activity
Turn-[:RAN_WITH_CONTEXT]->ContextSnapshot
Turn-[:COMPACTED_BY]->Compaction
```

## Activity flow

```text
Activity-[:NEXT]->Activity
Activity-[:CAUSED_BY]->Activity
Activity-[:RETRIED_AFTER]->Activity
Activity-[:FORKED_TO]->Activity
Activity-[:JOINED_FROM]->Activity
```

## Operations

```text
Activity-[:REQUESTED]->ToolCall
ToolCall-[:USES]->Tool
ToolCall-[:PRODUCED]->ToolResult

Activity-[:EXECUTED]->Command
Activity-[:READ]->FileVersion
Activity-[:WROTE]->FileVersion
FileVersion-[:VERSION_OF]->File
```

## Context

```text
ContextSnapshot-[:USES_MODEL_CONFIG]->ModelConfiguration
ContextSnapshot-[:ENFORCED_BY]->SandboxPolicy
SandboxPolicy-[:ALLOWS_WRITE_TO]->WritableScope
ContextSnapshot-[:USES_APPROVAL_POLICY]->ApprovalPolicy
ContextSnapshot-[:INCLUDES_INSTRUCTION_SET]->InstructionSet
ContextSnapshot-[:MADE_AVAILABLE]->SkillVersion
ContextSnapshot-[:USES_TRUNCATION_POLICY]->TruncationPolicy
```

## Assurance and risk

```text
Activity-[:FAILED_WITH]->Failure
Activity-[:VERIFIED_BY]->Verification
Verification-[:PRODUCED_EVIDENCE]->Evidence
Evidence-[:SUPPORTS]->Outcome

Activity-[:EXPOSED_TO]->Hazard
RiskExposure-[:INSTANCE_OF]->Hazard
RiskExposure-[:MITIGATED_BY]->Control
RiskExposure-[:MATERIALIZED_AS]->Incident

Run-[:RESULTED_IN]->Outcome
Run-[:COMPLETED_WITH]->ResidualConcern
```

## Learning

```text
Run-[:FOLLOWS]->PathPattern
Run-[:RAN_UNDER]->ExecutionContext

PathPattern-[:HAS_PROFILE]->PerformanceProfile
PerformanceProfile-[:FOR_CONTEXT]->ExecutionContext

Task-[:OPTIMIZED_BY]->OptimizationPolicy
Task-[:PLANNED_AS]->CandidatePlan
CandidatePlan-[:SUPPORTED_BY]->Run
CandidatePlan-[:AVOIDS]->PathPattern
PlanStep-[:REALIZED_AS]->Activity
```

## Additive enrichment

```text
SourceSnapshot-[:HAS_ENRICHMENT_RUN]->EnrichmentRun
EnrichmentRun-[:PRODUCED_EPISODE]->NarrativeEpisode
EnrichmentRun-[:PRODUCED_CLAIM]->KnowledgeClaim
EnrichmentRun-[:PRODUCED_ENTITY]->KnowledgeEntity
EnrichmentRun-[:PRODUCED_RELATION]->KnowledgeRelation

NarrativeEpisode-[:CITES_ACTIVITY]->Activity
NarrativeEpisode-[:NEXT_EPISODE]->NarrativeEpisode
KnowledgeClaim-[:SUPPORTED_BY]->TranscriptSpan
KnowledgeClaim-[:ABOUT]->KnowledgeEntity
KnowledgeClaim-[:CORROBORATED_BY]->Observation
KnowledgeRelation-[:SUBJECT]->KnowledgeEntity
KnowledgeRelation-[:OBJECT]->KnowledgeEntity
KnowledgeRelation-[:SUPPORTED_BY]->TranscriptSpan
TranscriptSpan-[:FROM_SOURCE]->SourceSnapshot
TranscriptSpan-[:MAPS_TO]->Observation
EnrichmentView-[:SELECTS]->EnrichmentRun
```

Model-supplied semantic relations are reified as `KnowledgeRelation` nodes with
a closed predicate enum. Mistral cannot create arbitrary Neo4j labels,
relationship types, Cypher, IDs, source references, or base-graph commands.
Reification keeps provider/model/prompt/schema provenance and citations attached
to every assertion instead of hiding mutable interpretation in edge properties.

---

# 10. Context snapshot deduplication

`turn_context` is an assertion event, not the context object itself. Its payload
may contain volatile assertion identity as well as stable execution policy.
Older records may carry instruction bodies while newer records may instead carry
component hashes, workspace roots, permission profiles, multi-agent settings,
date, and timezone.

Decode the native schema first, then separate volatile assertion fields from the
stable semantic context:

```rust
pub struct TurnContextAssertion {
    turn_id: TurnId,
    asserted_at: OccurredAt,
    context: CanonicalExecutionContext,
}

pub enum NativeContextShape {
    LegacyInstructionBearing(LegacyTurnContext),
    ModernComponentBased(ModernTurnContext),
}
```

`CanonicalExecutionContext` may contain typed references to model, sandbox,
approval policy, workspace scope, collaboration mode, and instruction digests.
It must not contain `turn_id`, assertion timestamp, human summary, or other
fields that change without changing execution semantics.

Canonicalize and hash only the stable context:

```rust
fn context_digest(
    context: &CanonicalExecutionContext,
) -> ContextDigest;
```

Then:

```text
ContextSnapshot ID = SHA-256(canonical context)
```

Neo4j:

```cypher
MERGE (context:ContextSnapshot {digest: $digest})
```

Repeated assertions update the relationship, while a real policy or environment
change produces a new context node:

```text
Turn-[:RAN_WITH_CONTEXT {
    first_seen_at,
    last_seen_at,
    assertion_count
}]->ContextSnapshot
```

Full instruction bodies remain only in the sensitive source archive. Neo4j may
contain their digest and a typed source reference, but not a copied body.

Neo4j should contain:

```text
digest
redacted summary
instruction counts
estimated token counts
schema version
payload reference
```

---

# 11. Correlation and deduplication

## Mirror-message deduplication

Codex may expose the same logical message as:

```text
event_msg/agent_message
response_item/message
```

Collapse them when:

```text
same timestamp
same actor
same content digest
```

Preserve both raw observations but create one logical `Message`.

## Tool call pairing

Pair calls and results by the native call ID:

```text
native call ID → native result ID
```

Do not use nearest-neighbor pairing for canonical archive imports. The native
stream carries call IDs, and guessed correlation would turn an interpretation
into a false fact.

Incomplete and interrupted streams are valid:

```rust
pub enum ToolCallLifecycle {
    Pending(PendingToolCall),
    Completed(CompletedToolCall),
    Interrupted(InterruptedToolCall),
    OrphanedResult(OrphanedToolResult),
}
```

Re-importing a later snapshot may transition `Pending` to `Completed`; it must
not create a second call. Illegal reverse transitions are rejected.

## Semantic episodes

Combine:

```text
visible reasoning summary
agent progress message
tool call
tool result
```

into one semantic activity when appropriate.

Example:

```text
reasoning: locate config
command: ls ~/.config/nvim
result: init.lua exists
```

becomes:

```text
Activity::Inspect
target: Neovim configuration
status: succeeded
```

## Projection safety and scale

The archive contains millions of records and individual sessions may exceed one
hundred thousand records. The importer must stream JSONL and never materialize a
whole archive or large session in memory.

Projection requirements:

* create uniqueness constraints before ingestion;
* use bounded, configurable Neo4j transaction batches;
* checkpoint only after a transaction commits;
* resume from the last committed source sequence;
* use `SessionId + SourceDigest + RecordSequence` as the observation identity;
* treat an unchanged source digest as an identity/no-op import;
* treat active-to-archived relocation as the same session, not a new run;
* persist typed ingestion receipts with counts for parsed, projected,
  quarantined, and failed records;
* keep retry behavior idempotent.

Minimum uniqueness model:

```text
Session(session_id)
SourceSnapshot(source_digest)
Observation(source_digest, record_sequence)
ContextSnapshot(context_digest)
ToolCall(session_id, native_call_id)
```

---

# 12. Classification strategy

## Deterministic first

Classification precedence is:

```text
typed native event semantics
→ structured tool/result status
→ deterministic command rules
→ Mistral only when ambiguity remains
```

Do not parse command text to rediscover facts already present in structured
`event_msg` or `response_item` payloads.

Examples:

```text
rg, grep, find             → Search
cat, sed, read_file        → Read
apply_patch, write_file    → Modify
cargo test, pytest         → Verify
cargo check, clippy        → Verify
failed check               → Diagnose trigger
edit after failed check    → Repair
permission request         → RequestPermission
```

## Mistral only for interpretation and additive enrichment

Use Mistral through Rig for:

* task classification;
* unclear command purpose;
* custom script intent;
* root-cause hypothesis;
* activity summarization;
* semantic similarity explanation;
* transcript-derived knowledge claims and entities;
* evidence-linked semantic relations;
* candidate-plan generation.

Implemented task classification consumes only a validated source-safe
`TaskBrief` and returns a closed `TaskCategory`, coarse semantic confidence,
and bounded explanation. It does not replace the deterministic activity
classifier. That existing path remains source-safe.

The new transcript-enrichment path may read meaningful raw conversation and
execution text from the verified canonical rollout under an explicit disclosure
authorization. It must pass the text through the local secret/PII scanner before
provider transfer, preserve exact source anchors, and store only validated,
paraphrased semantic output. This opt-in path does not permit raw invocation text
to enter the base classifier, graph, logs, fixtures, or repository.

Use strict structured output:

```rust
pub struct ClassifiedIntent {
    kind: ActivityKind,
    purpose: ActivityPurpose,
    target: Option<ActivityTarget>,
    confidence: ClassificationConfidence,
    explanation: ClassificationExplanation,
}
```

The model cannot invent new enum values.

---

# 13. Rig and Mistral boundaries

```rust
#[async_trait]
pub trait SemanticClassifier {
    async fn classify(
        &self,
        context: ClassificationContext,
    ) -> Result<ClassifiedIntent, ClassificationError>;
}

#[async_trait]
pub trait Pathfinder {
    async fn propose(
        &self,
        task: PlanningContext,
        precedents: NonEmptyVec<PrecedentPath>,
    ) -> Result<CandidatePlan, PlanningError>;
}
```

Implementation:

```rust
pub struct RigMistralClassifier {
    // Rig/Mistral implementation hidden here.
}
```

No Rig or Mistral types leave the adapter crate.

The implemented narrative boundary keeps evidence coverage deterministic:

```text
typed deterministic activities
→ Rust-owned contiguous citation groups
→ Mistral structured labels
→ citation-complete NarrativeSummary
```

Missing model group labels do not remove evidence. They become explicit
`deterministic_fallback` kind-only labels, while every Mistral-supplied label is
marked `mistral`. The provider is fixed by the Rig Mistral client and the model
name is validated as a Mistral-hosted family before construction.

Task classification and narrative extraction now compose as a bounded product:

```text
TaskClassificationRequest ── Mistral native JSON schema ──┐
                                                          ├─ synchronized result
NarrativeRequest ─────────── Mistral native JSON schema ──┘
```

Both I/O-bound morphisms begin concurrently through `tokio::join!` and share a
typed semaphore whose default bound is two. The join waits for both cost-bearing
calls to settle instead of cancelling a sibling on first failure. Each branch
uses one model turn, deterministic sampling parameters, and a 90-second bound.
Only two independently validated values form `SynchronizedInterpretation`;
classification and extraction usage remain separately attributable.

## Raw-transcript knowledge enrichment boundary

### Category-theory framing

The deterministic graph and enrichment graph are separate categories. The
enrichment functor maps verified transcript spans and deterministic activities to
versioned semantic assertions while preserving evidence citations:

```text
VerifiedSessionBundle
  → SensitiveTranscriptFragment
  → LocallySanitizedFragment
  → BoundedTranscriptChunk
  → MistralStructuredClaims
  → CitationValidatedEnrichment
  → AdditiveEnrichmentGraphCommand
```

Each fallible or asynchronous morphism composes through a typed `Result`. The
identity operation is an unchanged source plus unchanged disclosure, redaction,
chunking, provider, model, prompt, and output-schema versions. A change to any of
those objects creates a new parallel run; it never updates the old run or base
graph. Selecting a completed run for display is a read-layer natural
transformation, not a mutation of authoritative objects.

### Typed disclosure and lifecycle

Raw strings and unvalidated maps cannot cross the boundary. Required types
include:

```text
TranscriptDisclosureScope
DisclosureAuthorization
SensitiveTranscriptFragment
LocallySanitizedFragment
TranscriptChunkId
TranscriptSpanRef
RedactionReceipt
RedactionPolicyVersion
ChunkingPolicyVersion
EnrichmentRunId
EnrichmentFingerprint
EnrichmentSchemaVersion
PromptVersion
KnowledgeKind
KnowledgeConfidence
EpistemicStatus
KnowledgeClaim
KnowledgeEntity
KnowledgePredicate
KnowledgeRelation
EvidenceCitation
EnrichmentRunStatus
```

`EnrichmentRunStatus` is a closed state machine:

```text
Planned → Scanned → Submitted → Validated → Projected → Completed
            └────→ Blocked
                       Submitted → RetryableFailed | TerminalFailed
                       Validated → TerminalFailed
                       Projected → TerminalFailed
Completed → Superseded
```

Only `Completed` runs are query-visible. A partial or failed run cannot become
selected, cannot affect ingestion receipts, and cannot hide a previous completed
enrichment.

### Extraction contract

Mistral is the only foundation-model provider. The implementation reads the exact
`MISTRAL_API_KEY` from the project environment, validates that `MISTRAL_MODEL` is
a Mistral-hosted non-Labs model, and uses Rig only inside the infrastructure
adapter. Transcript text is quoted as untrusted evidence, never interpreted as
instructions; the request exposes no tools and accepts only native JSON-schema
structured output.

Closed knowledge kinds include goal, decision, constraint, artifact, dependency,
failure, root-cause hypothesis, repair, verification, risk, lesson, and open
question. Closed relation predicates include `USES`, `MODIFIES`, `DEPENDS_ON`,
`CAUSES`, `BLOCKED_BY`, `RESOLVES`, `VERIFIES`, `PRODUCES`, `CONTRIBUTES_TO`,
`CONTRADICTS`, and `RELATED_TO`.

Every claim and relation must cite one or more supplied transcript-span tokens.
Rust rejects unknown, missing, duplicated, conflicting, or out-of-scope citations;
empty/oversized text; unknown enum values; invalid relation endpoints; copied
secret patterns; and unsupported causal certainty. A Mistral causal statement is
a model inference until a deterministic observation explicitly corroborates it.
Mistral can never determine or override an outcome, risk, assurance result,
verification status, source identity, or activity status.

### Chunking, parallelism, and reduction

The reader streams `raw/rollout.jsonl` in bounded memory, preserves record and
turn boundaries, and redacts before chunking. Oversized textual fields split only
at safe UTF-8 paragraph boundaries; redaction placeholders never split. Each
fragment receives an opaque citation token derived from its typed source record,
field path, and part index.

Independent chunk extractions run concurrently behind the existing typed Mistral
semaphore (`MISTRAL_MAX_CONCURRENCY`, validated in the existing range of one to
four; default two) and settle all results. Session consolidation runs only after
all chunk outputs validate and receives validated claims rather than raw
transcript text. Classification and transcript extraction run concurrently when
independent, but projection waits for both required branches to settle.

The enrichment fingerprint is content-addressed from:

```text
namespace
+ source digest
+ transcript projection digest
+ disclosure scope and authorization policy digest
+ redaction policy version
+ chunking policy version
+ provider and model
+ prompt version/digest
+ output schema version
```

An exact completed fingerprint is a no-op. Chunk receipts allow retry to resume
without repeating validated cost-bearing calls. Only typed transient `429`, `5xx`,
timeout, or transport failures retry with bounded backoff and `Retry-After`;
privacy, checksum, scanner, schema, citation, secret-echo, or model-family
failures stop closed.

### Privacy and provider mode

The command is disabled by default and requires a versioned
`DisclosureAuthorization` naming session/archive scope, disclosure scope, policy
digest, and authorization identity/time. A dry run reports only eligible/blocked
session counts, redaction-category counts, chunk/token estimates, expected API
calls, and cost estimate—never text, paths, or credentials.

The initial implementation uses stateless Mistral chat completions through the EU
regional endpoint and verifies the account's training/retention controls before
the first real raw-transcript call. It does not use Files, Agents, Conversations,
feedback APIs, or stateful Batch processing. Batch may be evaluated only as a
later explicit data-governance decision because the EU regional endpoint does not
provide the stateful Batch/Files APIs.

No request/response-body logging is allowed. Provider errors are wrapped before
they can echo request content. The persisted run records only model/version
metadata, token/cost usage, timestamps, status, source/chunk counts, redaction
category counts, and citation-safe semantic output.

---

# 14. Outcome engine

The model does not decide success.

```rust
pub enum OutcomeClass {
    VerifiedSuccess,
    UnverifiedCompletion,
    Failed,
    Inconclusive,
    Cancelled,
}
```

## Verified success

```text
required verification passed
AND verification applies to current candidate digest
AND no relevant edit occurred afterward
AND protected evidence was not weakened
```

## Unverified completion

```text
agent completed
BUT no fresh verification supports the final state
```

## Conditional success

Useful for the Codex sample:

```text
verification passed
BUT unresolved semantic concern remained
```

Represent this as:

```rust
pub struct RunOutcome {
    class: OutcomeClass,
    verification: VerificationStatus,
    unresolved_concerns: Vec<ResidualConcernId>,
}
```

---

# 15. Risk engine

Use:

```text
Hazard
  → Exposure
  → Control
  → Incident
```

Implement these first:

```text
1. Unverified final edit
2. Protected-file modification
3. Repeated failing command
4. Tool-call loop
5. Scope drift
6. Secret exposure
7. Destructive command
8. Permission escalation
9. Cross-tenant access
10. Retry without idempotency
11. Budget exceeded
12. Missing or incomplete observation stream
13. Sandbox-policy mismatch
14. Network-policy violation
15. Context changed mid-run
16. Residual concern at completion
```

Risk exposure:

```rust
pub struct RiskExposure {
    hazard: HazardId,
    activity: ActivityId,

    likelihood: Likelihood,
    severity: Severity,
    detectability: Detectability,

    blast_radius: BlastRadius,
    reversibility: Reversibility,

    provenance: Provenance,
    confidence: RiskConfidence,
}
```

---

# 16. Optimization data

Do not store one universal edge weight.

Store independent dimensions.

## Activity measurements

```text
started_at
ended_at
active_duration_ms
wait_duration_ms

input_tokens
output_tokens
model_cost_microusd
tool_cost_microusd
compute_cost_microusd

attempt_number
is_retry
is_rework

risk_level
side_effect_level
```

## Model invocation

```text
model
latency
input tokens
output tokens
cached tokens
cost
retry number
prompt digest
response digest
```

## Performance profile

For each:

```text
PathPattern × ExecutionContext
```

store:

```text
sample count
successes
failures
verified-success probability
confidence lower bound

duration p50
duration p95
cost mean
cost p95

average attempts
average tool calls
average human attention

regression rate
rollback rate
policy-violation rate
unverified-completion rate
```

## Optimization policy

```rust
pub struct OptimizationPolicy {
    maximum_p95_duration: Option<Milliseconds>,
    maximum_expected_spend: Option<MoneyMicrousd>,

    minimum_verified_success: SuccessProbability,
    maximum_risk: RiskProbability,

    required_verifications: NonEmptyVec<VerificationKind>,
    objective: OptimizationObjective,
}
```

Optimization order:

```text
1. Context compatibility
2. Evidence sufficiency
3. Correctness threshold
4. Safety threshold
5. Time and cost constraints
6. Pareto frontier
7. Policy-specific ranking
```

---

# 17. Derived metrics

Calculate:

```text
expected cost per verified success
expected time to verified success
verified tasks per dollar
human minutes per verified success
rework ratio
tool-loop rate
verification freshness
scope expansion ratio
risk-adjusted cost
critical-path duration
parallelism efficiency
```

Never permanently label a path “optimal.”

Optimality depends on:

```text
current task
current constraints
current context
current optimization policy
```

---

# 18. Path extraction and clustering

Detailed path:

```text
Read
Read
Search
Edit
Test failed
Read
Edit
Test passed
```

Normalized path:

```text
Inspect
→ Modify
→ VerifyFailed
→ Diagnose
→ Repair
→ VerifyPassed
```

## Version 1

Exact typed signatures:

```text
inspect>modify>verify_failed>diagnose>repair>verify_passed
```

## Version 2

Weighted edit-distance between typed paths.

## Version 3

Context-aware similarity:

```text
path structure
task class
failure class
language
repository size
risk level
harness
model
sandbox
verification strategy
```

## Version 4

Mistral embeddings for task and failure summaries.

Embeddings should supplement, not replace, structural similarity.

---

# 19. Pathfinder agent

For a new task:

```text
1. Classify task and execution context.
2. Query successful and failed precedent paths.
3. Remove incompatible contexts.
4. Apply correctness and risk constraints.
5. Find Pareto-optimal candidates.
6. Ask Mistral to produce a bounded plan.
7. Require the plan to cite historical run IDs.
```

Candidate plan:

```rust
pub struct CandidatePlan {
    steps: NonEmptyVec<PlannedStep>,
    precedents: NonEmptyVec<RunId>,
    avoided_patterns: Vec<PathPatternId>,
    confidence: PlanConfidence,
    required_verification: NonEmptyVec<VerificationRequirement>,
}
```

The plan must explain:

```text
which prior runs support it
which failed patterns it avoids
what verification is required
where uncertainty remains
```

---

# 20. Visualization

Build five synchronized views.

## Timeline

```text
09:42 Read file
09:43 Run command
09:43 Command failed
09:44 Adapt environment
09:45 Verification passed
```

## Causal graph

```text
Sandbox failure
      ↓ triggered
Environment adaptation
      ↓ enabled
Successful verification
```

## Context panel

```text
Model: Mistral / Codex model
Sandbox: workspace-write
Network: disabled
Approval: on-request
Effort: xhigh
Skills available: ...
```

## Assurance panel

```text
Final candidate verified
Protected paths changed
Required approvals present
Residual concerns
Risk exposures
```

## Path comparison

```text
Run A: fast, unverified
Run B: slower, verified
Run C: repeated failure loop
```

Use:

```text
Axum
Server-sent events
Neo4j NVL or Cytoscape.js
```

---

# 21. Adapter order

## First: Codex historical importer

Consume verified exporter session bundles:

```text
metadata/checksum verifier
→ canonical raw rollout stream
→ typed native decoder
→ canonical observation adapter
```

Use the categorized records and timeline only for parity checks. Build
source-safe golden fixtures from real sessions; never commit a raw session,
transcript, instruction body, or asset.

## Second: OpenCode live adapter

Use the OpenCode plugin API to stream:

```text
session events
tool before/after
file edits
commands
permissions
idle/completion
```

## Third: Pi extension

Map Pi’s richer tool, turn, agent, and session events to the same protocol.

## Fourth: Vibe hooks

Use:

```text
pre_tool
post_tool
post_agent
```

The rest of the system remains unchanged.

---

# 22. Implementation phases

## Phase 0: discover and verify the export archive

Deliver:

```text
environment-backed archive-root resolver
active/archived session discovery
staging and derived-artifact exclusion
metadata parser
checksum verifier
session selector
source-safe fixture manifest
```

Exit criterion:

```text
published bundles are discovered without reading transcript or asset content
corrupt, unstable, or incomplete bundles fail before semantic parsing
the same session ID cannot be duplicated by active/archived relocation
```

## Phase 1: stream and type canonical Codex records

Deliver:

```text
streaming raw/rollout.jsonl parser
typed top-level and payload variants
unsupported-record quarantine
typed observations
context deduplication
typed partial tool-call lifecycle
ingestion receipts and checkpoints
```

Exit criterion:

```text
the 621-record golden session is reproduced exactly at the observation boundary
its 66 repeated assertions map to one stable semantic context
a modern multi-turn/multi-agent fixture parses without silent record loss
completed, pending, interrupted, and orphaned calls remain distinguishable
re-importing an unchanged source is a no-op
```

## Phase 2: build the execution graph

Deliver:

```text
Task
Run
Turn
Activity
ToolCall
Command
File
Failure
Verification
Outcome
ContextSnapshot
SourceSnapshot
IngestionReceipt
AgentThread
Compaction
```

Exit criterion:

```text
the Neovim session is replayable as a graph
uniqueness constraints prevent duplicate projection
an interrupted session remains replayable without fake completion
```

Bulk-import evidence (2026-07-18): the CLI now provides bounded concurrent
`import-all` execution over active, archived, or deduplicated all-session
catalogs. Every bundle still passes its complete checksum manifest before
projection. A typed Neo4j completion probe skips only an exact source digest
whose namespace, expected count, completed receipt, total, and
known-plus-quarantined count invariants agree. Session failures settle
independently, progress is emitted without raw payloads or paths, and any
partial bulk result exits nonzero after writing its structured summary. The
same command is therefore safe to resume without treating partial work as
success. A full-process real-Neo4j regression proves nonzero all-results-settle
behavior, repair and rerun, and two distinct sessions retaining separate
`IMPORTED_FROM` provenance while sharing one content-addressed source snapshot.
The same regression includes a one-record metadata-only source: raw ingestion
completes with a typed `insufficient_semantic_evidence` analysis result while
Neo4j receives no fabricated activity, outcome, or path nodes.

Live bulk evidence (2026-07-18): an initial four-way archived sweep exposed two
real composition failures. Older Codex archives report one command result
through both `event_msg/exec_command_end` and
`response_item/function_call_output`; the correlator now accumulates these
mirrored observations associatively, preserves both evidence references,
prefers a known outcome over an indeterminate mirror, and still rejects a true
success/failure contradiction. Concurrent Neo4j transactions also contended on
shared namespace-scoped nodes, so the adapter now serializes mutation
transactions while checksum verification, decoding, and analysis remain
bounded and concurrent. After those repairs, all 434 archived sessions settled
with zero failures: 79 pending sources completed and 355 exact completed
snapshots were skipped. The configured namespace then contained 125,531 typed
observations.

Full archive completion evidence (2026-07-18): the first active sweep settled
all 935 sessions and exposed four valid metadata-only snapshots at the
assurance boundary. They had one known record each but no activity evidence,
so the importer now records typed semantic unavailability and completes raw
ingestion without inventing outcomes or paths. The resumable rerun imported
those four snapshots, skipped 931 exact receipts, and finished with zero
failures. A final deduplicated all-scope proof discovered 1,369 sessions,
reported all 1,369 already complete, imported zero, and failed zero. The
configured Neo4j namespace contains 2,410,017 observations, matching the
verified active-plus-archived record inventory. The release live API was then
started on an available loopback port and returned a ready health response.

## Phase 3: deterministic semantic compression

Deliver:

```text
621 canonical raw records
      ↓
deterministic, evidence-preserving semantic episodes
      ↓
Mistral macro-summary of roughly 15 to 25 narrative activities
```

The deterministic episode count is source- and behavior-dependent, not a fixed
acceptance threshold. The current 621-record golden snapshot produces 56
stable episodes from 89 native-ID tool calls. Further compression into roughly
15 to 25 items belongs to the Mistral interpretation layer and must not erase,
merge, or rewrite the underlying evidence graph.

Exit criterion:

```text
the deterministic episode sequence and path signature are stable on re-import
every macro-summary item cites one or more deterministic activity IDs
Inspect
→ ExecuteFailed
→ AdaptEnvironment
→ VerifyPassed
→ Diagnose
→ Modify
→ Verify
→ Escalate
→ Install
→ FinalVerify
→ CompleteWithConcern
```

Implemented evidence (2026-07-18): the real 621-record snapshot deterministically
produces 56 episodes, then 19 narrative macro-activities covering all 56 unique
activity IDs exactly once. Model omissions are retained as provenance-marked
deterministic fallbacks rather than hidden or fabricated labels.

Synchronized-provider evidence (2026-07-18): a real full-process E2E used the
canonical project `MISTRAL_API_KEY`, classified one source-safe task while
extracting a verified 50-activity session, returned 17 ordered narrative groups,
covered all 50 activity citations exactly once, and retained separate nonzero
classification and extraction usage. The same test verifies the provider/model
boundary, closed classification enums, explanation bound, and concurrency of
two without asserting brittle wall-clock speed or exposing raw payloads.

## Phase 3A: additive raw-transcript knowledge enrichment

This phase expands semantic meaning without changing Phase 0–3 output. Its
first regression assertion snapshots the deterministic nodes, relationships,
keys, properties, outcomes, risks, receipts, and evidence before enrichment;
the same snapshot must remain logically identical afterward.

### Milestone 3A.0: governance and dry-run inventory

Deliver:

```text
typed disclosure scope and authorization
default-off transcript-enrichment configuration
EU stateless Mistral transport policy
privacy-control preflight
enrich-transcripts --dry-run
enrich-all-transcripts --dry-run
eligible/blocked session and token/cost inventory
```

Default scope is `ConversationAndExecution`; instruction bodies, hidden reasoning,
assets, binary content, and secrets remain excluded. The dry run must cover all
1,369 currently verified sessions without provider or Neo4j mutation and mark
metadata-only sessions as typed skips.

E2E gate: a real verified archive scan proves checksums, scope selection, bounded
memory, source-safe reporting, and zero Mistral/Neo4j writes. Commit and push this
milestone only after the focused tests and E2E pass.

### Milestone 3A.1: typed transcript extraction

Deliver:

```text
streaming canonical-text extractor
closed textual record classes
source sequence + field-path + role/turn/call anchors
oversized/binary/asset rejection
deterministic bounded chunker
opaque citation tokens
```

E2E gate: real temporary copies of captured source-safe records cover ordinary,
multi-turn, multi-agent, tool, command, patch, error, Unicode, huge-field, and
prompt-injection cases without using `transcript.md` as semantic input. Commit
and push only after focused tests and E2E pass.

### Milestone 3A.2: mandatory local redaction

Deliver:

```text
secret and PII scanner
local keyed stable pseudonyms
typed redaction receipts
scanner-uncertainty blocker
safe Debug and source-safe errors
provider-body logging prohibition
```

E2E gate: canary credentials, private keys, tokens, credential URLs, emails,
phones, IPs, home paths, user/customer identifiers, and high-entropy assignments
are absent from approved chunks, logs, CLI output, errors, and every Neo4j string
property. Scanner failure blocks the session. Commit and push only after focused
tests and E2E pass.

### Milestone 3A.3: Mistral structured extraction

Deliver:

```text
Mistral-only structured-output DTOs
closed knowledge/entity/predicate enums
bounded concurrent chunk map calls
validated-claim reduction without raw transcript replay
citation, size, endpoint, confidence, and secret-echo validation
typed usage/cost attribution
retry and all-results-settle behavior
```

E2E gate: one explicitly authorized, non-sensitive real session travels through
the real Mistral API using the project's `MISTRAL_API_KEY`; every returned claim
and relation resolves to supplied citations, transcript text is treated only as
data, and no request body appears in output or telemetry. A contract-faithful
local HTTP server separately proves timeout, `429`, `5xx`, malformed-schema, and
prompt-echo error semantics without mocks. Commit and push only after focused
tests and E2E pass.

### Milestone 3A.4: additive Neo4j projection and API

Deliver:

```text
new enrichment-only uniqueness constraints and indexes
HGEnrichmentRun + HGTranscriptSpan
HGNarrativeEpisode + HGKnowledgeEntity
HGKnowledgeClaim + HGKnowledgeRelation + HGEnrichmentView
separate EnrichmentGraphCommand/EnrichmentProjector boundary
latest-completed enrichment query with deterministic fallback
citation-aware enriched API response
```

The model output must never enter the existing deterministic `GraphCommand`
family. UI/API labels use enrichment display fields when a completed run is
selected and otherwise fall back to deterministic kind/status; internal `key`
properties are never repurposed as human labels.

E2E gate: a real Neo4j run performs `import → enrich → query`, proves the full
base-graph snapshot unchanged, resolves every citation, hides partial runs,
repeats the identical fingerprint as a no-op, and creates a parallel version
when model/prompt/schema/redaction/chunking policy changes. Commit and push only
after focused tests and E2E pass.

### Milestone 3A.5: pilot, bounded backfill, and enriched UI

Rollout order:

```text
one reviewed low-risk session
→ 10-session settlement
→ 50-session settlement
→ all eligible verified sessions
→ identical full rerun proving every completed fingerprint is skipped
```

Use bounded session/chunk concurrency, checkpoint only completed chunk/run
transactions, settle every session, and exit nonzero when any session is blocked
or fails. Reconcile request, token, and cost totals. Failed or partial enrichment
leaves the base graph and previous selected enrichment untouched.

The UI adds semantic title, summary, entity/claim/relation views, confidence and
epistemic-status badges, provider/model/prompt-version provenance, and clickable
source citation anchors. Authorized local source resolution may display an
excerpt on demand; raw transcript text is never stored in Neo4j.

E2E gate: the real Mistral + real Neo4j full workflow proves resumability,
idempotency, all-results-settle behavior, stable deterministic counts, no secret
or raw-text persistence, and deterministic UI fallback. Commit and push only
after all focused, workspace, security, and E2E checks pass.

## Phase 4: risk and assurance

Deliver:

```text
unverified-final-edit detector
tool-loop detector
sandbox mismatch
permission escalation
residual concern
incomplete-observation-stream finding
```

Exit criterion:

```text
each finding links to supporting observations and source digests
```

## Phase 5: visualization

Deliver:

```text
timeline view
causal graph
context panel
assurance panel
ingestion/quarantine diagnostics
```

Exit criterion:

```text
the golden session can be inspected without exposing raw sensitive payloads
```

## Phase 6: path profiles

Deliver:

```text
path signatures
execution contexts
outcome profiles
time and cost distributions
```

Exit criterion:

```text
compare successful and failed path families
```

## Phase 7: Pathfinder

Deliver:

```text
Rig tools
Mistral planner
Neo4j precedent queries
typed CandidatePlan
```

Exit criterion:

```text
new task retrieves and cites a successful precedent
```

Implemented evidence (2026-07-18): the typed `PrecedentReader` selects only
`verified_success` plus `fresh` paths from Neo4j. The real Pathfinder E2E
retrieved session `019c8b3b-2aa8-7183-ba61-379f5b0af31c`, generated five ordered
steps through Rig/Mistral, and validated every session and activity citation
against that retrieved precedent before returning the candidate plan.

## Phase 8: live OpenCode capture

Deliver:

```text
OpenCode plugin
Rust ingestion endpoint
append-only live journal
SSE graph updates
```

Exit criterion:

```text
new typed activities appear while OpenCode works
replay of the live journal reconstructs the same logical graph
```

Implemented journal/API evidence (2026-07-18): a typed JSONL journal assigns
contiguous durable sequences, hashes every event, calls `sync_data` before
acknowledgement, treats exact event retries as identity morphisms, rejects
identity conflicts, and fails closed on torn or corrupt replay. The Axum API
provides bounded POST ingestion, cursor replay, and SSE. Focused tests use real
temporary files, and both router-level and spawned-CLI E2E tests use real TCP
sockets. OpenCode hook packaging and graph projection remain pending in this
phase.

## Phase 9: Pi and Vibe adapters

Only after the shared domain works.

---

# 23. Hackathon MVP

Build this required golden path first:

```text
1. Resolve the archive root from the environment.
2. Select one external golden session through a typed fixture manifest.
3. Validate metadata and every declared checksum.
4. Stream its canonical raw rollout without copying it into the repository.
5. Decode all records into known or explicitly quarantined variants.
6. Deduplicate the 66 repeated context assertions by semantic context.
7. Convert the noisy history into a typed execution path.
8. Store the path, context, provenance, and ingestion receipt in Neo4j.
9. Re-import it and prove the graph is unchanged.
10. Visualize failures, recoveries, verification, and context.
```

Then add these stretch goals:

```text
1. Select two additional real historical sessions, or sanitized golden fixtures
   captured from real sessions:
   - an unsafe or unverified shortcut;
   - a repeated failure loop.
2. Ask Pathfinder for a plan for a similar task.
3. Require Pathfinder to select and cite the verified precedent.
4. Start a live OpenCode run and append activities through the live journal.
5. Let final verification update the outcome and path profile.
```

Do not fabricate successful, unsafe, or failing runs merely to make the demo
work. Every displayed path must be backed by real, source-referenced evidence or
a contract-faithful sanitized fixture captured from it.

---

# 24. Demo story

## Scene 1: raw complexity

```text
621 Codex records
66 repeated context snapshots
```

## Scene 2: semantic compression

```text
Inspect
→ Fail
→ Adapt
→ Verify
→ Diagnose
→ Repair
→ Escalate
→ Install
→ Verify
```

## Scene 3: explanation

Click the initial verification failure:

```text
Cause:
Neovim tried to write outside the workspace.

Context:
workspace-write sandbox
network disabled

Recovery:
redirect XDG state to /tmp
```

## Scene 4: learning

Show three evidence-backed path patterns from selected historical sessions:

```text
Unsafe shortcut
Verified repair
Failure loop
```

## Scene 5: planning

Ask:

```text
How should the next agent test and repair
a deprecated configuration under restricted sandboxing?
```

Pathfinder answers:

```text
Use the verified temporary-copy path.
Avoid repeated direct execution.
Require final verification of the installed target.
```

## Scene 6: live execution stretch goal

OpenCode begins working and the graph grows live.

---

# 25. Definition of done

```text
[x] The archive root is resolved from typed configuration, not hardcoded.
[x] Only published active/archived session bundles are discovered.
[x] Metadata and checksums are validated before parsing.
[x] Canonical raw rollouts stream without whole-session buffering.
[x] Historical raw data is referenced, not copied into this repository.
[x] Live events are stored in an append-only journal.
[x] Known native record families parse into typed variants.
[x] Unknown native variants are quarantined, counted, and source-referenced.
[x] Active/archived relocation does not duplicate a session.
[x] Re-importing an unchanged source is an identity/no-op operation.
[x] Context snapshots are content-addressed and deduplicated.
[x] Volatile assertion identity is excluded from semantic context hashes.
[x] No serde_json::Value escapes the protocol boundary.
[x] Every domain ID is a distinct newtype.
[ ] Every graph edge has an allowed source and target type.
[x] Tool calls and results correlate by native ID.
[x] Pending, interrupted, and orphaned tool-call states are preserved.
[x] Low-level events become semantic activities.
[x] Mistral task classification uses a closed source-safe structured-output boundary.
[x] Mistral classification and narrative extraction run concurrently and synchronize.
[ ] Transcript enrichment is opt-in and reads only checksum-verified canonical rollouts.
[ ] Local scanning removes secrets and PII before any raw-transcript provider transfer.
[ ] Mistral receives bounded, cited transcript chunks only through the typed sensitive boundary.
[ ] Every enrichment claim and relation cites resolvable source spans.
[ ] Enrichment creates only versioned overlay nodes and edges; deterministic graph state is unchanged.
[ ] Identical enrichment fingerprints are no-ops; changed versions create parallel runs.
[ ] Partial or failed enrichment never becomes selected and never changes ingestion success.
[ ] Real Mistral and real Neo4j E2E tests prove import, enrich, query, retry, and resumability.
[ ] A full eligible-session backfill and identical rerun settle with reconciled usage and zero duplicates.
[x] Neo4j reconstructs the full execution path.
[x] Neo4j projection uses uniqueness constraints, bounded batches, and checkpoints.
[ ] The UI shows timeline, graph, context, and assurance.
[x] Outcomes are determined from evidence.
[x] Risks link to concrete supporting observations.
[ ] Paths store time, cost, correctness, and uncertainty.
[x] Pathfinder uses typed tools rather than raw Cypher.
[x] Candidate plans cite supporting runs.
[ ] OpenCode can append events in real time.
[x] Raw transcripts, instruction bodies, assets, and absolute paths are never committed.
[x] Remote asset references are never fetched automatically.
[x] Tests use real captured fixtures and full-boundary validation, not mocks.
[x] No unwrap, expect, panic, or untyped domain strings.
```

# Final implementation order

```text
Import
→ Verify source
→ Type
→ Deduplicate
→ Correlate
→ Project
→ Enrich additively
→ Visualize
→ Evaluate
→ Profile
→ Retrieve
→ Plan
→ Learn
```

For the hackathon, the critical spine is:

> **Your real Codex session becomes an authoritative deterministic Neo4j execution graph, Mistral adds a separate evidence-linked knowledge layer, and Pathfinder uses both without ever replacing verified facts.**
