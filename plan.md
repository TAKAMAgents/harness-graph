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

## Mistral only for ambiguity

Use Mistral through Rig for:

* task classification;
* unclear command purpose;
* custom script intent;
* root-cause hypothesis;
* activity summarization;
* semantic similarity explanation;
* candidate-plan generation.

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
→ Visualize
→ Evaluate
→ Profile
→ Retrieve
→ Plan
→ Learn
```

For the hackathon, the critical spine is:

> **Your real Codex session becomes a context-aware Neo4j execution graph, then a Mistral-powered Pathfinder uses that graph to help the next coding agent avoid failed paths and reuse verified ones.**
