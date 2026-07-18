use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use harness_graph_domain::{OccurredAt, RecordSequence, SessionId, SourceDigest, SourceRecordRef};
use harness_graph_enrichment_application::{ClassifiedEnrichmentFailure, EnrichmentFailureClass};
use harness_graph_ingestion::{ArchiveRoot, MaxSourceRecordBytes, SessionScope};
use harness_graph_protocol::{TranscriptRecordProjection, project_codex_transcript_line};
use harness_graph_transcript_enrichment::{
    AuthorizationIdentity, AuthorizationPolicyDigest, BoundedTranscriptChunk,
    BoundedTranscriptChunks, ChunkByteLimit, ChunkingPolicyVersion, DisclosureAuthorization,
    EstimatedTokenLimit, FragmentByteLimit, LocalTranscriptRedactor, MicroUsd, PreparedTranscript,
    PseudonymizationKey, RedactionOutcome, RedactionPolicyVersion, SensitiveValue,
    SensitiveValueSet, SessionNarrative, TokenRatePerMillion, TranscriptChunkPolicy,
    TranscriptDisclosureScope, TranscriptEnrichmentError, TranscriptKnowledgeExtractor,
    TranscriptPreparation, TranscriptPreparationLimits, TranscriptTokenPricing,
    prepare_verified_transcript,
};
use rig::providers::mistral;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{Mutex, Notify},
    task::JoinHandle,
    time::Instant,
};

use super::{
    MAX_SOURCE_COPY_SCALARS, MISTRAL_TRANSCRIPT_KNOWLEDGE_MODEL, MistralTranscriptPromptProvenance,
    ProviderAttemptCount, TranscriptKnowledgeAdapterError, TranscriptKnowledgeOutputError,
    TranscriptProviderFailureClass, VerbatimSourceGuard, choose_aggregate_failure_class,
    output_failure_class,
};
use crate::{
    MistralConcurrencyLimit, MistralCredential, MistralModelName, RigMistralAdapter,
    TranscriptRequestTimeout,
    retry_http::{ProviderRetryGate, RetryAwareHttpClient},
};

const FIXTURE_SESSION_ID: &str = "019c63db-2995-74c3-b898-c1b92a8e1317";
const CONTRACT_KEY: &str = "contract-key-that-must-never-echo";
const CONTRACT_OTHER_SECRET: &str = "database-secret-that-must-never-echo";

#[test]
fn pinned_prompt_provenance_is_typed_content_addressed_and_source_safe()
-> Result<(), Box<dyn std::error::Error>> {
    let first = MistralTranscriptPromptProvenance::pinned()?;
    let identical = MistralTranscriptPromptProvenance::pinned()?;

    assert_eq!(first, identical);
    assert_eq!(first.model().as_str(), MISTRAL_TRANSCRIPT_KNOWLEDGE_MODEL);
    assert_eq!(
        first.prompt_version().as_str(),
        "mistral-transcript-knowledge-prompt-v1"
    );
    assert_eq!(
        first.schema_version().as_str(),
        "mistral-transcript-knowledge-schema-v1"
    );
    assert_eq!(
        first.prompt_digest().to_hex(),
        "5b885c46a3c268cd32607f95dc78fd9762d051e11bc27247f9aa14b8815f8def"
    );
    let rendered = format!("{first:?}");
    assert!(!rendered.contains("Extract only meaningful additive knowledge"));
    assert!(!rendered.contains("sanitized_text"));
    Ok(())
}

#[test]
fn concrete_adapter_failures_map_to_closed_application_classes() {
    let mappings = [
        (
            TranscriptProviderFailureClass::RateLimited,
            EnrichmentFailureClass::RateLimited,
        ),
        (
            TranscriptProviderFailureClass::TemporarilyUnavailable,
            EnrichmentFailureClass::TemporarilyUnavailable,
        ),
        (
            TranscriptProviderFailureClass::Timeout,
            EnrichmentFailureClass::Timeout,
        ),
        (
            TranscriptProviderFailureClass::Transport,
            EnrichmentFailureClass::Transport,
        ),
        (
            TranscriptProviderFailureClass::Authentication,
            EnrichmentFailureClass::Authentication,
        ),
        (
            TranscriptProviderFailureClass::Rejected,
            EnrichmentFailureClass::ProviderRejected,
        ),
        (
            TranscriptProviderFailureClass::InvalidStructuredOutput,
            EnrichmentFailureClass::InvalidStructuredOutput,
        ),
        (
            TranscriptProviderFailureClass::ConcurrencyUnavailable,
            EnrichmentFailureClass::ConcurrencyUnavailable,
        ),
        (
            TranscriptProviderFailureClass::RetryAfterExceedsBound,
            EnrichmentFailureClass::RetryAfterExceedsBound,
        ),
    ];
    for (class, expected) in mappings {
        let error = TranscriptKnowledgeAdapterError::Provider {
            class,
            attempts: ProviderAttemptCount::from_value(1),
        };
        assert_eq!(error.enrichment_failure_class(), expected);
    }
    assert_eq!(
        TranscriptKnowledgeAdapterError::UnpinnedModel.enrichment_failure_class(),
        EnrichmentFailureClass::PolicyBlocked
    );
    assert_eq!(
        output_failure_class(&TranscriptKnowledgeOutputError::SecretEcho),
        EnrichmentFailureClass::SecretEcho
    );
    assert_eq!(
        output_failure_class(&TranscriptKnowledgeOutputError::InvalidUsage),
        EnrichmentFailureClass::InvalidStructuredOutput
    );
    assert_eq!(
        output_failure_class(&TranscriptKnowledgeOutputError::Domain(
            TranscriptEnrichmentError::InvalidKnowledgeText {
                field: "claim statement",
                maximum: 2_000,
            }
        )),
        EnrichmentFailureClass::CitationValidation
    );
    assert_eq!(
        output_failure_class(&TranscriptKnowledgeOutputError::VerbatimSourceCopy),
        EnrichmentFailureClass::CitationValidation
    );
    assert_eq!(
        choose_aggregate_failure_class(
            EnrichmentFailureClass::RateLimited,
            EnrichmentFailureClass::SecretEcho,
        ),
        EnrichmentFailureClass::SecretEcho
    );
    assert_eq!(
        choose_aggregate_failure_class(
            EnrichmentFailureClass::SecretEcho,
            EnrichmentFailureClass::Transport,
        ),
        EnrichmentFailureClass::SecretEcho
    );
}

#[test]
fn verbatim_guard_rejects_exact_and_normalized_source_copy_without_echoing_it()
-> Result<(), Box<dyn std::error::Error>> {
    let chunks = multi_chunk_source_safe_fixture()?;
    let chunk = chunks.iter().next().ok_or("fixture has no chunk")?;
    let source = chunk
        .segments()
        .next()
        .map(
            harness_graph_transcript_enrichment::TranscriptChunkSegment::expose_sanitized_text_for_provider,
        )
        .ok_or("fixture chunk has no segment")?;
    let copied = source
        .chars()
        .take(MAX_SOURCE_COPY_SCALARS.saturating_add(16))
        .collect::<String>();
    assert!(copied.chars().count() >= MAX_SOURCE_COPY_SCALARS);
    let guard = VerbatimSourceGuard::from_chunk(chunk);
    let below_limit = source
        .chars()
        .take(MAX_SOURCE_COPY_SCALARS.saturating_sub(1))
        .collect::<String>();

    assert!(guard.validate(&below_limit).is_ok());
    let exact = guard
        .validate(&copied)
        .err()
        .ok_or("exact source copy was accepted")?;
    let normalized_copy = copied.to_uppercase().replace(' ', "  \n\t");
    let normalized = guard
        .validate(&normalized_copy)
        .err()
        .ok_or("case and whitespace normalized source copy was accepted")?;

    assert!(matches!(
        &exact,
        TranscriptKnowledgeOutputError::VerbatimSourceCopy
    ));
    assert!(matches!(
        &normalized,
        TranscriptKnowledgeOutputError::VerbatimSourceCopy
    ));
    assert!(!format!("{exact:?}").contains(&copied));
    assert!(guard.validate("Concise source-safe semantic label").is_ok());
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum ContractScenario {
    Success,
    RateLimitedOnce,
    UnboundedRetryAfter,
    UnavailableOnce,
    TimeoutOnce,
    MalformedSchema,
    PromptEchoRejected,
    SecretEcho,
    KnownSecretEcho,
    FirstRejected,
    UnknownCitation,
}

struct ContractState {
    scenario: ContractScenario,
    requests: AtomicU64,
    request_changed: Notify,
    request_arrivals: Mutex<Vec<Instant>>,
    observations: Mutex<Vec<ContractObservation>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContractObservation {
    Valid,
    Invalid,
}

struct ContractInspection {
    observation: ContractObservation,
    citation: Option<String>,
    user_content: String,
}

struct ContractServer {
    base_url: String,
    state: Arc<ContractState>,
    task: JoinHandle<()>,
}

impl ContractServer {
    async fn start(scenario: ContractScenario) -> Result<Self, Box<dyn std::error::Error>> {
        let state = Arc::new(ContractState {
            scenario,
            requests: AtomicU64::new(0),
            request_changed: Notify::new(),
            request_arrivals: Mutex::new(Vec::new()),
            observations: Mutex::new(Vec::new()),
        });
        let app = Router::new()
            .route("/v1/chat/completions", post(contract_completion))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let task = tokio::spawn(async move {
            let _result = axum::serve(listener, app).await;
        });
        Ok(Self {
            base_url: format!("http://{address}"),
            state,
            task,
        })
    }

    fn request_count(&self) -> u64 {
        self.state.requests.load(Ordering::SeqCst)
    }

    async fn wait_for_request_count(&self, minimum: u64) {
        loop {
            let changed = self.state.request_changed.notified();
            if self.request_count() >= minimum {
                return;
            }
            changed.await;
        }
    }

    async fn observations(&self) -> Vec<ContractObservation> {
        self.state.observations.lock().await.clone()
    }

    async fn request_arrivals(&self) -> Vec<Instant> {
        self.state.request_arrivals.lock().await.clone()
    }
}

impl Drop for ContractServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Deserialize)]
struct ContractRequest {
    model: String,
    messages: Vec<ContractMessage>,
    temperature: f64,
    max_tokens: u64,
    random_seed: u64,
    response_format: ContractResponseFormat,
    #[serde(default)]
    tools: Vec<Value>,
}

#[derive(Deserialize)]
struct ContractMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct ContractResponseFormat {
    r#type: String,
    json_schema: ContractJsonSchema,
}

#[derive(Deserialize)]
struct ContractJsonSchema {
    strict: bool,
    schema: Value,
}

#[derive(Deserialize)]
struct ContractEvidenceDocument {
    document_kind: String,
    segments: Vec<ContractEvidenceSegment>,
}

#[derive(Deserialize)]
struct ContractEvidenceSegment {
    citation_token: String,
}

async fn contract_completion(
    State(state): State<Arc<ContractState>>,
    headers: HeaderMap,
    Json(request): Json<ContractRequest>,
) -> Response {
    let request_index = state.requests.fetch_add(1, Ordering::SeqCst);
    state.request_arrivals.lock().await.push(Instant::now());
    state.request_changed.notify_waiters();
    let inspection = inspect_contract_request(&headers, &request);
    state.observations.lock().await.push(inspection.observation);
    if inspection.observation == ContractObservation::Invalid {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"message": "invalid contract"})),
        )
            .into_response();
    }
    let Some(citation) = inspection.citation else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"message": "missing citation"})),
        )
            .into_response();
    };
    contract_scenario_response(
        state.scenario,
        request_index,
        &citation,
        &inspection.user_content,
    )
    .await
}

fn inspect_contract_request(headers: &HeaderMap, request: &ContractRequest) -> ContractInspection {
    let user_content = request
        .messages
        .iter()
        .find(|message| message.role == "user")
        .map_or_else(String::new, |message| message.content.clone());
    let evidence = serde_json::from_str::<ContractEvidenceDocument>(&user_content).ok();
    let citation = evidence
        .as_ref()
        .and_then(|document| document.segments.first())
        .map(|segment| segment.citation_token.clone());
    let authenticated = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == format!("Bearer {CONTRACT_KEY}"));
    let schema_has_closed_root = request
        .response_format
        .json_schema
        .schema
        .get("properties")
        .and_then(Value::as_object)
        .is_some_and(|properties| {
            properties.contains_key("entities")
                && properties.contains_key("claims")
                && properties.contains_key("relations")
                && properties.contains_key("episodes")
        });
    let valid = authenticated
        && request.model == MISTRAL_TRANSCRIPT_KNOWLEDGE_MODEL
        && request.temperature == 0.0
        && request.random_seed == 0
        && request.max_tokens == 6_000
        && request.tools.is_empty()
        && request.response_format.r#type == "json_schema"
        && request.response_format.json_schema.strict
        && schema_has_closed_root
        && evidence.as_ref().is_some_and(|document| {
            document.document_kind == "untrusted_sanitized_transcript_evidence"
                && !document.segments.is_empty()
        });
    ContractInspection {
        observation: if valid {
            ContractObservation::Valid
        } else {
            ContractObservation::Invalid
        },
        citation,
        user_content,
    }
}

async fn contract_scenario_response(
    scenario: ContractScenario,
    request_index: u64,
    citation: &str,
    user_content: &str,
) -> Response {
    match scenario {
        ContractScenario::RateLimitedOnce if request_index == 0 => {
            let mut response = (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({"message": "transient rate limit"})),
            )
                .into_response();
            response
                .headers_mut()
                .insert("retry-after", HeaderValue::from_static("1"));
            response
        }
        ContractScenario::UnboundedRetryAfter => {
            let mut response = (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({"message": "retry window exceeds adapter bound"})),
            )
                .into_response();
            response
                .headers_mut()
                .insert("retry-after", HeaderValue::from_static("91"));
            response
        }
        ContractScenario::UnavailableOnce if request_index == 0 => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"message": "temporarily unavailable"})),
        )
            .into_response(),
        ContractScenario::TimeoutOnce if request_index == 0 => {
            tokio::time::sleep(Duration::from_millis(100)).await;
            valid_contract_response(citation, None)
        }
        ContractScenario::MalformedSchema => {
            contract_response_with_content(serde_json::to_string(&json!({"entities": []})))
        }
        ContractScenario::PromptEchoRejected => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "message": format!(
                    "rejected request body: {}",
                    user_content
                )
            })),
        )
            .into_response(),
        ContractScenario::SecretEcho => valid_contract_response(citation, Some(CONTRACT_KEY)),
        ContractScenario::KnownSecretEcho => {
            valid_contract_response(citation, Some(CONTRACT_OTHER_SECRET))
        }
        ContractScenario::FirstRejected if request_index == 0 => (
            StatusCode::BAD_REQUEST,
            Json(json!({"message": "terminal request rejection"})),
        )
            .into_response(),
        ContractScenario::UnknownCitation => valid_contract_response(&"0".repeat(64), None),
        ContractScenario::Success
        | ContractScenario::RateLimitedOnce
        | ContractScenario::UnavailableOnce
        | ContractScenario::TimeoutOnce
        | ContractScenario::FirstRejected => valid_contract_response(citation, None),
    }
}

fn valid_contract_response(citation: &str, secret_echo: Option<&str>) -> Response {
    let statement = secret_echo.unwrap_or("The execution produced source-safe output.");
    let output = json!({
        "entities": [
            {"entity_index": 1, "kind": "tool", "label": "exec_command"},
            {"entity_index": 2, "kind": "artifact", "label": "source-safe output"}
        ],
        "claims": [{
            "kind": "verification",
            "title": "Source-safe command completed",
            "statement": statement,
            "confidence": "high",
            "epistemic_status": "explicit",
            "subjects": {"scope": "entities", "entity_indices": [1, 2]},
            "citation_tokens": [citation]
        }],
        "relations": [{
            "predicate": "produces",
            "subject_entity_index": 1,
            "object_entity_index": 2,
            "confidence": "high",
            "epistemic_status": "explicit",
            "citation_tokens": [citation]
        }],
        "episodes": [{
            "title": "Source-safe execution",
            "summary": "The agent invoked a command and received source-safe output.",
            "confidence": "high",
            "epistemic_status": "explicit",
            "citation_tokens": [citation]
        }]
    });
    contract_response_with_content(serde_json::to_string(&output))
}

fn contract_response_with_content(content: Result<String, serde_json::Error>) -> Response {
    let Ok(content) = content else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    Json(json!({
        "id": "cmpl-contract",
        "object": "chat.completion",
        "created": 1_721_299_200_u64,
        "model": MISTRAL_TRANSCRIPT_KNOWLEDGE_MODEL,
        "system_fingerprint": null,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": content,
                "tool_calls": [],
                "prefix": false
            },
            "logprobs": null,
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "total_tokens": 150
        }
    }))
    .into_response()
}

fn source_safe_fixture(
    chunk_bytes: usize,
) -> Result<PreparedTranscript, Box<dyn std::error::Error>> {
    let fixture_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/source-safe");
    let catalog = ArchiveRoot::new(fixture_root)?.discover(SessionScope::All)?;
    let bundle = catalog
        .require(SessionId::parse(FIXTURE_SESSION_ID)?)?
        .verify()?;
    let authorization = DisclosureAuthorization::new(
        bundle.session_id(),
        bundle.source_digest(),
        TranscriptDisclosureScope::ConversationAndExecution,
        AuthorizationPolicyDigest::hash(b"mistral-adapter-contract-policy-v1"),
        AuthorizationIdentity::new("mistral-adapter-contract")?,
        OccurredAt::parse("2026-07-18T12:00:00Z")?,
    );
    let redactor = LocalTranscriptRedactor::new(
        RedactionPolicyVersion::new("contract-redaction-v1")?,
        PseudonymizationKey::new("contract-pseudonym-key-32-bytes-minimum")?,
        SensitiveValueSet::default(),
    )?;
    let policy = TranscriptChunkPolicy::new(
        ChunkByteLimit::new(chunk_bytes)?,
        EstimatedTokenLimit::new(
            u64::try_from(chunk_bytes.div_ceil(3))
                .unwrap_or(u64::MAX)
                .clamp(64, 256 * 1024),
        )?,
        FragmentByteLimit::new(chunk_bytes)?,
        ChunkingPolicyVersion::new("contract-chunking-v1")?,
    )?;
    match prepare_verified_transcript(
        bundle,
        &authorization,
        &redactor,
        &policy,
        MaxSourceRecordBytes::default(),
        TranscriptPreparationLimits::default(),
    )? {
        TranscriptPreparation::Prepared(prepared) => Ok(prepared),
        TranscriptPreparation::MetadataOnly(_) => Err("fixture produced metadata only".into()),
        TranscriptPreparation::Blocked(_) => Err("fixture was blocked".into()),
    }
}

fn multi_chunk_source_safe_fixture() -> Result<BoundedTranscriptChunks, Box<dyn std::error::Error>>
{
    let source = SourceRecordRef::new(
        SessionId::parse(FIXTURE_SESSION_ID)?,
        SourceDigest::hash(b"mistral-adapter-multi-chunk-source"),
        RecordSequence::from_zero_based(0),
    );
    let message = format!(
        "The agent inspected a source-safe module and verified its output. {}",
        "Additional source-safe execution evidence. ".repeat(96)
    );
    let line = format!(
        r#"{{"timestamp":"2026-07-18T12:00:00Z","type":"event_msg","payload":{{"type":"user_message","message":{}}}}}"#,
        serde_json::to_string(&message)?
    );
    let TranscriptRecordProjection::Eligible(fragments) =
        project_codex_transcript_line(&line, source.clone())?
    else {
        return Err("source-safe protocol fixture was excluded".into());
    };
    let authorization = DisclosureAuthorization::new(
        source.session_id(),
        source.source_digest(),
        TranscriptDisclosureScope::ConversationAndExecution,
        AuthorizationPolicyDigest::hash(b"mistral-adapter-multi-chunk-policy"),
        AuthorizationIdentity::new("mistral-adapter-contract")?,
        OccurredAt::parse("2026-07-18T12:00:00Z")?,
    );
    let redactor = LocalTranscriptRedactor::new(
        RedactionPolicyVersion::new("contract-redaction-v1")?,
        PseudonymizationKey::new("contract-pseudonym-key-32-bytes-minimum")?,
        SensitiveValueSet::default(),
    )?;
    let mut approved = Vec::new();
    for fragment in fragments.into_fragments() {
        let RedactionOutcome::Approved { fragment, .. } =
            redactor.sanitize(&fragment, &authorization)?
        else {
            return Err("source-safe fragment was outside authorization scope".into());
        };
        approved.push(*fragment);
    }
    let policy = TranscriptChunkPolicy::new(
        ChunkByteLimit::new(256)?,
        EstimatedTokenLimit::new(86)?,
        FragmentByteLimit::new(256)?,
        ChunkingPolicyVersion::new("contract-multi-chunk-v1")?,
    )?;
    Ok(harness_graph_transcript_enrichment::TranscriptChunker::new(policy).chunk(&approved)?)
}

fn contract_adapter(
    base_url: &str,
    timeout: Duration,
    concurrency: MistralConcurrencyLimit,
) -> Result<RigMistralAdapter, Box<dyn std::error::Error>> {
    let credential = MistralCredential::new(CONTRACT_KEY)?;
    let retry_gate = ProviderRetryGate::default();
    let http_client = RetryAwareHttpClient::new(
        rig::http_client::ReqwestClient::default(),
        retry_gate.clone(),
    );
    let client = mistral::Client::builder()
        .api_key(CONTRACT_KEY)
        .base_url(base_url)
        .http_client(http_client)
        .build()?;
    Ok(RigMistralAdapter {
        client,
        credential,
        model: MistralModelName::new(MISTRAL_TRANSCRIPT_KNOWLEDGE_MODEL)?,
        concurrency,
        permits: Arc::new(tokio::sync::Semaphore::new(concurrency.value())),
        retry_gate,
        transcript_request_timeout: TranscriptRequestTimeout(timeout),
        output_secret_canaries: SensitiveValueSet::new([SensitiveValue::new(
            CONTRACT_OTHER_SECRET,
        )?]),
    })
}

const fn contract_pricing() -> TranscriptTokenPricing {
    TranscriptTokenPricing::new(
        TokenRatePerMillion::new(MicroUsd::new(1_000_000)),
        TokenRatePerMillion::new(MicroUsd::new(1_000_000)),
    )
}

fn first_chunk(prepared: &PreparedTranscript) -> Result<&BoundedTranscriptChunk, &'static str> {
    prepared
        .chunks()
        .iter()
        .next()
        .ok_or("fixture has no chunks")
}

#[tokio::test]
async fn contract_success_validates_schema_citations_endpoints_usage_and_cost()
-> Result<(), Box<dyn std::error::Error>> {
    let prepared = source_safe_fixture(1024 * 1024)?;
    let server = ContractServer::start(ContractScenario::Success).await?;
    let adapter = contract_adapter(
        &server.base_url,
        Duration::from_secs(2),
        MistralConcurrencyLimit::new(2)?,
    )?;

    let result = adapter
        .extract_all_transcript_knowledge(prepared.chunks(), contract_pricing())
        .await?;

    assert_eq!(result.chunks().count(), prepared.chunks().count());
    assert_eq!(result.knowledge().entities().iter().count(), 2);
    assert_eq!(result.knowledge().claims().iter().count(), 1);
    assert_eq!(result.knowledge().relations().iter().count(), 1);
    assert_eq!(result.knowledge().episodes().iter().count(), 1);
    assert!(matches!(
        result.knowledge().narrative(),
        SessionNarrative::Cited(_)
    ));
    assert_eq!(result.usage().input_tokens().value(), 100);
    assert_eq!(result.usage().output_tokens().value(), 50);
    assert_eq!(result.usage().total_tokens().value(), 150);
    assert_eq!(result.usage().request_attempts().value(), 1);
    assert_eq!(result.usage().completed_responses().value(), 1);
    assert_eq!(result.cost().value(), 150);
    let observations = server.observations().await;
    assert_eq!(observations.len(), 1);
    assert!(
        observations
            .iter()
            .all(|observation| *observation == ContractObservation::Valid)
    );
    Ok(())
}

#[tokio::test]
async fn contract_429_honors_retry_after_before_retrying() -> Result<(), Box<dyn std::error::Error>>
{
    let prepared = source_safe_fixture(1024 * 1024)?;
    let server = ContractServer::start(ContractScenario::RateLimitedOnce).await?;
    let adapter = contract_adapter(
        &server.base_url,
        Duration::from_secs(2),
        MistralConcurrencyLimit::new(1)?,
    )?;
    let started = Instant::now();

    let result = adapter
        .extract_chunk_with_metadata(first_chunk(&prepared)?)
        .await?;

    assert!(started.elapsed() >= Duration::from_millis(950));
    assert_eq!(result.attempts, ProviderAttemptCount::from_value(2));
    assert_eq!(server.request_count(), 2);
    Ok(())
}

#[tokio::test]
async fn contract_shared_retry_gate_pre_gates_queued_and_new_requests()
-> Result<(), Box<dyn std::error::Error>> {
    let prepared = source_safe_fixture(1024 * 1024)?;
    let chunk = first_chunk(&prepared)?.clone();
    let server = ContractServer::start(ContractScenario::RateLimitedOnce).await?;
    let adapter = Arc::new(contract_adapter(
        &server.base_url,
        Duration::from_secs(5),
        MistralConcurrencyLimit::new(2)?,
    )?);

    let first_adapter = adapter.clone();
    let first_chunk = chunk.clone();
    let first = tokio::spawn(async move {
        first_adapter
            .extract_chunk_with_metadata(&first_chunk)
            .await
    });
    server.wait_for_request_count(1).await;
    adapter.retry_gate.wait_until_delayed().await;

    let second_adapter = adapter.clone();
    let second =
        tokio::spawn(async move { second_adapter.extract_chunk_with_metadata(&chunk).await });
    let first_result = first.await??;
    let second_result = second.await??;
    assert_eq!(first_result.attempts, ProviderAttemptCount::from_value(2));
    assert_eq!(second_result.attempts, ProviderAttemptCount::from_value(1));
    assert_eq!(server.request_count(), 3);
    let arrivals = server.request_arrivals().await;
    let first_arrival = arrivals
        .first()
        .copied()
        .ok_or("contract server recorded no request arrival")?;
    assert!(
        arrivals
            .iter()
            .skip(1)
            .all(|arrival| arrival.duration_since(first_arrival) >= Duration::from_millis(950)),
        "every queued or new request must honor the shared provider retry window"
    );
    Ok(())
}

#[tokio::test]
async fn contract_unbounded_retry_after_stops_closed_without_sleeping()
-> Result<(), Box<dyn std::error::Error>> {
    let prepared = source_safe_fixture(1024 * 1024)?;
    let server = ContractServer::start(ContractScenario::UnboundedRetryAfter).await?;
    let adapter = contract_adapter(
        &server.base_url,
        Duration::from_secs(2),
        MistralConcurrencyLimit::new(1)?,
    )?;

    let error = adapter
        .extract_chunk_with_metadata(first_chunk(&prepared)?)
        .await
        .err()
        .ok_or("unbounded Retry-After unexpectedly retried")?;

    assert!(matches!(
        error,
        TranscriptKnowledgeAdapterError::Provider {
            class: TranscriptProviderFailureClass::RetryAfterExceedsBound,
            attempts,
        } if attempts.value() == 1
    ));
    assert_eq!(server.request_count(), 1);
    Ok(())
}

#[tokio::test]
async fn contract_5xx_retries_with_bounded_backoff() -> Result<(), Box<dyn std::error::Error>> {
    let prepared = source_safe_fixture(1024 * 1024)?;
    let server = ContractServer::start(ContractScenario::UnavailableOnce).await?;
    let adapter = contract_adapter(
        &server.base_url,
        Duration::from_secs(2),
        MistralConcurrencyLimit::new(1)?,
    )?;

    let result = adapter
        .extract_chunk_with_metadata(first_chunk(&prepared)?)
        .await?;

    assert_eq!(result.attempts, ProviderAttemptCount::from_value(2));
    assert_eq!(server.request_count(), 2);
    Ok(())
}

#[tokio::test]
async fn contract_timeout_retries_after_releasing_the_permit()
-> Result<(), Box<dyn std::error::Error>> {
    let prepared = source_safe_fixture(1024 * 1024)?;
    let server = ContractServer::start(ContractScenario::TimeoutOnce).await?;
    let adapter = contract_adapter(
        &server.base_url,
        Duration::from_millis(25),
        MistralConcurrencyLimit::new(1)?,
    )?;

    let result = adapter
        .extract_chunk_with_metadata(first_chunk(&prepared)?)
        .await?;

    assert_eq!(result.attempts, ProviderAttemptCount::from_value(2));
    assert_eq!(server.request_count(), 2);
    Ok(())
}

#[tokio::test]
async fn transport_failures_retry_to_the_exact_attempt_bound()
-> Result<(), Box<dyn std::error::Error>> {
    let prepared = source_safe_fixture(1024 * 1024)?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let unreachable_url = format!("http://{}", listener.local_addr()?);
    drop(listener);
    let adapter = contract_adapter(
        &unreachable_url,
        Duration::from_secs(2),
        MistralConcurrencyLimit::new(1)?,
    )?;

    let error = adapter
        .extract_chunk_with_metadata(first_chunk(&prepared)?)
        .await
        .err()
        .ok_or("unreachable transport unexpectedly succeeded")?;

    assert!(matches!(
        error,
        TranscriptKnowledgeAdapterError::Provider {
            class: TranscriptProviderFailureClass::Transport,
            attempts,
        } if attempts.value() == 3
    ));
    Ok(())
}

#[tokio::test]
async fn contract_malformed_schema_is_terminal_and_source_safe()
-> Result<(), Box<dyn std::error::Error>> {
    let prepared = source_safe_fixture(1024 * 1024)?;
    let server = ContractServer::start(ContractScenario::MalformedSchema).await?;
    let adapter = contract_adapter(
        &server.base_url,
        Duration::from_secs(2),
        MistralConcurrencyLimit::new(1)?,
    )?;

    let error = adapter
        .extract_chunk_with_metadata(first_chunk(&prepared)?)
        .await
        .err()
        .ok_or("malformed output unexpectedly succeeded")?;

    assert!(matches!(
        error,
        TranscriptKnowledgeAdapterError::Provider {
            class: TranscriptProviderFailureClass::InvalidStructuredOutput,
            attempts,
        } if attempts.value() == 1
    ));
    assert_eq!(server.request_count(), 1);
    Ok(())
}

#[tokio::test]
async fn contract_prompt_echo_error_body_is_discarded() -> Result<(), Box<dyn std::error::Error>> {
    let prepared = source_safe_fixture(1024 * 1024)?;
    let chunk = first_chunk(&prepared)?;
    let evidence_marker = chunk
        .segments()
        .next()
        .map(
            harness_graph_transcript_enrichment::TranscriptChunkSegment::expose_sanitized_text_for_provider,
        )
        .ok_or("fixture chunk has no segment")?;
    let server = ContractServer::start(ContractScenario::PromptEchoRejected).await?;
    let adapter = contract_adapter(
        &server.base_url,
        Duration::from_secs(2),
        MistralConcurrencyLimit::new(1)?,
    )?;

    let error = adapter
        .extract_chunk_with_metadata(chunk)
        .await
        .err()
        .ok_or("prompt echo rejection unexpectedly succeeded")?;
    let rendered = format!("{error:?}");

    assert!(matches!(
        error,
        TranscriptKnowledgeAdapterError::Provider {
            class: TranscriptProviderFailureClass::Rejected,
            ..
        }
    ));
    assert!(!rendered.contains(evidence_marker));
    assert!(!rendered.contains(CONTRACT_KEY));
    assert_eq!(server.request_count(), 1);
    Ok(())
}

#[tokio::test]
async fn contract_secret_echo_is_rejected_but_billed_usage_is_retained()
-> Result<(), Box<dyn std::error::Error>> {
    let prepared = source_safe_fixture(1024 * 1024)?;
    let server = ContractServer::start(ContractScenario::SecretEcho).await?;
    let adapter = contract_adapter(
        &server.base_url,
        Duration::from_secs(2),
        MistralConcurrencyLimit::new(1)?,
    )?;

    let error = adapter
        .extract_chunk_with_metadata(first_chunk(&prepared)?)
        .await
        .err()
        .ok_or("secret echo unexpectedly succeeded")?;
    let rendered = format!("{error:?}");

    assert!(matches!(
        error,
        TranscriptKnowledgeAdapterError::InvalidOutput {
            source: TranscriptKnowledgeOutputError::SecretEcho,
            usage,
            attempts,
        } if usage.total_tokens().value() == 150 && attempts.value() == 1
    ));
    assert!(!rendered.contains(CONTRACT_KEY));
    assert_eq!(server.request_count(), 1);
    Ok(())
}

#[tokio::test]
async fn contract_any_locally_known_secret_echo_is_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let prepared = source_safe_fixture(1024 * 1024)?;
    let server = ContractServer::start(ContractScenario::KnownSecretEcho).await?;
    let adapter = contract_adapter(
        &server.base_url,
        Duration::from_secs(2),
        MistralConcurrencyLimit::new(1)?,
    )?;

    let error = adapter
        .extract_chunk_with_metadata(first_chunk(&prepared)?)
        .await
        .err()
        .ok_or("known secret echo unexpectedly succeeded")?;
    let rendered = format!("{error:?}");

    assert!(matches!(
        error,
        TranscriptKnowledgeAdapterError::InvalidOutput {
            source: TranscriptKnowledgeOutputError::SecretEcho,
            ..
        }
    ));
    assert!(!rendered.contains(CONTRACT_OTHER_SECRET));
    Ok(())
}

#[tokio::test]
async fn contract_unknown_citation_is_rejected_after_billed_response()
-> Result<(), Box<dyn std::error::Error>> {
    let prepared = source_safe_fixture(1024 * 1024)?;
    let server = ContractServer::start(ContractScenario::UnknownCitation).await?;
    let adapter = contract_adapter(
        &server.base_url,
        Duration::from_secs(2),
        MistralConcurrencyLimit::new(1)?,
    )?;

    let error = adapter
        .extract_chunk_with_metadata(first_chunk(&prepared)?)
        .await
        .err()
        .ok_or("unknown citation unexpectedly succeeded")?;

    assert!(matches!(
        error,
        TranscriptKnowledgeAdapterError::InvalidOutput {
            source: TranscriptKnowledgeOutputError::Domain(_),
            usage,
            ..
        } if usage.total_tokens().value() == 150
    ));
    assert_eq!(server.request_count(), 1);
    Ok(())
}

#[tokio::test]
async fn contract_map_settles_every_chunk_before_returning_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let chunks = multi_chunk_source_safe_fixture()?;
    let chunk_count = chunks.count();
    assert!(chunk_count.value() > 1);
    let server = ContractServer::start(ContractScenario::FirstRejected).await?;
    let adapter = contract_adapter(
        &server.base_url,
        Duration::from_secs(2),
        MistralConcurrencyLimit::new(2)?,
    )?;

    let error = adapter
        .extract_all_transcript_knowledge(&chunks, contract_pricing())
        .await
        .err()
        .ok_or("partial map unexpectedly succeeded")?;

    assert!(matches!(
        error,
        TranscriptKnowledgeAdapterError::IncompleteMap {
            class: EnrichmentFailureClass::ProviderRejected,
            successful_chunks,
            failed_chunks,
            usage,
        } if successful_chunks.value() == chunk_count.value().saturating_sub(1)
            && failed_chunks.value() == 1
            && usage.completed_responses().value() == chunk_count.value().saturating_sub(1)
            && usage.request_attempts().value() == chunk_count.value()
    ));
    assert_eq!(server.request_count(), chunk_count.value());
    Ok(())
}

#[tokio::test]
#[ignore = "requires the real MISTRAL_API_KEY in the repository .env"]
async fn live_mistral_extracts_citation_validated_source_safe_knowledge()
-> Result<(), Box<dyn std::error::Error>> {
    let env_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.env");
    let credential = dotenvy::from_path_iter(env_path)?.find_map(|entry| match entry {
        Ok((name, value)) if name == "MISTRAL_API_KEY" && !value.trim().is_empty() => {
            Some(Ok(value))
        }
        Ok(_) => None,
        Err(error) => Some(Err(error)),
    });
    let credential = credential
        .transpose()?
        .ok_or("repository .env is missing MISTRAL_API_KEY")?;
    let credential = MistralCredential::new(credential)?;
    let adapter = RigMistralAdapter::new(
        &credential,
        MistralModelName::new(MISTRAL_TRANSCRIPT_KNOWLEDGE_MODEL)?,
    )?;
    let prepared = source_safe_fixture(1024 * 1024)?;

    let extraction =
        TranscriptKnowledgeExtractor::extract_chunk(&adapter, first_chunk(&prepared)?).await?;

    assert!(extraction.usage().input_tokens().value() > 0);
    assert!(extraction.usage().output_tokens().value() > 0);
    assert_eq!(
        extraction.usage().total_tokens().value(),
        extraction
            .usage()
            .input_tokens()
            .value()
            .saturating_add(extraction.usage().output_tokens().value())
    );
    for claim in extraction.knowledge().claims().iter() {
        assert!(claim.citations().iter().next().is_some());
    }
    for relation in extraction.knowledge().relations().iter() {
        assert!(relation.citations().iter().next().is_some());
    }
    println!(
        "live_mistral_usage input={} output={} total={} entities={} claims={} relations={} episodes={}",
        extraction.usage().input_tokens().value(),
        extraction.usage().output_tokens().value(),
        extraction.usage().total_tokens().value(),
        extraction.knowledge().entities().iter().count(),
        extraction.knowledge().claims().iter().count(),
        extraction.knowledge().relations().iter().count(),
        extraction.knowledge().episodes().iter().count(),
    );
    Ok(())
}
