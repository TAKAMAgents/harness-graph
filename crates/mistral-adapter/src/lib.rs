//! Rig-backed Mistral adapter for bounded interpretation and planning.

mod retry_http;
mod transcript_knowledge;

pub use transcript_knowledge::*;

use async_trait::async_trait;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    future::IntoFuture,
    sync::Arc,
    time::Duration,
};

use harness_graph_domain::{ActivityId, ActivityKind, DomainError, SessionId, TokenCount};
use harness_graph_planning::{
    ActivityCitations, CandidatePlan, ClassificationConfidence, ClassificationExplanation,
    ClassifiedTask, ModelResult, ModelUsage, NarrativeActivity, NarrativeInterpreter,
    NarrativeOrigin, NarrativeRequest, NarrativeSummary, NarrativeTitle, Pathfinder, PlanRationale,
    PlannedStep, PlannedSteps, PlanningContext, PlanningError, PrecedentCitations, PrecedentPaths,
    SynchronizedInterpretation, TaskCategory, TaskClassificationRequest, TaskClassifier,
};
use harness_graph_transcript_enrichment::SensitiveValueSet;
use rig::{
    client::{CompletionClient, ModelListingClient},
    completion::{StructuredOutputError, TypedPrompt},
    providers::mistral,
};
use schemars::JsonSchema;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, SemaphorePermit};

use retry_http::{ProviderRetryGate, RetryAwareHttpClient};

const PROVIDER_REQUEST_TIMEOUT: Duration = Duration::from_secs(90);
const MISTRAL_EU_API_BASE_URL: &str = "https://api.eu.mistral.ai";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TranscriptRequestTimeout(Duration);

impl TranscriptRequestTimeout {
    const DEFAULT: Self = Self(PROVIDER_REQUEST_TIMEOUT);

    const fn duration(self) -> Duration {
        self.0
    }
}

/// Pinned Mistral model used for sensitive transcript knowledge extraction.
pub const MISTRAL_TRANSCRIPT_KNOWLEDGE_MODEL: &str = "mistral-small-2603";

/// One bounded operation at the Mistral provider boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MistralOperation {
    /// Credential verification through model listing.
    Health,
    /// Source-safe task classification.
    TaskClassification,
    /// Deterministic activity narrative extraction.
    NarrativeExtraction,
    /// Evidence-cited future-path proposal.
    Pathfinder,
}

impl std::fmt::Display for MistralOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Health => "health check",
            Self::TaskClassification => "task classification",
            Self::NarrativeExtraction => "narrative extraction",
            Self::Pathfinder => "Pathfinder proposal",
        })
    }
}

/// Mistral adapter construction or invocation failure.
#[derive(Debug, thiserror::Error)]
pub enum MistralAdapterError {
    /// Provider credential is empty.
    #[error("Mistral credential cannot be empty")]
    EmptyCredential,

    /// Model identifier is not recognizably a Mistral-hosted family.
    #[error("Mistral model name must use a supported Mistral family prefix")]
    InvalidModelName,

    /// Configured provider concurrency was outside its safety bound.
    #[error("Mistral concurrency must be between 1 and 4")]
    InvalidConcurrency,

    /// The adapter's bounded concurrency gate was closed unexpectedly.
    #[error("Mistral concurrency gate is closed")]
    ConcurrencyClosed,

    /// A provider operation exceeded its wall-clock safety bound.
    #[error("Mistral {operation} exceeded the 90-second request timeout")]
    RequestTimeout {
        /// Timed-out provider operation.
        operation: MistralOperation,
    },

    /// Classification failed after the parallel extraction still settled.
    #[error("parallel Mistral classification failed: {source}")]
    ParallelClassification {
        /// Classification failure.
        #[source]
        source: Box<MistralAdapterError>,
        /// Provider usage retained from the successful extraction sibling.
        extraction_usage: ModelUsage,
    },

    /// Extraction failed after the parallel classification still settled.
    #[error("parallel Mistral extraction failed: {source}")]
    ParallelExtraction {
        /// Extraction failure.
        #[source]
        source: Box<MistralAdapterError>,
        /// Provider usage retained from the successful classification sibling.
        classification_usage: ModelUsage,
    },

    /// Both synchronized provider operations failed after settling.
    #[error(
        "parallel Mistral classification and extraction both failed; classification: {classification}; extraction: {extraction}"
    )]
    ParallelOperations {
        /// Classification failure.
        classification: Box<MistralAdapterError>,
        /// Extraction failure.
        extraction: Box<MistralAdapterError>,
    },

    /// Rig could not construct the Mistral provider client.
    #[error("failed to construct Rig Mistral client: {source}")]
    Client {
        /// Rig HTTP client error.
        #[source]
        source: rig::http_client::Error,
    },

    /// Mistral credential verification failed.
    #[error("Mistral provider health check failed: {source}")]
    Health {
        /// Rig provider model-listing error.
        #[source]
        source: rig::model::ModelListingError,
    },

    /// The authenticated Mistral endpoint returned no models.
    #[error("Mistral provider health check returned an empty model catalog")]
    EmptyModelCatalog,

    /// Mistral returned a group identity outside the supplied range.
    #[error("Mistral narrative output returned an out-of-range group identity")]
    InvalidNarrativeGroupIdentity,

    /// Rig's native JSON-schema boundary failed.
    #[error("Mistral structured output failed: {source}")]
    StructuredOutput {
        /// Structured-output error.
        #[source]
        source: StructuredOutputError,
    },

    /// Model output failed typed planning validation.
    #[error(transparent)]
    Planning(#[from] PlanningError),

    /// A model-provided digest or session ID was malformed.
    #[error(transparent)]
    Domain(#[from] DomainError),
}

/// Secret Mistral credential with redacted diagnostics.
#[derive(Clone)]
pub struct MistralCredential(SecretString);

impl MistralCredential {
    /// Validate a non-empty provider credential at the configuration boundary.
    ///
    /// # Errors
    ///
    /// Returns an error when the credential is empty.
    pub fn new(value: impl Into<String>) -> Result<Self, MistralAdapterError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(MistralAdapterError::EmptyCredential);
        }
        Ok(Self(SecretString::from(value)))
    }
}

impl std::fmt::Debug for MistralCredential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MistralCredential([redacted])")
    }
}

/// Validated Mistral model identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MistralModelName(String);

impl MistralModelName {
    /// Validate a model family served by the Mistral provider.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty or non-Mistral family name.
    pub fn new(value: impl Into<String>) -> Result<Self, MistralAdapterError> {
        let value = value.into();
        let value = value.trim();
        let supported = ["mistral-", "ministral-", "codestral-", "pixtral-"];
        if value.is_empty() || !supported.iter().any(|prefix| value.starts_with(prefix)) {
            return Err(MistralAdapterError::InvalidModelName);
        }
        Ok(Self(value.to_owned()))
    }

    /// Borrow the validated model identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Validated upper bound for in-flight Mistral API calls from one adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MistralConcurrencyLimit(usize);

impl MistralConcurrencyLimit {
    /// Default permits classification and extraction to overlap once.
    pub const DEFAULT: Self = Self(2);

    /// Validate the provider concurrency safety bound.
    ///
    /// # Errors
    ///
    /// Returns an error outside `1..=4`.
    pub const fn new(value: usize) -> Result<Self, MistralAdapterError> {
        if value == 0 || value > 4 {
            Err(MistralAdapterError::InvalidConcurrency)
        } else {
            Ok(Self(value))
        }
    }

    /// Maximum in-flight Mistral requests.
    #[must_use]
    pub const fn value(self) -> usize {
        self.0
    }
}

/// Concrete Rig client pinned to the Mistral provider.
pub struct RigMistralAdapter {
    client: mistral::Client<RetryAwareHttpClient>,
    credential: MistralCredential,
    model: MistralModelName,
    concurrency: MistralConcurrencyLimit,
    permits: Arc<Semaphore>,
    retry_gate: ProviderRetryGate,
    transcript_request_timeout: TranscriptRequestTimeout,
    output_secret_canaries: SensitiveValueSet,
}

impl RigMistralAdapter {
    /// Construct a Mistral-only model adapter.
    ///
    /// # Errors
    ///
    /// Returns an error when Rig cannot initialize its Mistral client.
    pub fn new(
        credential: &MistralCredential,
        model: MistralModelName,
    ) -> Result<Self, MistralAdapterError> {
        Self::with_concurrency(credential, model, MistralConcurrencyLimit::DEFAULT)
    }

    /// Construct a Mistral-only adapter with bounded request concurrency.
    ///
    /// # Errors
    ///
    /// Returns an error when Rig cannot initialize its Mistral client.
    pub fn with_concurrency(
        credential: &MistralCredential,
        model: MistralModelName,
        concurrency: MistralConcurrencyLimit,
    ) -> Result<Self, MistralAdapterError> {
        Self::with_concurrency_and_output_secrets(
            credential,
            model,
            concurrency,
            SensitiveValueSet::default(),
        )
    }

    /// Construct a Mistral-only adapter with every locally known secret canary.
    ///
    /// The canaries are never exposed or iterated; transcript output is rejected
    /// when any supplied value appears as a substring.
    ///
    /// # Errors
    ///
    /// Returns an error when Rig cannot initialize its Mistral client.
    pub fn with_concurrency_and_output_secrets(
        credential: &MistralCredential,
        model: MistralModelName,
        concurrency: MistralConcurrencyLimit,
        output_secret_canaries: SensitiveValueSet,
    ) -> Result<Self, MistralAdapterError> {
        let retry_gate = ProviderRetryGate::default();
        let http_client = RetryAwareHttpClient::new(
            rig::http_client::ReqwestClient::default(),
            retry_gate.clone(),
        );
        let client = mistral::Client::builder()
            .api_key(credential.0.expose_secret())
            .base_url(MISTRAL_EU_API_BASE_URL)
            .http_client(http_client)
            .build()
            .map_err(|source| MistralAdapterError::Client { source })?;
        Ok(Self {
            client,
            credential: credential.clone(),
            model,
            concurrency,
            permits: Arc::new(Semaphore::new(concurrency.value())),
            retry_gate,
            transcript_request_timeout: TranscriptRequestTimeout::DEFAULT,
            output_secret_canaries,
        })
    }

    /// Verify the credential against Mistral's real model endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error for provider authentication or transport failures.
    pub async fn health(&self) -> Result<(), MistralAdapterError> {
        let _permit = self.acquire_permit().await?;
        let models = tokio::time::timeout(PROVIDER_REQUEST_TIMEOUT, self.client.list_models())
            .await
            .map_err(|_| MistralAdapterError::RequestTimeout {
                operation: MistralOperation::Health,
            })?
            .map_err(|source| MistralAdapterError::Health { source })?;
        if models.is_empty() {
            Err(MistralAdapterError::EmptyModelCatalog)
        } else {
            Ok(())
        }
    }

    /// Active Mistral model.
    #[must_use]
    pub const fn model(&self) -> &MistralModelName {
        &self.model
    }

    /// Configured maximum in-flight provider calls.
    #[must_use]
    pub const fn concurrency(&self) -> MistralConcurrencyLimit {
        self.concurrency
    }

    /// Start classification and narrative extraction concurrently, then join
    /// only their independently validated results.
    ///
    /// # Errors
    ///
    /// Returns the first classification, extraction, provider, or validation
    /// failure and emits no partial synchronized result.
    pub async fn classify_and_extract(
        &self,
        classification: TaskClassificationRequest,
        extraction: NarrativeRequest,
    ) -> Result<SynchronizedInterpretation, MistralAdapterError> {
        let (classification, extraction) = tokio::join!(
            TaskClassifier::classify(self, classification),
            NarrativeInterpreter::summarize(self, extraction),
        );
        match (classification, extraction) {
            (Ok(classification), Ok(extraction)) => {
                Ok(SynchronizedInterpretation::new(classification, extraction))
            }
            (Err(source), Ok(extraction)) => Err(MistralAdapterError::ParallelClassification {
                source: Box::new(source),
                extraction_usage: extraction.usage(),
            }),
            (Ok(classification), Err(source)) => Err(MistralAdapterError::ParallelExtraction {
                source: Box::new(source),
                classification_usage: classification.usage(),
            }),
            (Err(classification), Err(extraction)) => {
                Err(MistralAdapterError::ParallelOperations {
                    classification: Box::new(classification),
                    extraction: Box::new(extraction),
                })
            }
        }
    }

    async fn acquire_permit(&self) -> Result<SemaphorePermit<'_>, MistralAdapterError> {
        self.permits
            .acquire()
            .await
            .map_err(|_| MistralAdapterError::ConcurrencyClosed)
    }
}

impl std::fmt::Debug for RigMistralAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RigMistralAdapter")
            .field("provider", &"mistral")
            .field("model", &self.model)
            .field("concurrency", &self.concurrency)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ActivityKindDto {
    Start,
    Request,
    Inspect,
    Search,
    Modify,
    Repair,
    Verify,
    Install,
    Execute,
    Diagnose,
    RequestPermission,
    NetworkAccess,
    Destructive,
    ManageContext,
    Rollback,
    Complete,
}

impl From<ActivityKindDto> for ActivityKind {
    fn from(value: ActivityKindDto) -> Self {
        match value {
            ActivityKindDto::Start => Self::Start,
            ActivityKindDto::Request => Self::Request,
            ActivityKindDto::Inspect => Self::Inspect,
            ActivityKindDto::Search => Self::Search,
            ActivityKindDto::Modify => Self::Modify,
            ActivityKindDto::Repair => Self::Repair,
            ActivityKindDto::Verify => Self::Verify,
            ActivityKindDto::Install => Self::Install,
            ActivityKindDto::Execute => Self::Execute,
            ActivityKindDto::Diagnose => Self::Diagnose,
            ActivityKindDto::RequestPermission => Self::RequestPermission,
            ActivityKindDto::NetworkAccess => Self::NetworkAccess,
            ActivityKindDto::Destructive => Self::Destructive,
            ActivityKindDto::ManageContext => Self::ManageContext,
            ActivityKindDto::Rollback => Self::Rollback,
            ActivityKindDto::Complete => Self::Complete,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum TaskCategoryDto {
    BugFix,
    Feature,
    Refactor,
    Research,
    Operations,
    Documentation,
    Testing,
    DataAnalysis,
    Other,
}

impl From<TaskCategoryDto> for TaskCategory {
    fn from(value: TaskCategoryDto) -> Self {
        match value {
            TaskCategoryDto::BugFix => Self::BugFix,
            TaskCategoryDto::Feature => Self::Feature,
            TaskCategoryDto::Refactor => Self::Refactor,
            TaskCategoryDto::Research => Self::Research,
            TaskCategoryDto::Operations => Self::Operations,
            TaskCategoryDto::Documentation => Self::Documentation,
            TaskCategoryDto::Testing => Self::Testing,
            TaskCategoryDto::DataAnalysis => Self::DataAnalysis,
            TaskCategoryDto::Other => Self::Other,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ClassificationConfidenceDto {
    Low,
    Medium,
    High,
}

impl From<ClassificationConfidenceDto> for ClassificationConfidence {
    fn from(value: ClassificationConfidenceDto) -> Self {
        match value {
            ClassificationConfidenceDto::Low => Self::Low,
            ClassificationConfidenceDto::Medium => Self::Medium,
            ClassificationConfidenceDto::High => Self::High,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TaskClassificationDto {
    category: TaskCategoryDto,
    confidence: ClassificationConfidenceDto,
    explanation: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct NarrativeActivityDto {
    group_index: u16,
    title: String,
    kind: ActivityKindDto,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct NarrativeSummaryDto {
    activities: Vec<NarrativeActivityDto>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PlannedStepDto {
    kind: ActivityKindDto,
    rationale: String,
    cited_activity_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CandidatePlanDto {
    /// UUID values copied only from supplied `precedent_session_uuid` fields.
    cited_session_ids: Vec<uuid::Uuid>,
    /// Ordered, evidence-cited future activities.
    steps: Vec<PlannedStepDto>,
}

#[async_trait]
impl TaskClassifier for RigMistralAdapter {
    type Error = MistralAdapterError;

    #[tracing::instrument(
        name = "mistral.task_classification",
        skip_all,
        fields(provider = "mistral", model = %self.model.as_str())
    )]
    async fn classify(
        &self,
        request: TaskClassificationRequest,
    ) -> Result<ModelResult<ClassifiedTask>, Self::Error> {
        let _permit = self.acquire_permit().await?;
        let agent = self
            .client
            .agent(self.model.as_str())
            .preamble(
                "Classify the source-safe engineering task into exactly one supplied category. \
                 Use other only when no narrower category is supported. Confidence is coarse, \
                 not numeric. Explain the choice without adding facts absent from the brief.",
            )
            .temperature(0.0)
            .additional_params(serde_json::json!({ "random_seed": 0 }))
            .max_tokens(800)
            .build();
        let response = tokio::time::timeout(
            PROVIDER_REQUEST_TIMEOUT,
            agent
                .prompt_typed::<TaskClassificationDto>(request.task().as_str())
                .max_turns(1)
                .extended_details()
                .into_future(),
        )
        .await
        .map_err(|_| MistralAdapterError::RequestTimeout {
            operation: MistralOperation::TaskClassification,
        })?
        .map_err(|source| MistralAdapterError::StructuredOutput { source })?;
        let classified = ClassifiedTask::new(
            response.output.category.into(),
            response.output.confidence.into(),
            ClassificationExplanation::new(response.output.explanation)?,
        );
        Ok(ModelResult::new(classified, convert_usage(response.usage)))
    }
}

#[async_trait]
impl NarrativeInterpreter for RigMistralAdapter {
    type Error = MistralAdapterError;

    async fn summarize(
        &self,
        request: NarrativeRequest,
    ) -> Result<ModelResult<NarrativeSummary>, Self::Error> {
        let _permit = self.acquire_permit().await?;
        let source_count = request.activities().iter().count();
        let target = narrative_target(source_count);
        let groups = partition_narrative_evidence(&request, target);
        let prompt = render_narrative_prompt(&request, target, &groups);
        let agent = self
            .client
            .agent(self.model.as_str())
            .preamble(&format!(
                "Return exactly {target} ordered macro-activities, one for each supplied group_index. \
                 Preserve every group_index exactly once and in ascending order. Do not \
                 decide success, risk, or verification. Titles may state only the supplied semantic \
                 kinds and statuses; never invent a target, system, cause, file, or concern."
            ))
            .temperature(0.0)
            .additional_params(serde_json::json!({ "random_seed": 0 }))
            .max_tokens(5_000)
            .build();
        let response = tokio::time::timeout(
            PROVIDER_REQUEST_TIMEOUT,
            agent
                .prompt_typed::<NarrativeSummaryDto>(prompt)
                .max_turns(1)
                .extended_details()
                .into_future(),
        )
        .await
        .map_err(|_| MistralAdapterError::RequestTimeout {
            operation: MistralOperation::NarrativeExtraction,
        })?
        .map_err(|source| MistralAdapterError::StructuredOutput { source })?;
        let summary = narrative_from_dto(response.output, request.activities(), groups)?;
        Ok(ModelResult::new(summary, convert_usage(response.usage)))
    }
}

#[async_trait]
impl Pathfinder for RigMistralAdapter {
    type Error = MistralAdapterError;

    async fn propose(
        &self,
        context: PlanningContext,
        precedents: PrecedentPaths,
    ) -> Result<ModelResult<CandidatePlan>, Self::Error> {
        let _permit = self.acquire_permit().await?;
        let prompt = render_pathfinder_prompt(&context, &precedents);
        let agent = self
            .client
            .agent(self.model.as_str())
            .preamble(
                "Propose 3 to 10 ordered activities. Cite only supplied activity and session IDs. \
                 cited_session_ids must contain only UUID values copied verbatim from the supplied \
                 precedent_session_uuid fields. Never put an activity ID or path hash in that field. \
                 Prefer verified steps, include final verification, and never invent graph evidence.",
            )
            .temperature(0.0)
            .additional_params(serde_json::json!({ "random_seed": 0 }))
            .max_tokens(3_000)
            .build();
        let response = tokio::time::timeout(
            PROVIDER_REQUEST_TIMEOUT,
            agent
                .prompt_typed::<CandidatePlanDto>(prompt)
                .max_turns(1)
                .extended_details()
                .into_future(),
        )
        .await
        .map_err(|_| MistralAdapterError::RequestTimeout {
            operation: MistralOperation::Pathfinder,
        })?
        .map_err(|source| MistralAdapterError::StructuredOutput { source })?;
        let candidate = candidate_from_dto(response.output, &precedents)?;
        Ok(ModelResult::new(candidate, convert_usage(response.usage)))
    }
}

fn narrative_target(source_count: usize) -> usize {
    if source_count >= 15 {
        source_count.div_ceil(3).clamp(15, 25)
    } else {
        source_count.div_ceil(3).clamp(1, 25)
    }
}

fn partition_narrative_evidence(request: &NarrativeRequest, target: usize) -> Vec<Vec<ActivityId>> {
    let activity_ids: Vec<_> = request
        .activities()
        .iter()
        .map(harness_graph_domain::SemanticActivity::id)
        .collect();
    (0..target)
        .map(|index| {
            let start = index * activity_ids.len() / target;
            let end = (index + 1) * activity_ids.len() / target;
            activity_ids[start..end].to_vec()
        })
        .collect()
}

fn render_narrative_prompt(
    request: &NarrativeRequest,
    target: usize,
    groups: &[Vec<ActivityId>],
) -> String {
    let mut prompt = format!(
        "Label these {target} deterministic activity groups. Return one item per group_index.\n"
    );
    for (index, group) in groups.iter().enumerate() {
        let _ = writeln!(prompt, "group_index={}", index + 1);
        for activity_id in group {
            if let Some(activity) = request
                .activities()
                .iter()
                .find(|activity| activity.id() == *activity_id)
            {
                prompt.push_str(&activity.id().to_hex());
                prompt.push('|');
                prompt.push_str(activity.kind().as_str());
                prompt.push('|');
                prompt.push_str(activity.status().as_str());
                prompt.push('\n');
            }
        }
    }
    prompt
}

fn render_pathfinder_prompt(context: &PlanningContext, precedents: &PrecedentPaths) -> String {
    let mut prompt = format!("Task: {}\nVerified precedents:\n", context.task().as_str());
    for precedent in precedents.iter() {
        prompt.push_str("precedent_session_uuid=");
        prompt.push_str(&precedent.session_id().to_string());
        prompt.push('\n');
        for step in precedent.steps().iter() {
            prompt.push_str(&step.activity_id().to_hex());
            prompt.push('|');
            prompt.push_str(step.kind().as_str());
            prompt.push('|');
            prompt.push_str(step.status().as_str());
            prompt.push('\n');
        }
    }
    prompt
}

fn narrative_from_dto(
    dto: NarrativeSummaryDto,
    source: &harness_graph_domain::SemanticActivities,
    groups: Vec<Vec<ActivityId>>,
) -> Result<NarrativeSummary, MistralAdapterError> {
    let mut labels = BTreeMap::new();
    let mut duplicates = BTreeSet::new();
    for activity in dto.activities {
        let index = usize::from(activity.group_index);
        if index == 0 || index > groups.len() {
            return Err(MistralAdapterError::InvalidNarrativeGroupIdentity);
        }
        if labels.insert(index, activity).is_some() {
            duplicates.insert(index);
        }
    }
    for duplicate in duplicates {
        labels.remove(&duplicate);
    }
    let activities = groups
        .into_iter()
        .enumerate()
        .map(|(index, citations)| {
            let label = labels.remove(&(index + 1));
            let (title, kind, origin) = if let Some(label) = label {
                (
                    NarrativeTitle::new(label.title)?,
                    label.kind.into(),
                    NarrativeOrigin::Mistral,
                )
            } else {
                let first_citation = citations
                    .first()
                    .copied()
                    .ok_or(MistralAdapterError::InvalidNarrativeGroupIdentity)?;
                let kind = source
                    .iter()
                    .find(|activity| activity.id() == first_citation)
                    .map(harness_graph_domain::SemanticActivity::kind)
                    .ok_or(MistralAdapterError::InvalidNarrativeGroupIdentity)?;
                (
                    NarrativeTitle::new(format!("{} episode", kind.as_str()))?,
                    kind,
                    NarrativeOrigin::DeterministicFallback,
                )
            };
            Ok(NarrativeActivity::new(
                title,
                kind,
                origin,
                ActivityCitations::new(citations)?,
            ))
        })
        .collect::<Result<Vec<_>, MistralAdapterError>>()?;
    Ok(NarrativeSummary::new(activities, source)?)
}

fn candidate_from_dto(
    dto: CandidatePlanDto,
    source: &PrecedentPaths,
) -> Result<CandidatePlan, MistralAdapterError> {
    let sessions = dto
        .cited_session_ids
        .into_iter()
        .map(|value| SessionId::parse(&value.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    let steps = dto
        .steps
        .into_iter()
        .map(|step| {
            let citations = step
                .cited_activity_ids
                .into_iter()
                .map(|value| ActivityId::parse_hex(&value))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PlannedStep::new(
                step.kind.into(),
                PlanRationale::new(step.rationale)?,
                ActivityCitations::new(citations)?,
            ))
        })
        .collect::<Result<Vec<_>, MistralAdapterError>>()?;
    Ok(CandidatePlan::new(
        PlannedSteps::new(steps)?,
        PrecedentCitations::new(sessions)?,
        source,
    )?)
}

const fn convert_usage(usage: rig::completion::Usage) -> ModelUsage {
    ModelUsage::new(
        TokenCount::new(usage.input_tokens),
        TokenCount::new(usage.output_tokens),
        TokenCount::new(usage.total_tokens),
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        MistralAdapterError, MistralConcurrencyLimit, MistralCredential, MistralModelName,
        RigMistralAdapter, narrative_target,
    };

    #[test]
    fn model_boundary_accepts_only_mistral_hosted_families() {
        assert!(MistralModelName::new("mistral-small-latest").is_ok());
        assert!(MistralModelName::new("codestral-latest").is_ok());
        assert!(matches!(
            MistralModelName::new("gpt-5"),
            Err(MistralAdapterError::InvalidModelName)
        ));
    }

    #[test]
    fn credential_debug_output_is_redacted() -> Result<(), Box<dyn std::error::Error>> {
        let credential = MistralCredential::new("test-secret-that-must-not-appear")?;
        let debug = format!("{credential:?}");
        assert_eq!(debug, "MistralCredential([redacted])");
        Ok(())
    }

    #[test]
    fn concurrency_boundary_accepts_only_the_provider_safety_range() {
        assert_eq!(MistralConcurrencyLimit::DEFAULT.value(), 2);
        assert!(MistralConcurrencyLimit::new(1).is_ok());
        assert!(MistralConcurrencyLimit::new(4).is_ok());
        assert!(matches!(
            MistralConcurrencyLimit::new(0),
            Err(MistralAdapterError::InvalidConcurrency)
        ));
        assert!(matches!(
            MistralConcurrencyLimit::new(5),
            Err(MistralAdapterError::InvalidConcurrency)
        ));
    }

    #[test]
    fn narrative_target_preserves_the_large_source_floor_and_bound() {
        assert_eq!(narrative_target(1), 1);
        assert_eq!(narrative_target(14), 5);
        assert_eq!(narrative_target(15), 15);
        assert_eq!(narrative_target(42), 15);
        assert_eq!(narrative_target(50), 17);
        assert_eq!(narrative_target(100), 25);
    }

    #[test]
    fn shared_adapter_is_safe_for_concurrent_async_branches() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RigMistralAdapter>();
    }

    #[test]
    fn production_adapter_is_pinned_to_the_mistral_eu_endpoint()
    -> Result<(), Box<dyn std::error::Error>> {
        let credential = MistralCredential::new("source-safe-contract-key")?;
        let adapter =
            RigMistralAdapter::new(&credential, MistralModelName::new("mistral-small-latest")?)?;
        assert_eq!(adapter.client.base_url(), super::MISTRAL_EU_API_BASE_URL);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires the real MISTRAL_API_KEY in the repository .env"]
    async fn live_mistral_health_uses_canonical_project_credential()
    -> Result<(), Box<dyn std::error::Error>> {
        let env_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.env");
        let values = dotenvy::from_path_iter(env_path)?
            .filter_map(Result::ok)
            .collect::<std::collections::HashMap<_, _>>();
        let credential = values
            .get("MISTRAL_API_KEY")
            .ok_or("repository .env is missing MISTRAL_API_KEY")?;
        let model = values
            .get("MISTRAL_MODEL")
            .map_or("mistral-small-latest", String::as_str);
        let credential = MistralCredential::new(credential.clone())?;
        let adapter =
            RigMistralAdapter::new(&credential, MistralModelName::new(model.to_owned())?)?;

        adapter.health().await?;
        Ok(())
    }
}
