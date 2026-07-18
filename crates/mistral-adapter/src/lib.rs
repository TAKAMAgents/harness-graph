//! Rig-backed Mistral adapter for bounded interpretation and planning.

use async_trait::async_trait;
use std::{collections::BTreeMap, fmt::Write as _};

use harness_graph_domain::{ActivityId, ActivityKind, DomainError, SessionId, TokenCount};
use harness_graph_planning::{
    ActivityCitations, CandidatePlan, ModelResult, ModelUsage, NarrativeActivity,
    NarrativeInterpreter, NarrativeOrigin, NarrativeRequest, NarrativeSummary, NarrativeTitle,
    Pathfinder, PlanRationale, PlannedStep, PlannedSteps, PlanningContext, PlanningError,
    PrecedentCitations, PrecedentPaths,
};
use rig::{
    client::{CompletionClient, ModelListingClient},
    extractor::ExtractionError,
    providers::mistral,
};
use schemars::JsonSchema;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

/// Mistral adapter construction or invocation failure.
#[derive(Debug, thiserror::Error)]
pub enum MistralAdapterError {
    /// Provider credential is empty.
    #[error("Mistral credential cannot be empty")]
    EmptyCredential,

    /// Model identifier is not recognizably a Mistral-hosted family.
    #[error("Mistral model name must use a supported Mistral family prefix")]
    InvalidModelName,

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

    /// Rig's structured extractor failed to obtain valid tool output.
    #[error("Mistral structured extraction failed: {source}")]
    Extraction {
        /// Structured extraction error.
        #[source]
        source: ExtractionError,
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

/// Concrete Rig client pinned to the Mistral provider.
pub struct RigMistralAdapter {
    client: mistral::Client,
    model: MistralModelName,
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
        let client = mistral::Client::new(credential.0.expose_secret())
            .map_err(|source| MistralAdapterError::Client { source })?;
        Ok(Self { client, model })
    }

    /// Verify the credential against Mistral's real model endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error for provider authentication or transport failures.
    pub async fn health(&self) -> Result<(), MistralAdapterError> {
        let models = self
            .client
            .list_models()
            .await
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
}

impl std::fmt::Debug for RigMistralAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RigMistralAdapter")
            .field("provider", &"mistral")
            .field("model", &self.model)
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
    cited_session_ids: Vec<String>,
    steps: Vec<PlannedStepDto>,
}

#[async_trait]
impl NarrativeInterpreter for RigMistralAdapter {
    type Error = MistralAdapterError;

    async fn summarize(
        &self,
        request: NarrativeRequest,
    ) -> Result<ModelResult<NarrativeSummary>, Self::Error> {
        let target = request.activities().iter().count().div_ceil(3).clamp(1, 25);
        let groups = partition_narrative_evidence(&request, target);
        let prompt = render_narrative_prompt(&request, target, &groups);
        let extractor = self
            .client
            .extractor::<NarrativeSummaryDto>(self.model.as_str())
            .preamble(&format!(
                "Return exactly {target} ordered macro-activities, one for each supplied group_index. \
                 Preserve every group_index exactly once and in ascending order. Do not \
                 decide success, risk, or verification. Titles may state only the supplied semantic \
                 kinds and statuses; never invent a target, system, cause, file, or concern."
            ))
            .max_tokens(5_000)
            .retries(1)
            .build();
        let response = extractor
            .extract_with_usage(prompt)
            .await
            .map_err(|source| MistralAdapterError::Extraction { source })?;
        let summary = narrative_from_dto(response.data, request.activities(), groups)?;
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
        let prompt = render_pathfinder_prompt(&context, &precedents);
        let extractor = self
            .client
            .extractor::<CandidatePlanDto>(self.model.as_str())
            .preamble(
                "Propose 3 to 10 ordered activities. Cite only supplied activity and session IDs. \
                 Prefer verified steps, include final verification, and never invent graph evidence.",
            )
            .max_tokens(3_000)
            .retries(1)
            .build();
        let response = extractor
            .extract_with_usage(prompt)
            .await
            .map_err(|source| MistralAdapterError::Extraction { source })?;
        let candidate = candidate_from_dto(response.data, &precedents)?;
        Ok(ModelResult::new(candidate, convert_usage(response.usage)))
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
        prompt.push_str("session=");
        prompt.push_str(&precedent.session_id().to_string());
        prompt.push_str(" path=");
        prompt.push_str(&precedent.path_signature().to_hex());
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
    for activity in dto.activities {
        let index = usize::from(activity.group_index);
        if index == 0 || index > groups.len() {
            return Err(MistralAdapterError::InvalidNarrativeGroupIdentity);
        }
        labels.entry(index).or_insert(activity);
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
                let kind = source
                    .iter()
                    .find(|activity| activity.id() == citations[0])
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
        .map(|value| SessionId::parse(&value))
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

    use super::{MistralAdapterError, MistralCredential, MistralModelName, RigMistralAdapter};

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
