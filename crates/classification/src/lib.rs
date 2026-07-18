//! Deterministic semantic activity classification and episode compression.

use harness_graph_domain::{
    ActivityId, ActivityInvocation, ActivityKind, ActivityStatus, CorrelatedInvocation,
    CorrelatedOutcome, CorrelatedPurpose, CorrelatedToolCall, DomainError, EvidenceRefs,
    Observation, ObservationKind, OutcomeAssociation, SemanticActivities, SemanticActivity,
    SourceRecordRef, ToolCallCorrelations, ToolCallLifecycle, ToolOutcome, ToolPurpose,
};

/// Semantic classification could not preserve a domain invariant.
#[derive(Debug, thiserror::Error)]
pub enum ClassificationError {
    /// Derived activity evidence was invalid.
    #[error(transparent)]
    Domain(#[from] DomainError),
}

/// Streaming collector for non-tool semantic boundary observations.
#[derive(Debug, Default)]
pub struct ActivityBuilder {
    standalone: Vec<SemanticActivity>,
}

impl ActivityBuilder {
    /// Observe one canonical record without retaining raw payload content.
    ///
    /// # Errors
    ///
    /// Returns an error when derived evidence violates its non-empty invariant.
    pub fn observe(&mut self, observation: &Observation) -> Result<(), ClassificationError> {
        let semantic = standalone_semantics(observation);
        let Some((kind, status)) = semantic else {
            return Ok(());
        };
        let evidence = EvidenceRefs::new([observation.source().clone()])?;
        self.standalone.push(SemanticActivity::new(
            activity_id_for_source(observation.source(), kind),
            kind,
            status,
            ActivityInvocation::NotApplicable,
            evidence,
        ));
        Ok(())
    }

    /// Combine standalone observations and native-ID correlations into ordered,
    /// compressed semantic episodes.
    ///
    /// # Errors
    ///
    /// Returns an error when derived diagnostic evidence is empty.
    pub fn finish(
        mut self,
        correlations: &ToolCallCorrelations,
    ) -> Result<SemanticActivities, ClassificationError> {
        for correlation in correlations.iter() {
            self.standalone.push(activity_for_call(correlation));
        }
        self.standalone
            .sort_by_key(|activity| activity.evidence().first().sequence().value());
        let expanded = add_failure_transitions(self.standalone)?;
        Ok(SemanticActivities::new(compress_adjacent(expanded)))
    }
}

fn standalone_semantics(observation: &Observation) -> Option<(ActivityKind, ActivityStatus)> {
    let status_from_outcome = || match observation.outcome() {
        OutcomeAssociation::NotApplicable => ActivityStatus::Indeterminate,
        OutcomeAssociation::Tool(outcome) => activity_status(outcome),
    };
    match observation.kind() {
        ObservationKind::TaskStarted => Some((ActivityKind::Start, ActivityStatus::Succeeded)),
        ObservationKind::UserMessageReceived => {
            Some((ActivityKind::Request, ActivityStatus::Succeeded))
        }
        ObservationKind::TurnAborted => Some((ActivityKind::Complete, ActivityStatus::Interrupted)),
        ObservationKind::ErrorObserved => Some((ActivityKind::Diagnose, ActivityStatus::Failed)),
        ObservationKind::ContextCompacted => {
            Some((ActivityKind::ManageContext, ActivityStatus::Succeeded))
        }
        ObservationKind::ThreadRolledBack => {
            Some((ActivityKind::Rollback, ActivityStatus::Succeeded))
        }
        ObservationKind::TaskCompleted => Some((ActivityKind::Complete, ActivityStatus::Succeeded)),
        ObservationKind::VerificationCompleted => {
            Some((ActivityKind::Verify, status_from_outcome()))
        }
        ObservationKind::PatchApplied
            if matches!(
                observation.call(),
                harness_graph_domain::CallAssociation::NotApplicable
            ) =>
        {
            Some((ActivityKind::Modify, status_from_outcome()))
        }
        _ => None,
    }
}

fn activity_for_call(correlation: &CorrelatedToolCall) -> SemanticActivity {
    let kind = match correlation.purpose() {
        CorrelatedPurpose::Unknown => ActivityKind::Execute,
        CorrelatedPurpose::Known(purpose) => activity_kind(purpose),
    };
    let status = match correlation.lifecycle() {
        ToolCallLifecycle::Pending { .. } => ActivityStatus::Pending,
        ToolCallLifecycle::Interrupted { .. } => ActivityStatus::Interrupted,
        ToolCallLifecycle::OrphanedResult { .. } => ActivityStatus::Indeterminate,
        ToolCallLifecycle::Completed { .. } => match correlation.outcome() {
            CorrelatedOutcome::Missing => ActivityStatus::Indeterminate,
            CorrelatedOutcome::Known(outcome) => activity_status(outcome),
        },
    };
    let invocation = match correlation.invocation() {
        CorrelatedInvocation::Unknown => ActivityInvocation::Unknown,
        CorrelatedInvocation::Known(digest) => ActivityInvocation::Known(digest),
    };
    SemanticActivity::new(
        activity_id_for_call(correlation, kind),
        kind,
        status,
        invocation,
        correlation.evidence().clone(),
    )
}

const fn activity_kind(purpose: ToolPurpose) -> ActivityKind {
    match purpose {
        ToolPurpose::Inspect => ActivityKind::Inspect,
        ToolPurpose::Search => ActivityKind::Search,
        ToolPurpose::Modify => ActivityKind::Modify,
        ToolPurpose::Verify => ActivityKind::Verify,
        ToolPurpose::Install => ActivityKind::Install,
        ToolPurpose::Execute | ToolPurpose::Ambiguous => ActivityKind::Execute,
        ToolPurpose::PermissionEscalation => ActivityKind::RequestPermission,
        ToolPurpose::NetworkAccess => ActivityKind::NetworkAccess,
        ToolPurpose::Destructive => ActivityKind::Destructive,
    }
}

const fn activity_status(outcome: ToolOutcome) -> ActivityStatus {
    match outcome {
        ToolOutcome::Succeeded => ActivityStatus::Succeeded,
        ToolOutcome::Failed => ActivityStatus::Failed,
        ToolOutcome::Indeterminate => ActivityStatus::Indeterminate,
    }
}

fn add_failure_transitions(
    activities: Vec<SemanticActivity>,
) -> Result<Vec<SemanticActivity>, ClassificationError> {
    let mut expanded = Vec::with_capacity(activities.len());
    let mut failure_precedes = false;
    for activity in activities {
        if failure_precedes && activity.kind() == ActivityKind::Modify {
            let evidence = activity.evidence().clone();
            expanded.push(SemanticActivity::new(
                activity.id(),
                ActivityKind::Repair,
                activity.status(),
                activity.invocation(),
                evidence,
            ));
        } else {
            if failure_precedes && activity.kind() != ActivityKind::Diagnose {
                expanded.push(SemanticActivity::new(
                    diagnostic_id(activity.evidence().first()),
                    ActivityKind::Diagnose,
                    ActivityStatus::Succeeded,
                    ActivityInvocation::NotApplicable,
                    EvidenceRefs::new([activity.evidence().first().clone()])?,
                ));
            }
            expanded.push(activity);
        }
        failure_precedes = expanded
            .last()
            .is_some_and(|current| current.status() == ActivityStatus::Failed);
    }
    Ok(expanded)
}

fn compress_adjacent(activities: Vec<SemanticActivity>) -> Vec<SemanticActivity> {
    let mut compressed: Vec<SemanticActivity> = Vec::new();
    for activity in activities {
        let can_merge = compressed.last().is_some_and(|previous| {
            previous.kind() == activity.kind() && previous.status() == activity.status()
        });
        if can_merge {
            if let Some(previous) = compressed.pop() {
                compressed.push(previous.merge_evidence(activity.evidence().clone()));
            }
        } else {
            compressed.push(activity);
        }
    }
    compressed
}

fn activity_id_for_source(source: &SourceRecordRef, kind: ActivityKind) -> ActivityId {
    ActivityId::hash(
        format!(
            "{}:{}:{}",
            source.source_digest(),
            source.sequence().value(),
            kind.as_str()
        )
        .as_bytes(),
    )
}

fn activity_id_for_call(correlation: &CorrelatedToolCall, kind: ActivityKind) -> ActivityId {
    ActivityId::hash(
        format!(
            "{}:{}:{}",
            correlation.evidence().first().session_id(),
            correlation.call_id(),
            kind.as_str()
        )
        .as_bytes(),
    )
}

fn diagnostic_id(source: &SourceRecordRef) -> ActivityId {
    ActivityId::hash(
        format!(
            "{}:{}:diagnose",
            source.source_digest(),
            source.sequence().value()
        )
        .as_bytes(),
    )
}
