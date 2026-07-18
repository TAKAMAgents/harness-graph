//! Evidence-derived verification freshness and run outcome assessment.

use harness_graph_domain::{
    ActivityKind, ActivityStatus, DomainError, EvidenceRefs, OutcomeClass, RunOutcome,
    SemanticActivities, VerificationStatus,
};

/// Outcome assessment lacked the minimum semantic evidence.
#[derive(Debug, thiserror::Error)]
pub enum AssuranceError {
    /// No semantic activity could support an outcome.
    #[error("cannot assess a run with no semantic activities")]
    MissingActivityEvidence,

    /// Outcome evidence violated a domain invariant.
    #[error(transparent)]
    Domain(#[from] DomainError),
}

/// Assess success from typed evidence; language-model output is never used.
///
/// # Errors
///
/// Returns an error when there are no semantic activities.
pub fn assess_outcome(activities: &SemanticActivities) -> Result<RunOutcome, AssuranceError> {
    let ordered: Vec<_> = activities.iter().collect();
    let first = ordered
        .first()
        .ok_or(AssuranceError::MissingActivityEvidence)?;
    let last_modification = ordered.iter().rposition(|activity| {
        matches!(activity.kind(), ActivityKind::Modify | ActivityKind::Repair)
    });
    let last_successful_verification = ordered.iter().rposition(|activity| {
        activity.kind() == ActivityKind::Verify && activity.status() == ActivityStatus::Succeeded
    });
    let failed_verification = ordered.iter().any(|activity| {
        activity.kind() == ActivityKind::Verify && activity.status() == ActivityStatus::Failed
    });
    let verification = verification_status(
        last_modification,
        last_successful_verification,
        failed_verification,
    );
    let completed = ordered.iter().rev().find(|activity| {
        activity.kind() == ActivityKind::Complete
            && matches!(
                activity.status(),
                ActivityStatus::Succeeded | ActivityStatus::Interrupted
            )
    });
    let class = match completed.map(|activity| activity.status()) {
        Some(ActivityStatus::Interrupted) => OutcomeClass::Cancelled,
        Some(ActivityStatus::Succeeded) if verification == VerificationStatus::Fresh => {
            OutcomeClass::VerifiedSuccess
        }
        Some(ActivityStatus::Succeeded) => OutcomeClass::UnverifiedCompletion,
        _ if ordered
            .iter()
            .any(|activity| activity.status() == ActivityStatus::Failed) =>
        {
            OutcomeClass::Failed
        }
        _ => OutcomeClass::Inconclusive,
    };
    let last = ordered.last().copied().unwrap_or(first);
    Ok(RunOutcome::new(
        class,
        verification,
        EvidenceRefs::new([
            first.evidence().first().clone(),
            last.evidence().last().clone(),
        ])?,
    ))
}

const fn verification_status(
    last_modification: Option<usize>,
    last_successful_verification: Option<usize>,
    failed_verification: bool,
) -> VerificationStatus {
    match (last_modification, last_successful_verification) {
        (Some(modification), Some(verification)) if verification > modification => {
            VerificationStatus::Fresh
        }
        (None, Some(_)) => VerificationStatus::Fresh,
        (Some(_), Some(_)) => VerificationStatus::Stale,
        (_, None) if failed_verification => VerificationStatus::Failed,
        _ => VerificationStatus::Missing,
    }
}
