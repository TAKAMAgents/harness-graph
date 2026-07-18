//! Deterministic evidence-linked risk detection.

use std::collections::HashMap;

use harness_graph_domain::{
    ActivityKind, CorrelatedInvocation, CorrelatedOutcome, CorrelatedPurpose, DecodedNativeRecord,
    DomainError, EvidenceRefs, HazardKind, InvocationDigest, RiskExposure, RiskExposures, RiskId,
    RiskSeverity, RunOutcome, SemanticActivities, SourceRecordRef, ToolCallCorrelations,
    ToolCallLifecycle, ToolOutcome, ToolPurpose, VerificationStatus,
};

/// Risk derivation could not preserve an evidence invariant.
#[derive(Debug, thiserror::Error)]
pub enum RiskError {
    /// Risk evidence violated a domain invariant.
    #[error(transparent)]
    Domain(#[from] DomainError),
}

#[derive(Debug, Default)]
struct InvocationEvidence {
    attempts: Vec<SourceRecordRef>,
    failures: Vec<SourceRecordRef>,
}

/// Streaming collector for unsupported-observation evidence.
#[derive(Debug, Default)]
pub struct RiskEngine {
    quarantine: Vec<SourceRecordRef>,
}

impl RiskEngine {
    /// Observe one decoded record for stream-completeness risks.
    pub fn observe(&mut self, record: &DecodedNativeRecord) {
        if let DecodedNativeRecord::Unsupported(record) = record {
            self.quarantine.push(record.source().clone());
        }
    }

    /// Derive risks from typed activities, call correlations, and outcome.
    ///
    /// # Errors
    ///
    /// Returns an error when a detected risk has no supporting evidence.
    pub fn finish(
        self,
        activities: &SemanticActivities,
        correlations: &ToolCallCorrelations,
        outcome: &RunOutcome,
    ) -> Result<RiskExposures, RiskError> {
        let mut risks = Vec::new();
        derive_unverified_edit(&mut risks, activities, outcome);
        derive_invocation_risks(&mut risks, correlations)?;
        derive_call_state_risks(&mut risks, correlations)?;
        if !self.quarantine.is_empty() {
            push_risk(
                &mut risks,
                HazardKind::IncompleteObservationStream,
                RiskSeverity::Medium,
                EvidenceRefs::new(self.quarantine)?,
            );
        }
        Ok(RiskExposures::new(risks))
    }
}

fn derive_unverified_edit(
    risks: &mut Vec<RiskExposure>,
    activities: &SemanticActivities,
    outcome: &RunOutcome,
) {
    if outcome.verification() != VerificationStatus::Stale {
        return;
    }
    if let Some(activity) = activities
        .iter()
        .rev()
        .find(|activity| matches!(activity.kind(), ActivityKind::Modify | ActivityKind::Repair))
    {
        push_risk(
            risks,
            HazardKind::UnverifiedFinalEdit,
            RiskSeverity::High,
            activity.evidence().clone(),
        );
    }
}

fn derive_invocation_risks(
    risks: &mut Vec<RiskExposure>,
    correlations: &ToolCallCorrelations,
) -> Result<(), RiskError> {
    let mut invocations: HashMap<InvocationDigest, InvocationEvidence> = HashMap::new();
    for correlation in correlations.iter() {
        if let CorrelatedInvocation::Known(digest) = correlation.invocation() {
            let evidence = invocations.entry(digest).or_default();
            evidence
                .attempts
                .push(correlation.evidence().first().clone());
            if correlation.outcome() == CorrelatedOutcome::Known(ToolOutcome::Failed) {
                evidence
                    .failures
                    .push(correlation.evidence().last().clone());
            }
        }
        match correlation.purpose() {
            CorrelatedPurpose::Known(ToolPurpose::PermissionEscalation) => push_risk(
                risks,
                HazardKind::PermissionEscalation,
                RiskSeverity::High,
                correlation.evidence().clone(),
            ),
            CorrelatedPurpose::Known(ToolPurpose::Destructive) => push_risk(
                risks,
                HazardKind::DestructiveCommand,
                RiskSeverity::Critical,
                correlation.evidence().clone(),
            ),
            _ => {}
        }
    }
    for evidence in invocations.into_values() {
        if evidence.attempts.len() >= 3 {
            push_risk(
                risks,
                HazardKind::ToolCallLoop,
                RiskSeverity::Medium,
                EvidenceRefs::new(evidence.attempts)?,
            );
        }
        if evidence.failures.len() >= 2 {
            push_risk(
                risks,
                HazardKind::RepeatedFailingCommand,
                RiskSeverity::High,
                EvidenceRefs::new(evidence.failures)?,
            );
        }
    }
    Ok(())
}

fn derive_call_state_risks(
    risks: &mut Vec<RiskExposure>,
    correlations: &ToolCallCorrelations,
) -> Result<(), RiskError> {
    let incomplete: Vec<_> = correlations
        .iter()
        .filter(|correlation| {
            !matches!(correlation.lifecycle(), ToolCallLifecycle::Completed { .. })
        })
        .map(|correlation| correlation.evidence().first().clone())
        .collect();
    if !incomplete.is_empty() {
        push_risk(
            risks,
            HazardKind::IncompleteObservationStream,
            RiskSeverity::High,
            EvidenceRefs::new(incomplete)?,
        );
    }
    Ok(())
}

fn push_risk(
    risks: &mut Vec<RiskExposure>,
    hazard: HazardKind,
    severity: RiskSeverity,
    evidence: EvidenceRefs,
) {
    let source = evidence.first();
    let id = RiskId::hash(
        format!(
            "{}:{}:{}",
            source.source_digest(),
            source.sequence().value(),
            hazard.as_str()
        )
        .as_bytes(),
    );
    risks.push(RiskExposure::new(id, hazard, severity, evidence));
}
