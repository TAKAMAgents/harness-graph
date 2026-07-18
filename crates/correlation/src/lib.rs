//! Streaming native-ID tool-call correlation.

use std::collections::HashMap;

use harness_graph_domain::{
    CallAssociation, CorrelatedInvocation, CorrelatedOutcome, CorrelatedPurpose, CorrelatedTool,
    CorrelatedToolCall, DomainError, EvidenceRefs, InvocationAssociation, NativeCallId,
    Observation, ObservationKind, OutcomeAssociation, SourceRecordRef, ToolAssociation,
    ToolCallCorrelations, ToolCallLifecycle, ToolOutcome, TurnAssociation,
};

/// Correlation failed because native-ID evidence was contradictory.
#[derive(Debug, thiserror::Error)]
pub enum CorrelationError {
    /// Multiple distinct requests used one native call identity.
    #[error("native call identity has multiple request observations")]
    DuplicateRequest,

    /// Result evidence for one native call asserted both success and failure.
    #[error("native call identity has conflicting result outcomes: {existing:?} and {incoming:?}")]
    ConflictingResultOutcomes {
        /// Outcome already accumulated for the native call.
        existing: ToolOutcome,
        /// Contradictory outcome supplied by the new observation.
        incoming: ToolOutcome,
    },

    /// An incremental lifecycle transition attempted to move backward.
    #[error("illegal tool-call lifecycle transition")]
    IllegalLifecycleTransition,

    /// Derived evidence violated a domain invariant.
    #[error(transparent)]
    Domain(#[from] DomainError),
}

#[derive(Debug, Clone)]
struct RequestEvidence {
    source: SourceRecordRef,
    turn: TurnAssociation,
    tool: CorrelatedTool,
    purpose: CorrelatedPurpose,
    invocation: CorrelatedInvocation,
}

#[derive(Debug, Clone)]
struct ResultEvidence {
    outcome: ToolOutcome,
    sources: Vec<SourceRecordRef>,
}

impl ResultEvidence {
    fn new(source: SourceRecordRef, outcome: ToolOutcome) -> Self {
        Self {
            outcome,
            sources: vec![source],
        }
    }

    fn merge(
        &mut self,
        source: SourceRecordRef,
        incoming: ToolOutcome,
    ) -> Result<(), CorrelationError> {
        let outcome = merge_outcomes(self.outcome, incoming)?;
        if !self.sources.contains(&source) {
            self.sources.push(source);
            self.sources
                .sort_by_key(|evidence| evidence.sequence().value());
        }
        self.outcome = outcome;
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum PartialCall {
    Pending(RequestEvidence),
    Completed {
        request: RequestEvidence,
        result: ResultEvidence,
    },
    Interrupted(RequestEvidence),
    OrphanedResult(ResultEvidence),
}

/// Stateful streaming correlator keyed only by trusted native call IDs.
#[derive(Debug, Default)]
pub struct CorrelationEngine {
    calls: HashMap<NativeCallId, PartialCall>,
}

impl CorrelationEngine {
    /// Observe one typed canonical observation.
    ///
    /// # Errors
    ///
    /// Returns an error when the same native call ID has contradictory request
    /// or result evidence.
    pub fn observe(&mut self, observation: &Observation) -> Result<(), CorrelationError> {
        if observation.kind() == ObservationKind::TurnAborted {
            self.interrupt_turn(observation.turn());
        }
        let CallAssociation::Call(call_id) = observation.call() else {
            return Ok(());
        };
        if observation.kind() == ObservationKind::ToolRequested {
            self.observe_request(call_id.clone(), observation)
        } else if matches!(
            observation.kind(),
            ObservationKind::ToolCompleted
                | ObservationKind::CommandCompleted
                | ObservationKind::PatchApplied
        ) {
            self.observe_result(call_id.clone(), observation)
        } else {
            Ok(())
        }
    }

    /// Finalize ordered correlations without inventing completions.
    ///
    /// # Errors
    ///
    /// Returns an error when source evidence cannot satisfy its non-empty
    /// invariant.
    pub fn finish(self) -> Result<ToolCallCorrelations, CorrelationError> {
        let mut calls: Vec<_> = self
            .calls
            .into_iter()
            .map(|(call_id, partial)| finalize_call(call_id, partial))
            .collect::<Result<_, _>>()?;
        calls.sort_by_key(|call| call.evidence().first().sequence().value());
        Ok(ToolCallCorrelations::new(calls))
    }

    fn observe_request(
        &mut self,
        call_id: NativeCallId,
        observation: &Observation,
    ) -> Result<(), CorrelationError> {
        let request = RequestEvidence {
            source: observation.source().clone(),
            turn: observation.turn().clone(),
            tool: match observation.tool() {
                ToolAssociation::NotApplicable => CorrelatedTool::Unnamed,
                ToolAssociation::Tool(name) => CorrelatedTool::Named(name.clone()),
            },
            purpose: match observation.invocation() {
                InvocationAssociation::NotApplicable => CorrelatedPurpose::Unknown,
                InvocationAssociation::Classified { purpose, .. } => {
                    CorrelatedPurpose::Known(purpose)
                }
            },
            invocation: match observation.invocation() {
                InvocationAssociation::NotApplicable => CorrelatedInvocation::Unknown,
                InvocationAssociation::Classified { digest, .. } => {
                    CorrelatedInvocation::Known(digest)
                }
            },
        };
        match self.calls.remove(&call_id) {
            None => {
                self.calls.insert(call_id, PartialCall::Pending(request));
                Ok(())
            }
            Some(PartialCall::OrphanedResult(result)) => {
                self.calls
                    .insert(call_id, PartialCall::Completed { request, result });
                Ok(())
            }
            Some(existing) => {
                self.calls.insert(call_id, existing);
                Err(CorrelationError::DuplicateRequest)
            }
        }
    }

    fn observe_result(
        &mut self,
        call_id: NativeCallId,
        observation: &Observation,
    ) -> Result<(), CorrelationError> {
        let outcome = match observation.outcome() {
            OutcomeAssociation::NotApplicable => ToolOutcome::Indeterminate,
            OutcomeAssociation::Tool(outcome) => outcome,
        };
        let source = observation.source().clone();
        match self.calls.remove(&call_id) {
            None => {
                self.calls.insert(
                    call_id,
                    PartialCall::OrphanedResult(ResultEvidence::new(source, outcome)),
                );
                Ok(())
            }
            Some(PartialCall::Pending(request) | PartialCall::Interrupted(request)) => {
                self.calls.insert(
                    call_id,
                    PartialCall::Completed {
                        request,
                        result: ResultEvidence::new(source, outcome),
                    },
                );
                Ok(())
            }
            Some(PartialCall::Completed {
                request,
                mut result,
            }) => {
                let merged = result.merge(source, outcome);
                self.calls
                    .insert(call_id, PartialCall::Completed { request, result });
                merged
            }
            Some(PartialCall::OrphanedResult(mut result)) => {
                let merged = result.merge(source, outcome);
                self.calls
                    .insert(call_id, PartialCall::OrphanedResult(result));
                merged
            }
        }
    }

    fn interrupt_turn(&mut self, aborted_turn: &TurnAssociation) {
        for state in self.calls.values_mut() {
            if let PartialCall::Pending(request) = state
                && request.turn == *aborted_turn
            {
                *state = PartialCall::Interrupted(request.clone());
            }
        }
    }
}

fn merge_outcomes(
    existing: ToolOutcome,
    incoming: ToolOutcome,
) -> Result<ToolOutcome, CorrelationError> {
    match (existing, incoming) {
        (left, right) if left == right => Ok(left),
        (ToolOutcome::Indeterminate, known) | (known, ToolOutcome::Indeterminate) => Ok(known),
        (existing, incoming) => {
            Err(CorrelationError::ConflictingResultOutcomes { existing, incoming })
        }
    }
}

/// Validate a lifecycle change between immutable source snapshots.
///
/// # Errors
///
/// Returns an error for identity changes or reverse/illegal transitions.
pub fn validate_lifecycle_transition(
    previous: &ToolCallLifecycle,
    next: &ToolCallLifecycle,
) -> Result<(), CorrelationError> {
    if lifecycle_call_id(previous) != lifecycle_call_id(next) {
        return Err(CorrelationError::IllegalLifecycleTransition);
    }
    let allowed = matches!(
        (previous, next),
        (
            ToolCallLifecycle::Pending { .. },
            ToolCallLifecycle::Pending { .. }
        ) | (
            ToolCallLifecycle::Pending { .. },
            ToolCallLifecycle::Completed { .. }
        ) | (
            ToolCallLifecycle::Pending { .. },
            ToolCallLifecycle::Interrupted { .. }
        ) | (
            ToolCallLifecycle::Interrupted { .. },
            ToolCallLifecycle::Interrupted { .. }
        ) | (
            ToolCallLifecycle::Interrupted { .. },
            ToolCallLifecycle::Completed { .. }
        ) | (
            ToolCallLifecycle::OrphanedResult { .. },
            ToolCallLifecycle::OrphanedResult { .. }
        ) | (
            ToolCallLifecycle::OrphanedResult { .. },
            ToolCallLifecycle::Completed { .. }
        ) | (
            ToolCallLifecycle::Completed { .. },
            ToolCallLifecycle::Completed { .. }
        )
    );
    if allowed {
        Ok(())
    } else {
        Err(CorrelationError::IllegalLifecycleTransition)
    }
}

fn lifecycle_call_id(lifecycle: &ToolCallLifecycle) -> &NativeCallId {
    match lifecycle {
        ToolCallLifecycle::Pending { call_id }
        | ToolCallLifecycle::Completed { call_id }
        | ToolCallLifecycle::Interrupted { call_id }
        | ToolCallLifecycle::OrphanedResult { call_id } => call_id,
    }
}

fn finalize_call(
    call_id: NativeCallId,
    partial: PartialCall,
) -> Result<CorrelatedToolCall, CorrelationError> {
    match partial {
        PartialCall::Pending(request) => Ok(CorrelatedToolCall::new(
            call_id.clone(),
            ToolCallLifecycle::Pending { call_id },
            request.tool,
            request.purpose,
            request.invocation,
            CorrelatedOutcome::Missing,
            EvidenceRefs::new([request.source])?,
        )),
        PartialCall::Interrupted(request) => Ok(CorrelatedToolCall::new(
            call_id.clone(),
            ToolCallLifecycle::Interrupted { call_id },
            request.tool,
            request.purpose,
            request.invocation,
            CorrelatedOutcome::Missing,
            EvidenceRefs::new([request.source])?,
        )),
        PartialCall::OrphanedResult(result) => Ok(CorrelatedToolCall::new(
            call_id.clone(),
            ToolCallLifecycle::OrphanedResult { call_id },
            CorrelatedTool::Unnamed,
            CorrelatedPurpose::Unknown,
            CorrelatedInvocation::Unknown,
            CorrelatedOutcome::Known(result.outcome),
            ordered_evidence(result.sources)?,
        )),
        PartialCall::Completed { request, result } => Ok(CorrelatedToolCall::new(
            call_id.clone(),
            ToolCallLifecycle::Completed { call_id },
            request.tool,
            request.purpose,
            request.invocation,
            CorrelatedOutcome::Known(result.outcome),
            ordered_evidence(
                std::iter::once(request.source)
                    .chain(result.sources)
                    .collect(),
            )?,
        )),
    }
}

fn ordered_evidence(mut sources: Vec<SourceRecordRef>) -> Result<EvidenceRefs, DomainError> {
    sources.sort_by_key(|source| source.sequence().value());
    sources.dedup();
    EvidenceRefs::new(sources)
}

#[cfg(test)]
mod tests {
    use harness_graph_domain::{
        CallAssociation, ContextAssociation, CorrelatedOutcome, InvocationAssociation,
        NativeCallId, Observation, ObservationKind, OccurredAt, OutcomeAssociation, PayloadDigest,
        RecordSequence, SessionId, SourceDigest, SourceRecordRef, ToolAssociation,
        ToolCallLifecycle, ToolName, ToolOutcome, ToolPurpose, TurnAssociation, TurnId,
    };

    use super::{CorrelationEngine, CorrelationError, validate_lifecycle_transition};

    #[test]
    fn completed_call_cannot_regress_to_pending() -> Result<(), Box<dyn std::error::Error>> {
        let call_id = NativeCallId::new("call-1")?;
        let completed = ToolCallLifecycle::Completed {
            call_id: call_id.clone(),
        };
        let pending = ToolCallLifecycle::Pending { call_id };
        assert!(validate_lifecycle_transition(&completed, &pending).is_err());
        Ok(())
    }

    #[test]
    fn native_ids_preserve_every_partial_lifecycle_state() -> Result<(), Box<dyn std::error::Error>>
    {
        let pending_id = NativeCallId::new("pending")?;
        let interrupted_id = NativeCallId::new("interrupted")?;
        let orphan_id = NativeCallId::new("orphan")?;
        let completed_id = NativeCallId::new("completed")?;
        let turn_a = TurnId::new("turn-a")?;
        let turn_b = TurnId::new("turn-b")?;
        let mut engine = CorrelationEngine::default();

        engine.observe(&request(0, pending_id.clone(), turn_a)?)?;
        engine.observe(&request(1, interrupted_id.clone(), turn_b.clone())?)?;
        engine.observe(&turn_abort(2, turn_b)?)?;
        engine.observe(&result(3, orphan_id.clone())?)?;
        engine.observe(&request(4, completed_id.clone(), TurnId::new("turn-c")?)?)?;
        engine.observe(&result(5, completed_id.clone())?)?;

        let correlations = engine.finish()?;
        let states: Vec<_> = correlations
            .iter()
            .map(|correlation| correlation.lifecycle().clone())
            .collect();
        assert_eq!(
            states,
            vec![
                ToolCallLifecycle::Pending {
                    call_id: pending_id,
                },
                ToolCallLifecycle::Interrupted {
                    call_id: interrupted_id,
                },
                ToolCallLifecycle::OrphanedResult { call_id: orphan_id },
                ToolCallLifecycle::Completed {
                    call_id: completed_id,
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn mirrored_results_merge_without_discarding_evidence() -> Result<(), Box<dyn std::error::Error>>
    {
        let call_id = NativeCallId::new("mirrored-call")?;
        let mut engine = CorrelationEngine::default();

        engine.observe(&request(0, call_id.clone(), TurnId::new("mirrored-turn")?)?)?;
        engine.observe(&result_with_outcome(
            1,
            call_id.clone(),
            ObservationKind::CommandCompleted,
            ToolOutcome::Succeeded,
        )?)?;
        engine.observe(&result_with_outcome(
            2,
            call_id,
            ObservationKind::ToolCompleted,
            ToolOutcome::Indeterminate,
        )?)?;

        let correlations = engine.finish()?;
        let correlation = correlations
            .iter()
            .next()
            .ok_or_else(|| std::io::Error::other("expected a completed correlation"))?;
        assert!(matches!(
            correlation.lifecycle(),
            ToolCallLifecycle::Completed { .. }
        ));
        assert_eq!(
            correlation.outcome(),
            CorrelatedOutcome::Known(ToolOutcome::Succeeded)
        );
        assert_eq!(
            correlation
                .evidence()
                .iter()
                .map(|source| source.sequence().value())
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        Ok(())
    }

    #[test]
    fn result_accumulation_is_ordered_associative_and_idempotent()
    -> Result<(), Box<dyn std::error::Error>> {
        let call_id = NativeCallId::new("associative-call")?;
        let request = request(10, call_id.clone(), TurnId::new("associative-turn")?)?;
        let early_result = result_with_outcome(
            20,
            call_id.clone(),
            ObservationKind::ToolCompleted,
            ToolOutcome::Succeeded,
        )?;
        let late_result = result_with_outcome(
            30,
            call_id,
            ObservationKind::CommandCompleted,
            ToolOutcome::Indeterminate,
        )?;

        let mut orphan_first = CorrelationEngine::default();
        orphan_first.observe(&late_result)?;
        orphan_first.observe(&early_result)?;
        orphan_first.observe(&early_result)?;
        orphan_first.observe(&request)?;

        let mut request_first = CorrelationEngine::default();
        request_first.observe(&request)?;
        request_first.observe(&early_result)?;
        request_first.observe(&late_result)?;

        let orphan_first = orphan_first.finish()?;
        let request_first = request_first.finish()?;
        assert_eq!(orphan_first, request_first);
        let correlation = orphan_first
            .iter()
            .next()
            .ok_or_else(|| std::io::Error::other("expected an accumulated correlation"))?;
        assert_eq!(correlation.evidence().count().value(), 3);
        assert_eq!(
            correlation
                .evidence()
                .iter()
                .map(|source| source.sequence().value())
                .collect::<Vec<_>>(),
            vec![11, 21, 31]
        );
        Ok(())
    }

    #[test]
    fn success_and_failure_results_remain_a_typed_conflict()
    -> Result<(), Box<dyn std::error::Error>> {
        let call_id = NativeCallId::new("conflicting-call")?;
        let mut engine = CorrelationEngine::default();
        engine.observe(&request(
            0,
            call_id.clone(),
            TurnId::new("conflicting-turn")?,
        )?)?;
        engine.observe(&result_with_outcome(
            1,
            call_id.clone(),
            ObservationKind::CommandCompleted,
            ToolOutcome::Succeeded,
        )?)?;

        let conflict = engine.observe(&result_with_outcome(
            2,
            call_id,
            ObservationKind::ToolCompleted,
            ToolOutcome::Failed,
        )?);
        assert!(matches!(
            conflict,
            Err(CorrelationError::ConflictingResultOutcomes {
                existing: ToolOutcome::Succeeded,
                incoming: ToolOutcome::Failed,
            })
        ));

        let correlations = engine.finish()?;
        let correlation = correlations
            .iter()
            .next()
            .ok_or_else(|| std::io::Error::other("expected the valid result to remain"))?;
        assert_eq!(
            correlation.outcome(),
            CorrelatedOutcome::Known(ToolOutcome::Succeeded)
        );
        assert_eq!(correlation.evidence().count().value(), 2);
        Ok(())
    }

    fn request(
        offset: u64,
        call_id: NativeCallId,
        turn_id: TurnId,
    ) -> Result<Observation, Box<dyn std::error::Error>> {
        observation(
            offset,
            ObservationKind::ToolRequested,
            TurnAssociation::Turn(turn_id),
            CallAssociation::Call(call_id),
            ToolAssociation::Tool(ToolName::new("exec_command")?),
            InvocationAssociation::Classified {
                digest: harness_graph_domain::InvocationDigest::hash(b"request"),
                purpose: ToolPurpose::Execute,
            },
            OutcomeAssociation::NotApplicable,
        )
    }

    fn result(
        offset: u64,
        call_id: NativeCallId,
    ) -> Result<Observation, Box<dyn std::error::Error>> {
        result_with_outcome(
            offset,
            call_id,
            ObservationKind::CommandCompleted,
            ToolOutcome::Succeeded,
        )
    }

    fn result_with_outcome(
        offset: u64,
        call_id: NativeCallId,
        kind: ObservationKind,
        outcome: ToolOutcome,
    ) -> Result<Observation, Box<dyn std::error::Error>> {
        observation(
            offset,
            kind,
            TurnAssociation::SessionScoped,
            CallAssociation::Call(call_id),
            ToolAssociation::NotApplicable,
            InvocationAssociation::NotApplicable,
            OutcomeAssociation::Tool(outcome),
        )
    }

    fn turn_abort(offset: u64, turn_id: TurnId) -> Result<Observation, Box<dyn std::error::Error>> {
        observation(
            offset,
            ObservationKind::TurnAborted,
            TurnAssociation::Turn(turn_id),
            CallAssociation::NotApplicable,
            ToolAssociation::NotApplicable,
            InvocationAssociation::NotApplicable,
            OutcomeAssociation::NotApplicable,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn observation(
        offset: u64,
        kind: ObservationKind,
        turn: TurnAssociation,
        call: CallAssociation,
        tool: ToolAssociation,
        invocation: InvocationAssociation,
        outcome: OutcomeAssociation,
    ) -> Result<Observation, Box<dyn std::error::Error>> {
        let source = SourceRecordRef::new(
            SessionId::parse("019c63db-2995-74c3-b898-c1b92a8e1317")?,
            SourceDigest::hash(b"source-safe lifecycle fixture"),
            RecordSequence::from_zero_based(offset),
        );
        Ok(Observation::new(
            source,
            OccurredAt::parse("2026-02-16T10:00:00Z")?,
            kind,
            PayloadDigest::hash(format!("record-{offset}").as_bytes()),
            ContextAssociation::NotApplicable,
            turn,
            call,
            tool,
            invocation,
            outcome,
        ))
    }
}
