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

    /// Multiple distinct results used one native call identity.
    #[error("native call identity has multiple result observations")]
    DuplicateResult,

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
    source: SourceRecordRef,
    outcome: ToolOutcome,
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
        let result = ResultEvidence {
            source: observation.source().clone(),
            outcome,
        };
        match self.calls.remove(&call_id) {
            None => {
                self.calls
                    .insert(call_id, PartialCall::OrphanedResult(result));
                Ok(())
            }
            Some(PartialCall::Pending(request) | PartialCall::Interrupted(request)) => {
                self.calls
                    .insert(call_id, PartialCall::Completed { request, result });
                Ok(())
            }
            Some(existing) => {
                self.calls.insert(call_id, existing);
                Err(CorrelationError::DuplicateResult)
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
            EvidenceRefs::new([result.source])?,
        )),
        PartialCall::Completed { request, result } => Ok(CorrelatedToolCall::new(
            call_id.clone(),
            ToolCallLifecycle::Completed { call_id },
            request.tool,
            request.purpose,
            request.invocation,
            CorrelatedOutcome::Known(result.outcome),
            EvidenceRefs::new([request.source, result.source])?,
        )),
    }
}

#[cfg(test)]
mod tests {
    use harness_graph_domain::{
        CallAssociation, ContextAssociation, InvocationAssociation, NativeCallId, Observation,
        ObservationKind, OccurredAt, OutcomeAssociation, PayloadDigest, RecordSequence, SessionId,
        SourceDigest, SourceRecordRef, ToolAssociation, ToolCallLifecycle, ToolName, ToolOutcome,
        ToolPurpose, TurnAssociation, TurnId,
    };

    use super::{CorrelationEngine, validate_lifecycle_transition};

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
        observation(
            offset,
            ObservationKind::CommandCompleted,
            TurnAssociation::SessionScoped,
            CallAssociation::Call(call_id),
            ToolAssociation::NotApplicable,
            InvocationAssociation::NotApplicable,
            OutcomeAssociation::Tool(ToolOutcome::Succeeded),
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
