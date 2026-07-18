//! Content-addressed normalized execution-path derivation.

use harness_graph_domain::{
    DomainError, ExecutionPath, PathSignature, PathStep, PathSteps, SemanticActivities,
};

/// Execution-path derivation failed.
#[derive(Debug, thiserror::Error)]
pub enum PathAnalysisError {
    /// No semantic activity could form a path.
    #[error(transparent)]
    Domain(#[from] DomainError),
}

/// Derive a stable normalized path from ordered semantic activities.
///
/// # Errors
///
/// Returns an error when the activity sequence is empty.
pub fn derive_path(activities: &SemanticActivities) -> Result<ExecutionPath, PathAnalysisError> {
    let steps: Vec<_> = activities
        .iter()
        .map(|activity| PathStep::new(activity.kind(), activity.status()))
        .collect();
    let mut canonical = String::new();
    for step in &steps {
        if !canonical.is_empty() {
            canonical.push('>');
        }
        canonical.push_str(step.kind().as_str());
        canonical.push(':');
        canonical.push_str(step.status().as_str());
    }
    let steps = PathSteps::new(steps)?;
    Ok(ExecutionPath::new(
        PathSignature::hash(canonical.as_bytes()),
        steps,
    ))
}
