//! Typed Codex native-record decoder.

use harness_graph_domain::{
    CallAssociation, ContextAssociation, ContextDigest, DecodedNativeRecord, InvocationAssociation,
    InvocationDigest, KnownNativeRecord, NativeCallId, NativeRecordKind, Observation,
    ObservationKind, OccurredAt, OutcomeAssociation, PayloadDigest, SourceRecordRef,
    ToolAssociation, ToolName, ToolOutcome, ToolPurpose, TurnAssociation, TurnId,
    UnsupportedNativeRecord,
};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::ProtocolError;

#[derive(Deserialize)]
struct RawEnvelope {
    timestamp: String,
    #[serde(rename = "type")]
    record_type: String,
    payload: Value,
}

#[derive(Clone, Copy)]
enum Mapping {
    Known(ObservationKind),
    Unsupported,
}

/// Decode one line from a verified Codex `raw/rollout.jsonl` snapshot.
///
/// # Errors
///
/// Returns an error when the envelope is malformed, its timestamp or typed
/// identifiers are invalid, or the payload cannot be canonicalized safely.
pub fn decode_codex_line(
    line: &str,
    source: SourceRecordRef,
) -> Result<DecodedNativeRecord, ProtocolError> {
    let sequence = source.sequence();
    let envelope: RawEnvelope = serde_json::from_str(line)
        .map_err(|source| ProtocolError::InvalidJson { sequence, source })?;
    let occurred_at = OccurredAt::parse(&envelope.timestamp)
        .map_err(|source| ProtocolError::InvalidDomainValue { sequence, source })?;
    let payload_bytes = serde_json::to_vec(&envelope.payload)
        .map_err(|source| ProtocolError::Canonicalization { sequence, source })?;
    let payload_digest = PayloadDigest::hash(&payload_bytes);
    let payload_type = string_field(&envelope.payload, "type");
    let mapping = map_kind(&envelope.record_type, payload_type);

    match mapping {
        Mapping::Known(kind) => {
            let context = context_association(kind, &envelope.payload, sequence)?;
            let turn = turn_association(&envelope.payload, sequence)?;
            let call = call_association(&envelope.payload, sequence)?;
            let tool = tool_association(kind, payload_type, &envelope.payload, sequence)?;
            let invocation = invocation_association(kind, payload_type, &envelope.payload);
            let outcome = outcome_association(kind, payload_type, &envelope.payload);
            Ok(DecodedNativeRecord::Known(KnownNativeRecord::new(
                Observation::new(
                    source,
                    occurred_at,
                    kind,
                    payload_digest,
                    context,
                    turn,
                    call,
                    tool,
                    invocation,
                    outcome,
                ),
            )))
        }
        Mapping::Unsupported => {
            let qualified_kind = payload_type.map_or_else(
                || envelope.record_type.clone(),
                |payload_type| format!("{}/{payload_type}", envelope.record_type),
            );
            let native_kind = NativeRecordKind::new(qualified_kind)
                .map_err(|source| ProtocolError::InvalidDomainValue { sequence, source })?;
            Ok(DecodedNativeRecord::Unsupported(
                UnsupportedNativeRecord::new(source, occurred_at, native_kind, payload_digest),
            ))
        }
    }
}

fn map_kind(record_type: &str, payload_type: Option<&str>) -> Mapping {
    match (record_type, payload_type) {
        ("session_meta", _) => Mapping::Known(ObservationKind::SessionMetadataAsserted),
        ("turn_context", _) => Mapping::Known(ObservationKind::ContextAsserted),
        ("compacted", _) => Mapping::Known(ObservationKind::ContextCompacted),
        ("world_state", _) => Mapping::Known(ObservationKind::WorldStateAsserted),
        ("inter_agent_communication_metadata", _) => {
            Mapping::Known(ObservationKind::InterAgentMessageObserved)
        }
        ("response_item", Some(kind)) => map_response_item(kind),
        ("event_msg", Some(kind)) => map_event(kind),
        _ => Mapping::Unsupported,
    }
}

fn map_response_item(kind: &str) -> Mapping {
    match kind {
        "function_call"
        | "custom_tool_call"
        | "dynamic_tool_call_request"
        | "web_search_call"
        | "image_generation_call"
        | "tool_search_call" => Mapping::Known(ObservationKind::ToolRequested),
        "function_call_output"
        | "custom_tool_call_output"
        | "dynamic_tool_call_response"
        | "tool_search_output" => Mapping::Known(ObservationKind::ToolCompleted),
        "message" | "reasoning" => Mapping::Known(ObservationKind::AgentMessageReceived),
        "agent_message" => Mapping::Known(ObservationKind::InterAgentMessageObserved),
        _ => Mapping::Unsupported,
    }
}

fn map_event(kind: &str) -> Mapping {
    match kind {
        "task_started" => Mapping::Known(ObservationKind::TaskStarted),
        "task_complete" => Mapping::Known(ObservationKind::TaskCompleted),
        "turn_aborted" => Mapping::Known(ObservationKind::TurnAborted),
        "user_message" => Mapping::Known(ObservationKind::UserMessageReceived),
        "agent_message" | "agent_reasoning" => {
            Mapping::Known(ObservationKind::AgentMessageReceived)
        }
        "token_count" => Mapping::Known(ObservationKind::TokenUsageObserved),
        "thread_settings_applied" => Mapping::Known(ObservationKind::ThreadSettingsApplied),
        "thread_goal_updated" => Mapping::Known(ObservationKind::GoalUpdated),
        "context_compacted" => Mapping::Known(ObservationKind::ContextCompacted),
        "patch_apply_end" => Mapping::Known(ObservationKind::PatchApplied),
        "exec_command_end" => Mapping::Known(ObservationKind::CommandCompleted),
        "sub_agent_activity"
        | "collab_agent_spawn_end"
        | "collab_waiting_end"
        | "collab_close_end" => Mapping::Known(ObservationKind::SubAgentActivityObserved),
        "thread_rolled_back" => Mapping::Known(ObservationKind::ThreadRolledBack),
        "error" => Mapping::Known(ObservationKind::ErrorObserved),
        "web_search_end" | "mcp_tool_call_end" | "image_generation_end" => {
            Mapping::Known(ObservationKind::ToolCompleted)
        }
        _ => Mapping::Unsupported,
    }
}

fn context_association(
    kind: ObservationKind,
    payload: &Value,
    sequence: harness_graph_domain::RecordSequence,
) -> Result<ContextAssociation, ProtocolError> {
    if kind != ObservationKind::ContextAsserted {
        return Ok(ContextAssociation::NotApplicable);
    }

    let mut stable_context = payload
        .as_object()
        .cloned()
        .ok_or(ProtocolError::MissingField {
            sequence,
            field: "payload object",
        })?;
    remove_volatile_context_fields(&mut stable_context);
    let canonical = serde_json::to_vec(&stable_context)
        .map_err(|source| ProtocolError::Canonicalization { sequence, source })?;
    Ok(ContextAssociation::Asserted(ContextDigest::hash(
        &canonical,
    )))
}

fn remove_volatile_context_fields(context: &mut Map<String, Value>) {
    for field in ["turn_id", "summary", "current_date"] {
        context.remove(field);
    }
}

fn turn_association(
    payload: &Value,
    sequence: harness_graph_domain::RecordSequence,
) -> Result<TurnAssociation, ProtocolError> {
    string_field(payload, "turn_id").map_or(Ok(TurnAssociation::SessionScoped), |value| {
        TurnId::new(value)
            .map(TurnAssociation::Turn)
            .map_err(|source| ProtocolError::InvalidDomainValue { sequence, source })
    })
}

fn call_association(
    payload: &Value,
    sequence: harness_graph_domain::RecordSequence,
) -> Result<CallAssociation, ProtocolError> {
    string_field(payload, "call_id").map_or(Ok(CallAssociation::NotApplicable), |value| {
        NativeCallId::new(value)
            .map(CallAssociation::Call)
            .map_err(|source| ProtocolError::InvalidDomainValue { sequence, source })
    })
}

fn tool_association(
    kind: ObservationKind,
    payload_type: Option<&str>,
    payload: &Value,
    sequence: harness_graph_domain::RecordSequence,
) -> Result<ToolAssociation, ProtocolError> {
    if !matches!(
        kind,
        ObservationKind::ToolRequested
            | ObservationKind::ToolCompleted
            | ObservationKind::CommandCompleted
            | ObservationKind::PatchApplied
    ) {
        return Ok(ToolAssociation::NotApplicable);
    }

    let candidate = string_field(payload, "name").or(payload_type);
    candidate.map_or(Ok(ToolAssociation::NotApplicable), |value| {
        ToolName::new(value)
            .map(ToolAssociation::Tool)
            .map_err(|source| ProtocolError::InvalidDomainValue { sequence, source })
    })
}

fn invocation_association(
    kind: ObservationKind,
    payload_type: Option<&str>,
    payload: &Value,
) -> InvocationAssociation {
    if kind != ObservationKind::ToolRequested {
        return InvocationAssociation::NotApplicable;
    }
    let tool_name = string_field(payload, "name")
        .or(payload_type)
        .unwrap_or("unknown");
    let raw_input = string_field(payload, "arguments")
        .or_else(|| string_field(payload, "input"))
        .unwrap_or("");
    let mut fingerprint = Vec::with_capacity(tool_name.len() + raw_input.len() + 1);
    fingerprint.extend_from_slice(tool_name.as_bytes());
    fingerprint.push(0);
    fingerprint.extend_from_slice(raw_input.as_bytes());
    let purpose = classify_tool_purpose(tool_name, raw_input);
    InvocationAssociation::Classified {
        digest: InvocationDigest::hash(&fingerprint),
        purpose,
    }
}

fn classify_tool_purpose(tool_name: &str, raw_input: &str) -> ToolPurpose {
    let tool_name = tool_name.to_ascii_lowercase();
    if matches!(
        tool_name.as_str(),
        "apply_patch" | "write_file" | "edit_file"
    ) {
        return ToolPurpose::Modify;
    }
    if tool_name.contains("search") {
        return ToolPurpose::Search;
    }
    if matches!(tool_name.as_str(), "read_file" | "list_files") {
        return ToolPurpose::Inspect;
    }
    if tool_name != "exec_command" {
        return ToolPurpose::Ambiguous;
    }

    let command = extract_command(raw_input).to_ascii_lowercase();
    if contains_command(
        &command,
        &["rm ", "rm\t", "git reset", "drop database", "truncate "],
    ) {
        ToolPurpose::Destructive
    } else if contains_command(&command, &["sudo ", "doas ", "pkexec "]) {
        ToolPurpose::PermissionEscalation
    } else if contains_command(
        &command,
        &[
            "cargo test",
            "cargo check",
            "cargo clippy",
            "cargo fmt",
            "pytest",
            "pnpm test",
            "npm test",
            "typecheck",
            "--version",
        ],
    ) {
        ToolPurpose::Verify
    } else if contains_command(
        &command,
        &[
            "cargo install",
            "brew install",
            "apt install",
            "npm install",
            "pnpm add",
        ],
    ) {
        ToolPurpose::Install
    } else if contains_command(
        &command,
        &["rg ", "grep ", "find ", "git grep", "fd ", "ripgrep"],
    ) {
        ToolPurpose::Search
    } else if contains_command(
        &command,
        &[
            "ls ",
            "cat ",
            "sed ",
            "head ",
            "tail ",
            "git status",
            "git diff",
            "git log",
            "wc ",
            "file ",
            "stat ",
        ],
    ) {
        ToolPurpose::Inspect
    } else if contains_command(&command, &["mkdir ", "mv ", "cp ", "chmod ", "touch "]) {
        ToolPurpose::Modify
    } else if contains_command(&command, &["curl ", "wget ", "ssh ", "scp "]) {
        ToolPurpose::NetworkAccess
    } else {
        ToolPurpose::Execute
    }
}

fn extract_command(raw_input: &str) -> String {
    serde_json::from_str::<Value>(raw_input)
        .ok()
        .and_then(|value| value.get("cmd").and_then(Value::as_str).map(str::to_owned))
        .unwrap_or_else(|| raw_input.to_owned())
}

fn contains_command(command: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| {
        command.starts_with(needle)
            || command.contains(&format!(" {needle}"))
            || command.contains(&format!(";{needle}"))
            || command.contains(&format!("|{needle}"))
            || command.contains(&format!("&&{needle}"))
    })
}

fn outcome_association(
    kind: ObservationKind,
    payload_type: Option<&str>,
    payload: &Value,
) -> OutcomeAssociation {
    if !matches!(
        kind,
        ObservationKind::ToolCompleted
            | ObservationKind::CommandCompleted
            | ObservationKind::PatchApplied
    ) {
        return OutcomeAssociation::NotApplicable;
    }
    if let Some(success) = payload.get("success").and_then(Value::as_bool) {
        return OutcomeAssociation::Tool(if success {
            ToolOutcome::Succeeded
        } else {
            ToolOutcome::Failed
        });
    }
    if let Some(exit_code) = payload.get("exit_code").and_then(Value::as_i64) {
        return OutcomeAssociation::Tool(if exit_code == 0 {
            ToolOutcome::Succeeded
        } else {
            ToolOutcome::Failed
        });
    }
    if let Some(output) = string_field(payload, "output") {
        if output.contains("Process exited with code 0") {
            return OutcomeAssociation::Tool(ToolOutcome::Succeeded);
        }
        if output.contains("Process exited with code ") {
            return OutcomeAssociation::Tool(ToolOutcome::Failed);
        }
    }
    if matches!(
        payload_type,
        Some("custom_tool_call_output" | "patch_apply_end")
    ) {
        return OutcomeAssociation::Tool(ToolOutcome::Succeeded);
    }
    OutcomeAssociation::Tool(ToolOutcome::Indeterminate)
}

fn string_field<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(Value::as_str)
}

#[cfg(test)]
mod tests {
    use harness_graph_domain::{
        ContextAssociation, ContextDigest, DecodedNativeRecord, ObservationKind, RecordSequence,
        SessionId, SourceDigest, SourceRecordRef,
    };

    use super::decode_codex_line;

    fn source() -> Result<SourceRecordRef, Box<dyn std::error::Error>> {
        Ok(SourceRecordRef::new(
            SessionId::parse("019c63db-2995-74c3-b898-c1b92a8e1317")?,
            SourceDigest::hash(b"fixture"),
            RecordSequence::from_zero_based(0),
        ))
    }

    #[test]
    fn context_hash_ignores_turn_identity_and_summary() -> Result<(), Box<dyn std::error::Error>> {
        let left = r#"{"timestamp":"2026-02-16T10:00:00Z","type":"turn_context","payload":{"turn_id":"turn-a","summary":"first","model":"mistral"}}"#;
        let right = r#"{"timestamp":"2026-02-16T10:00:01Z","type":"turn_context","payload":{"turn_id":"turn-b","summary":"second","model":"mistral"}}"#;

        assert_eq!(
            extract_context_digest(left)?,
            extract_context_digest(right)?
        );
        Ok(())
    }

    fn extract_context_digest(line: &str) -> Result<ContextDigest, Box<dyn std::error::Error>> {
        match decode_codex_line(line, source()?)? {
            DecodedNativeRecord::Known(record) => match record.observation().context() {
                ContextAssociation::Asserted(digest) => Ok(digest),
                ContextAssociation::NotApplicable => Err("missing context digest".into()),
            },
            DecodedNativeRecord::Unsupported(_) => Err("unexpected quarantine".into()),
        }
    }

    #[test]
    fn unknown_payload_is_quarantined() -> Result<(), Box<dyn std::error::Error>> {
        let line = r#"{"timestamp":"2026-02-16T10:00:00Z","type":"event_msg","payload":{"type":"future_event"}}"#;
        let decoded = decode_codex_line(line, source()?)?;
        assert!(matches!(decoded, DecodedNativeRecord::Unsupported(_)));
        Ok(())
    }

    #[test]
    fn function_call_becomes_tool_request() -> Result<(), Box<dyn std::error::Error>> {
        let line = r#"{"timestamp":"2026-02-16T10:00:00Z","type":"response_item","payload":{"type":"function_call","name":"exec_command","call_id":"call-1","arguments":"{}"}}"#;
        let decoded = decode_codex_line(line, source()?)?;
        let DecodedNativeRecord::Known(record) = decoded else {
            return Err("known function call was quarantined".into());
        };
        assert_eq!(record.observation().kind(), ObservationKind::ToolRequested);
        Ok(())
    }
}
