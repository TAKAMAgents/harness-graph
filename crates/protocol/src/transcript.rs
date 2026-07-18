//! Strict textual projection from a canonical Codex JSONL record.
//!
//! The types in this module are deliberately not serializable. They hold
//! sensitive source text only long enough for the local disclosure and
//! redaction boundary to consume it.

use harness_graph_domain::{
    CallAssociation, NativeCallId, OccurredAt, RecordCount, SourceRecordRef, ToolAssociation,
    ToolName, TurnAssociation, TurnId,
};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use serde_json::Value;

use crate::ProtocolError;

#[derive(Deserialize)]
struct RawTranscriptEnvelope {
    timestamp: String,
    #[serde(rename = "type")]
    record_type: String,
    payload: Value,
}

/// Closed semantic class for transcript text eligible for local scanning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TranscriptRecordClass {
    /// Human request or follow-up.
    UserMessage,
    /// Agent response intended for the user.
    AgentMessage,
    /// Message exchanged between collaborating agents.
    InterAgentMessage,
    /// Tool invocation or command request.
    ToolRequest,
    /// Tool result.
    ToolResult,
    /// Command execution result.
    CommandResult,
    /// Patch application result.
    PatchResult,
    /// Explicit runtime error.
    Error,
    /// Verification evidence such as a completed search.
    Verification,
    /// Final task-completion summary.
    CompletionSummary,
}

impl TranscriptRecordClass {
    /// Stable provider and graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UserMessage => "user_message",
            Self::AgentMessage => "agent_message",
            Self::InterAgentMessage => "inter_agent_message",
            Self::ToolRequest => "tool_request",
            Self::ToolResult => "tool_result",
            Self::CommandResult => "command_result",
            Self::PatchResult => "patch_result",
            Self::Error => "error",
            Self::Verification => "verification",
            Self::CompletionSummary => "completion_summary",
        }
    }
}

/// Speaker or producer of a transcript fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TranscriptRole {
    /// Human operator.
    User,
    /// Coding agent.
    Agent,
    /// Collaborating sub-agent.
    Collaborator,
    /// Tool or runtime boundary.
    Tool,
}

impl TranscriptRole {
    /// Stable provider and graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Agent => "agent",
            Self::Collaborator => "collaborator",
            Self::Tool => "tool",
        }
    }
}

/// Allowlisted native field containing eligible text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TranscriptField {
    /// Native `message` field.
    Message,
    /// Text member of a response content array.
    ContentText,
    /// Tool arguments.
    Arguments,
    /// Custom-tool input.
    Input,
    /// Tool output.
    Output,
    /// Standard output.
    Stdout,
    /// Standard error.
    Stderr,
    /// Combined command output.
    AggregatedOutput,
    /// Final agent message reported by task completion.
    LastAgentMessage,
    /// Search query.
    Query,
    /// Structured tool action.
    Action,
    /// Structured tool invocation.
    Invocation,
    /// Structured tool result.
    Result,
    /// Structured patch changes.
    Changes,
    /// Tool-search execution metadata.
    Execution,
    /// Tool-search results.
    Tools,
}

impl TranscriptField {
    /// Stable source-safe field identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::ContentText => "content_text",
            Self::Arguments => "arguments",
            Self::Input => "input",
            Self::Output => "output",
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
            Self::AggregatedOutput => "aggregated_output",
            Self::LastAgentMessage => "last_agent_message",
            Self::Query => "query",
            Self::Action => "action",
            Self::Invocation => "invocation",
            Self::Result => "result",
            Self::Changes => "changes",
            Self::Execution => "execution",
            Self::Tools => "tools",
        }
    }
}

/// Stable field anchor within one native source record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TranscriptFieldPath {
    field: TranscriptField,
    ordinal: u32,
}

impl TranscriptFieldPath {
    fn from_offset(field: TranscriptField, offset: usize) -> Self {
        Self {
            field,
            ordinal: u32::try_from(offset).unwrap_or(u32::MAX),
        }
    }

    /// Allowlisted native field.
    #[must_use]
    pub const fn field(self) -> TranscriptField {
        self.field
    }

    /// Zero-based occurrence of that field in the record.
    #[must_use]
    pub const fn ordinal(self) -> u32 {
        self.ordinal
    }
}

/// Closed reason why a native record produced no eligible transcript text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscriptExclusionReason {
    /// System, developer, or instruction-bearing context is forbidden.
    InstructionBearing,
    /// Hidden or encrypted reasoning is forbidden.
    HiddenReasoning,
    /// The record contains only assets, binary data, or unsupported media.
    AssetOrBinary,
    /// The record is known but contains no allowlisted non-empty text.
    NoAllowedText,
    /// The native record kind is outside the closed transcript contract.
    UnsupportedRecord,
}

/// Sensitive text that cannot be printed or serialized accidentally.
#[derive(Clone)]
struct SensitiveTranscriptText(SecretString);

impl SensitiveTranscriptText {
    fn new(value: &str) -> Option<Self> {
        let value = value.trim();
        (!value.is_empty()).then(|| Self(SecretString::from(value.to_owned())))
    }

    fn expose(&self) -> &str {
        self.0.expose_secret()
    }
}

impl std::fmt::Debug for SensitiveTranscriptText {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SensitiveTranscriptText([redacted])")
    }
}

/// One allowlisted, still-sensitive transcript fragment.
#[derive(Clone)]
pub struct SensitiveTranscriptFragment {
    source: SourceRecordRef,
    occurred_at: OccurredAt,
    class: TranscriptRecordClass,
    role: TranscriptRole,
    field_path: TranscriptFieldPath,
    turn: TurnAssociation,
    call: CallAssociation,
    tool: ToolAssociation,
    text: SensitiveTranscriptText,
}

impl SensitiveTranscriptFragment {
    /// Source record anchor.
    #[must_use]
    pub const fn source(&self) -> &SourceRecordRef {
        &self.source
    }

    /// Source timestamp.
    #[must_use]
    pub const fn occurred_at(&self) -> OccurredAt {
        self.occurred_at
    }

    /// Closed record class.
    #[must_use]
    pub const fn class(&self) -> TranscriptRecordClass {
        self.class
    }

    /// Fragment producer.
    #[must_use]
    pub const fn role(&self) -> TranscriptRole {
        self.role
    }

    /// Allowlisted field anchor.
    #[must_use]
    pub const fn field_path(&self) -> TranscriptFieldPath {
        self.field_path
    }

    /// Native turn association.
    #[must_use]
    pub const fn turn(&self) -> &TurnAssociation {
        &self.turn
    }

    /// Native call association.
    #[must_use]
    pub const fn call(&self) -> &CallAssociation {
        &self.call
    }

    /// Native tool association.
    #[must_use]
    pub const fn tool(&self) -> &ToolAssociation {
        &self.tool
    }

    /// Expose source text only to the mandatory local scanner.
    ///
    /// Callers must never log, persist, or serialize the returned value.
    #[must_use]
    pub fn expose_for_local_scanner(&self) -> &str {
        self.text.expose()
    }
}

impl std::fmt::Debug for SensitiveTranscriptFragment {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SensitiveTranscriptFragment")
            .field("source", &self.source)
            .field("class", &self.class)
            .field("role", &self.role)
            .field("field_path", &self.field_path)
            .field("text", &"[redacted]")
            .finish_non_exhaustive()
    }
}

/// Non-empty collection of sensitive fragments from one native record.
#[derive(Clone)]
pub struct SensitiveTranscriptFragments(Vec<SensitiveTranscriptFragment>);

impl SensitiveTranscriptFragments {
    /// Borrow the projected fragments.
    pub fn iter(&self) -> impl Iterator<Item = &SensitiveTranscriptFragment> {
        self.0.iter()
    }

    /// Consume the typed collection.
    pub fn into_fragments(self) -> impl Iterator<Item = SensitiveTranscriptFragment> {
        self.0.into_iter()
    }

    /// Typed number of projected fragments.
    #[must_use]
    pub fn count(&self) -> RecordCount {
        RecordCount::new(u64::try_from(self.0.len()).unwrap_or(u64::MAX))
    }
}

impl std::fmt::Debug for SensitiveTranscriptFragments {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SensitiveTranscriptFragments")
            .field("count", &self.0.len())
            .finish()
    }
}

/// Strict transcript projection result for one canonical record.
#[derive(Debug, Clone)]
pub enum TranscriptRecordProjection {
    /// One or more allowlisted sensitive fragments require local scanning.
    Eligible(SensitiveTranscriptFragments),
    /// No content may cross the disclosure boundary.
    Excluded(TranscriptExclusionReason),
}

/// Project allowlisted text from one verified Codex JSONL record.
///
/// # Errors
///
/// Returns a source-safe protocol error for malformed JSON, timestamps, typed
/// anchors, or canonicalization failures.
pub fn project_codex_transcript_line(
    line: &str,
    source: SourceRecordRef,
) -> Result<TranscriptRecordProjection, ProtocolError> {
    let sequence = source.sequence();
    let envelope: RawTranscriptEnvelope = serde_json::from_str(line)
        .map_err(|source| ProtocolError::InvalidJson { sequence, source })?;
    let occurred_at = OccurredAt::parse(&envelope.timestamp)
        .map_err(|source| ProtocolError::InvalidDomainValue { sequence, source })?;
    let payload_type = string_field(&envelope.payload, "type");
    let turn = turn_association(&envelope.payload, sequence)?;
    let call = call_association(&envelope.payload, sequence)?;
    let tool = tool_association(&envelope.payload, sequence)?;

    let context = ProjectionContext {
        source,
        occurred_at,
        turn,
        call,
        tool,
    };
    project_envelope(
        &envelope.record_type,
        payload_type,
        &envelope.payload,
        &context,
    )
}

struct ProjectionContext {
    source: SourceRecordRef,
    occurred_at: OccurredAt,
    turn: TurnAssociation,
    call: CallAssociation,
    tool: ToolAssociation,
}

fn project_envelope(
    record_type: &str,
    payload_type: Option<&str>,
    payload: &Value,
    context: &ProjectionContext,
) -> Result<TranscriptRecordProjection, ProtocolError> {
    match record_type {
        "session_meta" | "turn_context" | "compacted" | "world_state" => Ok(
            TranscriptRecordProjection::Excluded(TranscriptExclusionReason::InstructionBearing),
        ),
        "event_msg" => project_event(payload_type, payload, context),
        "response_item" => project_response_item(payload_type, payload, context),
        _ => Ok(TranscriptRecordProjection::Excluded(
            TranscriptExclusionReason::UnsupportedRecord,
        )),
    }
}

fn project_event(
    payload_type: Option<&str>,
    payload: &Value,
    context: &ProjectionContext,
) -> Result<TranscriptRecordProjection, ProtocolError> {
    match payload_type {
        Some("agent_reasoning") => Ok(TranscriptRecordProjection::Excluded(
            TranscriptExclusionReason::HiddenReasoning,
        )),
        Some("user_message") => project_fields(
            payload,
            context,
            TranscriptRecordClass::UserMessage,
            TranscriptRole::User,
            &[(TranscriptField::Message, "message")],
        ),
        Some("agent_message") => project_fields(
            payload,
            context,
            TranscriptRecordClass::AgentMessage,
            TranscriptRole::Agent,
            &[(TranscriptField::Message, "message")],
        ),
        Some("task_complete") => project_fields(
            payload,
            context,
            TranscriptRecordClass::CompletionSummary,
            TranscriptRole::Agent,
            &[(TranscriptField::LastAgentMessage, "last_agent_message")],
        ),
        Some("error") => project_fields(
            payload,
            context,
            TranscriptRecordClass::Error,
            TranscriptRole::Tool,
            &[(TranscriptField::Message, "message")],
        ),
        Some("exec_command_end") => project_fields(
            payload,
            context,
            TranscriptRecordClass::CommandResult,
            TranscriptRole::Tool,
            &[
                (TranscriptField::AggregatedOutput, "aggregated_output"),
                (TranscriptField::Output, "output"),
                (TranscriptField::Stdout, "stdout"),
                (TranscriptField::Stderr, "stderr"),
            ],
        ),
        Some("patch_apply_end") => project_fields(
            payload,
            context,
            TranscriptRecordClass::PatchResult,
            TranscriptRole::Tool,
            &[
                (TranscriptField::Changes, "changes"),
                (TranscriptField::Stdout, "stdout"),
                (TranscriptField::Stderr, "stderr"),
            ],
        ),
        Some("web_search_end") => project_fields(
            payload,
            context,
            TranscriptRecordClass::Verification,
            TranscriptRole::Tool,
            &[
                (TranscriptField::Query, "query"),
                (TranscriptField::Action, "action"),
            ],
        ),
        Some("mcp_tool_call_end") => project_fields(
            payload,
            context,
            TranscriptRecordClass::ToolResult,
            TranscriptRole::Tool,
            &[
                (TranscriptField::Invocation, "invocation"),
                (TranscriptField::Result, "result"),
            ],
        ),
        _ => Ok(TranscriptRecordProjection::Excluded(
            TranscriptExclusionReason::UnsupportedRecord,
        )),
    }
}

fn project_response_item(
    payload_type: Option<&str>,
    payload: &Value,
    context: &ProjectionContext,
) -> Result<TranscriptRecordProjection, ProtocolError> {
    match payload_type {
        Some("reasoning") => Ok(TranscriptRecordProjection::Excluded(
            TranscriptExclusionReason::HiddenReasoning,
        )),
        Some("message") => Ok(project_response_message(payload, context)),
        Some("agent_message") => project_fields(
            payload,
            context,
            TranscriptRecordClass::InterAgentMessage,
            TranscriptRole::Collaborator,
            &[(TranscriptField::Message, "message")],
        ),
        Some(kind) if is_tool_request(kind) => project_fields(
            payload,
            context,
            TranscriptRecordClass::ToolRequest,
            TranscriptRole::Tool,
            &[
                (TranscriptField::Arguments, "arguments"),
                (TranscriptField::Input, "input"),
                (TranscriptField::Action, "action"),
                (TranscriptField::Query, "query"),
            ],
        ),
        Some(kind) if is_tool_result(kind) => project_fields(
            payload,
            context,
            TranscriptRecordClass::ToolResult,
            TranscriptRole::Tool,
            &[
                (TranscriptField::Output, "output"),
                (TranscriptField::Execution, "execution"),
                (TranscriptField::Tools, "tools"),
            ],
        ),
        Some("image_generation_call") => Ok(TranscriptRecordProjection::Excluded(
            TranscriptExclusionReason::AssetOrBinary,
        )),
        _ => Ok(TranscriptRecordProjection::Excluded(
            TranscriptExclusionReason::UnsupportedRecord,
        )),
    }
}

fn project_response_message(
    payload: &Value,
    context: &ProjectionContext,
) -> TranscriptRecordProjection {
    let Some(role) = string_field(payload, "role") else {
        return TranscriptRecordProjection::Excluded(TranscriptExclusionReason::NoAllowedText);
    };
    let (class, role) = match role {
        "user" => (TranscriptRecordClass::UserMessage, TranscriptRole::User),
        "assistant" | "agent" => (TranscriptRecordClass::AgentMessage, TranscriptRole::Agent),
        "system" | "developer" => {
            return TranscriptRecordProjection::Excluded(
                TranscriptExclusionReason::InstructionBearing,
            );
        }
        _ => {
            return TranscriptRecordProjection::Excluded(
                TranscriptExclusionReason::UnsupportedRecord,
            );
        }
    };
    let Some(content_parts) = payload.get("content").and_then(Value::as_array) else {
        return TranscriptRecordProjection::Excluded(TranscriptExclusionReason::NoAllowedText);
    };
    let mut fragments = Vec::new();
    let mut saw_asset = false;
    for (offset, part) in content_parts.iter().enumerate() {
        match string_field(part, "type") {
            Some("input_text" | "output_text") => {
                if let Some(text) =
                    string_field(part, "text").and_then(SensitiveTranscriptText::new)
                {
                    fragments.push(fragment(
                        context,
                        class,
                        role,
                        TranscriptFieldPath::from_offset(TranscriptField::ContentText, offset),
                        text,
                    ));
                }
            }
            Some("input_image" | "output_image" | "image") => saw_asset = true,
            _ => {}
        }
    }
    eligible_or_excluded(
        fragments,
        if saw_asset {
            TranscriptExclusionReason::AssetOrBinary
        } else {
            TranscriptExclusionReason::NoAllowedText
        },
    )
}

fn project_fields(
    payload: &Value,
    context: &ProjectionContext,
    class: TranscriptRecordClass,
    role: TranscriptRole,
    fields: &[(TranscriptField, &'static str)],
) -> Result<TranscriptRecordProjection, ProtocolError> {
    let mut fragments = Vec::new();
    for (offset, (field, key)) in fields.iter().enumerate() {
        let Some(value) = payload.get(*key) else {
            continue;
        };
        if let Some(text) = sensitive_text_from_value(value, context.source.sequence())? {
            fragments.push(fragment(
                context,
                class,
                role,
                TranscriptFieldPath::from_offset(*field, offset),
                text,
            ));
        }
    }
    Ok(eligible_or_excluded(
        fragments,
        TranscriptExclusionReason::NoAllowedText,
    ))
}

fn eligible_or_excluded(
    fragments: Vec<SensitiveTranscriptFragment>,
    empty_reason: TranscriptExclusionReason,
) -> TranscriptRecordProjection {
    if fragments.is_empty() {
        TranscriptRecordProjection::Excluded(empty_reason)
    } else {
        TranscriptRecordProjection::Eligible(SensitiveTranscriptFragments(fragments))
    }
}

fn fragment(
    context: &ProjectionContext,
    class: TranscriptRecordClass,
    role: TranscriptRole,
    field_path: TranscriptFieldPath,
    text: SensitiveTranscriptText,
) -> SensitiveTranscriptFragment {
    SensitiveTranscriptFragment {
        source: context.source.clone(),
        occurred_at: context.occurred_at,
        class,
        role,
        field_path,
        turn: context.turn.clone(),
        call: context.call.clone(),
        tool: context.tool.clone(),
        text,
    }
}

fn sensitive_text_from_value(
    value: &Value,
    sequence: harness_graph_domain::RecordSequence,
) -> Result<Option<SensitiveTranscriptText>, ProtocolError> {
    if let Some(value) = value.as_str() {
        return Ok(SensitiveTranscriptText::new(value));
    }
    if value.is_null() {
        return Ok(None);
    }
    let canonical = serde_json::to_string(value)
        .map_err(|source| ProtocolError::Canonicalization { sequence, source })?;
    Ok(SensitiveTranscriptText::new(&canonical))
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
    payload: &Value,
    sequence: harness_graph_domain::RecordSequence,
) -> Result<ToolAssociation, ProtocolError> {
    string_field(payload, "name").map_or(Ok(ToolAssociation::NotApplicable), |value| {
        ToolName::new(value)
            .map(ToolAssociation::Tool)
            .map_err(|source| ProtocolError::InvalidDomainValue { sequence, source })
    })
}

fn is_tool_request(kind: &str) -> bool {
    matches!(
        kind,
        "function_call"
            | "custom_tool_call"
            | "dynamic_tool_call_request"
            | "web_search_call"
            | "tool_search_call"
    )
}

fn is_tool_result(kind: &str) -> bool {
    matches!(
        kind,
        "function_call_output"
            | "custom_tool_call_output"
            | "dynamic_tool_call_response"
            | "tool_search_output"
    )
}

fn string_field<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(Value::as_str)
}

#[cfg(test)]
mod tests {
    use harness_graph_domain::{RecordSequence, SessionId, SourceDigest, SourceRecordRef};

    use super::{
        TranscriptExclusionReason, TranscriptRecordClass, TranscriptRecordProjection,
        project_codex_transcript_line,
    };

    fn source() -> Result<SourceRecordRef, Box<dyn std::error::Error>> {
        Ok(SourceRecordRef::new(
            SessionId::parse("019c63db-2995-74c3-b898-c1b92a8e1317")?,
            SourceDigest::hash(b"fixture"),
            RecordSequence::from_zero_based(0),
        ))
    }

    #[test]
    fn response_text_is_projected_without_exposing_debug_content()
    -> Result<(), Box<dyn std::error::Error>> {
        let line = r#"{"timestamp":"2026-02-16T10:00:00Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"private canary"},{"type":"input_image","image_url":"data:image/png;base64,AA=="}]}}"#;
        let TranscriptRecordProjection::Eligible(fragments) =
            project_codex_transcript_line(line, source()?)?
        else {
            return Err("eligible text was excluded".into());
        };
        assert_eq!(fragments.count().value(), 1);
        let fragment = fragments.iter().next().ok_or("missing fragment")?;
        assert_eq!(fragment.class(), TranscriptRecordClass::AgentMessage);
        assert_eq!(fragment.expose_for_local_scanner(), "private canary");
        assert!(!format!("{fragment:?}").contains("private canary"));
        Ok(())
    }

    #[test]
    fn reasoning_and_instructions_are_hard_exclusions() -> Result<(), Box<dyn std::error::Error>> {
        let reasoning = r#"{"timestamp":"2026-02-16T10:00:00Z","type":"response_item","payload":{"type":"reasoning","summary":[{"text":"hidden"}]}}"#;
        assert!(matches!(
            project_codex_transcript_line(reasoning, source()?)?,
            TranscriptRecordProjection::Excluded(TranscriptExclusionReason::HiddenReasoning)
        ));
        let context = r#"{"timestamp":"2026-02-16T10:00:00Z","type":"turn_context","payload":{"developer_instructions":"hidden"}}"#;
        assert!(matches!(
            project_codex_transcript_line(context, source()?)?,
            TranscriptRecordProjection::Excluded(TranscriptExclusionReason::InstructionBearing)
        ));
        Ok(())
    }

    #[test]
    fn object_arguments_are_canonicalized_inside_sensitive_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let line = r#"{"timestamp":"2026-02-16T10:00:00Z","type":"response_item","payload":{"type":"tool_search_call","call_id":"call-1","arguments":{"query":"typed graph"}}}"#;
        let TranscriptRecordProjection::Eligible(fragments) =
            project_codex_transcript_line(line, source()?)?
        else {
            return Err("object arguments were excluded".into());
        };
        let fragment = fragments.iter().next().ok_or("missing fragment")?;
        assert_eq!(
            fragment.expose_for_local_scanner(),
            r#"{"query":"typed graph"}"#
        );
        Ok(())
    }
}
