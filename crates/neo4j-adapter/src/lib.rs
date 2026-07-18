//! Neo4j implementation of the typed graph projection port.

mod application;
mod enrichment;
mod experience;

use std::sync::Arc;

use async_trait::async_trait;
use harness_graph_domain::{
    ActivityInvocation, CallAssociation, ContextAssociation, CorrelatedInvocation,
    CorrelatedOutcome, CorrelatedPurpose, DecodedNativeRecord, GraphNamespace, ObservationId,
    ObservationKind, RecordCount, SessionId, SourceDigest, ToolAssociation, TurnAssociation,
};
use harness_graph_graph_port::{
    AnalysisProjectionCommand, FinalizeIngestionCommand, GraphBatch, GraphCommand, GraphProjector,
    ProjectionReceipt, SourceSnapshotCommand,
};
use harness_graph_planning::{
    PlanningError, PrecedentLimit, PrecedentPath, PrecedentPaths, PrecedentReader, PrecedentStep,
    PrecedentSteps,
};
use neo4rs::{Graph, Query, query};
use secrecy::{ExposeSecret, SecretString};
use tokio::sync::Mutex;

/// Neo4j adapter failure with secret-safe operation context.
#[derive(Debug, thiserror::Error)]
pub enum Neo4jAdapterError {
    /// Driver connection failed.
    #[error("Neo4j connection failed: {source}")]
    Connection {
        /// Driver error.
        #[source]
        source: neo4rs::Error,
    },

    /// A schema or projection query failed.
    #[error("Neo4j operation {operation} failed: {source}")]
    Operation {
        /// Static operation name.
        operation: &'static str,
        /// Driver error.
        #[source]
        source: neo4rs::Error,
    },

    /// A typed unsigned value could not fit Neo4j's signed integer model.
    #[error("Neo4j integer conversion failed for {field}")]
    IntegerRange {
        /// Static field name.
        field: &'static str,
    },

    /// A parsed occurrence timestamp could not be formatted.
    #[error("occurrence timestamp formatting failed: {source}")]
    TimestampFormat {
        /// Time formatter error.
        #[source]
        source: time::error::Format,
    },

    /// A read query returned no row or an incompatible property type.
    #[error("Neo4j read result was missing expected field {field}")]
    InvalidReadResult {
        /// Static property name.
        field: &'static str,
    },

    /// A graph property did not map back into the typed semantic vocabulary.
    #[error("Neo4j semantic property {field} contained an unsupported value")]
    InvalidSemanticProperty {
        /// Property name only; the untrusted value is not echoed.
        field: &'static str,
    },

    /// Retrieved precedent data violated a planning invariant.
    #[error(transparent)]
    Planning(#[from] PlanningError),

    /// Retrieved graph identity failed domain validation.
    #[error(transparent)]
    Domain(#[from] harness_graph_domain::DomainError),

    /// Enrichment projection or persisted overlay data violated its typed contract.
    #[error(transparent)]
    Enrichment(#[from] harness_graph_graph_port::EnrichmentGraphError),

    /// Source-safe experience projection violated its public response contract.
    #[error(transparent)]
    Experience(#[from] harness_graph_graph_port::ExperienceGraphError),

    /// A persisted enrichment identity disagreed with an idempotent command.
    #[error("existing enrichment {object} conflicts with the requested immutable identity")]
    ConflictingEnrichment {
        /// Closed source-safe overlay object name.
        object: &'static str,
    },

    /// An enrichment lifecycle transition failed its graph preconditions.
    #[error("enrichment transition {transition} rejected by graph invariants")]
    EnrichmentTransition {
        /// Closed source-safe transition name.
        transition: &'static str,
    },
}

/// Exact completion state for one content-addressed source snapshot.
///
/// `Pending` intentionally covers both a missing receipt and any receipt whose
/// namespace, digest, status, relationship, or record-count invariants do not
/// match the requested verified snapshot. Callers may therefore skip work only
/// for `Complete`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceIngestionStatus {
    /// The completed receipt exists and all source and count invariants agree.
    Complete,
    /// No trustworthy completed receipt exists for the exact source snapshot.
    Pending,
}

/// Concrete idempotent Neo4j graph adapter.
#[derive(Clone)]
pub struct Neo4jAdapter {
    graph: Graph,
    projection_gate: Arc<Mutex<()>>,
}

#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
struct AnalysisEntityCounts {
    activities: RecordCount,
    outcomes: RecordCount,
    risks: RecordCount,
    paths: RecordCount,
}

impl Neo4jAdapter {
    /// Connect using a Bolt host/port, username, and secret password.
    ///
    /// # Errors
    ///
    /// Returns an error when the Neo4j driver cannot authenticate or connect.
    pub async fn connect(
        bolt_address: &str,
        username: &str,
        password: SecretString,
    ) -> Result<Self, Neo4jAdapterError> {
        let graph = Graph::new(bolt_address, username, password.expose_secret())
            .await
            .map_err(|source| Neo4jAdapterError::Connection { source })?;
        Ok(Self {
            graph,
            projection_gate: Arc::new(Mutex::new(())),
        })
    }

    /// Count projected observations in one graph namespace.
    ///
    /// # Errors
    ///
    /// Returns an error when Neo4j cannot execute or decode the count query.
    pub async fn observation_count(
        &self,
        namespace: &GraphNamespace,
    ) -> Result<RecordCount, Neo4jAdapterError> {
        let mut rows = self
            .graph
            .execute(
                query(
                    "MATCH (o:HGObservation {hg_namespace: $namespace}) RETURN count(o) AS count",
                )
                .param("namespace", namespace.as_str()),
            )
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "count observations",
                source,
            })?;
        let row = rows
            .next()
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "read observation count",
                source,
            })?
            .ok_or(Neo4jAdapterError::InvalidReadResult { field: "count" })?;
        let count: i64 = row
            .get("count")
            .map_err(|_| Neo4jAdapterError::InvalidReadResult { field: "count" })?;
        let count =
            u64::try_from(count).map_err(|_| Neo4jAdapterError::IntegerRange { field: "count" })?;
        Ok(RecordCount::new(count))
    }

    /// Read the trustworthy completion state for one exact source snapshot.
    ///
    /// A source is complete only when its snapshot metadata agrees with
    /// `expected_records` and a linked completed receipt reports the same total
    /// with known and quarantined counts that add up to that total. Missing or
    /// inconsistent graph state is reported as [`SourceIngestionStatus::Pending`].
    ///
    /// # Errors
    ///
    /// Returns an error when the expected count cannot fit Neo4j's integer
    /// model or Neo4j cannot execute or decode the parameterized query.
    pub async fn source_ingestion_status(
        &self,
        namespace: &GraphNamespace,
        source_digest: SourceDigest,
        expected_records: RecordCount,
    ) -> Result<SourceIngestionStatus, Neo4jAdapterError> {
        let source_digest = source_digest.to_hex();
        let expected_records = to_i64(expected_records.value(), "source expected records")?;
        let mut rows = self
            .graph
            .execute(
                query(
                    "OPTIONAL MATCH (src:HGSourceSnapshot {key: $source_key}) \
                     OPTIONAL MATCH (r:HGIngestionReceipt {key: $receipt_key})-[:VERIFIED]->(src) \
                     RETURN coalesce( \
                         src.hg_namespace = $namespace AND \
                         src.source_digest = $source_digest AND \
                         src.expected_records = $expected_records AND \
                         r.hg_namespace = $namespace AND \
                         r.source_digest = $source_digest AND \
                         r.status = 'completed' AND \
                         r.completed_at IS NOT NULL AND \
                         r.total_records = $expected_records AND \
                         r.known_records + r.quarantined_records = r.total_records, \
                         false \
                     ) AS completed",
                )
                .param("source_key", source_key(namespace.as_str(), &source_digest))
                .param(
                    "receipt_key",
                    receipt_key(namespace.as_str(), &source_digest),
                )
                .param("namespace", namespace.as_str())
                .param("source_digest", source_digest)
                .param("expected_records", expected_records),
            )
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "read source ingestion status",
                source,
            })?;
        let row = rows
            .next()
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "decode source ingestion status",
                source,
            })?
            .ok_or(Neo4jAdapterError::InvalidReadResult { field: "completed" })?;
        let completed: bool = row
            .get("completed")
            .map_err(|_| Neo4jAdapterError::InvalidReadResult { field: "completed" })?;
        Ok(if completed {
            SourceIngestionStatus::Complete
        } else {
            SourceIngestionStatus::Pending
        })
    }

    #[cfg(test)]
    async fn purge_namespace(&self, namespace: &GraphNamespace) -> Result<(), Neo4jAdapterError> {
        self.graph
            .run(
                query("MATCH (n {hg_namespace: $namespace}) DETACH DELETE n")
                    .param("namespace", namespace.as_str()),
            )
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "purge isolated test namespace",
                source,
            })
    }

    #[cfg(test)]
    async fn receipt_completed_at(
        &self,
        namespace: &GraphNamespace,
        source_digest: SourceDigest,
    ) -> Result<String, Neo4jAdapterError> {
        let mut rows = self
            .graph
            .execute(
                query(
                    "MATCH (r:HGIngestionReceipt {key: $receipt_key}) \
                     RETURN toString(r.completed_at) AS completed_at",
                )
                .param(
                    "receipt_key",
                    receipt_key(namespace.as_str(), &source_digest.to_hex()),
                ),
            )
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "read ingestion completion timestamp",
                source,
            })?;
        let row = rows
            .next()
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "decode ingestion completion timestamp",
                source,
            })?
            .ok_or(Neo4jAdapterError::InvalidReadResult {
                field: "completed_at",
            })?;
        row.get("completed_at")
            .map_err(|_| Neo4jAdapterError::InvalidReadResult {
                field: "completed_at",
            })
    }

    #[cfg(test)]
    async fn corrupt_receipt_counts(
        &self,
        namespace: &GraphNamespace,
        source_digest: SourceDigest,
    ) -> Result<(), Neo4jAdapterError> {
        self.graph
            .run(
                query(
                    "MATCH (r:HGIngestionReceipt {key: $receipt_key}) \
                     SET r.known_records = 0, r.quarantined_records = 0",
                )
                .param(
                    "receipt_key",
                    receipt_key(namespace.as_str(), &source_digest.to_hex()),
                ),
            )
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "corrupt isolated test receipt",
                source,
            })
    }

    #[cfg(test)]
    async fn analysis_entity_counts(
        &self,
        namespace: &GraphNamespace,
    ) -> Result<AnalysisEntityCounts, Neo4jAdapterError> {
        let mut rows = self
            .graph
            .execute(
                query(
                    "OPTIONAL MATCH (a:HGActivity {hg_namespace: $namespace}) \
                     WITH collect(DISTINCT a) AS activities \
                     OPTIONAL MATCH (o:HGOutcome {hg_namespace: $namespace}) \
                     WITH activities, collect(DISTINCT o) AS outcomes \
                     OPTIONAL MATCH (r:HGRiskExposure {hg_namespace: $namespace}) \
                     WITH activities, outcomes, collect(DISTINCT r) AS risks \
                     OPTIONAL MATCH (p:HGPathPattern {hg_namespace: $namespace}) \
                     RETURN size(activities) AS activities, size(outcomes) AS outcomes, \
                            size(risks) AS risks, count(DISTINCT p) AS paths",
                )
                .param("namespace", namespace.as_str()),
            )
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "count analysis entities",
                source,
            })?;
        let row = rows
            .next()
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "read analysis entity counts",
                source,
            })?
            .ok_or(Neo4jAdapterError::InvalidReadResult {
                field: "analysis entity counts",
            })?;
        Ok(AnalysisEntityCounts {
            activities: read_count(&row, "activities")?,
            outcomes: read_count(&row, "outcomes")?,
            risks: read_count(&row, "risks")?,
            paths: read_count(&row, "paths")?,
        })
    }
}

#[cfg(test)]
fn read_count(row: &neo4rs::Row, field: &'static str) -> Result<RecordCount, Neo4jAdapterError> {
    let count: i64 = row
        .get(field)
        .map_err(|_| Neo4jAdapterError::InvalidReadResult { field })?;
    let count = u64::try_from(count).map_err(|_| Neo4jAdapterError::IntegerRange { field })?;
    Ok(RecordCount::new(count))
}

#[async_trait]
impl GraphProjector for Neo4jAdapter {
    type Error = Neo4jAdapterError;

    async fn health(&self) -> Result<(), Self::Error> {
        let mut rows = self
            .graph
            .execute(query("RETURN 1 AS ok"))
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "health check",
                source,
            })?;
        let row = rows
            .next()
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "read health check",
                source,
            })?
            .ok_or(Neo4jAdapterError::InvalidReadResult { field: "ok" })?;
        let ok: i64 = row
            .get("ok")
            .map_err(|_| Neo4jAdapterError::InvalidReadResult { field: "ok" })?;
        if ok != 1 {
            return Err(Neo4jAdapterError::InvalidReadResult { field: "ok" });
        }
        Ok(())
    }

    async fn ensure_schema(&self) -> Result<(), Self::Error> {
        for constraint in constraints() {
            self.graph.run(query(constraint)).await.map_err(|source| {
                Neo4jAdapterError::Operation {
                    operation: "ensure schema constraint",
                    source,
                }
            })?;
        }
        Ok(())
    }

    async fn project(&self, batch: GraphBatch) -> Result<ProjectionReceipt, Self::Error> {
        // Independent archive verification and analysis remain concurrent, but
        // projections touch shared namespace-scoped nodes such as HGTool.
        // Serializing only mutation transactions prevents Neo4j uniqueness-lock
        // races while retaining bounded concurrency outside this critical section.
        let _projection_guard = self.projection_gate.lock().await;
        let logical_count = to_i64(batch.command_count().value(), "batch command count")?;
        let mut queries = Vec::new();
        for command in batch.into_commands() {
            queries.extend(command_queries(command)?);
        }
        let mut transaction =
            self.graph
                .start_txn()
                .await
                .map_err(|source| Neo4jAdapterError::Operation {
                    operation: "start projection transaction",
                    source,
                })?;
        transaction
            .run_queries(queries)
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "run projection transaction",
                source,
            })?;
        transaction
            .commit()
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "commit projection transaction",
                source,
            })?;
        let committed =
            u64::try_from(logical_count).map_err(|_| Neo4jAdapterError::IntegerRange {
                field: "batch command count",
            })?;
        Ok(ProjectionReceipt::new(RecordCount::new(committed)))
    }
}

#[async_trait]
impl PrecedentReader for Neo4jAdapter {
    type Error = Neo4jAdapterError;

    async fn verified_precedents(
        &self,
        namespace: &GraphNamespace,
        limit: PrecedentLimit,
    ) -> Result<PrecedentPaths, Self::Error> {
        let mut rows = self
            .graph
            .execute(
                query(
                    "MATCH (session:HGSession {hg_namespace: $namespace})-[:IMPORTED_FROM]->(src) \
                     MATCH (src)-[:HAS_OUTCOME]->(outcome:HGOutcome) \
                     WHERE outcome.class = 'verified_success' AND outcome.verification = 'fresh' \
                     MATCH (src)-[:FOLLOWED_PATH]->(path:HGPathPattern) \
                     MATCH (src)-[:HAS_ACTIVITY]->(activity:HGActivity) \
                     WITH session, src, path, activity ORDER BY activity.first_sequence \
                     WITH session, src, path, \
                          collect(activity.activity_id) AS activity_ids, \
                          collect(activity.kind) AS activity_kinds, \
                          collect(activity.status) AS activity_statuses \
                     RETURN session.session_id AS session_id, \
                            src.source_digest AS source_digest, path.signature AS path_signature, \
                            activity_ids, activity_kinds, activity_statuses \
                     ORDER BY size(activity_ids) ASC LIMIT $limit",
                )
                .param("namespace", namespace.as_str())
                .param("limit", to_i64(limit.value(), "verified precedent limit")?),
            )
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "query verified precedents",
                source,
            })?;
        let mut precedents = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "read verified precedents",
                source,
            })?
        {
            precedents.push(precedent_from_row(&row)?);
        }
        Ok(PrecedentPaths::new(precedents)?)
    }
}

fn precedent_from_row(row: &neo4rs::Row) -> Result<PrecedentPath, Neo4jAdapterError> {
    let session_id: String = read_property(row, "session_id")?;
    let source_digest: String = read_property(row, "source_digest")?;
    let path_signature: String = read_property(row, "path_signature")?;
    let activity_ids: Vec<String> = read_property(row, "activity_ids")?;
    let activity_kinds: Vec<String> = read_property(row, "activity_kinds")?;
    let activity_statuses: Vec<String> = read_property(row, "activity_statuses")?;
    if activity_ids.len() != activity_kinds.len() || activity_ids.len() != activity_statuses.len() {
        return Err(Neo4jAdapterError::InvalidReadResult {
            field: "precedent activity arrays",
        });
    }
    let steps = activity_ids
        .into_iter()
        .zip(activity_kinds)
        .zip(activity_statuses)
        .map(|((id, kind), status)| {
            Ok(PrecedentStep::new(
                harness_graph_domain::ActivityId::parse_hex(&id)?,
                parse_activity_kind(&kind)?,
                parse_activity_status(&status)?,
            ))
        })
        .collect::<Result<Vec<_>, Neo4jAdapterError>>()?;
    Ok(PrecedentPath::new(
        SessionId::parse(&session_id)?,
        SourceDigest::parse_hex(&source_digest)?,
        harness_graph_domain::PathSignature::parse_hex(&path_signature)?,
        PrecedentSteps::new(steps)?,
    ))
}

fn read_property<T>(row: &neo4rs::Row, field: &'static str) -> Result<T, Neo4jAdapterError>
where
    T: for<'de> serde::Deserialize<'de>,
{
    row.get(field)
        .map_err(|_| Neo4jAdapterError::InvalidReadResult { field })
}

fn parse_activity_kind(
    value: &str,
) -> Result<harness_graph_domain::ActivityKind, Neo4jAdapterError> {
    use harness_graph_domain::ActivityKind;
    match value {
        "start" => Ok(ActivityKind::Start),
        "request" => Ok(ActivityKind::Request),
        "inspect" => Ok(ActivityKind::Inspect),
        "search" => Ok(ActivityKind::Search),
        "modify" => Ok(ActivityKind::Modify),
        "repair" => Ok(ActivityKind::Repair),
        "verify" => Ok(ActivityKind::Verify),
        "install" => Ok(ActivityKind::Install),
        "execute" => Ok(ActivityKind::Execute),
        "diagnose" => Ok(ActivityKind::Diagnose),
        "request_permission" => Ok(ActivityKind::RequestPermission),
        "network_access" => Ok(ActivityKind::NetworkAccess),
        "destructive" => Ok(ActivityKind::Destructive),
        "manage_context" => Ok(ActivityKind::ManageContext),
        "rollback" => Ok(ActivityKind::Rollback),
        "complete" => Ok(ActivityKind::Complete),
        _ => Err(Neo4jAdapterError::InvalidSemanticProperty { field: "kind" }),
    }
}

fn parse_activity_status(
    value: &str,
) -> Result<harness_graph_domain::ActivityStatus, Neo4jAdapterError> {
    use harness_graph_domain::ActivityStatus;
    match value {
        "pending" => Ok(ActivityStatus::Pending),
        "succeeded" => Ok(ActivityStatus::Succeeded),
        "failed" => Ok(ActivityStatus::Failed),
        "interrupted" => Ok(ActivityStatus::Interrupted),
        "indeterminate" => Ok(ActivityStatus::Indeterminate),
        _ => Err(Neo4jAdapterError::InvalidSemanticProperty { field: "status" }),
    }
}

fn constraints() -> &'static [&'static str] {
    &[
        "CREATE CONSTRAINT hg_session_key IF NOT EXISTS FOR (n:HGSession) REQUIRE n.key IS UNIQUE",
        "CREATE CONSTRAINT hg_source_key IF NOT EXISTS FOR (n:HGSourceSnapshot) REQUIRE n.key IS UNIQUE",
        "CREATE CONSTRAINT hg_observation_key IF NOT EXISTS FOR (n:HGObservation) REQUIRE n.key IS UNIQUE",
        "CREATE CONSTRAINT hg_context_key IF NOT EXISTS FOR (n:HGContextSnapshot) REQUIRE n.key IS UNIQUE",
        "CREATE CONSTRAINT hg_turn_key IF NOT EXISTS FOR (n:HGTurn) REQUIRE n.key IS UNIQUE",
        "CREATE CONSTRAINT hg_call_key IF NOT EXISTS FOR (n:HGToolCall) REQUIRE n.key IS UNIQUE",
        "CREATE CONSTRAINT hg_tool_key IF NOT EXISTS FOR (n:HGTool) REQUIRE n.key IS UNIQUE",
        "CREATE CONSTRAINT hg_receipt_key IF NOT EXISTS FOR (n:HGIngestionReceipt) REQUIRE n.key IS UNIQUE",
        "CREATE CONSTRAINT hg_activity_key IF NOT EXISTS FOR (n:HGActivity) REQUIRE n.key IS UNIQUE",
        "CREATE CONSTRAINT hg_outcome_key IF NOT EXISTS FOR (n:HGOutcome) REQUIRE n.key IS UNIQUE",
        "CREATE CONSTRAINT hg_risk_key IF NOT EXISTS FOR (n:HGRiskExposure) REQUIRE n.key IS UNIQUE",
        "CREATE CONSTRAINT hg_path_key IF NOT EXISTS FOR (n:HGPathPattern) REQUIRE n.key IS UNIQUE",
    ]
}

fn command_queries(command: GraphCommand) -> Result<Vec<Query>, Neo4jAdapterError> {
    match command {
        GraphCommand::UpsertSourceSnapshot(command) => source_queries(&command),
        GraphCommand::UpsertObservation { namespace, record } => {
            observation_queries(&namespace, record)
        }
        GraphCommand::FinalizeIngestion(command) => finalize_queries(&command),
        GraphCommand::UpsertAnalysis(command) => analysis_queries(&command),
    }
}

fn source_queries(command: &SourceSnapshotCommand) -> Result<Vec<Query>, Neo4jAdapterError> {
    let namespace = command.namespace().as_str();
    let session_id = command.session_id().to_string();
    let source_digest = command.source_digest().to_hex();
    let expected_records = to_i64(
        command.expected_records().value(),
        "source expected records",
    )?;
    Ok(vec![
        query(
            "MERGE (s:HGSession {key: $session_key}) \
         ON CREATE SET s.hg_namespace = $namespace, s.session_id = $session_id \
         MERGE (src:HGSourceSnapshot {key: $source_key}) \
         ON CREATE SET src.hg_namespace = $namespace, src.source_digest = $source_digest \
         SET src.expected_records = $expected_records \
         MERGE (s)-[:IMPORTED_FROM]->(src)",
        )
        .param("session_key", session_key(namespace, &session_id))
        .param("source_key", source_key(namespace, &source_digest))
        .param("namespace", namespace)
        .param("session_id", session_id)
        .param("source_digest", source_digest)
        .param("expected_records", expected_records),
    ])
}

fn observation_queries(
    namespace: &GraphNamespace,
    record: DecodedNativeRecord,
) -> Result<Vec<Query>, Neo4jAdapterError> {
    match record {
        DecodedNativeRecord::Known(record) => {
            let observation = record.observation();
            let source = observation.source();
            let mut queries = vec![base_observation_query(
                namespace,
                source.session_id(),
                source.source_digest(),
                source.sequence().value(),
                observation
                    .occurred_at()
                    .to_rfc3339()
                    .map_err(|source| Neo4jAdapterError::TimestampFormat { source })?,
                observation.kind().as_str(),
                observation.payload_digest().to_hex(),
                false,
                "",
            )?];
            append_context_query(&mut queries, namespace, observation);
            append_turn_query(&mut queries, namespace, observation);
            append_call_query(&mut queries, namespace, observation);
            append_tool_query(&mut queries, namespace, observation);
            Ok(queries)
        }
        DecodedNativeRecord::Unsupported(record) => {
            let source = record.source();
            Ok(vec![base_observation_query(
                namespace,
                source.session_id(),
                source.source_digest(),
                source.sequence().value(),
                record
                    .occurred_at()
                    .to_rfc3339()
                    .map_err(|source| Neo4jAdapterError::TimestampFormat { source })?,
                "unsupported",
                record.payload_digest().to_hex(),
                true,
                record.native_kind().as_str(),
            )?])
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn base_observation_query(
    namespace: &GraphNamespace,
    session_id: SessionId,
    source_digest: SourceDigest,
    sequence: u64,
    occurred_at: String,
    kind: &str,
    payload_digest: String,
    quarantined: bool,
    native_kind: &str,
) -> Result<Query, Neo4jAdapterError> {
    let namespace = namespace.as_str();
    let session_id = session_id.to_string();
    let observation_id = ObservationId::from_source(
        source_digest,
        harness_graph_domain::RecordSequence::from_zero_based(sequence.saturating_sub(1)),
    );
    let source_digest = source_digest.to_hex();
    let sequence_i64 = to_i64(sequence, "record sequence")?;
    let query_text = if quarantined {
        "MERGE (src:HGSourceSnapshot {key: $source_key}) \
         MERGE (o:HGObservation:HGQuarantinedObservation {key: $observation_key}) \
         ON CREATE SET o.hg_namespace = $namespace, o.observation_id = $observation_id, \
                       o.sequence = $sequence, o.occurred_at = $occurred_at \
         SET o.kind = $kind, o.payload_digest = $payload_digest, o.native_kind = $native_kind \
         MERGE (src)-[:CONTAINS]->(o)"
    } else {
        "MERGE (src:HGSourceSnapshot {key: $source_key}) \
         MERGE (o:HGObservation {key: $observation_key}) \
         ON CREATE SET o.hg_namespace = $namespace, o.observation_id = $observation_id, \
                       o.sequence = $sequence, o.occurred_at = $occurred_at \
         SET o.kind = $kind, o.payload_digest = $payload_digest \
         MERGE (src)-[:CONTAINS]->(o)"
    };
    Ok(query(query_text)
        .param("source_key", source_key(namespace, &source_digest))
        .param(
            "observation_key",
            observation_key(namespace, observation_id.as_str()),
        )
        .param("namespace", namespace)
        .param("observation_id", observation_id.as_str())
        .param("sequence", sequence_i64)
        .param("occurred_at", occurred_at)
        .param("kind", kind)
        .param("payload_digest", payload_digest)
        .param("native_kind", native_kind)
        .param("session_id", session_id))
}

fn append_context_query(
    queries: &mut Vec<Query>,
    namespace: &GraphNamespace,
    observation: &harness_graph_domain::Observation,
) {
    let ContextAssociation::Asserted(context_digest) = observation.context() else {
        return;
    };
    let source = observation.source();
    let observation_id = ObservationId::from_source(source.source_digest(), source.sequence());
    let context_digest = context_digest.to_hex();
    queries.push(
        query(
            "MATCH (o:HGObservation {key: $observation_key}) \
             MERGE (c:HGContextSnapshot {key: $context_key}) \
             ON CREATE SET c.hg_namespace = $namespace, c.context_digest = $context_digest \
             MERGE (o)-[:ASSERTS_CONTEXT]->(c)",
        )
        .param(
            "observation_key",
            observation_key(namespace.as_str(), observation_id.as_str()),
        )
        .param(
            "context_key",
            context_key(namespace.as_str(), &context_digest),
        )
        .param("namespace", namespace.as_str())
        .param("context_digest", context_digest),
    );
}

fn append_turn_query(
    queries: &mut Vec<Query>,
    namespace: &GraphNamespace,
    observation: &harness_graph_domain::Observation,
) {
    let TurnAssociation::Turn(turn_id) = observation.turn() else {
        return;
    };
    let source = observation.source();
    let session_id = source.session_id().to_string();
    let observation_id = ObservationId::from_source(source.source_digest(), source.sequence());
    queries.push(
        query(
            "MATCH (o:HGObservation {key: $observation_key}) \
             MERGE (s:HGSession {key: $session_key}) \
             MERGE (t:HGTurn {key: $turn_key}) \
             ON CREATE SET t.hg_namespace = $namespace, t.turn_id = $turn_id \
             MERGE (s)-[:HAS_TURN]->(t) \
             MERGE (o)-[:IN_TURN]->(t)",
        )
        .param(
            "observation_key",
            observation_key(namespace.as_str(), observation_id.as_str()),
        )
        .param("session_key", session_key(namespace.as_str(), &session_id))
        .param(
            "turn_key",
            turn_key(namespace.as_str(), &session_id, turn_id.as_str()),
        )
        .param("namespace", namespace.as_str())
        .param("turn_id", turn_id.as_str()),
    );
}

fn append_call_query(
    queries: &mut Vec<Query>,
    namespace: &GraphNamespace,
    observation: &harness_graph_domain::Observation,
) {
    let CallAssociation::Call(call_id) = observation.call() else {
        return;
    };
    let source = observation.source();
    let session_id = source.session_id().to_string();
    let observation_id = ObservationId::from_source(source.source_digest(), source.sequence());
    let relationship = match observation.kind() {
        ObservationKind::ToolRequested => "REQUESTED_CALL",
        ObservationKind::ToolCompleted
        | ObservationKind::CommandCompleted
        | ObservationKind::PatchApplied => "COMPLETED_CALL",
        _ => "REFERENCES_CALL",
    };
    let query_text = match relationship {
        "REQUESTED_CALL" => {
            "MATCH (o:HGObservation {key: $observation_key}) \
             MERGE (c:HGToolCall {key: $call_key}) \
             ON CREATE SET c.hg_namespace = $namespace, c.call_id = $call_id, c.state = 'pending' \
             MERGE (o)-[:REQUESTED_CALL]->(c)"
        }
        "COMPLETED_CALL" => {
            "MATCH (o:HGObservation {key: $observation_key}) \
             MERGE (c:HGToolCall {key: $call_key}) \
             ON CREATE SET c.hg_namespace = $namespace, c.call_id = $call_id \
             SET c.state = 'completed' \
             MERGE (o)-[:COMPLETED_CALL]->(c)"
        }
        _ => {
            "MATCH (o:HGObservation {key: $observation_key}) \
             MERGE (c:HGToolCall {key: $call_key}) \
             ON CREATE SET c.hg_namespace = $namespace, c.call_id = $call_id, c.state = 'referenced' \
             MERGE (o)-[:REFERENCES_CALL]->(c)"
        }
    };
    queries.push(
        query(query_text)
            .param(
                "observation_key",
                observation_key(namespace.as_str(), observation_id.as_str()),
            )
            .param(
                "call_key",
                call_key(namespace.as_str(), &session_id, call_id.as_str()),
            )
            .param("namespace", namespace.as_str())
            .param("call_id", call_id.as_str()),
    );
}

fn append_tool_query(
    queries: &mut Vec<Query>,
    namespace: &GraphNamespace,
    observation: &harness_graph_domain::Observation,
) {
    let ToolAssociation::Tool(tool_name) = observation.tool() else {
        return;
    };
    let source = observation.source();
    let observation_id = ObservationId::from_source(source.source_digest(), source.sequence());
    queries.push(
        query(
            "MATCH (o:HGObservation {key: $observation_key}) \
             MERGE (t:HGTool {key: $tool_key}) \
             ON CREATE SET t.hg_namespace = $namespace, t.name = $tool_name \
             MERGE (o)-[:USES_TOOL]->(t)",
        )
        .param(
            "observation_key",
            observation_key(namespace.as_str(), observation_id.as_str()),
        )
        .param("tool_key", tool_key(namespace.as_str(), tool_name.as_str()))
        .param("namespace", namespace.as_str())
        .param("tool_name", tool_name.as_str()),
    );
}

fn finalize_queries(command: &FinalizeIngestionCommand) -> Result<Vec<Query>, Neo4jAdapterError> {
    let namespace = command.namespace().as_str();
    let session_id = command.session_id().to_string();
    let source_digest = command.source_digest().to_hex();
    Ok(vec![
        query(
            "MATCH (src:HGSourceSnapshot {key: $source_key}) \
         MERGE (r:HGIngestionReceipt {key: $receipt_key}) \
         SET r.hg_namespace = $namespace, r.source_digest = $source_digest, \
             r.status = 'completed', r.known_records = $known_records, \
             r.quarantined_records = $quarantined_records, r.total_records = $total_records, \
             r.completed_at = coalesce(r.completed_at, datetime()) \
         MERGE (r)-[:VERIFIED]->(src)",
        )
        .param("source_key", source_key(namespace, &source_digest))
        .param("receipt_key", receipt_key(namespace, &source_digest))
        .param("namespace", namespace)
        .param("session_id", session_id)
        .param("source_digest", source_digest)
        .param(
            "known_records",
            to_i64(command.known_records().value(), "known records")?,
        )
        .param(
            "quarantined_records",
            to_i64(command.quarantined_records().value(), "quarantined records")?,
        )
        .param(
            "total_records",
            to_i64(command.total_records().value(), "total records")?,
        ),
    ])
}

fn analysis_queries(command: &AnalysisProjectionCommand) -> Result<Vec<Query>, Neo4jAdapterError> {
    let mut queries = Vec::new();
    append_correlation_queries(&mut queries, command);
    append_activity_queries(&mut queries, command)?;
    append_outcome_queries(&mut queries, command);
    append_risk_queries(&mut queries, command);
    append_path_query(&mut queries, command)?;
    Ok(queries)
}

fn append_correlation_queries(queries: &mut Vec<Query>, command: &AnalysisProjectionCommand) {
    let namespace = command.namespace().as_str();
    let session_id = command.session_id().to_string();
    for correlation in command.report().correlations().iter() {
        let purpose = match correlation.purpose() {
            CorrelatedPurpose::Unknown => "unknown",
            CorrelatedPurpose::Known(purpose) => purpose.as_str(),
        };
        let invocation_digest = match correlation.invocation() {
            CorrelatedInvocation::Unknown => String::new(),
            CorrelatedInvocation::Known(digest) => digest.to_hex(),
        };
        let outcome = match correlation.outcome() {
            CorrelatedOutcome::Missing => "missing",
            CorrelatedOutcome::Known(outcome) => outcome.as_str(),
        };
        queries.push(
            query(
                "MERGE (c:HGToolCall {key: $call_key}) \
                 ON CREATE SET c.hg_namespace = $namespace, c.call_id = $call_id \
                 SET c.state = $state, c.purpose = $purpose, \
                     c.invocation_digest = $invocation_digest, c.outcome = $outcome",
            )
            .param(
                "call_key",
                call_key(namespace, &session_id, correlation.call_id().as_str()),
            )
            .param("namespace", namespace)
            .param("call_id", correlation.call_id().as_str())
            .param("state", correlation.lifecycle().as_str())
            .param("purpose", purpose)
            .param("invocation_digest", invocation_digest)
            .param("outcome", outcome),
        );
    }
}

fn append_activity_queries(
    queries: &mut Vec<Query>,
    command: &AnalysisProjectionCommand,
) -> Result<(), Neo4jAdapterError> {
    let namespace = command.namespace().as_str();
    let source_digest = command.source_digest().to_hex();
    let mut previous_activity_key = None;
    for activity in command.report().activities().iter() {
        let activity_id = activity.id().to_hex();
        let current_activity_key = activity_key(namespace, &activity_id);
        let invocation_digest = match activity.invocation() {
            ActivityInvocation::NotApplicable | ActivityInvocation::Unknown => String::new(),
            ActivityInvocation::Known(digest) => digest.to_hex(),
        };
        queries.push(
            query(
                "MATCH (src:HGSourceSnapshot {key: $source_key}) \
                 MERGE (a:HGActivity {key: $activity_key}) \
                 ON CREATE SET a.hg_namespace = $namespace, a.activity_id = $activity_id \
                 SET a.kind = $kind, a.status = $status, \
                     a.invocation_digest = $invocation_digest, \
                     a.first_sequence = $first_sequence, a.last_sequence = $last_sequence, \
                     a.evidence_count = $evidence_count, a.analysis_version = 1 \
                 MERGE (src)-[:HAS_ACTIVITY]->(a)",
            )
            .param("source_key", source_key(namespace, &source_digest))
            .param("activity_key", current_activity_key.clone())
            .param("namespace", namespace)
            .param("activity_id", activity_id)
            .param("kind", activity.kind().as_str())
            .param("status", activity.status().as_str())
            .param("invocation_digest", invocation_digest)
            .param(
                "first_sequence",
                to_i64(
                    activity.evidence().first().sequence().value(),
                    "activity first sequence",
                )?,
            )
            .param(
                "last_sequence",
                to_i64(
                    activity.evidence().last().sequence().value(),
                    "activity last sequence",
                )?,
            )
            .param(
                "evidence_count",
                to_i64(
                    activity.evidence().count().value(),
                    "activity evidence count",
                )?,
            ),
        );
        for evidence in activity.evidence().iter() {
            let observation_id =
                ObservationId::from_source(evidence.source_digest(), evidence.sequence());
            queries.push(
                query(
                    "MATCH (o:HGObservation {key: $observation_key}) \
                     MATCH (a:HGActivity {key: $activity_key}) \
                     MERGE (o)-[:EVIDENCE_FOR]->(a)",
                )
                .param(
                    "observation_key",
                    observation_key(namespace, observation_id.as_str()),
                )
                .param("activity_key", current_activity_key.clone()),
            );
        }
        if let Some(previous_activity_key) = previous_activity_key {
            queries.push(
                query(
                    "MATCH (previous:HGActivity {key: $previous_key}) \
                     MATCH (current:HGActivity {key: $current_key}) \
                     MERGE (previous)-[:NEXT_ACTIVITY]->(current)",
                )
                .param("previous_key", previous_activity_key)
                .param("current_key", current_activity_key.clone()),
            );
        }
        previous_activity_key = Some(current_activity_key);
    }
    Ok(())
}

fn append_outcome_queries(queries: &mut Vec<Query>, command: &AnalysisProjectionCommand) {
    let namespace = command.namespace().as_str();
    let source_digest = command.source_digest().to_hex();
    let outcome = command.report().outcome();
    let outcome_key = outcome_key(namespace, &source_digest);
    queries.push(
        query(
            "MATCH (src:HGSourceSnapshot {key: $source_key}) \
             MERGE (outcome:HGOutcome {key: $outcome_key}) \
             ON CREATE SET outcome.hg_namespace = $namespace, outcome.source_digest = $source_digest \
             SET outcome.class = $class, outcome.verification = $verification, \
                 outcome.analysis_version = 1 \
             MERGE (src)-[:HAS_OUTCOME]->(outcome)",
        )
        .param("source_key", source_key(namespace, &source_digest))
        .param("outcome_key", outcome_key.clone())
        .param("namespace", namespace)
        .param("source_digest", source_digest)
        .param("class", outcome.class().as_str())
        .param("verification", outcome.verification().as_str()),
    );
    for evidence in outcome.evidence().iter() {
        let observation_id =
            ObservationId::from_source(evidence.source_digest(), evidence.sequence());
        queries.push(
            query(
                "MATCH (o:HGObservation {key: $observation_key}) \
                 MATCH (outcome:HGOutcome {key: $outcome_key}) \
                 MERGE (o)-[:EVIDENCE_FOR]->(outcome)",
            )
            .param(
                "observation_key",
                observation_key(namespace, observation_id.as_str()),
            )
            .param("outcome_key", outcome_key.clone()),
        );
    }
}

fn append_risk_queries(queries: &mut Vec<Query>, command: &AnalysisProjectionCommand) {
    let namespace = command.namespace().as_str();
    let source_digest = command.source_digest().to_hex();
    for risk in command.report().risks().iter() {
        let risk_key = risk_key(namespace, &risk.id().to_hex());
        queries.push(
            query(
                "MATCH (src:HGSourceSnapshot {key: $source_key}) \
                 MERGE (risk:HGRiskExposure {key: $risk_key}) \
                 ON CREATE SET risk.hg_namespace = $namespace, risk.risk_id = $risk_id \
                 SET risk.hazard = $hazard, risk.severity = $severity, risk.analysis_version = 1 \
                 MERGE (src)-[:HAS_RISK]->(risk)",
            )
            .param("source_key", source_key(namespace, &source_digest))
            .param("risk_key", risk_key.clone())
            .param("namespace", namespace)
            .param("risk_id", risk.id().to_hex())
            .param("hazard", risk.hazard().as_str())
            .param("severity", risk.severity().as_str()),
        );
        for evidence in risk.evidence().iter() {
            let observation_id =
                ObservationId::from_source(evidence.source_digest(), evidence.sequence());
            queries.push(
                query(
                    "MATCH (o:HGObservation {key: $observation_key}) \
                     MATCH (risk:HGRiskExposure {key: $risk_key}) \
                     MERGE (o)-[:EVIDENCE_FOR]->(risk)",
                )
                .param(
                    "observation_key",
                    observation_key(namespace, observation_id.as_str()),
                )
                .param("risk_key", risk_key.clone()),
            );
        }
    }
}

fn append_path_query(
    queries: &mut Vec<Query>,
    command: &AnalysisProjectionCommand,
) -> Result<(), Neo4jAdapterError> {
    let namespace = command.namespace().as_str();
    let source_digest = command.source_digest().to_hex();
    let path = command.report().path();
    let signature = path.signature().to_hex();
    let mut compact = String::new();
    for step in path.steps().iter() {
        if !compact.is_empty() {
            compact.push('>');
        }
        compact.push_str(step.kind().as_str());
        compact.push(':');
        compact.push_str(step.status().as_str());
    }
    queries.push(
        query(
            "MATCH (src:HGSourceSnapshot {key: $source_key}) \
             MERGE (path:HGPathPattern {key: $path_key}) \
             ON CREATE SET path.hg_namespace = $namespace, path.signature = $signature, \
                           path.compact = $compact, path.step_count = $step_count, \
                           path.analysis_version = 1 \
             MERGE (src)-[:FOLLOWED_PATH]->(path)",
        )
        .param("source_key", source_key(namespace, &source_digest))
        .param("path_key", path_key(namespace, &signature))
        .param("namespace", namespace)
        .param("signature", signature)
        .param("compact", compact)
        .param(
            "step_count",
            to_i64(path.steps().count().value(), "path step count")?,
        ),
    );
    Ok(())
}

fn to_i64(value: impl TryInto<i64>, field: &'static str) -> Result<i64, Neo4jAdapterError> {
    value
        .try_into()
        .map_err(|_| Neo4jAdapterError::IntegerRange { field })
}

fn session_key(namespace: &str, session_id: &str) -> String {
    format!("{namespace}:session:{session_id}")
}

fn source_key(namespace: &str, source_digest: &str) -> String {
    format!("{namespace}:source:{source_digest}")
}

fn observation_key(namespace: &str, observation_id: &str) -> String {
    format!("{namespace}:observation:{observation_id}")
}

fn context_key(namespace: &str, context_digest: &str) -> String {
    format!("{namespace}:context:{context_digest}")
}

fn turn_key(namespace: &str, session_id: &str, turn_id: &str) -> String {
    format!("{namespace}:turn:{session_id}:{turn_id}")
}

fn call_key(namespace: &str, session_id: &str, call_id: &str) -> String {
    format!("{namespace}:call:{session_id}:{call_id}")
}

fn tool_key(namespace: &str, tool_name: &str) -> String {
    format!("{namespace}:tool:{tool_name}")
}

fn receipt_key(namespace: &str, source_digest: &str) -> String {
    format!("{namespace}:receipt:{source_digest}")
}

fn activity_key(namespace: &str, activity_id: &str) -> String {
    format!("{namespace}:activity:{activity_id}")
}

fn outcome_key(namespace: &str, source_digest: &str) -> String {
    format!("{namespace}:outcome:{source_digest}")
}

fn risk_key(namespace: &str, risk_id: &str) -> String {
    format!("{namespace}:risk:{risk_id}")
}

fn path_key(namespace: &str, signature: &str) -> String {
    format!("{namespace}:path:{signature}")
}

#[cfg(test)]
mod tests {
    use std::{env, path::PathBuf};

    use harness_graph_assurance::assess_outcome;
    use harness_graph_classification::ActivityBuilder;
    use harness_graph_correlation::CorrelationEngine;
    use harness_graph_domain::{
        AnalysisReport, DecodedNativeRecord, GraphNamespace, RecordCount, SessionId, SourceDigest,
    };
    use harness_graph_graph_port::{
        AnalysisProjectionCommand, FinalizeIngestionCommand, GraphBatch, GraphCommand,
        GraphProjector, SourceSnapshotCommand,
    };
    use harness_graph_ingestion::{ArchiveRoot, DecodedRecordStream, SessionScope};
    use harness_graph_path_analysis::derive_path;
    use harness_graph_planning::{PrecedentLimit, PrecedentReader};
    use harness_graph_risk::RiskEngine;
    use secrecy::SecretString;
    use url::Url;

    use super::{Neo4jAdapter, SourceIngestionStatus};

    #[tokio::test]
    #[ignore = "requires configured real Neo4j"]
    async fn live_neo4j_projection_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
        let _dotenv = dotenvy::dotenv().ok();
        let adapter = connect_from_environment().await?;
        adapter.health().await?;
        adapter.ensure_schema().await?;
        let namespace = GraphNamespace::new(format!("e2e_{}", uuid::Uuid::now_v7().simple()))?;
        let result = run_projection_scenario(&adapter, &namespace).await;
        let cleanup = adapter.purge_namespace(&namespace).await;
        cleanup?;
        result
    }

    #[tokio::test]
    #[ignore = "requires configured real Neo4j and the real Codex archive"]
    async fn live_neo4j_returns_only_typed_verified_precedents()
    -> Result<(), Box<dyn std::error::Error>> {
        let _dotenv = dotenvy::dotenv().ok();
        let adapter = connect_from_environment().await?;
        adapter.health().await?;
        adapter.ensure_schema().await?;
        let namespace =
            GraphNamespace::new(format!("precedent_e2e_{}", uuid::Uuid::now_v7().simple()))?;
        let result = run_verified_precedent_scenario(&adapter, &namespace).await;
        let cleanup = adapter.purge_namespace(&namespace).await;
        cleanup?;
        result
    }

    async fn run_verified_precedent_scenario(
        adapter: &Neo4jAdapter,
        namespace: &GraphNamespace,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let archive = ArchiveRoot::new(PathBuf::from(required_env(
            "CODEX_SESSION_RAW_DATA_PATH",
            "CODEX_SESSION_RAW_DATA_PATH",
        )?))?;
        let session_id = SessionId::parse("019c8b3b-2aa8-7183-ba61-379f5b0af31c")?;
        let bundle = archive
            .discover(SessionScope::All)?
            .require(session_id)?
            .verify()?;
        let source_digest = bundle.source_digest();
        let expected_records = bundle.expected_records();

        adapter
            .project(GraphBatch::first(GraphCommand::UpsertSourceSnapshot(
                SourceSnapshotCommand::new(
                    namespace.clone(),
                    session_id,
                    source_digest,
                    expected_records,
                ),
            )))
            .await?;
        let records: Result<Vec<DecodedNativeRecord>, _> =
            DecodedRecordStream::open(bundle)?.collect();
        let records = records?;
        project_records(adapter, namespace, records.clone()).await?;
        project_analysis(
            adapter,
            AnalysisProjectionCommand::new(
                namespace.clone(),
                session_id,
                source_digest,
                analyze_records(&records)?,
            ),
        )
        .await?;

        let precedents = adapter
            .verified_precedents(namespace, PrecedentLimit::new(1)?)
            .await?;
        let precedent = precedents.iter().next().ok_or("expected one precedent")?;
        assert_eq!(precedent.session_id(), session_id);
        assert_eq!(precedent.steps().count().value(), 34);
        Ok(())
    }

    pub(crate) async fn run_projection_scenario(
        adapter: &Neo4jAdapter,
        namespace: &GraphNamespace,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let fixture_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/source-safe")
            .canonicalize()?;
        let archive = ArchiveRoot::new(fixture_root)?;
        let session_id = SessionId::parse("019c63db-2995-74c3-b898-c1b92a8e1317")?;
        let catalog = archive.discover(SessionScope::All)?;
        let bundle = catalog.require(session_id)?.verify()?;
        let source_digest = bundle.source_digest();
        let expected_records = bundle.expected_records();

        assert_eq!(
            adapter
                .source_ingestion_status(namespace, source_digest, expected_records)
                .await?,
            SourceIngestionStatus::Pending
        );

        adapter
            .project(GraphBatch::first(GraphCommand::UpsertSourceSnapshot(
                SourceSnapshotCommand::new(
                    namespace.clone(),
                    session_id,
                    source_digest,
                    expected_records,
                ),
            )))
            .await?;

        let records: Result<Vec<DecodedNativeRecord>, _> =
            DecodedRecordStream::open(bundle.clone())?.collect();
        let records = records?;
        project_records(adapter, namespace, records.clone()).await?;

        let analysis = analyze_records(&records)?;
        let analysis_command =
            AnalysisProjectionCommand::new(namespace.clone(), session_id, source_digest, analysis);
        project_analysis(adapter, analysis_command.clone()).await?;
        project_analysis(adapter, analysis_command).await?;

        assert_eq!(
            adapter
                .source_ingestion_status(namespace, source_digest, expected_records)
                .await?,
            SourceIngestionStatus::Pending
        );

        let receipt = FinalizeIngestionCommand::new(
            namespace.clone(),
            session_id,
            source_digest,
            RecordCount::new(11),
            RecordCount::new(1),
            RecordCount::new(12),
        );
        project_receipt(adapter, receipt.clone()).await?;
        let first_completed_at = adapter
            .receipt_completed_at(namespace, source_digest)
            .await?;
        assert_eq!(
            adapter
                .source_ingestion_status(namespace, source_digest, expected_records)
                .await?,
            SourceIngestionStatus::Complete
        );
        assert_eq!(
            adapter
                .source_ingestion_status(namespace, source_digest, RecordCount::new(13))
                .await?,
            SourceIngestionStatus::Pending
        );

        assert_receipt_repair(
            adapter,
            namespace,
            source_digest,
            expected_records,
            &receipt,
            &first_completed_at,
        )
        .await?;

        project_records(adapter, namespace, records).await?;
        project_receipt(adapter, receipt).await?;
        let second_completed_at = adapter
            .receipt_completed_at(namespace, source_digest)
            .await?;
        assert_eq!(adapter.observation_count(namespace).await?.value(), 12);
        assert_eq!(
            adapter.analysis_entity_counts(namespace).await?,
            super::AnalysisEntityCounts {
                activities: RecordCount::new(4),
                outcomes: RecordCount::new(1),
                risks: RecordCount::new(2),
                paths: RecordCount::new(1),
            }
        );
        assert_eq!(first_completed_at, second_completed_at);
        assert_eq!(
            adapter
                .source_ingestion_status(namespace, source_digest, expected_records)
                .await?,
            SourceIngestionStatus::Complete
        );
        Ok(())
    }

    async fn assert_receipt_repair(
        adapter: &Neo4jAdapter,
        namespace: &GraphNamespace,
        source_digest: SourceDigest,
        expected_records: RecordCount,
        receipt: &FinalizeIngestionCommand,
        original_completed_at: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        adapter
            .corrupt_receipt_counts(namespace, source_digest)
            .await?;
        assert_eq!(
            adapter
                .source_ingestion_status(namespace, source_digest, expected_records)
                .await?,
            SourceIngestionStatus::Pending
        );
        project_receipt(adapter, receipt.clone()).await?;
        assert_eq!(
            adapter
                .source_ingestion_status(namespace, source_digest, expected_records)
                .await?,
            SourceIngestionStatus::Complete
        );
        assert_eq!(
            original_completed_at,
            adapter
                .receipt_completed_at(namespace, source_digest)
                .await?
        );
        Ok(())
    }

    fn analyze_records(
        records: &[DecodedNativeRecord],
    ) -> Result<AnalysisReport, Box<dyn std::error::Error>> {
        let mut correlations = CorrelationEngine::default();
        let mut activities = ActivityBuilder::default();
        let mut risks = RiskEngine::default();
        for record in records {
            risks.observe(record);
            if let DecodedNativeRecord::Known(known) = record {
                correlations.observe(known.observation())?;
                activities.observe(known.observation())?;
            }
        }
        let correlations = correlations.finish()?;
        let activities = activities.finish(&correlations)?;
        let outcome = assess_outcome(&activities)?;
        let risks = risks.finish(&activities, &correlations, &outcome)?;
        let path = derive_path(&activities)?;
        Ok(AnalysisReport::new(
            correlations,
            activities,
            outcome,
            risks,
            path,
        ))
    }

    async fn project_analysis(
        adapter: &Neo4jAdapter,
        analysis: AnalysisProjectionCommand,
    ) -> Result<(), Box<dyn std::error::Error>> {
        adapter
            .project(GraphBatch::first(GraphCommand::UpsertAnalysis(analysis)))
            .await?;
        Ok(())
    }

    async fn project_receipt(
        adapter: &Neo4jAdapter,
        receipt: FinalizeIngestionCommand,
    ) -> Result<(), Box<dyn std::error::Error>> {
        adapter
            .project(GraphBatch::first(GraphCommand::FinalizeIngestion(receipt)))
            .await?;
        Ok(())
    }

    async fn project_records(
        adapter: &Neo4jAdapter,
        namespace: &GraphNamespace,
        records: Vec<DecodedNativeRecord>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut iter = records.into_iter();
        let first = iter.next().ok_or("fixture must contain records")?;
        let mut batch = GraphBatch::first(GraphCommand::UpsertObservation {
            namespace: namespace.clone(),
            record: first,
        });
        for record in iter {
            batch.push(GraphCommand::UpsertObservation {
                namespace: namespace.clone(),
                record,
            });
        }
        adapter.project(batch).await?;
        Ok(())
    }

    pub(crate) async fn connect_from_environment()
    -> Result<Neo4jAdapter, Box<dyn std::error::Error>> {
        let connection_url = required_env("NEO4J_CONNECTION_URL", "NEO4J_CONECTION_URL")?;
        let password = required_env("NEO4J_PASSWORD", "NEO4J_INATANSE_PASSWORD")?;
        let username = env::var("NEO4J_USERNAME").unwrap_or_else(|_| "neo4j".to_owned());
        let url = Url::parse(&connection_url)?;
        let host = url.host_str().ok_or("Neo4j URL requires a host")?;
        let port = url.port().unwrap_or(7687);
        Ok(Neo4jAdapter::connect(
            &format!("{host}:{port}"),
            &username,
            SecretString::from(password),
        )
        .await?)
    }

    fn required_env(
        canonical: &'static str,
        alias: &'static str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let file_value = dotenvy::dotenv_iter().ok().and_then(|iterator| {
            iterator
                .filter_map(Result::ok)
                .find_map(|(key, value)| (key == canonical || key == alias).then_some(value))
        });
        file_value
            .or_else(|| env::var(canonical).ok())
            .or_else(|| env::var(alias).ok())
            .ok_or_else(|| format!("missing {canonical}").into())
    }
}
