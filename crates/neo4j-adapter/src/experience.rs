//! Read-only Neo4j adapter for source-safe experience projections.

use async_trait::async_trait;
use harness_graph_domain::{
    ActivityId, GraphNamespace, OutcomeClass, PayloadDigest, RecordCount, RecordSequence, SessionId,
};
use harness_graph_graph_port::{
    EnrichmentLookup, EnrichmentQuery, EnrichmentReader, EpistemicStatus, ExperienceActivities,
    ExperienceActivity, ExperienceEnrichment, ExperienceEnrichmentUnavailableReason,
    ExperienceEnrichmentVisibility, ExperienceEpisodeActivityBinding,
    ExperienceEpisodeActivityBindings, ExperienceReader, ExperienceScope, ExperienceSessionDetail,
    ExperienceSessionLookup, ExperienceSessionQuery, ExperienceSessionSummaries,
    ExperienceSessionSummary, ExperienceSourceAnchor, ExperienceSourceAnchors,
    ExperienceSourceKind, KnowledgeConfidence, TranscriptSpanId,
};
use neo4rs::{Query, Row, query};

use super::{
    Neo4jAdapter, Neo4jAdapterError, parse_activity_kind, parse_activity_status, read_property,
    session_key, source_key,
};

const SESSION_LIST_QUERY: &str = "MATCH (session:HGSession {hg_namespace: $namespace}) \
     MATCH (session)-[:IMPORTED_FROM]->(src:HGSourceSnapshot)<-[:VERIFIED]-(receipt:HGIngestionReceipt {status: 'completed'}) \
     WHERE receipt.total_records = src.expected_records \
       AND receipt.known_records + receipt.quarantined_records = receipt.total_records \
     WITH session, src, receipt ORDER BY receipt.completed_at DESC, src.source_digest ASC \
     WITH session, collect(src)[0] AS src \
     OPTIONAL MATCH (src)-[:HAS_OUTCOME]->(outcome:HGOutcome) \
     WITH session, src, head(collect(outcome.class)) AS outcome \
     OPTIONAL MATCH (src)-[:HAS_ACTIVITY]->(activity:HGActivity) \
     WITH session, src, outcome, count(DISTINCT activity) AS activity_count \
     OPTIONAL MATCH (src)-[:HAS_ENRICHMENT_RUN]->(candidate:HGEnrichmentRun) \
     WITH session, src, outcome, activity_count, count(DISTINCT candidate) AS run_count \
     OPTIONAL MATCH (view:HGEnrichmentView {hg_namespace: $namespace, source_digest: src.source_digest})-[:SELECTS]->(run:HGEnrichmentRun {status: 'completed'}) \
     WHERE run.hg_namespace = $namespace AND run.source_digest = src.source_digest \
     OPTIONAL MATCH (run)-[:PRODUCED_EPISODE]->(episode:HGNarrativeEpisode)-[:SUPPORTED_BY]->(:HGTranscriptSpan) \
     WITH session, outcome, activity_count, run_count, run, episode ORDER BY episode.ordinal \
     RETURN session.session_id AS session_id, coalesce(outcome, 'inconclusive') AS outcome, \
       activity_count, run_count, coalesce(run.run_id, '') AS run_id, \
       coalesce(head(collect(episode.title)), '') AS title, \
       coalesce(head(collect(episode.summary)), '') AS summary, \
       coalesce(head(collect(episode.confidence)), '') AS confidence, \
       coalesce(head(collect(episode.epistemic_status)), '') AS epistemic_status \
     ORDER BY session_id LIMIT 100001";

const SESSION_HEADER_QUERY: &str = "OPTIONAL MATCH (session:HGSession {key: $session_key}) \
     OPTIONAL MATCH (session)-[:IMPORTED_FROM]->(candidate_src:HGSourceSnapshot)<-[:VERIFIED]-(receipt:HGIngestionReceipt {status: 'completed'}) \
     WHERE receipt.total_records = candidate_src.expected_records \
       AND receipt.known_records + receipt.quarantined_records = receipt.total_records \
     WITH session, candidate_src, receipt ORDER BY receipt.completed_at DESC, candidate_src.source_digest ASC \
     WITH session, collect(candidate_src)[0] AS src \
     OPTIONAL MATCH (src)-[:HAS_OUTCOME]->(outcome:HGOutcome) \
     WITH session, src, head(collect(outcome.class)) AS outcome \
     OPTIONAL MATCH (src)-[:HAS_ACTIVITY]->(activity:HGActivity) \
     WITH session, src, outcome, count(DISTINCT activity) AS activity_count \
     OPTIONAL MATCH (src)-[:HAS_ENRICHMENT_RUN]->(run:HGEnrichmentRun) \
     RETURN session IS NOT NULL AS session_exists, src IS NOT NULL AS source_exists, \
       coalesce(src.source_digest, '') AS source_digest, \
       coalesce(outcome, 'inconclusive') AS outcome, activity_count, \
       count(DISTINCT run) AS run_count";

const ACTIVITIES_QUERY: &str = "MATCH (src:HGSourceSnapshot {key: $source_key})-[:HAS_ACTIVITY]->(activity:HGActivity) \
     RETURN activity.activity_id AS activity_id, activity.first_sequence AS sequence, \
       activity.kind AS kind, activity.status AS status \
     ORDER BY sequence, activity_id";

const SOURCE_ANCHORS_QUERY: &str = "MATCH (run:HGEnrichmentRun {hg_namespace: $namespace, source_digest: $source_digest, fingerprint: $fingerprint, status: 'completed'}) \
     MATCH (run)-[:USED_SPAN]->(span:HGTranscriptSpan)-[:MAPS_TO]->(observation:HGObservation) \
     RETURN span.span_id AS anchor_id, span.sequence AS sequence, \
       span.content_digest AS content_digest, observation.kind AS observation_kind \
     ORDER BY sequence, span.field, span.field_ordinal, span.part_index, anchor_id";

const EPISODE_ACTIVITY_BINDINGS_QUERY: &str = "MATCH (run:HGEnrichmentRun {hg_namespace: $namespace, source_digest: $source_digest, fingerprint: $fingerprint, status: 'completed'}) \
     MATCH (run)-[:PRODUCED_EPISODE]->(episode:HGNarrativeEpisode)-[:SUPPORTED_BY]->(span:HGTranscriptSpan)-[:MAPS_TO]->(observation:HGObservation) \
     OPTIONAL MATCH (observation)-[:EVIDENCE_FOR]->(activity:HGActivity) \
     WITH episode, activity ORDER BY activity.first_sequence, activity.activity_id \
     RETURN episode.episode_id AS episode_id, episode.ordinal AS ordinal, \
       collect(DISTINCT activity.activity_id) AS activity_ids \
     ORDER BY ordinal, episode_id";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExperienceHeader {
    NotFound,
    Verified {
        source_digest: harness_graph_domain::SourceDigest,
        outcome: OutcomeClass,
        activity_count: RecordCount,
        run_count: RecordCount,
    },
}

#[async_trait]
impl ExperienceReader for Neo4jAdapter {
    type Error = Neo4jAdapterError;

    async fn experience_sessions(
        &self,
        scope: &ExperienceScope,
    ) -> Result<ExperienceSessionSummaries, Self::Error> {
        let rows = self
            .collect_experience_rows(
                query(SESSION_LIST_QUERY).param("namespace", scope.namespace().as_str()),
                "read experience session list",
            )
            .await?;
        let summaries = rows
            .iter()
            .map(|row| session_summary_from_row(row, scope.enrichment_visibility()))
            .collect::<Result<Vec<_>, _>>()?;
        ExperienceSessionSummaries::new(summaries).map_err(Neo4jAdapterError::from)
    }

    async fn experience_session(
        &self,
        request: &ExperienceSessionQuery,
    ) -> Result<ExperienceSessionLookup, Self::Error> {
        let namespace = request.scope().namespace();
        let header = self
            .read_experience_header(namespace, request.session_id())
            .await?;
        let ExperienceHeader::Verified {
            source_digest,
            outcome,
            activity_count,
            run_count,
        } = header
        else {
            return Ok(ExperienceSessionLookup::NotFound);
        };
        let activities = self
            .read_experience_activities(namespace, source_digest)
            .await?;
        let fallback_reason = unavailable_reason(activity_count, run_count);
        if request.scope().enrichment_visibility() == ExperienceEnrichmentVisibility::Disabled {
            return fallback_detail(
                request.session_id(),
                outcome,
                activities,
                ExperienceEnrichmentUnavailableReason::Disabled,
            );
        }

        let lookup = self
            .selected_enrichment(&EnrichmentQuery::new(
                namespace.clone(),
                request.session_id(),
            ))
            .await?;
        let EnrichmentLookup::Selected(selected) = lookup else {
            return fallback_detail(request.session_id(), outcome, activities, fallback_reason);
        };

        let bindings = self.read_episode_activity_bindings(&selected).await?;
        let anchors = self.read_experience_source_anchors(&selected).await?;
        let Ok(enrichment) = ExperienceEnrichment::from_selected(&selected, &bindings) else {
            return fallback_detail(
                request.session_id(),
                outcome,
                activities,
                ExperienceEnrichmentUnavailableReason::FailedOrPartial,
            );
        };
        match ExperienceSessionDetail::new(
            request.session_id(),
            outcome,
            activities.clone(),
            enrichment,
            anchors,
        ) {
            Ok(detail) => Ok(ExperienceSessionLookup::Found(Box::new(detail))),
            Err(_) => fallback_detail(
                request.session_id(),
                outcome,
                activities,
                ExperienceEnrichmentUnavailableReason::FailedOrPartial,
            ),
        }
    }
}

impl Neo4jAdapter {
    async fn read_experience_header(
        &self,
        namespace: &GraphNamespace,
        session_id: SessionId,
    ) -> Result<ExperienceHeader, Neo4jAdapterError> {
        let row = self
            .single_experience_row(
                query(SESSION_HEADER_QUERY).param(
                    "session_key",
                    session_key(namespace.as_str(), &session_id.to_string()),
                ),
                "read experience session header",
            )
            .await?;
        let session_exists: bool = read_property(&row, "session_exists")?;
        let source_exists: bool = read_property(&row, "source_exists")?;
        if !session_exists || !source_exists {
            return Ok(ExperienceHeader::NotFound);
        }
        Ok(ExperienceHeader::Verified {
            source_digest: harness_graph_domain::SourceDigest::parse_hex(
                &read_property::<String>(&row, "source_digest")?,
            )?,
            outcome: parse_outcome(&read_property::<String>(&row, "outcome")?)?,
            activity_count: read_record_count(&row, "activity_count")?,
            run_count: read_record_count(&row, "run_count")?,
        })
    }

    async fn read_experience_activities(
        &self,
        namespace: &GraphNamespace,
        source_digest: harness_graph_domain::SourceDigest,
    ) -> Result<ExperienceActivities, Neo4jAdapterError> {
        let rows = self
            .collect_experience_rows(
                query(ACTIVITIES_QUERY).param(
                    "source_key",
                    source_key(namespace.as_str(), &source_digest.to_hex()),
                ),
                "read experience activities",
            )
            .await?;
        let activities = rows
            .iter()
            .map(|row| {
                ExperienceActivity::new(
                    ActivityId::parse_hex(&read_property::<String>(row, "activity_id")?)?,
                    read_sequence(row, "sequence")?,
                    parse_activity_kind(&read_property::<String>(row, "kind")?)?,
                    parse_activity_status(&read_property::<String>(row, "status")?)?,
                )
                .map_err(Neo4jAdapterError::from)
            })
            .collect::<Result<Vec<_>, _>>()?;
        ExperienceActivities::new(activities).map_err(Neo4jAdapterError::from)
    }

    async fn read_experience_source_anchors(
        &self,
        selected: &harness_graph_graph_port::SelectedEnrichment,
    ) -> Result<ExperienceSourceAnchors, Neo4jAdapterError> {
        let rows = self
            .collect_experience_rows(
                selected_query(SOURCE_ANCHORS_QUERY, selected),
                "read experience source anchors",
            )
            .await?;
        let anchors = rows
            .iter()
            .map(|row| {
                ExperienceSourceAnchor::new(
                    TranscriptSpanId::parse_hex(&read_property::<String>(row, "anchor_id")?)?,
                    parse_source_kind(&read_property::<String>(row, "observation_kind")?)?,
                    read_sequence(row, "sequence")?,
                    PayloadDigest::parse_hex(&read_property::<String>(row, "content_digest")?)?,
                )
                .map_err(Neo4jAdapterError::from)
            })
            .collect::<Result<Vec<_>, _>>()?;
        ExperienceSourceAnchors::new(anchors).map_err(Neo4jAdapterError::from)
    }

    async fn read_episode_activity_bindings(
        &self,
        selected: &harness_graph_graph_port::SelectedEnrichment,
    ) -> Result<ExperienceEpisodeActivityBindings, Neo4jAdapterError> {
        let rows = self
            .collect_experience_rows(
                selected_query(EPISODE_ACTIVITY_BINDINGS_QUERY, selected),
                "read episode activity bindings",
            )
            .await?;
        let bindings = rows
            .iter()
            .map(|row| {
                let activities = read_property::<Vec<String>>(row, "activity_ids")?
                    .into_iter()
                    .map(|id| ActivityId::parse_hex(&id))
                    .collect::<Result<Vec<_>, _>>()?;
                ExperienceEpisodeActivityBinding::new(
                    harness_graph_graph_port::NarrativeEpisodeId::parse_hex(&read_property::<
                        String,
                    >(
                        row, "episode_id"
                    )?)?,
                    activities,
                )
                .map_err(Neo4jAdapterError::from)
            })
            .collect::<Result<Vec<_>, _>>()?;
        ExperienceEpisodeActivityBindings::new(bindings).map_err(Neo4jAdapterError::from)
    }

    async fn collect_experience_rows(
        &self,
        statement: Query,
        operation: &'static str,
    ) -> Result<Vec<Row>, Neo4jAdapterError> {
        let mut stream = self
            .graph
            .execute(statement)
            .await
            .map_err(|source| Neo4jAdapterError::Operation { operation, source })?;
        let mut rows = Vec::new();
        while let Some(row) = stream
            .next()
            .await
            .map_err(|source| Neo4jAdapterError::Operation { operation, source })?
        {
            rows.push(row);
        }
        Ok(rows)
    }

    async fn single_experience_row(
        &self,
        statement: Query,
        operation: &'static str,
    ) -> Result<Row, Neo4jAdapterError> {
        self.collect_experience_rows(statement, operation)
            .await?
            .into_iter()
            .next()
            .ok_or(Neo4jAdapterError::InvalidReadResult {
                field: "experience query row",
            })
    }
}

fn selected_query(
    statement: &'static str,
    selected: &harness_graph_graph_port::SelectedEnrichment,
) -> Query {
    query(statement)
        .param("namespace", selected.run().namespace().as_str())
        .param("source_digest", selected.run().source_digest().to_hex())
        .param("fingerprint", selected.run().fingerprint().to_hex())
}

fn session_summary_from_row(
    row: &Row,
    visibility: ExperienceEnrichmentVisibility,
) -> Result<ExperienceSessionSummary, Neo4jAdapterError> {
    let session_id = SessionId::parse(&read_property::<String>(row, "session_id")?)?;
    let outcome = parse_outcome(&read_property::<String>(row, "outcome")?)?;
    let activity_count = read_record_count(row, "activity_count")?;
    let run_count = read_record_count(row, "run_count")?;
    if visibility == ExperienceEnrichmentVisibility::Disabled {
        return Ok(ExperienceSessionSummary::unavailable(
            session_id,
            outcome,
            activity_count,
            ExperienceEnrichmentUnavailableReason::Disabled,
        ));
    }

    let run_id = read_property::<String>(row, "run_id")?;
    let title = read_property::<String>(row, "title")?;
    let summary = read_property::<String>(row, "summary")?;
    let confidence = read_property::<String>(row, "confidence")?;
    let epistemic_status = read_property::<String>(row, "epistemic_status")?;
    if !run_id.is_empty()
        && !title.is_empty()
        && !summary.is_empty()
        && !confidence.is_empty()
        && !epistemic_status.is_empty()
    {
        let completed = harness_graph_graph_port::EnrichmentRunId::parse_hex(&run_id)
            .and_then(|run_id| {
                Ok((
                    run_id,
                    KnowledgeConfidence::parse(&confidence)?,
                    EpistemicStatus::parse(&epistemic_status)?,
                ))
            })
            .ok()
            .and_then(|(run_id, confidence, epistemic_status)| {
                ExperienceSessionSummary::completed(
                    session_id,
                    outcome,
                    activity_count,
                    run_id,
                    &title,
                    &summary,
                    confidence,
                    epistemic_status,
                )
                .ok()
            });
        if let Some(completed) = completed {
            return Ok(completed);
        }
    }
    Ok(ExperienceSessionSummary::unavailable(
        session_id,
        outcome,
        activity_count,
        unavailable_reason(activity_count, run_count),
    ))
}

fn fallback_detail(
    session_id: SessionId,
    outcome: OutcomeClass,
    activities: ExperienceActivities,
    reason: ExperienceEnrichmentUnavailableReason,
) -> Result<ExperienceSessionLookup, Neo4jAdapterError> {
    Ok(ExperienceSessionLookup::Found(Box::new(
        ExperienceSessionDetail::new(
            session_id,
            outcome,
            activities,
            ExperienceEnrichment::unavailable(reason),
            ExperienceSourceAnchors::default(),
        )?,
    )))
}

fn unavailable_reason(
    activity_count: RecordCount,
    run_count: RecordCount,
) -> ExperienceEnrichmentUnavailableReason {
    if activity_count.value() == 0 {
        ExperienceEnrichmentUnavailableReason::NotEligible
    } else if run_count.value() > 0 {
        ExperienceEnrichmentUnavailableReason::FailedOrPartial
    } else {
        ExperienceEnrichmentUnavailableReason::NoCompletedRun
    }
}

fn read_record_count(row: &Row, field: &'static str) -> Result<RecordCount, Neo4jAdapterError> {
    let value: i64 = read_property(row, field)?;
    let value = u64::try_from(value).map_err(|_| Neo4jAdapterError::IntegerRange { field })?;
    Ok(RecordCount::new(value))
}

fn read_sequence(row: &Row, field: &'static str) -> Result<RecordSequence, Neo4jAdapterError> {
    let value: i64 = read_property(row, field)?;
    let value = u64::try_from(value).map_err(|_| Neo4jAdapterError::IntegerRange { field })?;
    if value == 0 {
        return Err(Neo4jAdapterError::InvalidReadResult { field });
    }
    Ok(RecordSequence::from_zero_based(value - 1))
}

fn parse_outcome(value: &str) -> Result<OutcomeClass, Neo4jAdapterError> {
    match value {
        "verified_success" => Ok(OutcomeClass::VerifiedSuccess),
        "unverified_completion" => Ok(OutcomeClass::UnverifiedCompletion),
        "failed" => Ok(OutcomeClass::Failed),
        "inconclusive" => Ok(OutcomeClass::Inconclusive),
        "cancelled" => Ok(OutcomeClass::Cancelled),
        _ => Err(Neo4jAdapterError::InvalidSemanticProperty {
            field: "experience outcome",
        }),
    }
}

fn parse_source_kind(value: &str) -> Result<ExperienceSourceKind, Neo4jAdapterError> {
    match value {
        "user_message_received"
        | "agent_message_received"
        | "inter_agent_message_observed"
        | "sub_agent_activity_observed" => Ok(ExperienceSourceKind::Conversation),
        "tool_requested" => Ok(ExperienceSourceKind::ToolRequest),
        "tool_completed" => Ok(ExperienceSourceKind::ToolResult),
        "verification_completed" => Ok(ExperienceSourceKind::Verification),
        "command_completed" | "patch_applied" | "error_observed" | "task_started"
        | "task_completed" | "goal_updated" => Ok(ExperienceSourceKind::Execution),
        _ => Err(Neo4jAdapterError::InvalidSemanticProperty {
            field: "experience source kind",
        }),
    }
}

#[cfg(test)]
mod tests {
    use harness_graph_domain::{ObservationId, SourceDigest, TokenCount};
    use harness_graph_graph_port::{
        BeginEnrichmentRunCommand, ChunkingPolicyVersion, CompleteEnrichmentRunCommand,
        EnrichmentChunkCount, EnrichmentChunkId, EnrichmentChunkProjection, EnrichmentFingerprint,
        EnrichmentGraphCommand, EnrichmentModelName, EnrichmentOutputDigest, EnrichmentProjector,
        EnrichmentProvider, EnrichmentRunId, EnrichmentRunRef, EnrichmentRunSpec,
        EnrichmentSchemaVersion, ExperienceEnrichmentVisibility, ExperienceReader, ExperienceScope,
        ExperienceSessionLookup, ExperienceSessionQuery, KnowledgeClaims, KnowledgeConfidence,
        KnowledgeEntities, KnowledgeRelations, NarrativeEpisodeId, NarrativeEpisodeProjection,
        NarrativeEpisodes, NarrativeSummary, NarrativeTitle, ProjectEnrichmentChunkCommand,
        PromptVersion, RedactionPolicyVersion, SpanCitations, TranscriptByteCount, TranscriptField,
        TranscriptFieldOrdinal, TranscriptPartIndex, TranscriptRole, TranscriptSpanProjection,
        TranscriptSpans,
    };

    use super::*;

    #[test]
    fn source_kind_mapping_is_closed_and_does_not_expose_native_fields() {
        assert_eq!(
            parse_source_kind("verification_completed").ok(),
            Some(ExperienceSourceKind::Verification)
        );
        assert_eq!(
            parse_source_kind("tool_requested").ok(),
            Some(ExperienceSourceKind::ToolRequest)
        );
        assert!(parse_source_kind("unknown_native_payload").is_err());
    }

    #[tokio::test]
    #[ignore = "requires configured real Neo4j"]
    async fn live_neo4j_experience_reader_returns_fallback_and_selected_enrichment()
    -> Result<(), Box<dyn std::error::Error>> {
        let _dotenv = dotenvy::dotenv().ok();
        let adapter = crate::tests::connect_from_environment().await?;
        let namespace =
            GraphNamespace::new(format!("experience_e2e_{}", uuid::Uuid::now_v7().simple()))?;
        let scenario = async {
            use harness_graph_graph_port::GraphProjector;

            adapter.ensure_schema().await?;
            adapter.ensure_enrichment_schema().await?;
            crate::tests::run_projection_scenario(&adapter, &namespace).await?;
            let scope =
                ExperienceScope::new(namespace.clone(), ExperienceEnrichmentVisibility::Enabled);
            let sessions = adapter.experience_sessions(&scope).await?;
            let session = sessions.iter().next().ok_or("missing experience session")?;
            let lookup = adapter
                .experience_session(&ExperienceSessionQuery::new(scope, session.session_id()))
                .await?;
            let ExperienceSessionLookup::Found(detail) = lookup else {
                return Err("projected experience session was not found".into());
            };
            let serialized = serde_json::to_string(&detail)?;
            assert!(serialized.contains("deterministic_fallback"));
            for forbidden in [
                "\"key\"",
                "field_path",
                "raw_transcript",
                "local_path",
                "provider_body",
                "MISTRAL_API_KEY",
            ] {
                assert!(!serialized.contains(forbidden));
            }

            project_source_safe_enrichment(&adapter, &namespace, session.session_id()).await?;
            let enriched = adapter
                .experience_session(&ExperienceSessionQuery::new(
                    ExperienceScope::new(
                        namespace.clone(),
                        ExperienceEnrichmentVisibility::Enabled,
                    ),
                    session.session_id(),
                ))
                .await?;
            let ExperienceSessionLookup::Found(enriched) = enriched else {
                return Err("enriched experience session was not found".into());
            };
            let enriched_json = serde_json::to_value(&enriched)?;
            assert_eq!(enriched_json["display"]["source"], "enrichment");
            assert_eq!(enriched_json["enrichment"]["state"], "completed");
            assert_eq!(enriched_json["enrichment"]["provider"], "mistral");
            assert_eq!(
                enriched_json["enrichment"]["disclosure_scope"],
                "conversation_and_execution"
            );
            assert_eq!(
                enriched_json["enrichment"]["authorization_policy_digest"],
                "e".repeat(64)
            );
            assert_eq!(enriched_json["enrichment"]["prompt_digest"], "f".repeat(64));
            assert_eq!(
                enriched_json["enrichment"]["episodes"][0]["epistemic_status"],
                "explicit"
            );
            assert_eq!(
                enriched_json["enrichment"]["episodes"][0]["citations"][0]["anchor_id"],
                enriched_json["source_anchors"][0]["anchor_id"]
            );
            let bound_activity = &enriched_json["enrichment"]["episodes"][0]["activity_ids"][0];
            assert!(
                enriched_json["activities"]
                    .as_array()
                    .is_some_and(|activities| activities
                        .iter()
                        .any(|activity| &activity["activity_id"] == bound_activity))
            );
            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
        let cleanup = adapter.purge_namespace(&namespace).await;
        cleanup?;
        scenario
    }

    #[derive(Debug)]
    struct EnrichmentAnchor {
        source_digest: SourceDigest,
        observation_id: ObservationId,
        sequence: RecordSequence,
    }

    async fn project_source_safe_enrichment(
        adapter: &Neo4jAdapter,
        namespace: &GraphNamespace,
        session_id: SessionId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let anchor = read_enrichment_anchor(adapter, namespace, session_id).await?;
        let run_id = EnrichmentRunId::parse_hex(&"a".repeat(64))?;
        let fingerprint = EnrichmentFingerprint::parse_hex(&"b".repeat(64))?;
        let run = EnrichmentRunSpec::new(
            namespace.clone(),
            session_id,
            anchor.source_digest,
            run_id,
            fingerprint,
            EnrichmentProvider::Mistral,
            EnrichmentModelName::new("mistral-small-2603")?,
            PromptVersion::new("experience-e2e-prompt-v1")?,
            harness_graph_graph_port::EnrichmentRunAuditProvenance::new(
                harness_graph_graph_port::EnrichmentDisclosureScope::ConversationAndExecution,
                harness_graph_graph_port::EnrichmentAuthorizationPolicyDigest::parse_hex(
                    &"e".repeat(64),
                )?,
                harness_graph_graph_port::EnrichmentPromptDigest::parse_hex(&"f".repeat(64))?,
            ),
            EnrichmentSchemaVersion::new("experience-e2e-schema-v1")?,
            RedactionPolicyVersion::new("experience-e2e-redaction-v1")?,
            ChunkingPolicyVersion::new("experience-e2e-chunking-v1")?,
            EnrichmentChunkCount::new(1)?,
        );
        let run_ref = EnrichmentRunRef::new(namespace.clone(), anchor.source_digest, fingerprint);
        adapter
            .project_enrichment(EnrichmentGraphCommand::BeginRun(
                BeginEnrichmentRunCommand::new(run),
            ))
            .await?;

        let span_id = TranscriptSpanId::parse_hex(&"c".repeat(64))?;
        let spans = TranscriptSpans::new([TranscriptSpanProjection::new(
            span_id,
            anchor.observation_id,
            anchor.sequence,
            TranscriptField::ContentText,
            TranscriptFieldOrdinal::new(0),
            TranscriptPartIndex::new(0),
            TranscriptRole::Agent,
            TranscriptByteCount::new(28),
            TokenCount::new(7),
            PayloadDigest::hash(b"source-safe experience evidence"),
        )])?;
        let episodes = NarrativeEpisodes::new([NarrativeEpisodeProjection::new(
            NarrativeEpisodeId::parse_hex(&"d".repeat(64))?,
            harness_graph_graph_port::EpisodeOrdinal::new(1)?,
            NarrativeTitle::new("Validated experience projection")?,
            NarrativeSummary::new(
                "A selected Mistral overlay remains cited and separate from deterministic state.",
            )?,
            KnowledgeConfidence::High,
            EpistemicStatus::Explicit,
            SpanCitations::new([span_id])?,
        )])?;
        let chunk = EnrichmentChunkProjection::new(
            EnrichmentChunkId::parse_hex(&"e".repeat(64))?,
            EnrichmentOutputDigest::parse_hex(&"f".repeat(64))?,
            spans,
            episodes,
            KnowledgeEntities::default(),
            KnowledgeClaims::default(),
            KnowledgeRelations::default(),
            TokenCount::new(40),
            TokenCount::new(20),
        )?;
        adapter
            .project_enrichment(EnrichmentGraphCommand::ProjectChunk(
                ProjectEnrichmentChunkCommand::new(run_ref.clone(), chunk),
            ))
            .await?;
        adapter
            .project_enrichment(EnrichmentGraphCommand::CompleteRun(
                CompleteEnrichmentRunCommand::new(run_ref),
            ))
            .await?;
        Ok(())
    }

    async fn read_enrichment_anchor(
        adapter: &Neo4jAdapter,
        namespace: &GraphNamespace,
        session_id: SessionId,
    ) -> Result<EnrichmentAnchor, Box<dyn std::error::Error>> {
        let mut rows = adapter
            .graph
            .execute(
                query(
                    "MATCH (session:HGSession {key: $session_key})-[:IMPORTED_FROM]->(src:HGSourceSnapshot) \
                     MATCH (src)-[:CONTAINS]->(observation:HGObservation)-[:EVIDENCE_FOR]->(:HGActivity) \
                     WHERE observation.kind IN ['user_message_received', 'agent_message_received', \
                       'tool_requested', 'tool_completed', 'command_completed', 'patch_applied', \
                       'verification_completed', 'error_observed'] \
                     RETURN src.source_digest AS source_digest, observation.observation_id AS observation_id, \
                       observation.sequence AS sequence ORDER BY sequence LIMIT 1",
                )
                .param(
                    "session_key",
                    session_key(namespace.as_str(), &session_id.to_string()),
                ),
            )
            .await?;
        let row = rows
            .next()
            .await?
            .ok_or("missing exact enrichment anchor")?;
        let source_digest = SourceDigest::parse_hex(&row.get::<String>("source_digest")?)?;
        let sequence = read_sequence(&row, "sequence")?;
        let observation_id = ObservationId::from_source(source_digest, sequence);
        if row.get::<String>("observation_id")? != observation_id.as_str() {
            return Err("enrichment observation identity mismatch".into());
        }
        Ok(EnrichmentAnchor {
            source_digest,
            observation_id,
            sequence,
        })
    }
}
