//! Additive Neo4j projection and completed-only reads for semantic enrichment.

use async_trait::async_trait;
use harness_graph_domain::{
    GraphNamespace, ObservationId, PayloadDigest, RecordSequence, SourceDigest, TokenCount,
};
use harness_graph_graph_port::{
    BeginEnrichmentRunCommand, ChunkingPolicyVersion, ClaimEnrichmentChunkCommand,
    ClaimedEnrichmentChunk, CommittedEnrichmentChunk, CompleteEnrichmentRunCommand,
    EnrichmentChunkCheckpoint, EnrichmentChunkCheckpointQuery, EnrichmentChunkClaim,
    EnrichmentChunkCount, EnrichmentChunkLeaseRelease, EnrichmentChunkProjection,
    EnrichmentFailureStatus, EnrichmentGraphCommand, EnrichmentLookup, EnrichmentModelName,
    EnrichmentProjectionDisposition, EnrichmentProjectionReceipt, EnrichmentProjector,
    EnrichmentProvider, EnrichmentQuery, EnrichmentReader, EnrichmentRunAuditProvenance,
    EnrichmentRunId, EnrichmentRunLifecycle, EnrichmentRunLifecycleQuery, EnrichmentRunRef,
    EnrichmentRunSpec, EnrichmentSchemaVersion, EnrichmentUnavailableReason, EpisodeOrdinal,
    EpistemicStatus, KnowledgeClaimId, KnowledgeClaimProjection, KnowledgeClaimSubjects,
    KnowledgeClaimTitle, KnowledgeClaims, KnowledgeConfidence, KnowledgeEntities,
    KnowledgeEntityId, KnowledgeEntityKind, KnowledgeEntityName, KnowledgeEntityProjection,
    KnowledgeKind, KnowledgePredicate, KnowledgeRelationId, KnowledgeRelationProjection,
    KnowledgeRelations, KnowledgeStatement, MarkEnrichmentRunFailedCommand, NarrativeEpisodeId,
    NarrativeEpisodeProjection, NarrativeEpisodes, NarrativeSummary, NarrativeTitle,
    ObservationCorroboration, ProjectClaimedEnrichmentChunkCommand, ProjectEnrichmentChunkCommand,
    PromptVersion, RedactionPolicyVersion, ReleaseEnrichmentChunkLeaseCommand, SelectedEnrichment,
    SpanCitations, TranscriptByteCount, TranscriptField, TranscriptFieldOrdinal,
    TranscriptPartIndex, TranscriptRole, TranscriptSpanId, TranscriptSpanProjection,
    TranscriptSpans,
};
use neo4rs::{Query, Row, query};

use super::{Neo4jAdapter, Neo4jAdapterError, observation_key, session_key, source_key, to_i64};

const ENRICHMENT_CONSTRAINTS: &[&str] = &[
    "CREATE CONSTRAINT hg_enrichment_run_key IF NOT EXISTS FOR (n:HGEnrichmentRun) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT hg_enrichment_chunk_receipt_key IF NOT EXISTS FOR (n:HGEnrichmentChunkReceipt) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT hg_enrichment_chunk_lease_key IF NOT EXISTS FOR (n:HGEnrichmentChunkLease) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT hg_transcript_span_key IF NOT EXISTS FOR (n:HGTranscriptSpan) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT hg_narrative_episode_key IF NOT EXISTS FOR (n:HGNarrativeEpisode) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT hg_knowledge_entity_key IF NOT EXISTS FOR (n:HGKnowledgeEntity) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT hg_knowledge_claim_key IF NOT EXISTS FOR (n:HGKnowledgeClaim) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT hg_knowledge_relation_key IF NOT EXISTS FOR (n:HGKnowledgeRelation) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT hg_enrichment_view_key IF NOT EXISTS FOR (n:HGEnrichmentView) REQUIRE n.key IS UNIQUE",
    "CREATE INDEX hg_enrichment_run_lookup IF NOT EXISTS FOR (n:HGEnrichmentRun) ON (n.hg_namespace, n.source_digest, n.status)",
    "CREATE INDEX hg_enrichment_span_lookup IF NOT EXISTS FOR (n:HGTranscriptSpan) ON (n.hg_namespace, n.run_fingerprint, n.chunk_id)",
];

const BEGIN_RUN_QUERY: &str = "MATCH (session:HGSession {key: $session_key})-[:IMPORTED_FROM]->(src:HGSourceSnapshot {key: $source_key}) \
     MATCH (receipt:HGIngestionReceipt {key: $receipt_key, status: 'completed'})-[:VERIFIED]->(src) \
     WHERE receipt.total_records = src.expected_records \
       AND receipt.known_records + receipt.quarantined_records = receipt.total_records \
     MERGE (run:HGEnrichmentRun {key: $run_key}) \
     ON CREATE SET run.hg_namespace = $namespace, run.run_id = $run_id, \
       run.source_digest = $source_digest, run.fingerprint = $fingerprint, \
       run.provider = $provider, run.model = $model, run.prompt_version = $prompt_version, \
       run.disclosure_scope = $disclosure_scope, \
       run.authorization_policy_digest = $authorization_policy_digest, \
       run.prompt_digest = $prompt_digest, \
       run.schema_version = $schema_version, run.redaction_version = $redaction_version, \
       run.chunking_version = $chunking_version, run.expected_chunks = $expected_chunks, \
       run.status = 'planned', run.created_at = datetime() \
     WITH src, run \
     WHERE run.hg_namespace = $namespace AND run.run_id = $run_id \
       AND run.source_digest = $source_digest AND run.fingerprint = $fingerprint \
       AND run.provider = $provider AND run.model = $model \
       AND run.prompt_version = $prompt_version \
       AND run.disclosure_scope = $disclosure_scope \
       AND run.authorization_policy_digest = $authorization_policy_digest \
       AND run.prompt_digest = $prompt_digest AND run.schema_version = $schema_version \
       AND run.redaction_version = $redaction_version \
       AND run.chunking_version = $chunking_version AND run.expected_chunks = $expected_chunks \
     MERGE (src)-[:HAS_ENRICHMENT_RUN]->(run) \
     RETURN count(run) AS accepted";

const RUN_PREFLIGHT_QUERY: &str = "OPTIONAL MATCH (run:HGEnrichmentRun {key: $run_key}) \
     RETURN run IS NOT NULL AS exists, coalesce( \
       run.hg_namespace = $namespace AND run.run_id = $run_id \
       AND run.source_digest = $source_digest AND run.fingerprint = $fingerprint \
       AND run.provider = $provider AND run.model = $model \
       AND run.prompt_version = $prompt_version \
       AND run.disclosure_scope = $disclosure_scope \
       AND run.authorization_policy_digest = $authorization_policy_digest \
       AND run.prompt_digest = $prompt_digest AND run.schema_version = $schema_version \
       AND run.redaction_version = $redaction_version \
       AND run.chunking_version = $chunking_version AND run.expected_chunks = $expected_chunks, \
       false) AS identical";

const CHUNK_PREFLIGHT_QUERY: &str = "OPTIONAL MATCH (receipt:HGEnrichmentChunkReceipt {key: $chunk_receipt_key}) \
     RETURN receipt IS NOT NULL AS exists, \
       coalesce(receipt.hg_namespace = $namespace AND receipt.run_fingerprint = $fingerprint \
         AND receipt.chunk_id = $chunk_id AND receipt.output_digest = $output_digest, false) AS identical";

const FAILURE_PREFLIGHT_QUERY: &str = "OPTIONAL MATCH (run:HGEnrichmentRun {key: $run_key}) \
     RETURN run IS NOT NULL AS exists, \
       coalesce(run.status = $failure_status AND run.failure_class = $failure_class, false) AS identical, \
       coalesce(run.status IN ['retryable_failed', 'terminal_failed'], false) AS failed, \
       coalesce(run.status = 'retryable_failed', false) AS retryable_failed";

const MARK_RUN_FAILED_QUERY: &str = "MATCH (run:HGEnrichmentRun {key: $run_key}) \
     WHERE run.status IN ['planned', 'projecting', 'retryable_failed'] \
     SET run.status = $failure_status, run.failure_class = $failure_class, \
       run.failed_at = coalesce(run.failed_at, datetime()) \
     RETURN count(run) AS accepted";

const START_CHUNK_QUERY: &str = "MATCH (run:HGEnrichmentRun {key: $run_key}) \
     WHERE run.status IN ['planned', 'projecting', 'retryable_failed'] \
     SET run.status = 'projecting'";

const COMPLETE_RUN_QUERY: &str = "MATCH (run:HGEnrichmentRun {key: $run_key})<-[:HAS_ENRICHMENT_RUN]-(src:HGSourceSnapshot {key: $source_key}) \
     WHERE run.status IN ['planned', 'projecting', 'retryable_failed'] \
     OPTIONAL MATCH (run)-[:HAS_CHUNK_RECEIPT]->(chunk:HGEnrichmentChunkReceipt) \
     WITH run, src, count(DISTINCT chunk) AS completed_chunks \
     WHERE completed_chunks = run.expected_chunks \
     SET run.status = 'completed', run.completed_at = datetime() \
     MERGE (view:HGEnrichmentView {key: $view_key}) \
     ON CREATE SET view.hg_namespace = $namespace, view.source_digest = $source_digest \
     WITH run, view \
     OPTIONAL MATCH (view)-[old:SELECTS]->(:HGEnrichmentRun) \
     DELETE old \
     WITH run, view \
     MERGE (view)-[:SELECTS]->(run) \
     RETURN count(run) AS accepted";

const SELECTED_RUN_QUERY: &str = "OPTIONAL MATCH (session:HGSession {key: $session_key}) \
     OPTIONAL MATCH (session)-[:IMPORTED_FROM]->(src:HGSourceSnapshot)<-[:VERIFIED]-(receipt:HGIngestionReceipt {status: 'completed'}) \
     WITH session, src, receipt ORDER BY receipt.completed_at DESC, src.source_digest ASC \
     WITH session, collect(src)[0] AS src \
     OPTIONAL MATCH (view:HGEnrichmentView {hg_namespace: $namespace, source_digest: src.source_digest})-[:SELECTS]->(run:HGEnrichmentRun {status: 'completed'}) \
     WHERE run.hg_namespace = $namespace AND run.source_digest = src.source_digest \
     RETURN session IS NOT NULL AS session_exists, src IS NOT NULL AS source_exists, \
       run IS NOT NULL AS selected, coalesce(src.source_digest, '') AS source_digest, \
       coalesce(run.run_id, '') AS run_id, coalesce(run.fingerprint, '') AS fingerprint, \
       coalesce(run.provider, '') AS provider, coalesce(run.model, '') AS model, \
       coalesce(run.prompt_version, '') AS prompt_version, \
       coalesce(run.disclosure_scope, '') AS disclosure_scope, \
       coalesce(run.authorization_policy_digest, '') AS authorization_policy_digest, \
       coalesce(run.prompt_digest, '') AS prompt_digest, \
       coalesce(run.schema_version, '') AS schema_version, \
       coalesce(run.redaction_version, '') AS redaction_version, \
       coalesce(run.chunking_version, '') AS chunking_version, \
       coalesce(run.expected_chunks, 0) AS expected_chunks";

const CHUNK_CHECKPOINT_QUERY: &str = "OPTIONAL MATCH (run:HGEnrichmentRun {key: $run_key}) \
     OPTIONAL MATCH (receipt:HGEnrichmentChunkReceipt {key: $chunk_receipt_key}) \
     OPTIONAL MATCH (run)-[checkpoint:HAS_CHUNK_RECEIPT]->(receipt) \
     RETURN receipt IS NOT NULL AS exists, coalesce( \
       run IS NOT NULL AND checkpoint IS NOT NULL \
       AND receipt.hg_namespace = $namespace AND receipt.run_fingerprint = $fingerprint \
       AND receipt.chunk_id = $chunk_id, false) AS valid, \
       coalesce(receipt.output_digest, '') AS output_digest, \
       coalesce(receipt.input_tokens, 0) AS input_tokens, \
       coalesce(receipt.output_tokens, 0) AS output_tokens";

const RUN_LIFECYCLE_QUERY: &str = "OPTIONAL MATCH (run:HGEnrichmentRun {key: $run_key}) \
     RETURN run IS NOT NULL AS exists, coalesce( \
       run.hg_namespace = $namespace AND run.source_digest = $source_digest \
       AND run.fingerprint = $fingerprint, false) AS valid, \
       coalesce(run.status, '') AS status";

const CLAIM_CHUNK_QUERY: &str = "MATCH (run:HGEnrichmentRun {key: $run_key}) \
     WHERE run.hg_namespace = $namespace AND run.source_digest = $source_digest \
       AND run.fingerprint = $fingerprint \
       AND run.status IN ['planned', 'projecting', 'retryable_failed'] \
     MERGE (lease:HGEnrichmentChunkLease {key: $lease_key}) \
     ON CREATE SET lease.hg_namespace = $namespace, lease.run_fingerprint = $fingerprint, \
       lease.chunk_id = $chunk_id, lease.owner = $owner, \
       lease.expires_at = datetime() + duration({seconds: $lease_seconds}) \
     MERGE (run)-[held:HAS_CHUNK_LEASE]->(lease) \
     WITH run, held, lease, \
       lease.hg_namespace = $namespace AND lease.run_fingerprint = $fingerprint \
         AND lease.chunk_id = $chunk_id AS lease_valid \
     OPTIONAL MATCH (run)-[checkpoint:HAS_CHUNK_RECEIPT]->(receipt:HGEnrichmentChunkReceipt {key: $chunk_receipt_key}) \
     WITH held, lease, lease_valid, checkpoint, receipt, \
       coalesce(checkpoint IS NOT NULL AND receipt.hg_namespace = $namespace \
         AND receipt.run_fingerprint = $fingerprint AND receipt.chunk_id = $chunk_id, false) AS receipt_valid, \
       receipt IS NULL AND lease_valid \
         AND (lease.owner = $owner OR lease.expires_at <= datetime()) AS may_claim \
     FOREACH (_ IN CASE WHEN may_claim THEN [1] ELSE [] END | \
       SET lease.owner = $owner, \
         lease.expires_at = datetime() + duration({seconds: $lease_seconds}), \
         lease.claimed_at = datetime()) \
     FOREACH (_ IN CASE WHEN receipt IS NOT NULL THEN [1] ELSE [] END | DELETE held, lease) \
     RETURN receipt IS NOT NULL AS committed, receipt_valid, lease_valid, may_claim AS claimed, \
       coalesce(receipt.output_digest, '') AS output_digest, \
       coalesce(receipt.input_tokens, 0) AS input_tokens, \
       coalesce(receipt.output_tokens, 0) AS output_tokens";

const START_CLAIMED_CHUNK_QUERY: &str = "MATCH (run:HGEnrichmentRun {key: $run_key})-[held:HAS_CHUNK_LEASE]->(lease:HGEnrichmentChunkLease {key: $lease_key}) \
     WHERE run.hg_namespace = $namespace AND run.source_digest = $source_digest \
       AND run.fingerprint = $fingerprint \
       AND run.status IN ['planned', 'projecting', 'retryable_failed'] \
       AND lease.hg_namespace = $namespace AND lease.run_fingerprint = $fingerprint \
       AND lease.chunk_id = $chunk_id AND lease.owner = $owner \
       AND lease.expires_at > datetime() \
     SET run.status = 'projecting', lease.projecting_at = datetime() \
     RETURN count(held) AS accepted";

const CONSUME_CHUNK_LEASE_QUERY: &str = "MATCH (run:HGEnrichmentRun {key: $run_key})-[:HAS_CHUNK_RECEIPT]->(receipt:HGEnrichmentChunkReceipt {key: $chunk_receipt_key}) \
     MATCH (run)-[held:HAS_CHUNK_LEASE]->(lease:HGEnrichmentChunkLease {key: $lease_key}) \
     WHERE run.hg_namespace = $namespace AND run.source_digest = $source_digest \
       AND run.fingerprint = $fingerprint AND receipt.chunk_id = $chunk_id \
       AND lease.hg_namespace = $namespace AND lease.run_fingerprint = $fingerprint \
       AND lease.chunk_id = $chunk_id AND lease.owner = $owner \
     DELETE held, lease RETURN count(receipt) AS accepted";

const RELEASE_CHUNK_LEASE_QUERY: &str = "MATCH (run:HGEnrichmentRun {key: $run_key}) \
     WHERE run.hg_namespace = $namespace AND run.source_digest = $source_digest \
       AND run.fingerprint = $fingerprint \
     OPTIONAL MATCH (run)-[checkpoint:HAS_CHUNK_RECEIPT]->(receipt:HGEnrichmentChunkReceipt {key: $chunk_receipt_key}) \
     OPTIONAL MATCH (run)-[held:HAS_CHUNK_LEASE]->(lease:HGEnrichmentChunkLease {key: $lease_key}) \
     WITH checkpoint, receipt, held, lease, \
       coalesce(checkpoint IS NOT NULL AND receipt.hg_namespace = $namespace \
         AND receipt.run_fingerprint = $fingerprint AND receipt.chunk_id = $chunk_id, false) AS receipt_valid, \
       coalesce(lease.hg_namespace = $namespace AND lease.run_fingerprint = $fingerprint \
         AND lease.chunk_id = $chunk_id, false) AS lease_valid, \
       coalesce(lease.owner = $owner, false) AS owned \
     WITH receipt, held, lease, receipt_valid, lease IS NOT NULL AS lease_exists, \
       lease_valid, owned, receipt IS NULL AND lease_valid AND owned AS released \
     FOREACH (_ IN CASE WHEN released THEN [1] ELSE [] END | DELETE held, lease) \
     RETURN receipt IS NOT NULL AS committed, receipt_valid, lease_exists, \
       lease_valid, owned, released, \
       coalesce(receipt.output_digest, '') AS output_digest, \
       coalesce(receipt.input_tokens, 0) AS input_tokens, \
       coalesce(receipt.output_tokens, 0) AS output_tokens";

const SPANS_QUERY: &str = "MATCH (run:HGEnrichmentRun {key: $run_key, status: 'completed'})-[:USED_SPAN]->(span:HGTranscriptSpan) \
     RETURN span.span_id AS span_id, span.observation_id AS observation_id, \
       span.sequence AS sequence, span.field AS field, span.field_ordinal AS field_ordinal, \
       span.part_index AS part_index, span.role AS role, span.byte_count AS byte_count, span.token_count AS token_count, \
       span.content_digest AS content_digest ORDER BY span.sequence, span.field, span.field_ordinal, span.part_index";

const EPISODES_QUERY: &str = "MATCH (run:HGEnrichmentRun {key: $run_key, status: 'completed'})-[:PRODUCED_EPISODE]->(episode:HGNarrativeEpisode) \
     MATCH (episode)-[:SUPPORTED_BY]->(span:HGTranscriptSpan) \
     WITH episode, span ORDER BY span.sequence, span.field, span.field_ordinal, span.part_index \
     RETURN episode.episode_id AS episode_id, episode.ordinal AS ordinal, \
       episode.title AS title, episode.summary AS summary, \
       episode.confidence AS confidence, episode.epistemic_status AS epistemic_status, \
       collect(span.span_id) AS span_ids ORDER BY ordinal";

const ENTITIES_QUERY: &str = "MATCH (run:HGEnrichmentRun {key: $run_key, status: 'completed'})-[:PRODUCED_ENTITY]->(entity:HGKnowledgeEntity) \
     RETURN DISTINCT entity.entity_id AS entity_id, entity.kind AS kind, entity.name AS name \
     ORDER BY entity.entity_id";

const CLAIMS_QUERY: &str = "MATCH (run:HGEnrichmentRun {key: $run_key, status: 'completed'})-[:PRODUCED_CLAIM]->(claim:HGKnowledgeClaim) \
     MATCH (claim)-[:SUPPORTED_BY]->(span:HGTranscriptSpan) \
     OPTIONAL MATCH (claim)-[:ABOUT]->(entity:HGKnowledgeEntity) \
     OPTIONAL MATCH (claim)-[:CORROBORATED_BY]->(observation:HGObservation) \
     WITH claim, span, observation, entity ORDER BY entity.entity_id \
     RETURN claim.claim_id AS claim_id, claim.kind AS kind, claim.title AS title, \
       claim.statement AS statement, claim.subject_scope AS subject_scope, \
       claim.confidence AS confidence, claim.epistemic_status AS epistemic_status, \
       collect(DISTINCT entity.entity_id) AS entity_ids, collect(DISTINCT span.span_id) AS span_ids, \
       collect(DISTINCT observation.observation_id) AS observation_ids ORDER BY claim.claim_id";

const RELATIONS_QUERY: &str = "MATCH (run:HGEnrichmentRun {key: $run_key, status: 'completed'})-[:PRODUCED_RELATION]->(relation:HGKnowledgeRelation) \
     MATCH (relation)-[:SUBJECT]->(subject:HGKnowledgeEntity) \
     MATCH (relation)-[:OBJECT]->(object:HGKnowledgeEntity) \
     MATCH (relation)-[:SUPPORTED_BY]->(span:HGTranscriptSpan) \
     RETURN relation.relation_id AS relation_id, relation.predicate AS predicate, \
       relation.confidence AS confidence, relation.epistemic_status AS epistemic_status, \
       subject.entity_id AS subject_id, object.entity_id AS object_id, \
       collect(DISTINCT span.span_id) AS span_ids ORDER BY relation.relation_id";

const ENTITY_PROJECTION_QUERY: &str = "MATCH (run:HGEnrichmentRun {key: $run_key, status: 'projecting'}) \
     MERGE (entity:HGKnowledgeEntity {key: $entity_key}) \
     ON CREATE SET entity.hg_namespace = $namespace, entity.run_fingerprint = $fingerprint, \
       entity.entity_id = $entity_id, entity.kind = $kind, entity.name = $name \
     MERGE (run)-[:PRODUCED_ENTITY {chunk_id: $chunk_id}]->(entity)";

#[async_trait]
impl EnrichmentProjector for Neo4jAdapter {
    type Error = Neo4jAdapterError;

    async fn ensure_enrichment_schema(&self) -> Result<(), Self::Error> {
        for statement in ENRICHMENT_CONSTRAINTS {
            self.graph.run(query(statement)).await.map_err(|source| {
                Neo4jAdapterError::Operation {
                    operation: "ensure enrichment schema",
                    source,
                }
            })?;
        }
        Ok(())
    }

    async fn claim_enrichment_chunk(
        &self,
        command: &ClaimEnrichmentChunkCommand,
    ) -> Result<EnrichmentChunkClaim, Self::Error> {
        let row = self
            .single_row(claim_chunk_query(command), "claim enrichment chunk lease")
            .await?;
        if !read_bool(&row, "lease_valid")? {
            return Err(Neo4jAdapterError::InvalidReadResult {
                field: "enrichment chunk lease identity",
            });
        }
        if read_bool(&row, "committed")? {
            if !read_bool(&row, "receipt_valid")? {
                return Err(Neo4jAdapterError::InvalidReadResult {
                    field: "enrichment chunk claim receipt identity",
                });
            }
            return Ok(EnrichmentChunkClaim::Committed(committed_from_row(
                &row,
                command.chunk_id(),
            )?));
        }
        if read_bool(&row, "claimed")? {
            return Ok(EnrichmentChunkClaim::Claimed(ClaimedEnrichmentChunk::new(
                command.chunk_id(),
                command.owner(),
            )));
        }
        Ok(EnrichmentChunkClaim::Busy)
    }

    async fn project_claimed_enrichment_chunk(
        &self,
        command: ProjectClaimedEnrichmentChunkCommand,
    ) -> Result<EnrichmentProjectionReceipt, Self::Error> {
        let _guard = self.projection_gate.lock().await;
        self.project_claimed_chunk(&command).await
    }

    async fn release_enrichment_chunk_lease(
        &self,
        command: &ReleaseEnrichmentChunkLeaseCommand,
    ) -> Result<EnrichmentChunkLeaseRelease, Self::Error> {
        let row = self
            .single_row(
                release_chunk_lease_query(command),
                "release enrichment chunk lease",
            )
            .await?;
        if read_bool(&row, "committed")? {
            if !read_bool(&row, "receipt_valid")? {
                return Err(Neo4jAdapterError::InvalidReadResult {
                    field: "enrichment chunk release receipt identity",
                });
            }
            return Ok(EnrichmentChunkLeaseRelease::Committed(committed_from_row(
                &row,
                command.lease().chunk_id(),
            )?));
        }
        if read_bool(&row, "released")? {
            return Ok(EnrichmentChunkLeaseRelease::Released);
        }
        if read_bool(&row, "lease_exists")? && !read_bool(&row, "lease_valid")? {
            return Err(Neo4jAdapterError::InvalidReadResult {
                field: "enrichment chunk release lease identity",
            });
        }
        Ok(EnrichmentChunkLeaseRelease::NotOwned)
    }

    async fn project_enrichment(
        &self,
        command: EnrichmentGraphCommand,
    ) -> Result<EnrichmentProjectionReceipt, Self::Error> {
        let _guard = self.projection_gate.lock().await;
        match command {
            EnrichmentGraphCommand::BeginRun(command) => self.begin_run(&command).await,
            EnrichmentGraphCommand::ProjectChunk(command) => self.project_chunk(&command).await,
            EnrichmentGraphCommand::CompleteRun(command) => self.complete_run(&command).await,
            EnrichmentGraphCommand::MarkRunFailed(command) => self.mark_run_failed(&command).await,
        }
    }
}

impl Neo4jAdapter {
    async fn begin_run(
        &self,
        command: &BeginEnrichmentRunCommand,
    ) -> Result<EnrichmentProjectionReceipt, Neo4jAdapterError> {
        let run = command.run();
        let preflight = self
            .single_row(run_preflight_query(run)?, "read enrichment run")
            .await?;
        let exists = read_bool(&preflight, "exists")?;
        let identical = read_bool(&preflight, "identical")?;
        if exists {
            if identical {
                return Ok(EnrichmentProjectionReceipt::new(
                    EnrichmentProjectionDisposition::Unchanged,
                ));
            }
            return Err(Neo4jAdapterError::ConflictingEnrichment { object: "run" });
        }
        let row = self
            .single_row(begin_run_query(run)?, "begin enrichment run")
            .await?;
        require_accepted(&row, "begin run")?;
        Ok(EnrichmentProjectionReceipt::new(
            EnrichmentProjectionDisposition::Applied,
        ))
    }

    async fn project_chunk(
        &self,
        command: &ProjectEnrichmentChunkCommand,
    ) -> Result<EnrichmentProjectionReceipt, Neo4jAdapterError> {
        let row = self
            .single_row(
                chunk_preflight_query(command),
                "read enrichment chunk receipt",
            )
            .await?;
        let exists = read_bool(&row, "exists")?;
        let identical = read_bool(&row, "identical")?;
        if exists {
            if identical {
                return Ok(EnrichmentProjectionReceipt::new(
                    EnrichmentProjectionDisposition::Unchanged,
                ));
            }
            return Err(Neo4jAdapterError::ConflictingEnrichment {
                object: "chunk receipt",
            });
        }

        let chunk_queries = chunk_queries(command)?;
        let chunk_receipt_query = chunk_receipt_query(command)?;
        let mut transaction =
            self.graph
                .start_txn()
                .await
                .map_err(|source| Neo4jAdapterError::Operation {
                    operation: "start enrichment chunk transaction",
                    source,
                })?;
        if let Err(error) = run_chunk_transaction(
            &mut transaction,
            start_chunk_query(command.run()),
            chunk_queries,
            chunk_receipt_query,
        )
        .await
        {
            transaction
                .rollback()
                .await
                .map_err(|source| Neo4jAdapterError::Operation {
                    operation: "rollback rejected enrichment chunk",
                    source,
                })?;
            return Err(error);
        }
        transaction
            .commit()
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "commit enrichment chunk",
                source,
            })?;
        Ok(EnrichmentProjectionReceipt::new(
            EnrichmentProjectionDisposition::Applied,
        ))
    }

    async fn project_claimed_chunk(
        &self,
        command: &ProjectClaimedEnrichmentChunkCommand,
    ) -> Result<EnrichmentProjectionReceipt, Neo4jAdapterError> {
        let legacy =
            ProjectEnrichmentChunkCommand::new(command.run().clone(), command.projection().clone());
        let row = self
            .single_row(
                chunk_preflight_query(&legacy),
                "read claimed enrichment chunk receipt",
            )
            .await?;
        let exists = read_bool(&row, "exists")?;
        let identical = read_bool(&row, "identical")?;
        if exists {
            if identical {
                return Ok(EnrichmentProjectionReceipt::new(
                    EnrichmentProjectionDisposition::Unchanged,
                ));
            }
            return Err(Neo4jAdapterError::ConflictingEnrichment {
                object: "chunk receipt",
            });
        }

        let chunk_queries = chunk_queries(&legacy)?;
        let chunk_receipt_query = chunk_receipt_query(&legacy)?;
        let mut transaction =
            self.graph
                .start_txn()
                .await
                .map_err(|source| Neo4jAdapterError::Operation {
                    operation: "start claimed enrichment chunk transaction",
                    source,
                })?;
        if let Err(error) = run_claimed_chunk_transaction(
            &mut transaction,
            start_claimed_chunk_query(command),
            chunk_queries,
            chunk_receipt_query,
            consume_chunk_lease_query(command),
        )
        .await
        {
            transaction
                .rollback()
                .await
                .map_err(|source| Neo4jAdapterError::Operation {
                    operation: "rollback rejected claimed enrichment chunk",
                    source,
                })?;
            return Err(error);
        }
        transaction
            .commit()
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "commit claimed enrichment chunk",
                source,
            })?;
        Ok(EnrichmentProjectionReceipt::new(
            EnrichmentProjectionDisposition::Applied,
        ))
    }

    async fn complete_run(
        &self,
        command: &CompleteEnrichmentRunCommand,
    ) -> Result<EnrichmentProjectionReceipt, Neo4jAdapterError> {
        let run_key = enrichment_run_key(command.run());
        let row = self.single_row(
            query("OPTIONAL MATCH (run:HGEnrichmentRun {key: $run_key}) RETURN coalesce(run.status = 'completed', false) AS completed")
                .param("run_key", run_key),
            "read enrichment completion",
        ).await?;
        if read_bool(&row, "completed")? {
            return Ok(EnrichmentProjectionReceipt::new(
                EnrichmentProjectionDisposition::Unchanged,
            ));
        }
        let row = self
            .single_row(complete_run_query(command), "complete enrichment run")
            .await?;
        require_accepted(&row, "complete run")?;
        Ok(EnrichmentProjectionReceipt::new(
            EnrichmentProjectionDisposition::Applied,
        ))
    }

    async fn mark_run_failed(
        &self,
        command: &MarkEnrichmentRunFailedCommand,
    ) -> Result<EnrichmentProjectionReceipt, Neo4jAdapterError> {
        let row = self
            .single_row(
                failure_preflight_query(command),
                "read enrichment failure state",
            )
            .await?;
        if !read_bool(&row, "exists")? {
            return Err(Neo4jAdapterError::EnrichmentTransition {
                transition: "mark missing run failed",
            });
        }
        if read_bool(&row, "identical")? {
            return Ok(EnrichmentProjectionReceipt::new(
                EnrichmentProjectionDisposition::Unchanged,
            ));
        }
        if read_bool(&row, "failed")?
            && !(read_bool(&row, "retryable_failed")?
                && command.status() == EnrichmentFailureStatus::TerminalFailed)
        {
            return Err(Neo4jAdapterError::ConflictingEnrichment {
                object: "run failure",
            });
        }
        let row = self
            .single_row(mark_run_failed_query(command), "mark enrichment run failed")
            .await?;
        require_accepted(&row, "mark run failed")?;
        Ok(EnrichmentProjectionReceipt::new(
            EnrichmentProjectionDisposition::Applied,
        ))
    }

    async fn single_row(
        &self,
        statement: Query,
        operation: &'static str,
    ) -> Result<Row, Neo4jAdapterError> {
        let mut rows = self
            .graph
            .execute(statement)
            .await
            .map_err(|source| Neo4jAdapterError::Operation { operation, source })?;
        rows.next()
            .await
            .map_err(|source| Neo4jAdapterError::Operation { operation, source })?
            .ok_or(Neo4jAdapterError::InvalidReadResult {
                field: "enrichment query row",
            })
    }
}

async fn run_chunk_transaction(
    transaction: &mut neo4rs::Txn,
    start: Query,
    statements: Vec<Query>,
    receipt: Query,
) -> Result<(), Neo4jAdapterError> {
    transaction
        .run(start)
        .await
        .map_err(|source| Neo4jAdapterError::Operation {
            operation: "start enrichment chunk projection",
            source,
        })?;
    transaction
        .run_queries(statements)
        .await
        .map_err(|source| Neo4jAdapterError::Operation {
            operation: "project enrichment chunk",
            source,
        })?;
    let mut rows =
        transaction
            .execute(receipt)
            .await
            .map_err(|source| Neo4jAdapterError::Operation {
                operation: "validate enrichment chunk",
                source,
            })?;
    let row = rows
        .next(&mut *transaction)
        .await
        .map_err(|source| Neo4jAdapterError::Operation {
            operation: "read enrichment chunk validation",
            source,
        })?
        .ok_or(Neo4jAdapterError::EnrichmentTransition {
            transition: "project chunk",
        })?;
    require_accepted(&row, "project chunk")
}

async fn run_claimed_chunk_transaction(
    transaction: &mut neo4rs::Txn,
    start: Query,
    statements: Vec<Query>,
    receipt: Query,
    consume_lease: Query,
) -> Result<(), Neo4jAdapterError> {
    require_transaction_accepted(
        transaction,
        start,
        "validate claimed enrichment chunk lease",
        "project claimed chunk",
    )
    .await?;
    transaction
        .run_queries(statements)
        .await
        .map_err(|source| Neo4jAdapterError::Operation {
            operation: "project claimed enrichment chunk",
            source,
        })?;
    require_transaction_accepted(
        transaction,
        receipt,
        "validate claimed enrichment chunk",
        "project claimed chunk",
    )
    .await?;
    require_transaction_accepted(
        transaction,
        consume_lease,
        "consume committed enrichment chunk lease",
        "consume claimed chunk lease",
    )
    .await
}

async fn require_transaction_accepted(
    transaction: &mut neo4rs::Txn,
    statement: Query,
    operation: &'static str,
    transition: &'static str,
) -> Result<(), Neo4jAdapterError> {
    let mut rows = transaction
        .execute(statement)
        .await
        .map_err(|source| Neo4jAdapterError::Operation { operation, source })?;
    let row = rows
        .next(&mut *transaction)
        .await
        .map_err(|source| Neo4jAdapterError::Operation { operation, source })?
        .ok_or(Neo4jAdapterError::EnrichmentTransition { transition })?;
    require_accepted(&row, transition)
}

#[async_trait]
impl EnrichmentReader for Neo4jAdapter {
    type Error = Neo4jAdapterError;

    async fn selected_enrichment(
        &self,
        request: &EnrichmentQuery,
    ) -> Result<EnrichmentLookup, Self::Error> {
        let row = self
            .single_row(selected_run_query(request), "read selected enrichment")
            .await?;
        if !read_bool(&row, "session_exists")? {
            return Ok(EnrichmentLookup::Unavailable(
                EnrichmentUnavailableReason::SessionNotFound,
            ));
        }
        if !read_bool(&row, "source_exists")? {
            return Ok(EnrichmentLookup::Unavailable(
                EnrichmentUnavailableReason::VerifiedSourceNotFound,
            ));
        }
        if !read_bool(&row, "selected")? {
            return Ok(EnrichmentLookup::Unavailable(
                EnrichmentUnavailableReason::NoCompletedSelection,
            ));
        }
        let run = read_run_spec(request, &row)?;
        let run_key = enrichment_run_key(&EnrichmentRunRef::new(
            request.namespace().clone(),
            run.source_digest(),
            run.fingerprint(),
        ));
        let spans = self.read_spans(&run_key, run.source_digest()).await?;
        let episodes = self.read_episodes(&run_key).await?;
        let entities = self.read_entities(&run_key).await?;
        let claims = self.read_claims(&run_key).await?;
        let relations = self.read_relations(&run_key).await?;
        Ok(EnrichmentLookup::Selected(Box::new(
            SelectedEnrichment::new(run, spans, episodes, entities, claims, relations),
        )))
    }

    async fn enrichment_chunk_checkpoint(
        &self,
        request: &EnrichmentChunkCheckpointQuery,
    ) -> Result<EnrichmentChunkCheckpoint, Self::Error> {
        let row = self
            .single_row(
                chunk_checkpoint_query(request),
                "read enrichment chunk checkpoint",
            )
            .await?;
        if !read_bool(&row, "exists")? {
            return Ok(EnrichmentChunkCheckpoint::Required);
        }
        if !read_bool(&row, "valid")? {
            return Err(Neo4jAdapterError::InvalidReadResult {
                field: "enrichment chunk checkpoint identity",
            });
        }
        Ok(EnrichmentChunkCheckpoint::Committed(
            CommittedEnrichmentChunk::new(
                request.chunk_id(),
                harness_graph_graph_port::EnrichmentOutputDigest::parse_hex(&read_property::<
                    String,
                >(
                    &row,
                    "output_digest",
                )?)?,
                TokenCount::new(read_u64(&row, "input_tokens")?),
                TokenCount::new(read_u64(&row, "output_tokens")?),
            ),
        ))
    }

    async fn enrichment_run_lifecycle(
        &self,
        request: &EnrichmentRunLifecycleQuery,
    ) -> Result<EnrichmentRunLifecycle, Self::Error> {
        let row = self
            .single_row(
                run_lifecycle_query(request),
                "read enrichment run lifecycle",
            )
            .await?;
        if !read_bool(&row, "exists")? {
            return Ok(EnrichmentRunLifecycle::Absent);
        }
        if !read_bool(&row, "valid")? {
            return Err(Neo4jAdapterError::InvalidReadResult {
                field: "enrichment run lifecycle identity",
            });
        }
        match read_property::<String>(&row, "status")?.as_str() {
            "planned" | "projecting" | "retryable_failed" => Ok(EnrichmentRunLifecycle::Resumable),
            "terminal_failed" => Ok(EnrichmentRunLifecycle::TerminalFailed),
            "completed" => Ok(EnrichmentRunLifecycle::Completed),
            _ => Err(Neo4jAdapterError::InvalidReadResult {
                field: "enrichment run lifecycle status",
            }),
        }
    }
}

impl Neo4jAdapter {
    async fn read_spans(
        &self,
        run_key: &str,
        source_digest: SourceDigest,
    ) -> Result<TranscriptSpans, Neo4jAdapterError> {
        let rows = self
            .collect_rows(
                query(SPANS_QUERY).param("run_key", run_key),
                "read enrichment spans",
            )
            .await?;
        let mut spans = Vec::with_capacity(rows.len());
        for row in rows {
            let sequence = read_sequence(&row, "sequence")?;
            let observation_id = ObservationId::from_source(source_digest, sequence);
            let stored_observation: String = read_property(&row, "observation_id")?;
            if stored_observation != observation_id.as_str() {
                return Err(Neo4jAdapterError::InvalidReadResult {
                    field: "span observation identity",
                });
            }
            spans.push(TranscriptSpanProjection::new(
                TranscriptSpanId::parse_hex(&read_property::<String>(&row, "span_id")?)?,
                observation_id,
                sequence,
                TranscriptField::parse(&read_property::<String>(&row, "field")?)?,
                TranscriptFieldOrdinal::new(read_u32(&row, "field_ordinal")?),
                TranscriptPartIndex::new(read_u32(&row, "part_index")?),
                TranscriptRole::parse(&read_property::<String>(&row, "role")?)?,
                TranscriptByteCount::new(read_u64(&row, "byte_count")?),
                TokenCount::new(read_u64(&row, "token_count")?),
                PayloadDigest::parse_hex(&read_property::<String>(&row, "content_digest")?)?,
            ));
        }
        TranscriptSpans::new(spans).map_err(Neo4jAdapterError::from)
    }

    async fn read_episodes(&self, run_key: &str) -> Result<NarrativeEpisodes, Neo4jAdapterError> {
        let rows = self
            .collect_rows(
                query(EPISODES_QUERY).param("run_key", run_key),
                "read enrichment episodes",
            )
            .await?;
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            let ids: Vec<String> = read_property(&row, "span_ids")?;
            let spans = ids
                .into_iter()
                .map(|id| TranscriptSpanId::parse_hex(&id))
                .collect::<Result<Vec<_>, _>>()?;
            values.push(NarrativeEpisodeProjection::new(
                NarrativeEpisodeId::parse_hex(&read_property::<String>(&row, "episode_id")?)?,
                EpisodeOrdinal::new(read_u64(&row, "ordinal")?)?,
                NarrativeTitle::new(read_property::<String>(&row, "title")?)?,
                NarrativeSummary::new(read_property::<String>(&row, "summary")?)?,
                KnowledgeConfidence::parse(&read_property::<String>(&row, "confidence")?)?,
                EpistemicStatus::parse(&read_property::<String>(&row, "epistemic_status")?)?,
                SpanCitations::new(spans)?,
            ));
        }
        NarrativeEpisodes::new(values).map_err(Neo4jAdapterError::from)
    }

    async fn read_entities(&self, run_key: &str) -> Result<KnowledgeEntities, Neo4jAdapterError> {
        let rows = self
            .collect_rows(
                query(ENTITIES_QUERY).param("run_key", run_key),
                "read enrichment entities",
            )
            .await?;
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            values.push(KnowledgeEntityProjection::new(
                KnowledgeEntityId::parse_hex(&read_property::<String>(&row, "entity_id")?)?,
                KnowledgeEntityKind::parse(&read_property::<String>(&row, "kind")?)?,
                KnowledgeEntityName::new(read_property::<String>(&row, "name")?)?,
            ));
        }
        KnowledgeEntities::new(values).map_err(Neo4jAdapterError::from)
    }

    async fn read_claims(&self, run_key: &str) -> Result<KnowledgeClaims, Neo4jAdapterError> {
        let rows = self
            .collect_rows(
                query(CLAIMS_QUERY).param("run_key", run_key),
                "read enrichment claims",
            )
            .await?;
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            let entity_ids = read_property::<Vec<String>>(&row, "entity_ids")?
                .into_iter()
                .map(|id| KnowledgeEntityId::parse_hex(&id))
                .collect::<Result<Vec<_>, _>>()?;
            let subjects = match read_property::<String>(&row, "subject_scope")?.as_str() {
                "session_wide" if entity_ids.is_empty() => KnowledgeClaimSubjects::SessionWide,
                "entities" => KnowledgeClaimSubjects::entities(entity_ids)?,
                _ => {
                    return Err(Neo4jAdapterError::InvalidSemanticProperty {
                        field: "claim subject scope",
                    });
                }
            };
            let spans = read_property::<Vec<String>>(&row, "span_ids")?
                .into_iter()
                .map(|id| TranscriptSpanId::parse_hex(&id))
                .collect::<Result<Vec<_>, _>>()?;
            let observations = read_property::<Vec<String>>(&row, "observation_ids")?;
            let corroboration =
                if observations.is_empty() {
                    ObservationCorroboration::Unavailable
                } else {
                    // Observation identities are reconstructed by the graph projection boundary;
                    // selected reads only need their opaque persisted identities. The query joins
                    // real observation nodes, so use their source sequence through a follow-up-free
                    // typed representation derived below.
                    let mut resolved = Vec::with_capacity(observations.len());
                    for value in observations {
                        let sequence = value
                            .rsplit(':')
                            .next()
                            .ok_or(Neo4jAdapterError::InvalidReadResult {
                                field: "observation citation",
                            })?
                            .parse::<u64>()
                            .map_err(|_| Neo4jAdapterError::InvalidReadResult {
                                field: "observation citation",
                            })?;
                        if sequence == 0 {
                            return Err(Neo4jAdapterError::InvalidReadResult {
                                field: "observation citation",
                            });
                        }
                        let digest = value.split(':').next().ok_or(
                            Neo4jAdapterError::InvalidReadResult {
                                field: "observation citation",
                            },
                        )?;
                        resolved.push(ObservationId::from_source(
                            SourceDigest::parse_hex(digest)?,
                            RecordSequence::from_zero_based(sequence - 1),
                        ));
                    }
                    ObservationCorroboration::available(resolved)?
                };
            values.push(KnowledgeClaimProjection::new(
                KnowledgeClaimId::parse_hex(&read_property::<String>(&row, "claim_id")?)?,
                KnowledgeKind::parse(&read_property::<String>(&row, "kind")?)?,
                KnowledgeClaimTitle::new(read_property::<String>(&row, "title")?)?,
                KnowledgeStatement::new(read_property::<String>(&row, "statement")?)?,
                KnowledgeConfidence::parse(&read_property::<String>(&row, "confidence")?)?,
                EpistemicStatus::parse(&read_property::<String>(&row, "epistemic_status")?)?,
                subjects,
                SpanCitations::new(spans)?,
                corroboration,
            )?);
        }
        KnowledgeClaims::new(values).map_err(Neo4jAdapterError::from)
    }

    async fn read_relations(&self, run_key: &str) -> Result<KnowledgeRelations, Neo4jAdapterError> {
        let rows = self
            .collect_rows(
                query(RELATIONS_QUERY).param("run_key", run_key),
                "read enrichment relations",
            )
            .await?;
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            let spans = read_property::<Vec<String>>(&row, "span_ids")?
                .into_iter()
                .map(|id| TranscriptSpanId::parse_hex(&id))
                .collect::<Result<Vec<_>, _>>()?;
            values.push(KnowledgeRelationProjection::new(
                KnowledgeRelationId::parse_hex(&read_property::<String>(&row, "relation_id")?)?,
                KnowledgePredicate::parse(&read_property::<String>(&row, "predicate")?)?,
                KnowledgeEntityId::parse_hex(&read_property::<String>(&row, "subject_id")?)?,
                KnowledgeEntityId::parse_hex(&read_property::<String>(&row, "object_id")?)?,
                KnowledgeConfidence::parse(&read_property::<String>(&row, "confidence")?)?,
                EpistemicStatus::parse(&read_property::<String>(&row, "epistemic_status")?)?,
                SpanCitations::new(spans)?,
            )?);
        }
        KnowledgeRelations::new(values).map_err(Neo4jAdapterError::from)
    }

    async fn collect_rows(
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
}

fn run_preflight_query(run: &EnrichmentRunSpec) -> Result<Query, Neo4jAdapterError> {
    Ok(run_params(query(RUN_PREFLIGHT_QUERY), run)?.param(
        "run_key",
        enrichment_run_key_from_parts(run.namespace(), run.fingerprint()),
    ))
}

fn begin_run_query(run: &EnrichmentRunSpec) -> Result<Query, Neo4jAdapterError> {
    let source_digest = run.source_digest().to_hex();
    Ok(run_params(query(BEGIN_RUN_QUERY), run)?
        .param(
            "session_key",
            session_key(run.namespace().as_str(), &run.session_id().to_string()),
        )
        .param(
            "source_key",
            source_key(run.namespace().as_str(), &source_digest),
        )
        .param(
            "receipt_key",
            super::receipt_key(run.namespace().as_str(), &source_digest),
        )
        .param(
            "run_key",
            enrichment_run_key_from_parts(run.namespace(), run.fingerprint()),
        ))
}

fn run_params(mut statement: Query, run: &EnrichmentRunSpec) -> Result<Query, Neo4jAdapterError> {
    statement = statement
        .param("namespace", run.namespace().as_str())
        .param("run_id", run.run_id().to_hex())
        .param("source_digest", run.source_digest().to_hex())
        .param("fingerprint", run.fingerprint().to_hex())
        .param("provider", run.provider().as_str())
        .param("model", run.model().as_str())
        .param("prompt_version", run.prompt_version().as_str())
        .param(
            "disclosure_scope",
            run.audit_provenance().disclosure_scope().as_str(),
        )
        .param(
            "authorization_policy_digest",
            run.audit_provenance()
                .authorization_policy_digest()
                .to_hex(),
        )
        .param(
            "prompt_digest",
            run.audit_provenance().prompt_digest().to_hex(),
        )
        .param("schema_version", run.schema_version().as_str())
        .param("redaction_version", run.redaction_version().as_str())
        .param("chunking_version", run.chunking_version().as_str())
        .param(
            "expected_chunks",
            to_i64(run.expected_chunks().value(), "expected enrichment chunks")?,
        );
    Ok(statement)
}

fn chunk_preflight_query(command: &ProjectEnrichmentChunkCommand) -> Query {
    query(CHUNK_PREFLIGHT_QUERY)
        .param(
            "chunk_receipt_key",
            chunk_receipt_key(command.run(), command.projection().chunk_id()),
        )
        .param(
            "output_digest",
            command.projection().output_digest().to_hex(),
        )
        .param("namespace", command.run().namespace().as_str())
        .param("fingerprint", command.run().fingerprint().to_hex())
        .param("chunk_id", command.projection().chunk_id().to_hex())
}

fn claim_chunk_query(command: &ClaimEnrichmentChunkCommand) -> Query {
    let run = command.run();
    query(CLAIM_CHUNK_QUERY)
        .param("run_key", enrichment_run_key(run))
        .param("namespace", run.namespace().as_str())
        .param("source_digest", run.source_digest().to_hex())
        .param("fingerprint", run.fingerprint().to_hex())
        .param("lease_key", chunk_lease_key(run, command.chunk_id()))
        .param(
            "chunk_receipt_key",
            chunk_receipt_key(run, command.chunk_id()),
        )
        .param("chunk_id", command.chunk_id().to_hex())
        .param("owner", command.owner().to_hex())
        .param("lease_seconds", i64::from(command.duration().seconds()))
}

fn start_claimed_chunk_query(command: &ProjectClaimedEnrichmentChunkCommand) -> Query {
    claimed_chunk_query_params(query(START_CLAIMED_CHUNK_QUERY), command)
}

fn consume_chunk_lease_query(command: &ProjectClaimedEnrichmentChunkCommand) -> Query {
    claimed_chunk_query_params(query(CONSUME_CHUNK_LEASE_QUERY), command).param(
        "chunk_receipt_key",
        chunk_receipt_key(command.run(), command.lease().chunk_id()),
    )
}

fn claimed_chunk_query_params(
    statement: Query,
    command: &ProjectClaimedEnrichmentChunkCommand,
) -> Query {
    let run = command.run();
    statement
        .param("run_key", enrichment_run_key(run))
        .param("namespace", run.namespace().as_str())
        .param("source_digest", run.source_digest().to_hex())
        .param("fingerprint", run.fingerprint().to_hex())
        .param(
            "lease_key",
            chunk_lease_key(run, command.lease().chunk_id()),
        )
        .param("chunk_id", command.lease().chunk_id().to_hex())
        .param("owner", command.lease().owner().to_hex())
}

fn release_chunk_lease_query(command: &ReleaseEnrichmentChunkLeaseCommand) -> Query {
    let run = command.run();
    query(RELEASE_CHUNK_LEASE_QUERY)
        .param("run_key", enrichment_run_key(run))
        .param("namespace", run.namespace().as_str())
        .param("source_digest", run.source_digest().to_hex())
        .param("fingerprint", run.fingerprint().to_hex())
        .param(
            "lease_key",
            chunk_lease_key(run, command.lease().chunk_id()),
        )
        .param(
            "chunk_receipt_key",
            chunk_receipt_key(run, command.lease().chunk_id()),
        )
        .param("chunk_id", command.lease().chunk_id().to_hex())
        .param("owner", command.lease().owner().to_hex())
}

fn failure_preflight_query(command: &MarkEnrichmentRunFailedCommand) -> Query {
    query(FAILURE_PREFLIGHT_QUERY)
        .param("run_key", enrichment_run_key(command.run()))
        .param("failure_status", command.status().as_str())
        .param("failure_class", command.class().as_str())
}

fn mark_run_failed_query(command: &MarkEnrichmentRunFailedCommand) -> Query {
    query(MARK_RUN_FAILED_QUERY)
        .param("run_key", enrichment_run_key(command.run()))
        .param("failure_status", command.status().as_str())
        .param("failure_class", command.class().as_str())
}

fn start_chunk_query(run: &EnrichmentRunRef) -> Query {
    query(START_CHUNK_QUERY).param("run_key", enrichment_run_key(run))
}

fn chunk_queries(command: &ProjectEnrichmentChunkCommand) -> Result<Vec<Query>, Neo4jAdapterError> {
    let mut statements = Vec::new();
    let run = command.run();
    let chunk = command.projection();
    for span in chunk.spans().iter() {
        statements.push(span_query(run, chunk, span)?);
    }
    for episode in chunk.episodes().iter() {
        statements.push(episode_query(run, chunk, episode)?);
        for span in episode.spans().iter() {
            statements.push(episode_span_query(run, episode, *span));
        }
    }
    for entity in chunk.entities().iter() {
        statements.push(entity_query(run, chunk, entity));
    }
    for claim in chunk.claims().iter() {
        statements.push(claim_query(run, chunk, claim));
        for subject in claim.subjects().iter() {
            statements.push(claim_subject_query(run, claim, *subject));
        }
        for span in claim.spans().iter() {
            statements.push(claim_span_query(run, claim, *span));
        }
        for observation in claim.corroboration().iter() {
            statements.push(claim_observation_query(run, claim, observation));
        }
    }
    for relation in chunk.relations().iter() {
        statements.push(relation_query(run, chunk, relation));
        statements.push(relation_endpoints_query(run, relation));
        for span in relation.spans().iter() {
            statements.push(relation_span_query(run, relation, *span));
        }
    }
    Ok(statements)
}

fn span_query(
    run: &EnrichmentRunRef,
    chunk: &EnrichmentChunkProjection,
    span: &TranscriptSpanProjection,
) -> Result<Query, Neo4jAdapterError> {
    let fingerprint = run.fingerprint().to_hex();
    let chunk_id = chunk.chunk_id().to_hex();
    let source_digest = run.source_digest().to_hex();
    Ok(query(
        "MATCH (run:HGEnrichmentRun {key: $run_key})<-[:HAS_ENRICHMENT_RUN]-(src:HGSourceSnapshot {key: $source_key}) \
         MATCH (src)-[:CONTAINS]->(observation:HGObservation {key: $observation_key}) \
         WHERE run.status = 'projecting' AND observation.sequence = $sequence \
         MERGE (span:HGTranscriptSpan {key: $span_key}) \
         ON CREATE SET span.hg_namespace = $namespace, span.run_fingerprint = $fingerprint, \
           span.chunk_id = $chunk_id, span.span_id = $span_id, span.observation_id = $observation_id, \
           span.sequence = $sequence, span.field = $field, span.field_ordinal = $field_ordinal, \
           span.part_index = $part_index, span.role = $role, span.byte_count = $byte_count, span.token_count = $token_count, \
           span.content_digest = $content_digest \
         MERGE (run)-[:USED_SPAN]->(span) MERGE (span)-[:FROM_SOURCE]->(src) \
         MERGE (span)-[:MAPS_TO]->(observation)",
    )
    .param("run_key", enrichment_run_key(run))
    .param("source_key", source_key(run.namespace().as_str(), &source_digest))
    .param("observation_key", observation_key(run.namespace().as_str(), span.observation_id().as_str()))
    .param("span_key", span_key(run, span.id()))
    .param("namespace", run.namespace().as_str())
    .param("fingerprint", fingerprint)
    .param("chunk_id", chunk_id)
    .param("span_id", span.id().to_hex())
    .param("observation_id", span.observation_id().as_str())
    .param("sequence", to_i64(span.sequence().value(), "span sequence")?)
    .param("field", span.field().as_str())
    .param("field_ordinal", i64::from(span.field_ordinal().value()))
    .param("part_index", i64::from(span.part_index().value()))
    .param("role", span.role().as_str())
    .param("byte_count", to_i64(span.byte_count().value(), "span bytes")?)
    .param("token_count", to_i64(span.token_count().value(), "span tokens")?)
    .param("content_digest", span.content_digest().to_hex()))
}

fn episode_query(
    run: &EnrichmentRunRef,
    chunk: &EnrichmentChunkProjection,
    episode: &NarrativeEpisodeProjection,
) -> Result<Query, Neo4jAdapterError> {
    Ok(query(
        "MATCH (run:HGEnrichmentRun {key: $run_key, status: 'projecting'}) \
         MERGE (episode:HGNarrativeEpisode {key: $episode_key}) \
         ON CREATE SET episode.hg_namespace = $namespace, episode.run_fingerprint = $fingerprint, \
           episode.chunk_id = $chunk_id, episode.episode_id = $episode_id, episode.ordinal = $ordinal, \
           episode.title = $title, episode.summary = $summary, episode.confidence = $confidence, \
           episode.epistemic_status = $epistemic_status \
         MERGE (run)-[:PRODUCED_EPISODE]->(episode)",
    )
    .param("run_key", enrichment_run_key(run)).param("episode_key", episode_key(run, episode.id()))
    .param("namespace", run.namespace().as_str()).param("fingerprint", run.fingerprint().to_hex())
    .param("chunk_id", chunk.chunk_id().to_hex()).param("episode_id", episode.id().to_hex())
    .param("ordinal", to_i64(episode.ordinal().value(), "episode ordinal")?)
    .param("title", episode.title().as_str()).param("summary", episode.summary().as_str())
    .param("confidence", episode.confidence().as_str())
    .param("epistemic_status", episode.epistemic_status().as_str()))
}

fn episode_span_query(
    run: &EnrichmentRunRef,
    episode: &NarrativeEpisodeProjection,
    span: TranscriptSpanId,
) -> Query {
    query("MATCH (episode:HGNarrativeEpisode {key: $episode_key}) MATCH (span:HGTranscriptSpan {key: $span_key}) MERGE (episode)-[:SUPPORTED_BY]->(span)")
        .param("episode_key", episode_key(run, episode.id()))
        .param("span_key", span_key(run, span))
}

fn entity_query(
    run: &EnrichmentRunRef,
    chunk: &EnrichmentChunkProjection,
    entity: &KnowledgeEntityProjection,
) -> Query {
    query(ENTITY_PROJECTION_QUERY)
        .param("run_key", enrichment_run_key(run))
        .param("entity_key", entity_key(run, entity.id()))
        .param("namespace", run.namespace().as_str())
        .param("fingerprint", run.fingerprint().to_hex())
        .param("chunk_id", chunk.chunk_id().to_hex())
        .param("entity_id", entity.id().to_hex())
        .param("kind", entity.kind().as_str())
        .param("name", entity.name().as_str())
}

fn claim_query(
    run: &EnrichmentRunRef,
    chunk: &EnrichmentChunkProjection,
    claim: &KnowledgeClaimProjection,
) -> Query {
    query(
        "MATCH (run:HGEnrichmentRun {key: $run_key, status: 'projecting'}) \
         MERGE (claim:HGKnowledgeClaim {key: $claim_key}) \
         ON CREATE SET claim.hg_namespace = $namespace, claim.run_fingerprint = $fingerprint, \
           claim.chunk_id = $chunk_id, claim.claim_id = $claim_id, claim.kind = $kind, \
           claim.title = $title, claim.statement = $statement, claim.subject_scope = $subject_scope, \
           claim.confidence = $confidence, claim.epistemic_status = $epistemic_status \
         MERGE (run)-[:PRODUCED_CLAIM]->(claim)",
    ).param("run_key", enrichment_run_key(run)).param("claim_key", claim_key(run, claim.id()))
        .param("namespace", run.namespace().as_str()).param("fingerprint", run.fingerprint().to_hex())
        .param("chunk_id", chunk.chunk_id().to_hex()).param("claim_id", claim.id().to_hex())
        .param("kind", claim.kind().as_str()).param("title", claim.title().as_str())
        .param("statement", claim.statement().as_str()).param("subject_scope", claim.subjects().scope())
        .param("confidence", claim.confidence().as_str()).param("epistemic_status", claim.epistemic_status().as_str())
}

fn claim_subject_query(
    run: &EnrichmentRunRef,
    claim: &KnowledgeClaimProjection,
    subject: KnowledgeEntityId,
) -> Query {
    query("MATCH (claim:HGKnowledgeClaim {key: $claim_key}) MATCH (entity:HGKnowledgeEntity {key: $entity_key}) MERGE (claim)-[:ABOUT]->(entity)")
        .param("claim_key", claim_key(run, claim.id())).param("entity_key", entity_key(run, subject))
}

fn claim_span_query(
    run: &EnrichmentRunRef,
    claim: &KnowledgeClaimProjection,
    span: TranscriptSpanId,
) -> Query {
    query("MATCH (claim:HGKnowledgeClaim {key: $claim_key}) MATCH (span:HGTranscriptSpan {key: $span_key}) MERGE (claim)-[:SUPPORTED_BY]->(span)")
        .param("claim_key", claim_key(run, claim.id())).param("span_key", span_key(run, span))
}

fn claim_observation_query(
    run: &EnrichmentRunRef,
    claim: &KnowledgeClaimProjection,
    observation: &ObservationId,
) -> Query {
    query("MATCH (claim:HGKnowledgeClaim {key: $claim_key}) MATCH (observation:HGObservation {key: $observation_key}) MERGE (claim)-[:CORROBORATED_BY]->(observation)")
        .param("claim_key", claim_key(run, claim.id()))
        .param("observation_key", observation_key(run.namespace().as_str(), observation.as_str()))
}

fn relation_query(
    run: &EnrichmentRunRef,
    chunk: &EnrichmentChunkProjection,
    relation: &KnowledgeRelationProjection,
) -> Query {
    query(
        "MATCH (run:HGEnrichmentRun {key: $run_key, status: 'projecting'}) \
         MERGE (relation:HGKnowledgeRelation {key: $relation_key}) \
         ON CREATE SET relation.hg_namespace = $namespace, relation.run_fingerprint = $fingerprint, \
           relation.chunk_id = $chunk_id, relation.relation_id = $relation_id, relation.predicate = $predicate, \
           relation.confidence = $confidence, relation.epistemic_status = $epistemic_status \
         MERGE (run)-[:PRODUCED_RELATION]->(relation)",
    ).param("run_key", enrichment_run_key(run)).param("relation_key", relation_key(run, relation.id()))
        .param("namespace", run.namespace().as_str()).param("fingerprint", run.fingerprint().to_hex())
        .param("chunk_id", chunk.chunk_id().to_hex()).param("relation_id", relation.id().to_hex())
        .param("predicate", relation.predicate().as_str()).param("confidence", relation.confidence().as_str())
        .param("epistemic_status", relation.epistemic_status().as_str())
}

fn relation_endpoints_query(
    run: &EnrichmentRunRef,
    relation: &KnowledgeRelationProjection,
) -> Query {
    query(
        "MATCH (relation:HGKnowledgeRelation {key: $relation_key}) \
         MATCH (subject:HGKnowledgeEntity {key: $subject_key}) MATCH (object:HGKnowledgeEntity {key: $object_key}) \
         MERGE (relation)-[:SUBJECT]->(subject) MERGE (relation)-[:OBJECT]->(object)",
    ).param("relation_key", relation_key(run, relation.id()))
        .param("subject_key", entity_key(run, relation.subject())).param("object_key", entity_key(run, relation.object()))
}

fn relation_span_query(
    run: &EnrichmentRunRef,
    relation: &KnowledgeRelationProjection,
    span: TranscriptSpanId,
) -> Query {
    query("MATCH (relation:HGKnowledgeRelation {key: $relation_key}) MATCH (span:HGTranscriptSpan {key: $span_key}) MERGE (relation)-[:SUPPORTED_BY]->(span)")
        .param("relation_key", relation_key(run, relation.id())).param("span_key", span_key(run, span))
}

const CHUNK_RECEIPT_QUERY: &str = "MATCH (run:HGEnrichmentRun {key: $run_key, status: 'projecting'}) \
     OPTIONAL MATCH (run)-[:USED_SPAN]->(span:HGTranscriptSpan {chunk_id: $chunk_id}) \
     WITH run, count(DISTINCT span) AS span_count \
     OPTIONAL MATCH (mapped:HGTranscriptSpan {hg_namespace: $namespace, run_fingerprint: $fingerprint, chunk_id: $chunk_id})-[map_rel:MAPS_TO]->(:HGObservation) \
     WITH run, span_count, count(DISTINCT map_rel) AS map_count \
     OPTIONAL MATCH (run)-[:PRODUCED_EPISODE]->(episode:HGNarrativeEpisode {chunk_id: $chunk_id}) \
     OPTIONAL MATCH (episode)-[episode_span_rel:SUPPORTED_BY]->(:HGTranscriptSpan) \
     WITH run, span_count, map_count, count(DISTINCT episode) AS episode_count, count(DISTINCT episode_span_rel) AS episode_citations \
     OPTIONAL MATCH (run)-[entity_membership:PRODUCED_ENTITY {chunk_id: $chunk_id}]->(:HGKnowledgeEntity) \
     WITH run, span_count, map_count, episode_count, episode_citations, count(DISTINCT entity_membership) AS entity_count \
     OPTIONAL MATCH (run)-[:PRODUCED_CLAIM]->(claim:HGKnowledgeClaim {chunk_id: $chunk_id}) \
     OPTIONAL MATCH (claim)-[about_rel:ABOUT]->(:HGKnowledgeEntity) \
     OPTIONAL MATCH (claim)-[claim_span_rel:SUPPORTED_BY]->(:HGTranscriptSpan) \
     OPTIONAL MATCH (claim)-[corroboration_rel:CORROBORATED_BY]->(:HGObservation) \
     WITH run, span_count, map_count, episode_count, episode_citations, entity_count, \
          count(DISTINCT claim) AS claim_count, count(DISTINCT about_rel) AS about_count, \
          count(DISTINCT claim_span_rel) AS claim_span_count, count(DISTINCT corroboration_rel) AS corroboration_count \
     OPTIONAL MATCH (run)-[:PRODUCED_RELATION]->(relation:HGKnowledgeRelation {chunk_id: $chunk_id}) \
     OPTIONAL MATCH (relation)-[subject_rel:SUBJECT]->(:HGKnowledgeEntity) \
     OPTIONAL MATCH (relation)-[object_rel:OBJECT]->(:HGKnowledgeEntity) \
     OPTIONAL MATCH (relation)-[relation_span_rel:SUPPORTED_BY]->(:HGTranscriptSpan) \
     WITH run, span_count, map_count, episode_count, episode_citations, entity_count, claim_count, about_count, claim_span_count, corroboration_count, \
          count(DISTINCT relation) AS relation_count, count(DISTINCT subject_rel) AS subject_count, \
          count(DISTINCT object_rel) AS object_count, count(DISTINCT relation_span_rel) AS relation_span_count \
     WHERE span_count = $span_count AND map_count = $span_count \
       AND episode_count = $episode_count AND episode_citations = $episode_citations \
       AND entity_count = $entity_count AND claim_count = $claim_count AND about_count = $claim_subject_count \
       AND claim_span_count = $claim_span_count AND corroboration_count = $corroboration_count \
       AND relation_count = $relation_count AND subject_count = $relation_count \
       AND object_count = $relation_count AND relation_span_count = $relation_span_count \
     MERGE (receipt:HGEnrichmentChunkReceipt {key: $chunk_receipt_key}) \
     ON CREATE SET receipt.hg_namespace = $namespace, receipt.run_fingerprint = $fingerprint, \
       receipt.chunk_id = $chunk_id, receipt.output_digest = $output_digest, \
       receipt.input_tokens = $input_tokens, receipt.output_tokens = $output_tokens, receipt.completed_at = datetime() \
     MERGE (run)-[:HAS_CHUNK_RECEIPT]->(receipt) RETURN count(receipt) AS accepted";

fn chunk_receipt_query(
    command: &ProjectEnrichmentChunkCommand,
) -> Result<Query, Neo4jAdapterError> {
    let chunk = command.projection();
    let expected_episode_citations: u64 = chunk
        .episodes()
        .iter()
        .map(|value| value.spans().count().value())
        .sum();
    let expected_claim_spans: u64 = chunk
        .claims()
        .iter()
        .map(|value| value.spans().count().value())
        .sum();
    let expected_claim_subjects: u64 = chunk
        .claims()
        .iter()
        .map(|value| value.subjects().iter().count() as u64)
        .sum();
    let expected_claim_observations: u64 = chunk
        .claims()
        .iter()
        .map(|value| value.corroboration().iter().count() as u64)
        .sum();
    let expected_relation_spans: u64 = chunk
        .relations()
        .iter()
        .map(|value| value.spans().count().value())
        .sum();
    Ok(query(CHUNK_RECEIPT_QUERY)
        .param("run_key", enrichment_run_key(command.run()))
        .param("namespace", command.run().namespace().as_str())
        .param("fingerprint", command.run().fingerprint().to_hex())
        .param("chunk_id", chunk.chunk_id().to_hex())
        .param(
            "span_count",
            to_i64(chunk.spans().count().value(), "chunk spans")?,
        )
        .param(
            "episode_count",
            to_i64(chunk.episodes().count().value(), "chunk episodes")?,
        )
        .param(
            "episode_citations",
            to_i64(expected_episode_citations, "episode citations")?,
        )
        .param(
            "entity_count",
            to_i64(chunk.entities().count().value(), "chunk entities")?,
        )
        .param(
            "claim_count",
            to_i64(chunk.claims().count().value(), "chunk claims")?,
        )
        .param(
            "claim_subject_count",
            to_i64(expected_claim_subjects, "claim subjects")?,
        )
        .param(
            "claim_span_count",
            to_i64(expected_claim_spans, "claim spans")?,
        )
        .param(
            "corroboration_count",
            to_i64(expected_claim_observations, "claim observations")?,
        )
        .param(
            "relation_count",
            to_i64(chunk.relations().count().value(), "chunk relations")?,
        )
        .param(
            "relation_span_count",
            to_i64(expected_relation_spans, "relation spans")?,
        )
        .param(
            "chunk_receipt_key",
            chunk_receipt_key(command.run(), chunk.chunk_id()),
        )
        .param("output_digest", chunk.output_digest().to_hex())
        .param(
            "input_tokens",
            to_i64(chunk.input_tokens().value(), "chunk input tokens")?,
        )
        .param(
            "output_tokens",
            to_i64(chunk.output_tokens().value(), "chunk output tokens")?,
        ))
}

fn complete_run_query(command: &CompleteEnrichmentRunCommand) -> Query {
    let source_digest = command.run().source_digest().to_hex();
    query(COMPLETE_RUN_QUERY)
        .param("run_key", enrichment_run_key(command.run()))
        .param(
            "source_key",
            source_key(command.run().namespace().as_str(), &source_digest),
        )
        .param(
            "view_key",
            enrichment_view_key(command.run().namespace(), command.run().source_digest()),
        )
        .param("namespace", command.run().namespace().as_str())
        .param("source_digest", source_digest)
}

fn selected_run_query(request: &EnrichmentQuery) -> Query {
    query(SELECTED_RUN_QUERY)
        .param(
            "session_key",
            session_key(
                request.namespace().as_str(),
                &request.session_id().to_string(),
            ),
        )
        .param("namespace", request.namespace().as_str())
}

fn chunk_checkpoint_query(request: &EnrichmentChunkCheckpointQuery) -> Query {
    query(CHUNK_CHECKPOINT_QUERY)
        .param("run_key", enrichment_run_key(request.run()))
        .param(
            "chunk_receipt_key",
            chunk_receipt_key(request.run(), request.chunk_id()),
        )
        .param("namespace", request.run().namespace().as_str())
        .param("fingerprint", request.run().fingerprint().to_hex())
        .param("chunk_id", request.chunk_id().to_hex())
}

fn run_lifecycle_query(request: &EnrichmentRunLifecycleQuery) -> Query {
    query(RUN_LIFECYCLE_QUERY)
        .param("run_key", enrichment_run_key(request.run()))
        .param("namespace", request.run().namespace().as_str())
        .param("source_digest", request.run().source_digest().to_hex())
        .param("fingerprint", request.run().fingerprint().to_hex())
}

fn read_run_spec(
    request: &EnrichmentQuery,
    row: &Row,
) -> Result<EnrichmentRunSpec, Neo4jAdapterError> {
    let provider: String = read_property(row, "provider")?;
    if provider != EnrichmentProvider::Mistral.as_str() {
        return Err(Neo4jAdapterError::InvalidReadResult {
            field: "enrichment provider",
        });
    }
    Ok(EnrichmentRunSpec::new(
        request.namespace().clone(),
        request.session_id(),
        SourceDigest::parse_hex(&read_property::<String>(row, "source_digest")?)?,
        EnrichmentRunId::parse_hex(&read_property::<String>(row, "run_id")?)?,
        harness_graph_graph_port::EnrichmentFingerprint::parse_hex(&read_property::<String>(
            row,
            "fingerprint",
        )?)?,
        EnrichmentProvider::Mistral,
        EnrichmentModelName::new(read_property::<String>(row, "model")?)?,
        PromptVersion::new(read_property::<String>(row, "prompt_version")?)?,
        EnrichmentRunAuditProvenance::new(
            harness_graph_graph_port::EnrichmentDisclosureScope::parse(&read_property::<String>(
                row,
                "disclosure_scope",
            )?)?,
            harness_graph_graph_port::EnrichmentAuthorizationPolicyDigest::parse_hex(
                &read_property::<String>(row, "authorization_policy_digest")?,
            )?,
            harness_graph_graph_port::EnrichmentPromptDigest::parse_hex(&read_property::<String>(
                row,
                "prompt_digest",
            )?)?,
        ),
        EnrichmentSchemaVersion::new(read_property::<String>(row, "schema_version")?)?,
        RedactionPolicyVersion::new(read_property::<String>(row, "redaction_version")?)?,
        ChunkingPolicyVersion::new(read_property::<String>(row, "chunking_version")?)?,
        EnrichmentChunkCount::new(read_u64(row, "expected_chunks")?)?,
    ))
}

fn read_property<T: for<'de> serde::Deserialize<'de>>(
    row: &Row,
    field: &'static str,
) -> Result<T, Neo4jAdapterError> {
    row.get(field)
        .map_err(|_| Neo4jAdapterError::InvalidReadResult { field })
}

fn read_bool(row: &Row, field: &'static str) -> Result<bool, Neo4jAdapterError> {
    read_property(row, field)
}

fn read_u64(row: &Row, field: &'static str) -> Result<u64, Neo4jAdapterError> {
    let value: i64 = read_property(row, field)?;
    u64::try_from(value).map_err(|_| Neo4jAdapterError::IntegerRange { field })
}

fn read_u32(row: &Row, field: &'static str) -> Result<u32, Neo4jAdapterError> {
    let value = read_u64(row, field)?;
    u32::try_from(value).map_err(|_| Neo4jAdapterError::IntegerRange { field })
}

fn committed_from_row(
    row: &Row,
    chunk_id: harness_graph_graph_port::EnrichmentChunkId,
) -> Result<CommittedEnrichmentChunk, Neo4jAdapterError> {
    Ok(CommittedEnrichmentChunk::new(
        chunk_id,
        harness_graph_graph_port::EnrichmentOutputDigest::parse_hex(&read_property::<String>(
            row,
            "output_digest",
        )?)?,
        TokenCount::new(read_u64(row, "input_tokens")?),
        TokenCount::new(read_u64(row, "output_tokens")?),
    ))
}

fn read_sequence(row: &Row, field: &'static str) -> Result<RecordSequence, Neo4jAdapterError> {
    let value = read_u64(row, field)?;
    if value == 0 {
        return Err(Neo4jAdapterError::InvalidReadResult { field });
    }
    Ok(RecordSequence::from_zero_based(value - 1))
}

fn require_accepted(row: &Row, transition: &'static str) -> Result<(), Neo4jAdapterError> {
    if read_u64(row, "accepted")? == 1 {
        Ok(())
    } else {
        Err(Neo4jAdapterError::EnrichmentTransition { transition })
    }
}

fn enrichment_run_key(run: &EnrichmentRunRef) -> String {
    enrichment_run_key_from_parts(run.namespace(), run.fingerprint())
}
fn enrichment_run_key_from_parts(
    namespace: &GraphNamespace,
    fingerprint: harness_graph_graph_port::EnrichmentFingerprint,
) -> String {
    format!(
        "{}:enrichment-run:{}",
        namespace.as_str(),
        fingerprint.to_hex()
    )
}
fn chunk_receipt_key(
    run: &EnrichmentRunRef,
    chunk: harness_graph_graph_port::EnrichmentChunkId,
) -> String {
    format!(
        "{}:enrichment-chunk:{}:{}",
        run.namespace().as_str(),
        run.fingerprint().to_hex(),
        chunk.to_hex()
    )
}
fn chunk_lease_key(
    run: &EnrichmentRunRef,
    chunk: harness_graph_graph_port::EnrichmentChunkId,
) -> String {
    format!(
        "{}:enrichment-chunk-lease:{}:{}",
        run.namespace().as_str(),
        run.fingerprint().to_hex(),
        chunk.to_hex()
    )
}
fn span_key(run: &EnrichmentRunRef, id: TranscriptSpanId) -> String {
    format!(
        "{}:transcript-span:{}:{}",
        run.namespace().as_str(),
        run.fingerprint().to_hex(),
        id.to_hex()
    )
}
fn episode_key(run: &EnrichmentRunRef, id: NarrativeEpisodeId) -> String {
    format!(
        "{}:narrative-episode:{}:{}",
        run.namespace().as_str(),
        run.fingerprint().to_hex(),
        id.to_hex()
    )
}
fn entity_key(run: &EnrichmentRunRef, id: KnowledgeEntityId) -> String {
    format!(
        "{}:knowledge-entity:{}:{}",
        run.namespace().as_str(),
        run.fingerprint().to_hex(),
        id.to_hex()
    )
}
fn claim_key(run: &EnrichmentRunRef, id: KnowledgeClaimId) -> String {
    format!(
        "{}:knowledge-claim:{}:{}",
        run.namespace().as_str(),
        run.fingerprint().to_hex(),
        id.to_hex()
    )
}
fn relation_key(run: &EnrichmentRunRef, id: KnowledgeRelationId) -> String {
    format!(
        "{}:knowledge-relation:{}:{}",
        run.namespace().as_str(),
        run.fingerprint().to_hex(),
        id.to_hex()
    )
}
fn enrichment_view_key(namespace: &GraphNamespace, source: SourceDigest) -> String {
    format!("{}:enrichment-view:{}", namespace.as_str(), source.to_hex())
}

#[cfg(test)]
mod tests {
    use harness_graph_domain::{GraphNamespace, RecordCount, SessionId};
    use harness_graph_graph_port::{
        EnrichmentChunkId, EnrichmentFailureClass, EnrichmentFingerprint, EnrichmentGraphCommand,
        EnrichmentInvocationOwner, EnrichmentLeaseDuration, EnrichmentOutputDigest,
        EnrichmentProjector, EnrichmentQuery, EnrichmentReader, GraphProjector,
    };

    use super::*;

    #[test]
    fn enrichment_queries_never_set_properties_on_authoritative_nodes() {
        for statement in [
            BEGIN_RUN_QUERY,
            START_CHUNK_QUERY,
            COMPLETE_RUN_QUERY,
            MARK_RUN_FAILED_QUERY,
        ] {
            assert!(!statement.contains("SET src."));
            assert!(!statement.contains("SET session."));
            assert!(!statement.contains("SET receipt."));
        }
    }

    #[test]
    fn chunk_validation_is_namespace_scoped() {
        assert!(CHUNK_RECEIPT_QUERY.contains("mapped:HGTranscriptSpan {hg_namespace: $namespace"));
    }

    #[test]
    fn semantic_entities_are_deduplicated_with_explicit_chunk_membership() {
        assert!(
            ENTITY_PROJECTION_QUERY.contains("PRODUCED_ENTITY {chunk_id: $chunk_id}"),
            "each chunk must retain membership without duplicating its semantic entity"
        );
        assert!(
            CHUNK_RECEIPT_QUERY.contains("entity_membership:PRODUCED_ENTITY {chunk_id: $chunk_id}"),
            "receipt validation must count chunk membership rather than node ownership"
        );
        assert!(ENTITIES_QUERY.contains("RETURN DISTINCT entity.entity_id"));
        assert!(!ENTITY_PROJECTION_QUERY.contains("entity.chunk_id = $chunk_id"));
    }

    #[test]
    fn failed_runs_never_move_selected_view() {
        assert!(FAILURE_PREFLIGHT_QUERY.contains("retryable_failed"));
        assert!(FAILURE_PREFLIGHT_QUERY.contains("terminal_failed"));
        assert!(!MARK_RUN_FAILED_QUERY.contains("SELECTS"));
        assert!(SELECTED_RUN_QUERY.contains("HGEnrichmentRun {status: 'completed'}"));
    }

    #[test]
    fn run_audit_provenance_is_persisted_identity_checked_and_selected() {
        for field in [
            "disclosure_scope",
            "authorization_policy_digest",
            "prompt_digest",
        ] {
            let parameter = format!("${field}");
            assert!(BEGIN_RUN_QUERY.contains(&format!("run.{field} = {parameter}")));
            assert!(RUN_PREFLIGHT_QUERY.contains(&format!("run.{field} = {parameter}")));
            assert!(SELECTED_RUN_QUERY.contains(&format!("run.{field}")));
        }
    }

    #[test]
    fn checkpoint_lookup_validates_full_run_identity() {
        for fragment in [
            "checkpoint IS NOT NULL",
            "receipt.hg_namespace = $namespace",
            "receipt.run_fingerprint = $fingerprint",
            "receipt.chunk_id = $chunk_id",
        ] {
            assert!(CHUNK_CHECKPOINT_QUERY.contains(fragment));
        }
        assert!(CHUNK_PREFLIGHT_QUERY.contains("receipt.hg_namespace = $namespace"));
        assert!(CHUNK_PREFLIGHT_QUERY.contains("receipt.run_fingerprint = $fingerprint"));
    }

    #[test]
    fn paid_call_claim_is_atomic_owner_bound_and_expiring() {
        for fragment in [
            "MERGE (lease:HGEnrichmentChunkLease {key: $lease_key})",
            "run.source_digest = $source_digest",
            "lease.run_fingerprint = $fingerprint",
            "lease.chunk_id = $chunk_id",
            "lease.owner = $owner",
            "lease.expires_at <= datetime()",
            "duration({seconds: $lease_seconds})",
            "receipt IS NULL",
            "CASE WHEN receipt IS NOT NULL",
            "DELETE held, lease",
        ] {
            assert!(CLAIM_CHUNK_QUERY.contains(fragment));
        }
        assert!(START_CLAIMED_CHUNK_QUERY.contains("lease.owner = $owner"));
        assert!(START_CLAIMED_CHUNK_QUERY.contains("lease.expires_at > datetime()"));
        assert!(START_CLAIMED_CHUNK_QUERY.contains("lease.projecting_at = datetime()"));
        assert!(CONSUME_CHUNK_LEASE_QUERY.contains("DELETE held, lease"));
    }

    #[test]
    fn paid_call_release_cannot_delete_another_owner_lease() {
        assert!(RELEASE_CHUNK_LEASE_QUERY.contains("lease.owner = $owner"));
        assert!(RELEASE_CHUNK_LEASE_QUERY.contains("CASE WHEN released"));
        assert!(RELEASE_CHUNK_LEASE_QUERY.contains("DELETE held, lease"));
        assert!(RELEASE_CHUNK_LEASE_QUERY.contains("receipt IS NULL"));
    }

    #[test]
    fn run_lifecycle_lookup_validates_exact_source_and_fingerprint() {
        for fragment in [
            "run.hg_namespace = $namespace",
            "run.source_digest = $source_digest",
            "run.fingerprint = $fingerprint",
        ] {
            assert!(RUN_LIFECYCLE_QUERY.contains(fragment));
        }
    }

    #[test]
    fn selected_read_requires_completed_run_and_latest_verified_source() {
        assert!(SELECTED_RUN_QUERY.contains("HGIngestionReceipt {status: 'completed'}"));
        assert!(SELECTED_RUN_QUERY.contains("HGEnrichmentRun {status: 'completed'}"));
        assert!(SELECTED_RUN_QUERY.contains("receipt.completed_at DESC"));
    }

    #[test]
    fn enrichment_schema_is_overlay_only_and_idempotent() {
        assert!(
            ENRICHMENT_CONSTRAINTS
                .iter()
                .all(|statement| statement.contains("IF NOT EXISTS"))
        );
        assert!(ENRICHMENT_CONSTRAINTS.iter().all(|statement| {
            !statement.contains("HGSourceSnapshot")
                && !statement.contains("HGObservation")
                && !statement.contains("HGActivity")
        }));
    }

    #[tokio::test]
    #[ignore = "requires configured real Neo4j"]
    async fn live_enrichment_overlay_preserves_base_and_selects_only_completed_runs()
    -> Result<(), Box<dyn std::error::Error>> {
        let _dotenv = dotenvy::dotenv().ok();
        let adapter = crate::tests::connect_from_environment().await?;
        let contender = crate::tests::connect_from_environment().await?;
        adapter.health().await?;
        contender.health().await?;
        adapter.ensure_schema().await?;
        adapter.ensure_enrichment_schema().await?;
        let namespace =
            GraphNamespace::new(format!("enrichment_e2e_{}", uuid::Uuid::now_v7().simple()))?;
        let result = run_live_overlay_scenario(&adapter, &contender, &namespace).await;
        let cleanup = adapter.purge_namespace(&namespace).await;
        cleanup?;
        result
    }

    #[tokio::test]
    #[ignore = "requires configured real Neo4j"]
    async fn live_two_chunk_run_checkpoints_one_repeated_semantic_entity()
    -> Result<(), Box<dyn std::error::Error>> {
        let _dotenv = dotenvy::dotenv().ok();
        let adapter = crate::tests::connect_from_environment().await?;
        adapter.health().await?;
        adapter.ensure_schema().await?;
        adapter.ensure_enrichment_schema().await?;
        let namespace = GraphNamespace::new(format!(
            "enrichment_entity_membership_e2e_{}",
            uuid::Uuid::now_v7().simple()
        ))?;
        let result = run_repeated_entity_scenario(&adapter, &namespace).await;
        let cleanup = adapter.purge_namespace(&namespace).await;
        cleanup?;
        result
    }

    async fn run_repeated_entity_scenario(
        adapter: &Neo4jAdapter,
        namespace: &GraphNamespace,
    ) -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::run_projection_scenario(adapter, namespace).await?;
        let session_id = SessionId::parse("019c63db-2995-74c3-b898-c1b92a8e1317")?;
        let anchor = read_base_anchor(adapter, namespace, session_id).await?;
        let run = run_spec_with_chunk_count(
            namespace,
            session_id,
            anchor.source_digest,
            '4',
            "prompt-repeated-entity-v1",
            EnrichmentChunkCount::new(2)?,
        )?;
        let run_ref =
            EnrichmentRunRef::new(namespace.clone(), anchor.source_digest, run.fingerprint());
        adapter
            .project_enrichment(EnrichmentGraphCommand::BeginRun(
                BeginEnrichmentRunCommand::new(run),
            ))
            .await?;

        let shared_entity = KnowledgeEntityId::parse_hex(&"de".repeat(32))?;
        let chunks = [
            repeated_entity_chunk_projection(&anchor, 'a', 0, shared_entity)?,
            repeated_entity_chunk_projection(&anchor, 'b', 1, shared_entity)?,
        ];
        for chunk in &chunks {
            let owner = EnrichmentInvocationOwner::from_bytes(*uuid::Uuid::now_v7().as_bytes());
            let claim = adapter
                .claim_enrichment_chunk(&ClaimEnrichmentChunkCommand::new(
                    run_ref.clone(),
                    chunk.chunk_id(),
                    owner,
                    EnrichmentLeaseDuration::new(60)?,
                ))
                .await?;
            let EnrichmentChunkClaim::Claimed(lease) = claim else {
                return Err("fresh repeated-entity chunk was not claimed".into());
            };
            adapter
                .project_claimed_enrichment_chunk(ProjectClaimedEnrichmentChunkCommand::new(
                    run_ref.clone(),
                    lease,
                    chunk.clone(),
                )?)
                .await?;
            assert!(matches!(
                adapter
                    .enrichment_chunk_checkpoint(&EnrichmentChunkCheckpointQuery::new(
                        run_ref.clone(),
                        chunk.chunk_id(),
                    ))
                    .await?,
                EnrichmentChunkCheckpoint::Committed(receipt)
                    if receipt.chunk_id() == chunk.chunk_id()
            ));
        }

        adapter
            .project_enrichment(EnrichmentGraphCommand::CompleteRun(
                CompleteEnrichmentRunCommand::new(run_ref.clone()),
            ))
            .await?;
        let EnrichmentLookup::Selected(selected) = adapter
            .selected_enrichment(&EnrichmentQuery::new(namespace.clone(), session_id))
            .await?
        else {
            return Err("two-chunk repeated-entity run was not selected".into());
        };
        assert_eq!(selected.entities().count(), RecordCount::new(1));
        assert_repeated_entity_memberships(adapter, &run_ref, shared_entity).await
    }

    async fn assert_repeated_entity_memberships(
        adapter: &Neo4jAdapter,
        run: &EnrichmentRunRef,
        entity_id: KnowledgeEntityId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let row = adapter
            .single_row(
                query(
                    "MATCH (run:HGEnrichmentRun {key: $run_key})-[membership:PRODUCED_ENTITY]->(entity:HGKnowledgeEntity {entity_id: $entity_id}) \
                     RETURN count(membership) AS membership_count, count(DISTINCT entity) AS entity_count",
                )
                .param("run_key", enrichment_run_key(run))
                .param("entity_id", entity_id.to_hex()),
                "read repeated entity chunk memberships",
            )
            .await?;
        assert_eq!(row.get::<i64>("membership_count")?, 2);
        assert_eq!(row.get::<i64>("entity_count")?, 1);
        Ok(())
    }

    async fn run_live_overlay_scenario(
        adapter: &Neo4jAdapter,
        contender: &Neo4jAdapter,
        namespace: &GraphNamespace,
    ) -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::run_projection_scenario(adapter, namespace).await?;
        let session_id = SessionId::parse("019c63db-2995-74c3-b898-c1b92a8e1317")?;
        let anchor = read_base_anchor(adapter, namespace, session_id).await?;
        let baseline = deterministic_snapshot(adapter, namespace).await?;
        let initial = project_initial_run(
            adapter, contender, namespace, session_id, &anchor, &baseline,
        )
        .await?;
        verify_idempotent_replay(adapter, &initial).await?;
        let selected_fingerprint = project_changed_run(
            adapter,
            namespace,
            session_id,
            &anchor,
            initial.overlay_count,
        )
        .await?;
        verify_failed_run_is_not_selected(
            adapter,
            namespace,
            session_id,
            &anchor,
            selected_fingerprint,
        )
        .await?;

        // Replaying an older completed fingerprint is an identity operation and
        // must not move the selected view backwards.
        adapter
            .project_enrichment(EnrichmentGraphCommand::CompleteRun(
                CompleteEnrichmentRunCommand::new(initial.run_ref),
            ))
            .await?;
        assert_selected(adapter, namespace, session_id, selected_fingerprint).await?;
        assert_eq!(baseline, deterministic_snapshot(adapter, namespace).await?);
        Ok(())
    }

    struct InitialRunState {
        run: EnrichmentRunSpec,
        run_ref: EnrichmentRunRef,
        chunk: EnrichmentChunkProjection,
        overlay_count: RecordCount,
    }

    async fn project_initial_run(
        adapter: &Neo4jAdapter,
        contender: &Neo4jAdapter,
        namespace: &GraphNamespace,
        session_id: SessionId,
        anchor: &BaseAnchor,
        baseline: &DeterministicSnapshot,
    ) -> Result<InitialRunState, Box<dyn std::error::Error>> {
        let first_run = run_spec(
            namespace,
            session_id,
            anchor.source_digest,
            '1',
            "prompt-v1",
        )?;
        let first_ref = EnrichmentRunRef::new(
            namespace.clone(),
            anchor.source_digest,
            first_run.fingerprint(),
        );
        let lifecycle_query = EnrichmentRunLifecycleQuery::new(first_ref.clone());
        assert_eq!(
            adapter.enrichment_run_lifecycle(&lifecycle_query).await?,
            EnrichmentRunLifecycle::Absent
        );
        assert_eq!(
            adapter
                .project_enrichment(EnrichmentGraphCommand::BeginRun(
                    BeginEnrichmentRunCommand::new(first_run.clone()),
                ))
                .await?
                .disposition(),
            EnrichmentProjectionDisposition::Applied
        );
        assert_eq!(
            adapter.enrichment_run_lifecycle(&lifecycle_query).await?,
            EnrichmentRunLifecycle::Resumable
        );
        assert!(matches!(
            adapter
                .selected_enrichment(&EnrichmentQuery::new(namespace.clone(), session_id))
                .await?,
            EnrichmentLookup::Unavailable(EnrichmentUnavailableReason::NoCompletedSelection)
        ));

        let first_chunk = chunk_projection(anchor, '1')?;
        let checkpoint_query =
            EnrichmentChunkCheckpointQuery::new(first_ref.clone(), first_chunk.chunk_id());
        assert_eq!(
            adapter
                .enrichment_chunk_checkpoint(&checkpoint_query)
                .await?,
            EnrichmentChunkCheckpoint::Required
        );
        claim_release_recover_and_project(adapter, contender, &first_ref, &first_chunk).await?;
        let EnrichmentChunkCheckpoint::Committed(checkpoint) = adapter
            .enrichment_chunk_checkpoint(&checkpoint_query)
            .await?
        else {
            return Err("projected chunk checkpoint was not committed".into());
        };
        assert_eq!(checkpoint.chunk_id(), first_chunk.chunk_id());
        assert_eq!(checkpoint.output_digest(), first_chunk.output_digest());
        assert_eq!(checkpoint.input_tokens(), first_chunk.input_tokens());
        assert_eq!(checkpoint.output_tokens(), first_chunk.output_tokens());
        assert!(matches!(
            adapter
                .selected_enrichment(&EnrichmentQuery::new(namespace.clone(), session_id))
                .await?,
            EnrichmentLookup::Unavailable(EnrichmentUnavailableReason::NoCompletedSelection)
        ));
        adapter
            .project_enrichment(EnrichmentGraphCommand::CompleteRun(
                CompleteEnrichmentRunCommand::new(first_ref.clone()),
            ))
            .await?;
        assert_eq!(
            adapter.enrichment_run_lifecycle(&lifecycle_query).await?,
            EnrichmentRunLifecycle::Completed
        );
        assert_selected(adapter, namespace, session_id, first_run.fingerprint()).await?;
        assert_eq!(*baseline, deterministic_snapshot(adapter, namespace).await?);
        let overlay_count = enrichment_node_count(adapter, namespace).await?;
        Ok(InitialRunState {
            run: first_run,
            run_ref: first_ref,
            chunk: first_chunk,
            overlay_count,
        })
    }

    async fn claim_release_recover_and_project(
        adapter: &Neo4jAdapter,
        contender: &Neo4jAdapter,
        run: &EnrichmentRunRef,
        chunk: &EnrichmentChunkProjection,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let first_owner = EnrichmentInvocationOwner::from_bytes(*uuid::Uuid::now_v7().as_bytes());
        let second_owner = EnrichmentInvocationOwner::from_bytes(*uuid::Uuid::now_v7().as_bytes());
        let first_claim = ClaimEnrichmentChunkCommand::new(
            run.clone(),
            chunk.chunk_id(),
            first_owner,
            EnrichmentLeaseDuration::new(60)?,
        );
        let second_claim = ClaimEnrichmentChunkCommand::new(
            run.clone(),
            chunk.chunk_id(),
            second_owner,
            EnrichmentLeaseDuration::new(60)?,
        );
        let (first_result, second_result) = tokio::join!(
            adapter.claim_enrichment_chunk(&first_claim),
            contender.claim_enrichment_chunk(&second_claim)
        );
        let first_result = first_result?;
        let second_result = second_result?;
        let (winner, waiting_claim, waiting_owner) = match (first_result, second_result) {
            (EnrichmentChunkClaim::Claimed(lease), EnrichmentChunkClaim::Busy) => {
                (lease, second_claim, second_owner)
            }
            (EnrichmentChunkClaim::Busy, EnrichmentChunkClaim::Claimed(lease)) => {
                (lease, first_claim, first_owner)
            }
            _ => return Err("concurrent paid-call claims did not elect exactly one owner".into()),
        };

        let non_owner = ClaimedEnrichmentChunk::new(chunk.chunk_id(), waiting_owner);
        assert_eq!(
            contender
                .release_enrichment_chunk_lease(&ReleaseEnrichmentChunkLeaseCommand::new(
                    run.clone(),
                    non_owner,
                ))
                .await?,
            EnrichmentChunkLeaseRelease::NotOwned
        );
        assert_eq!(
            adapter
                .release_enrichment_chunk_lease(&ReleaseEnrichmentChunkLeaseCommand::new(
                    run.clone(),
                    winner,
                ))
                .await?,
            EnrichmentChunkLeaseRelease::Released
        );

        let EnrichmentChunkClaim::Claimed(reclaimed) =
            contender.claim_enrichment_chunk(&waiting_claim).await?
        else {
            return Err("released paid-call lease was not reclaimable".into());
        };
        recover_expired_lease_and_commit(adapter, contender, run, chunk, &waiting_claim, reclaimed)
            .await
    }

    async fn recover_expired_lease_and_commit(
        adapter: &Neo4jAdapter,
        contender: &Neo4jAdapter,
        run: &EnrichmentRunRef,
        chunk: &EnrichmentChunkProjection,
        waiting_claim: &ClaimEnrichmentChunkCommand,
        reclaimed: ClaimedEnrichmentChunk,
    ) -> Result<(), Box<dyn std::error::Error>> {
        adapter
            .graph
            .run(
                query(
                    "MATCH (lease:HGEnrichmentChunkLease {key: $lease_key}) \
                     SET lease.expires_at = datetime() - duration({seconds: 1})",
                )
                .param("lease_key", chunk_lease_key(run, chunk.chunk_id())),
            )
            .await?;
        let recovery_owner =
            EnrichmentInvocationOwner::from_bytes(*uuid::Uuid::now_v7().as_bytes());
        let recovery_claim = ClaimEnrichmentChunkCommand::new(
            run.clone(),
            chunk.chunk_id(),
            recovery_owner,
            EnrichmentLeaseDuration::new(60)?,
        );
        let EnrichmentChunkClaim::Claimed(recovered) =
            adapter.claim_enrichment_chunk(&recovery_claim).await?
        else {
            return Err("expired paid-call lease was not recoverable".into());
        };
        assert!(
            contender
                .project_claimed_enrichment_chunk(ProjectClaimedEnrichmentChunkCommand::new(
                    run.clone(),
                    reclaimed,
                    chunk.clone(),
                )?)
                .await
                .is_err()
        );
        assert_eq!(
            adapter
                .project_claimed_enrichment_chunk(ProjectClaimedEnrichmentChunkCommand::new(
                    run.clone(),
                    recovered,
                    chunk.clone(),
                )?)
                .await?
                .disposition(),
            EnrichmentProjectionDisposition::Applied
        );
        assert!(matches!(
            contender.claim_enrichment_chunk(waiting_claim).await?,
            EnrichmentChunkClaim::Committed(receipt)
                if receipt.chunk_id() == chunk.chunk_id()
                    && receipt.output_digest() == chunk.output_digest()
        ));
        Ok(())
    }

    async fn verify_idempotent_replay(
        adapter: &Neo4jAdapter,
        initial: &InitialRunState,
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            adapter
                .project_enrichment(EnrichmentGraphCommand::BeginRun(
                    BeginEnrichmentRunCommand::new(initial.run.clone()),
                ))
                .await?
                .disposition(),
            EnrichmentProjectionDisposition::Unchanged
        );
        assert_eq!(
            adapter
                .project_enrichment(EnrichmentGraphCommand::ProjectChunk(
                    ProjectEnrichmentChunkCommand::new(
                        initial.run_ref.clone(),
                        initial.chunk.clone(),
                    ),
                ))
                .await?
                .disposition(),
            EnrichmentProjectionDisposition::Unchanged
        );
        assert_eq!(
            adapter
                .project_enrichment(EnrichmentGraphCommand::CompleteRun(
                    CompleteEnrichmentRunCommand::new(initial.run_ref.clone()),
                ))
                .await?
                .disposition(),
            EnrichmentProjectionDisposition::Unchanged
        );
        assert_eq!(
            initial.overlay_count,
            enrichment_node_count(adapter, initial.run_ref.namespace()).await?
        );
        Ok(())
    }

    async fn project_changed_run(
        adapter: &Neo4jAdapter,
        namespace: &GraphNamespace,
        session_id: SessionId,
        anchor: &BaseAnchor,
        initial_overlay_count: RecordCount,
    ) -> Result<EnrichmentFingerprint, Box<dyn std::error::Error>> {
        let second_run = run_spec(
            namespace,
            session_id,
            anchor.source_digest,
            '2',
            "prompt-v2",
        )?;
        let second_ref = EnrichmentRunRef::new(
            namespace.clone(),
            anchor.source_digest,
            second_run.fingerprint(),
        );
        adapter
            .project_enrichment(EnrichmentGraphCommand::BeginRun(
                BeginEnrichmentRunCommand::new(second_run.clone()),
            ))
            .await?;
        adapter
            .project_enrichment(EnrichmentGraphCommand::ProjectChunk(
                ProjectEnrichmentChunkCommand::new(
                    second_ref.clone(),
                    chunk_projection(anchor, '2')?,
                ),
            ))
            .await?;
        adapter
            .project_enrichment(EnrichmentGraphCommand::CompleteRun(
                CompleteEnrichmentRunCommand::new(second_ref),
            ))
            .await?;
        assert!(
            enrichment_node_count(adapter, namespace).await?.value()
                > initial_overlay_count.value()
        );
        assert_selected(adapter, namespace, session_id, second_run.fingerprint()).await?;
        Ok(second_run.fingerprint())
    }

    async fn verify_failed_run_is_not_selected(
        adapter: &Neo4jAdapter,
        namespace: &GraphNamespace,
        session_id: SessionId,
        anchor: &BaseAnchor,
        selected_fingerprint: EnrichmentFingerprint,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let failed_run = run_spec(
            namespace,
            session_id,
            anchor.source_digest,
            '3',
            "prompt-v3",
        )?;
        let failed_ref = EnrichmentRunRef::new(
            namespace.clone(),
            anchor.source_digest,
            failed_run.fingerprint(),
        );
        adapter
            .project_enrichment(EnrichmentGraphCommand::BeginRun(
                BeginEnrichmentRunCommand::new(failed_run),
            ))
            .await?;
        let lifecycle_query = EnrichmentRunLifecycleQuery::new(failed_ref.clone());
        assert_eq!(
            adapter.enrichment_run_lifecycle(&lifecycle_query).await?,
            EnrichmentRunLifecycle::Resumable
        );
        let retryable_failure = MarkEnrichmentRunFailedCommand::new(
            failed_ref.clone(),
            EnrichmentFailureClass::RateLimited,
        );
        assert_eq!(
            adapter
                .project_enrichment(EnrichmentGraphCommand::MarkRunFailed(
                    retryable_failure.clone(),
                ))
                .await?
                .disposition(),
            EnrichmentProjectionDisposition::Applied
        );
        assert_eq!(
            adapter.enrichment_run_lifecycle(&lifecycle_query).await?,
            EnrichmentRunLifecycle::Resumable
        );
        assert_eq!(
            adapter
                .project_enrichment(EnrichmentGraphCommand::MarkRunFailed(retryable_failure,))
                .await?
                .disposition(),
            EnrichmentProjectionDisposition::Unchanged
        );
        assert_eq!(
            adapter
                .project_enrichment(EnrichmentGraphCommand::MarkRunFailed(
                    MarkEnrichmentRunFailedCommand::new(
                        failed_ref.clone(),
                        EnrichmentFailureClass::PolicyBlocked,
                    ),
                ))
                .await?
                .disposition(),
            EnrichmentProjectionDisposition::Applied
        );
        assert_eq!(
            adapter.enrichment_run_lifecycle(&lifecycle_query).await?,
            EnrichmentRunLifecycle::TerminalFailed
        );
        assert_failed_state(adapter, &failed_ref).await?;
        assert!(
            adapter
                .project_enrichment(EnrichmentGraphCommand::CompleteRun(
                    CompleteEnrichmentRunCommand::new(failed_ref),
                ))
                .await
                .is_err()
        );
        assert_selected(adapter, namespace, session_id, selected_fingerprint).await
    }

    async fn assert_failed_state(
        adapter: &Neo4jAdapter,
        run: &EnrichmentRunRef,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let row = adapter
            .single_row(
                query(
                    "MATCH (run:HGEnrichmentRun {key: $run_key}) \
                     RETURN run.status AS status, run.failure_class AS failure_class, \
                       run.failure_message IS NULL AS source_safe",
                )
                .param("run_key", enrichment_run_key(run)),
                "read isolated failed-run state",
            )
            .await?;
        assert_eq!(row.get::<String>("status")?, "terminal_failed");
        assert_eq!(row.get::<String>("failure_class")?, "policy_blocked");
        assert!(row.get::<bool>("source_safe")?);
        Ok(())
    }

    #[derive(Debug)]
    struct BaseAnchor {
        source_digest: SourceDigest,
        observation_id: ObservationId,
        sequence: RecordSequence,
    }

    async fn read_base_anchor(
        adapter: &Neo4jAdapter,
        namespace: &GraphNamespace,
        session_id: SessionId,
    ) -> Result<BaseAnchor, Box<dyn std::error::Error>> {
        let mut rows = adapter
            .graph
            .execute(
                query(
                    "MATCH (session:HGSession {key: $session_key})-[:IMPORTED_FROM]->(src:HGSourceSnapshot) \
                     MATCH (src)-[:CONTAINS]->(observation:HGObservation) \
                     RETURN src.source_digest AS source_digest, observation.observation_id AS observation_id, \
                       observation.sequence AS sequence \
                     ORDER BY observation.sequence LIMIT 1",
                )
                .param(
                    "session_key",
                    session_key(namespace.as_str(), &session_id.to_string()),
                ),
            )
            .await?;
        let row = rows.next().await?.ok_or("base anchor not found")?;
        let source_digest = SourceDigest::parse_hex(&row.get::<String>("source_digest")?)?;
        let sequence = read_sequence(&row, "sequence")?;
        let observation_id = ObservationId::from_source(source_digest, sequence);
        if row.get::<String>("observation_id")? != observation_id.as_str() {
            return Err("base observation identity mismatch".into());
        }
        Ok(BaseAnchor {
            source_digest,
            observation_id,
            sequence,
        })
    }

    fn run_spec(
        namespace: &GraphNamespace,
        session_id: SessionId,
        source_digest: SourceDigest,
        identity: char,
        prompt: &str,
    ) -> Result<EnrichmentRunSpec, Box<dyn std::error::Error>> {
        run_spec_with_chunk_count(
            namespace,
            session_id,
            source_digest,
            identity,
            prompt,
            EnrichmentChunkCount::new(1)?,
        )
    }

    fn run_spec_with_chunk_count(
        namespace: &GraphNamespace,
        session_id: SessionId,
        source_digest: SourceDigest,
        identity: char,
        prompt: &str,
        expected_chunks: EnrichmentChunkCount,
    ) -> Result<EnrichmentRunSpec, Box<dyn std::error::Error>> {
        let value = identity.to_string().repeat(64);
        Ok(EnrichmentRunSpec::new(
            namespace.clone(),
            session_id,
            source_digest,
            EnrichmentRunId::parse_hex(&value)?,
            EnrichmentFingerprint::parse_hex(&value)?,
            EnrichmentProvider::Mistral,
            EnrichmentModelName::new("mistral-small-latest")?,
            PromptVersion::new(prompt)?,
            EnrichmentRunAuditProvenance::new(
                harness_graph_graph_port::EnrichmentDisclosureScope::ConversationAndExecution,
                harness_graph_graph_port::EnrichmentAuthorizationPolicyDigest::parse_hex(&value)?,
                harness_graph_graph_port::EnrichmentPromptDigest::parse_hex(&value)?,
            ),
            EnrichmentSchemaVersion::new("schema-v1")?,
            RedactionPolicyVersion::new("redaction-v1")?,
            ChunkingPolicyVersion::new("chunking-v1")?,
            expected_chunks,
        ))
    }

    fn repeated_entity_chunk_projection(
        anchor: &BaseAnchor,
        identity: char,
        part_index: u32,
        shared_entity: KnowledgeEntityId,
    ) -> Result<EnrichmentChunkProjection, Box<dyn std::error::Error>> {
        let hex = |suffix: char| format!("{identity}{suffix}").repeat(32);
        let span_id = TranscriptSpanId::parse_hex(&hex('1'))?;
        Ok(EnrichmentChunkProjection::new(
            EnrichmentChunkId::parse_hex(&hex('7'))?,
            EnrichmentOutputDigest::parse_hex(&hex('8'))?,
            TranscriptSpans::new([TranscriptSpanProjection::new(
                span_id,
                anchor.observation_id.clone(),
                anchor.sequence,
                TranscriptField::Message,
                TranscriptFieldOrdinal::new(0),
                TranscriptPartIndex::new(part_index),
                TranscriptRole::Agent,
                TranscriptByteCount::new(32),
                TokenCount::new(8),
                PayloadDigest::hash(format!("repeated-entity-{identity}").as_bytes()),
            )])?,
            NarrativeEpisodes::default(),
            KnowledgeEntities::new([KnowledgeEntityProjection::new(
                shared_entity,
                KnowledgeEntityKind::Tool,
                KnowledgeEntityName::new("exec_command")?,
            )])?,
            KnowledgeClaims::default(),
            KnowledgeRelations::default(),
            TokenCount::new(40),
            TokenCount::new(12),
        )?)
    }

    fn chunk_projection(
        anchor: &BaseAnchor,
        identity: char,
    ) -> Result<EnrichmentChunkProjection, Box<dyn std::error::Error>> {
        let hex = |suffix: char| format!("{identity}{suffix}").repeat(32);
        let span_id = TranscriptSpanId::parse_hex(&hex('1'))?;
        let subject = KnowledgeEntityId::parse_hex(&hex('2'))?;
        let object = KnowledgeEntityId::parse_hex(&hex('3'))?;
        let spans = TranscriptSpans::new([TranscriptSpanProjection::new(
            span_id,
            anchor.observation_id.clone(),
            anchor.sequence,
            TranscriptField::Message,
            TranscriptFieldOrdinal::new(0),
            TranscriptPartIndex::new(0),
            TranscriptRole::User,
            TranscriptByteCount::new(24),
            TokenCount::new(6),
            PayloadDigest::hash(format!("sanitized-{identity}").as_bytes()),
        )])?;
        let episodes = NarrativeEpisodes::new([NarrativeEpisodeProjection::new(
            NarrativeEpisodeId::parse_hex(&hex('4'))?,
            EpisodeOrdinal::new(1)?,
            NarrativeTitle::new("Verified graph episode")?,
            NarrativeSummary::new("An evidence-linked additive enrichment episode")?,
            KnowledgeConfidence::High,
            EpistemicStatus::Explicit,
            SpanCitations::new([span_id])?,
        )])?;
        let entities = KnowledgeEntities::new([
            KnowledgeEntityProjection::new(
                subject,
                KnowledgeEntityKind::Configuration,
                KnowledgeEntityName::new("additive overlay")?,
            ),
            KnowledgeEntityProjection::new(
                object,
                KnowledgeEntityKind::Artifact,
                KnowledgeEntityName::new("deterministic graph")?,
            ),
        ])?;
        let claims = KnowledgeClaims::new([
            KnowledgeClaimProjection::new(
                KnowledgeClaimId::parse_hex(&hex('5'))?,
                KnowledgeKind::Decision,
                KnowledgeClaimTitle::new("Preserve deterministic state")?,
                KnowledgeStatement::new("The overlay preserves the deterministic graph")?,
                KnowledgeConfidence::High,
                EpistemicStatus::Explicit,
                KnowledgeClaimSubjects::entities([subject, object])?,
                SpanCitations::new([span_id])?,
                ObservationCorroboration::available([anchor.observation_id.clone()])?,
            )?,
            KnowledgeClaimProjection::new(
                KnowledgeClaimId::parse_hex(&hex('9'))?,
                KnowledgeKind::Lesson,
                KnowledgeClaimTitle::new("Session-wide lesson")?,
                KnowledgeStatement::new("Evidence stays linked across the enriched session")?,
                KnowledgeConfidence::High,
                EpistemicStatus::Explicit,
                KnowledgeClaimSubjects::SessionWide,
                SpanCitations::new([span_id])?,
                ObservationCorroboration::Unavailable,
            )?,
        ])?;
        let relations = KnowledgeRelations::new([KnowledgeRelationProjection::new(
            KnowledgeRelationId::parse_hex(&hex('6'))?,
            KnowledgePredicate::RelatedTo,
            subject,
            object,
            KnowledgeConfidence::High,
            EpistemicStatus::Explicit,
            SpanCitations::new([span_id])?,
        )?])?;
        Ok(EnrichmentChunkProjection::new(
            EnrichmentChunkId::parse_hex(&hex('7'))?,
            EnrichmentOutputDigest::parse_hex(&hex('8'))?,
            spans,
            episodes,
            entities,
            claims,
            relations,
            TokenCount::new(100),
            TokenCount::new(40),
        )?)
    }

    async fn assert_selected(
        adapter: &Neo4jAdapter,
        namespace: &GraphNamespace,
        session_id: SessionId,
        fingerprint: EnrichmentFingerprint,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let lookup = adapter
            .selected_enrichment(&EnrichmentQuery::new(namespace.clone(), session_id))
            .await?;
        let EnrichmentLookup::Selected(selected) = lookup else {
            return Err("expected selected completed enrichment".into());
        };
        assert_eq!(selected.run().fingerprint(), fingerprint);
        assert_eq!(
            selected.run().audit_provenance().disclosure_scope(),
            harness_graph_graph_port::EnrichmentDisclosureScope::ConversationAndExecution
        );
        assert_eq!(
            selected
                .run()
                .audit_provenance()
                .authorization_policy_digest()
                .to_hex(),
            fingerprint.to_hex()
        );
        assert_eq!(
            selected.run().audit_provenance().prompt_digest().to_hex(),
            fingerprint.to_hex()
        );
        assert_eq!(selected.spans().count(), RecordCount::new(1));
        assert_eq!(
            selected
                .spans()
                .iter()
                .next()
                .ok_or("missing selected transcript span")?
                .part_index(),
            TranscriptPartIndex::new(0)
        );
        assert_eq!(selected.episodes().count(), RecordCount::new(1));
        assert_eq!(selected.entities().count(), RecordCount::new(2));
        assert_eq!(selected.claims().count(), RecordCount::new(2));
        assert_eq!(selected.relations().count(), RecordCount::new(1));
        let episode = selected
            .episodes()
            .iter()
            .next()
            .ok_or("missing selected narrative episode")?;
        assert_eq!(episode.title().as_str(), "Verified graph episode");
        assert_eq!(
            episode.summary().as_str(),
            "An evidence-linked additive enrichment episode"
        );
        assert_eq!(episode.confidence(), KnowledgeConfidence::High);
        assert_eq!(episode.epistemic_status(), EpistemicStatus::Explicit);
        assert_eq!(episode.spans().count(), RecordCount::new(1));
        assert!(selected.claims().iter().any(|claim| {
            matches!(
                claim.subjects(),
                KnowledgeClaimSubjects::Entities(subjects) if subjects.len() == 2
            ) && claim.title().as_str() == "Preserve deterministic state"
        }));
        assert!(selected.claims().iter().any(|claim| {
            matches!(claim.subjects(), KnowledgeClaimSubjects::SessionWide)
                && claim.title().as_str() == "Session-wide lesson"
        }));
        assert_eq!(
            selected
                .claims()
                .iter()
                .next()
                .ok_or("missing selected claim")?
                .spans()
                .count(),
            RecordCount::new(1)
        );
        Ok(())
    }

    #[derive(Debug, PartialEq, Eq)]
    struct DeterministicSnapshot {
        nodes: Vec<String>,
        relationships: Vec<String>,
    }

    async fn deterministic_snapshot(
        adapter: &Neo4jAdapter,
        namespace: &GraphNamespace,
    ) -> Result<DeterministicSnapshot, Box<dyn std::error::Error>> {
        const BASE_FILTER: &str = "none(label IN labels(n) WHERE label IN ['HGEnrichmentRun', 'HGEnrichmentChunkReceipt', 'HGEnrichmentChunkLease', \
             'HGTranscriptSpan', 'HGNarrativeEpisode', 'HGKnowledgeEntity', 'HGKnowledgeClaim', \
             'HGKnowledgeRelation', 'HGEnrichmentView'])";
        let node_query = format!(
            "MATCH (n {{hg_namespace: $namespace}}) WHERE {BASE_FILTER} \
             RETURN n AS node ORDER BY n.key"
        );
        let mut node_rows = adapter
            .graph
            .execute(query(&node_query).param("namespace", namespace.as_str()))
            .await?;
        let mut nodes = Vec::new();
        while let Some(row) = node_rows.next().await? {
            nodes.push(canonical_node(&row.get::<neo4rs::Node>("node")?)?);
        }

        let relationship_query = "MATCH (left {hg_namespace: $namespace})-[relationship]->(right {hg_namespace: $namespace}) \
             WHERE none(label IN labels(left) WHERE label IN ['HGEnrichmentRun', 'HGEnrichmentChunkReceipt', 'HGEnrichmentChunkLease', \
               'HGTranscriptSpan', 'HGNarrativeEpisode', 'HGKnowledgeEntity', 'HGKnowledgeClaim', \
               'HGKnowledgeRelation', 'HGEnrichmentView']) \
               AND none(label IN labels(right) WHERE label IN ['HGEnrichmentRun', 'HGEnrichmentChunkReceipt', 'HGEnrichmentChunkLease', \
               'HGTranscriptSpan', 'HGNarrativeEpisode', 'HGKnowledgeEntity', 'HGKnowledgeClaim', \
               'HGKnowledgeRelation', 'HGEnrichmentView']) \
             RETURN left.key AS left_key, type(relationship) AS relationship_type, \
               right.key AS right_key, relationship AS relationship \
             ORDER BY left_key, relationship_type, right_key";
        let mut relationship_rows = adapter
            .graph
            .execute(query(relationship_query).param("namespace", namespace.as_str()))
            .await?;
        let mut relationships = Vec::new();
        while let Some(row) = relationship_rows.next().await? {
            let relationship = row.get::<neo4rs::Relation>("relationship")?;
            relationships.push(format!(
                "{}|{}|{}|{}",
                row.get::<String>("left_key")?,
                row.get::<String>("relationship_type")?,
                row.get::<String>("right_key")?,
                canonical_relation_properties(&relationship)?,
            ));
        }
        Ok(DeterministicSnapshot {
            nodes,
            relationships,
        })
    }

    fn canonical_node(node: &neo4rs::Node) -> Result<String, Box<dyn std::error::Error>> {
        let mut labels = node
            .labels()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        labels.sort_unstable();
        Ok(format!(
            "{}|{}",
            labels.join(","),
            canonical_properties(node.keys(), |key| node.get::<neo4rs::BoltType>(key))?
        ))
    }

    fn canonical_relation_properties(
        relationship: &neo4rs::Relation,
    ) -> Result<String, Box<dyn std::error::Error>> {
        canonical_properties(relationship.keys(), |key| {
            relationship.get::<neo4rs::BoltType>(key)
        })
    }

    fn canonical_properties(
        keys: Vec<&str>,
        read: impl Fn(&str) -> Result<neo4rs::BoltType, neo4rs::DeError>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let mut keys = keys.into_iter().map(str::to_owned).collect::<Vec<_>>();
        keys.sort_unstable();
        let mut properties = Vec::with_capacity(keys.len());
        for key in keys {
            properties.push(format!("{key}={}", canonical_bolt_value(&read(&key)?)?));
        }
        Ok(properties.join("|"))
    }

    fn canonical_bolt_value(
        value: &neo4rs::BoltType,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let canonical = match value {
            neo4rs::BoltType::String(value) => format!("string:{:?}", value.value),
            neo4rs::BoltType::Boolean(value) => format!("boolean:{}", value.value),
            neo4rs::BoltType::Null(_) => "null".to_owned(),
            neo4rs::BoltType::Integer(value) => format!("integer:{}", value.value),
            neo4rs::BoltType::Float(value) => format!("float:{:016x}", value.value.to_bits()),
            neo4rs::BoltType::List(values) => {
                let values = values
                    .iter()
                    .map(canonical_bolt_value)
                    .collect::<Result<Vec<_>, _>>()?;
                format!("list:[{}]", values.join(","))
            }
            neo4rs::BoltType::Map(values) => {
                let mut entries = values.value.iter().collect::<Vec<_>>();
                entries.sort_unstable_by(|(left, _), (right, _)| left.value.cmp(&right.value));
                let mut canonical_entries = Vec::with_capacity(entries.len());
                for (key, value) in entries {
                    canonical_entries.push(format!(
                        "{:?}={}",
                        key.value,
                        canonical_bolt_value(value)?
                    ));
                }
                format!("map:{{{}}}", canonical_entries.join(","))
            }
            neo4rs::BoltType::Node(_)
            | neo4rs::BoltType::Relation(_)
            | neo4rs::BoltType::UnboundedRelation(_)
            | neo4rs::BoltType::Path(_) => {
                return Err("nested graph values cannot be canonical graph properties".into());
            }
            neo4rs::BoltType::Point2D(value) => format!("point-2d:{value:?}"),
            neo4rs::BoltType::Point3D(value) => format!("point-3d:{value:?}"),
            neo4rs::BoltType::Bytes(value) => format!("bytes:{value:?}"),
            neo4rs::BoltType::Duration(value) => format!("duration:{value:?}"),
            neo4rs::BoltType::Date(value) => format!("date:{value:?}"),
            neo4rs::BoltType::Time(value) => format!("time:{value:?}"),
            neo4rs::BoltType::LocalTime(value) => format!("local-time:{value:?}"),
            neo4rs::BoltType::DateTime(value) => format!("date-time:{value:?}"),
            neo4rs::BoltType::LocalDateTime(value) => {
                format!("local-date-time:{value:?}")
            }
            neo4rs::BoltType::DateTimeZoneId(value) => {
                format!("date-time-zone-id:{value:?}")
            }
        };
        Ok(canonical)
    }

    async fn enrichment_node_count(
        adapter: &Neo4jAdapter,
        namespace: &GraphNamespace,
    ) -> Result<RecordCount, Box<dyn std::error::Error>> {
        let mut rows = adapter
            .graph
            .execute(
                query(
                    "MATCH (n {hg_namespace: $namespace}) \
                     WHERE any(label IN labels(n) WHERE label IN ['HGEnrichmentRun', 'HGEnrichmentChunkReceipt', 'HGEnrichmentChunkLease', \
                       'HGTranscriptSpan', 'HGNarrativeEpisode', 'HGKnowledgeEntity', 'HGKnowledgeClaim', \
                       'HGKnowledgeRelation', 'HGEnrichmentView']) RETURN count(n) AS count",
                )
                .param("namespace", namespace.as_str()),
            )
            .await?;
        let count = rows
            .next()
            .await?
            .ok_or("enrichment count missing")?
            .get::<i64>("count")?;
        Ok(RecordCount::new(u64::try_from(count)?))
    }
}
