//! Total, text-free conversion from validated extraction into graph projection.

use harness_graph_domain::{ObservationId, PayloadDigest, TokenCount};
use harness_graph_graph_port as graph;
use harness_graph_protocol as protocol;
use harness_graph_transcript_enrichment as transcript;
use sha2::{Digest, Sha256};

use crate::{ConversionStage, EnrichmentApplicationError, EnrichmentRunConfiguration};

pub(crate) fn convert_chunk(
    chunk: &transcript::BoundedTranscriptChunk,
    extraction: &transcript::ChunkKnowledgeExtraction,
    configuration: &EnrichmentRunConfiguration,
) -> Result<graph::EnrichmentChunkProjection, EnrichmentApplicationError> {
    if extraction.knowledge().chunk_id() != chunk.id() {
        return Err(conversion_error(ConversionStage::ChunkIdentity));
    }

    let chunk_id = graph::EnrichmentChunkId::parse_hex(&chunk.id().to_hex())
        .map_err(|_| conversion_error(ConversionStage::ChunkIdentity))?;
    let spans = graph::TranscriptSpans::new(
        chunk
            .segments()
            .map(|segment| convert_span(segment, configuration))
            .collect::<Result<Vec<_>, _>>()?,
    )
    .map_err(|_| conversion_error(ConversionStage::TranscriptSpan))?;
    let entities = graph::KnowledgeEntities::new(
        extraction
            .knowledge()
            .entities()
            .iter()
            .map(convert_entity)
            .collect::<Result<Vec<_>, _>>()?,
    )
    .map_err(|_| conversion_error(ConversionStage::KnowledgeEntity))?;
    let episodes = graph::NarrativeEpisodes::new(
        extraction
            .knowledge()
            .episodes()
            .iter()
            .map(convert_episode)
            .collect::<Result<Vec<_>, _>>()?,
    )
    .map_err(|_| conversion_error(ConversionStage::NarrativeEpisode))?;
    let claims = graph::KnowledgeClaims::new(
        extraction
            .knowledge()
            .claims()
            .iter()
            .map(convert_claim)
            .collect::<Result<Vec<_>, _>>()?,
    )
    .map_err(|_| conversion_error(ConversionStage::KnowledgeClaim))?;
    let relations = graph::KnowledgeRelations::new(
        extraction
            .knowledge()
            .relations()
            .iter()
            .map(convert_relation)
            .collect::<Result<Vec<_>, _>>()?,
    )
    .map_err(|_| conversion_error(ConversionStage::KnowledgeRelation))?;
    let output_digest = output_digest(extraction)?;
    let usage = extraction.usage();

    graph::EnrichmentChunkProjection::new(
        chunk_id,
        output_digest,
        spans,
        episodes,
        entities,
        claims,
        relations,
        usage.input_tokens(),
        usage.output_tokens(),
    )
    .map_err(|_| conversion_error(ConversionStage::ChunkProjection))
}

fn convert_span(
    segment: &transcript::TranscriptChunkSegment,
    configuration: &EnrichmentRunConfiguration,
) -> Result<graph::TranscriptSpanProjection, EnrichmentApplicationError> {
    let source = segment.span().source();
    if source.session_id() != configuration.session_id() {
        return Err(conversion_error(ConversionStage::TranscriptSpan));
    }
    if source.source_digest() != configuration.source_digest() {
        return Err(conversion_error(ConversionStage::TranscriptSpan));
    }
    let id = span_id(segment.citation_token())?;
    let content_digest = PayloadDigest::parse_hex(&segment.sanitized_content_digest().to_hex())
        .map_err(|_| conversion_error(ConversionStage::TranscriptSpan))?;
    Ok(graph::TranscriptSpanProjection::new(
        id,
        ObservationId::from_source(source.source_digest(), source.sequence()),
        source.sequence(),
        map_field(segment.span().field_path().field()),
        graph::TranscriptFieldOrdinal::new(segment.span().field_path().ordinal()),
        graph::TranscriptPartIndex::new(segment.span().part_index().value()),
        map_role(segment.role()),
        graph::TranscriptByteCount::new(segment.byte_count().value()),
        TokenCount::new(segment.estimated_token_count().value()),
        content_digest,
    ))
}

fn convert_episode(
    episode: &transcript::NarrativeEpisode,
) -> Result<graph::NarrativeEpisodeProjection, EnrichmentApplicationError> {
    let id = graph::NarrativeEpisodeId::parse_hex(&episode.id().to_hex())
        .map_err(|_| conversion_error(ConversionStage::NarrativeEpisode))?;
    let ordinal = episode
        .citations()
        .iter()
        .map(|citation| citation.span().source().sequence().value())
        .min()
        .ok_or_else(|| conversion_error(ConversionStage::NarrativeEpisode))?;
    let title = graph::NarrativeTitle::new(episode.title().as_str().to_owned())
        .map_err(|_| conversion_error(ConversionStage::NarrativeEpisode))?;
    let summary = graph::NarrativeSummary::new(episode.summary().as_str().to_owned())
        .map_err(|_| conversion_error(ConversionStage::NarrativeEpisode))?;
    Ok(graph::NarrativeEpisodeProjection::new(
        id,
        graph::EpisodeOrdinal::new(ordinal)
            .map_err(|_| conversion_error(ConversionStage::NarrativeEpisode))?,
        title,
        summary,
        map_confidence(episode.confidence()),
        map_epistemic_status(episode.epistemic_status()),
        convert_citations(episode.citations(), ConversionStage::NarrativeEpisode)?,
    ))
}

fn convert_entity(
    entity: &transcript::KnowledgeEntity,
) -> Result<graph::KnowledgeEntityProjection, EnrichmentApplicationError> {
    let id = graph::KnowledgeEntityId::parse_hex(&entity.id().to_hex())
        .map_err(|_| conversion_error(ConversionStage::KnowledgeEntity))?;
    let name = graph::KnowledgeEntityName::new(entity.label().as_str().to_owned())
        .map_err(|_| conversion_error(ConversionStage::KnowledgeEntity))?;
    Ok(graph::KnowledgeEntityProjection::new(
        id,
        map_entity_kind(entity.kind()),
        name,
    ))
}

fn convert_claim(
    claim: &transcript::KnowledgeClaim,
) -> Result<graph::KnowledgeClaimProjection, EnrichmentApplicationError> {
    let id = graph::KnowledgeClaimId::parse_hex(&claim.id().to_hex())
        .map_err(|_| conversion_error(ConversionStage::KnowledgeClaim))?;
    let subjects = convert_claim_subjects(claim.subjects())?;
    graph::KnowledgeClaimProjection::new(
        id,
        map_knowledge_kind(claim.kind()),
        graph::KnowledgeClaimTitle::new(claim.title().as_str().to_owned())
            .map_err(|_| conversion_error(ConversionStage::KnowledgeClaim))?,
        graph::KnowledgeStatement::new(claim.statement().as_str().to_owned())
            .map_err(|_| conversion_error(ConversionStage::KnowledgeClaim))?,
        map_confidence(claim.confidence()),
        map_epistemic_status(claim.epistemic_status()),
        subjects,
        convert_citations(claim.citations(), ConversionStage::KnowledgeClaim)?,
        graph::ObservationCorroboration::Unavailable,
    )
    .map_err(|_| conversion_error(ConversionStage::KnowledgeClaim))
}

fn convert_claim_subjects(
    subjects: &transcript::ClaimSubjects,
) -> Result<graph::KnowledgeClaimSubjects, EnrichmentApplicationError> {
    let converted = match subjects {
        transcript::ClaimSubjects::SessionWide => graph::KnowledgeClaimSubjects::SessionWide,
        transcript::ClaimSubjects::Entities(values) => {
            let converted = values
                .iter()
                .map(|value| {
                    graph::KnowledgeEntityId::parse_hex(&value.to_hex())
                        .map_err(|_| conversion_error(ConversionStage::KnowledgeClaim))
                })
                .collect::<Result<Vec<_>, _>>()?;
            graph::KnowledgeClaimSubjects::entities(converted)
                .map_err(|_| conversion_error(ConversionStage::KnowledgeClaim))?
        }
    };
    Ok(converted)
}

fn convert_relation(
    relation: &transcript::KnowledgeRelation,
) -> Result<graph::KnowledgeRelationProjection, EnrichmentApplicationError> {
    let id = graph::KnowledgeRelationId::parse_hex(&relation.id().to_hex())
        .map_err(|_| conversion_error(ConversionStage::KnowledgeRelation))?;
    let subject = graph::KnowledgeEntityId::parse_hex(&relation.subject().to_hex())
        .map_err(|_| conversion_error(ConversionStage::KnowledgeRelation))?;
    let object = graph::KnowledgeEntityId::parse_hex(&relation.object().to_hex())
        .map_err(|_| conversion_error(ConversionStage::KnowledgeRelation))?;
    graph::KnowledgeRelationProjection::new(
        id,
        map_predicate(relation.predicate()),
        subject,
        object,
        map_confidence(relation.confidence()),
        map_epistemic_status(relation.epistemic_status()),
        convert_citations(relation.citations(), ConversionStage::KnowledgeRelation)?,
    )
    .map_err(|_| conversion_error(ConversionStage::KnowledgeRelation))
}

fn convert_citations(
    citations: &transcript::EvidenceCitations,
    stage: ConversionStage,
) -> Result<graph::SpanCitations, EnrichmentApplicationError> {
    graph::SpanCitations::new(
        citations
            .iter()
            .map(|citation| span_id(citation.token()))
            .collect::<Result<Vec<_>, _>>()?,
    )
    .map_err(|_| conversion_error(stage))
}

fn span_id(
    token: transcript::TranscriptSpanToken,
) -> Result<graph::TranscriptSpanId, EnrichmentApplicationError> {
    graph::TranscriptSpanId::parse_hex(&token.to_hex())
        .map_err(|_| conversion_error(ConversionStage::TranscriptSpan))
}

fn output_digest(
    extraction: &transcript::ChunkKnowledgeExtraction,
) -> Result<graph::EnrichmentOutputDigest, EnrichmentApplicationError> {
    let knowledge = extraction.knowledge();
    let mut hasher = Sha256::new();
    append_digest_field(&mut hasher, b"harness-graph-enrichment-output-v1");
    append_digest_field(&mut hasher, &knowledge.chunk_id().bytes());
    for episode in knowledge.episodes().iter() {
        append_digest_field(&mut hasher, episode.id().to_hex().as_bytes());
    }
    for entity in knowledge.entities().iter() {
        append_digest_field(&mut hasher, entity.id().to_hex().as_bytes());
    }
    for claim in knowledge.claims().iter() {
        append_digest_field(&mut hasher, claim.id().to_hex().as_bytes());
    }
    for relation in knowledge.relations().iter() {
        append_digest_field(&mut hasher, relation.id().to_hex().as_bytes());
    }
    let digest: [u8; 32] = hasher.finalize().into();
    graph::EnrichmentOutputDigest::parse_hex(&encode_hex(digest))
        .map_err(|_| conversion_error(ConversionStage::ChunkProjection))
}

fn append_digest_field(hasher: &mut Sha256, bytes: &[u8]) {
    let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
}

fn encode_hex(bytes: [u8; 32]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(TABLE[usize::from(byte >> 4)]));
        encoded.push(char::from(TABLE[usize::from(byte & 0x0f)]));
    }
    encoded
}

const fn conversion_error(stage: ConversionStage) -> EnrichmentApplicationError {
    EnrichmentApplicationError::Conversion { stage }
}

const fn map_role(value: protocol::TranscriptRole) -> graph::TranscriptRole {
    match value {
        protocol::TranscriptRole::User => graph::TranscriptRole::User,
        protocol::TranscriptRole::Agent => graph::TranscriptRole::Agent,
        protocol::TranscriptRole::Collaborator => graph::TranscriptRole::Collaborator,
        protocol::TranscriptRole::Tool => graph::TranscriptRole::Tool,
    }
}

const fn map_field(value: protocol::TranscriptField) -> graph::TranscriptField {
    match value {
        protocol::TranscriptField::Message => graph::TranscriptField::Message,
        protocol::TranscriptField::ContentText => graph::TranscriptField::ContentText,
        protocol::TranscriptField::Arguments => graph::TranscriptField::Arguments,
        protocol::TranscriptField::Input => graph::TranscriptField::Input,
        protocol::TranscriptField::Output => graph::TranscriptField::Output,
        protocol::TranscriptField::Stdout => graph::TranscriptField::Stdout,
        protocol::TranscriptField::Stderr => graph::TranscriptField::Stderr,
        protocol::TranscriptField::AggregatedOutput => graph::TranscriptField::AggregatedOutput,
        protocol::TranscriptField::LastAgentMessage => graph::TranscriptField::LastAgentMessage,
        protocol::TranscriptField::Query => graph::TranscriptField::Query,
        protocol::TranscriptField::Action => graph::TranscriptField::Action,
        protocol::TranscriptField::Invocation => graph::TranscriptField::Invocation,
        protocol::TranscriptField::Result => graph::TranscriptField::Result,
        protocol::TranscriptField::Changes => graph::TranscriptField::Changes,
        protocol::TranscriptField::Execution => graph::TranscriptField::Execution,
        protocol::TranscriptField::Tools => graph::TranscriptField::Tools,
    }
}

const fn map_knowledge_kind(value: transcript::KnowledgeKind) -> graph::KnowledgeKind {
    match value {
        transcript::KnowledgeKind::Goal => graph::KnowledgeKind::Goal,
        transcript::KnowledgeKind::Decision => graph::KnowledgeKind::Decision,
        transcript::KnowledgeKind::Constraint => graph::KnowledgeKind::Constraint,
        transcript::KnowledgeKind::Artifact => graph::KnowledgeKind::Artifact,
        transcript::KnowledgeKind::Dependency => graph::KnowledgeKind::Dependency,
        transcript::KnowledgeKind::Failure => graph::KnowledgeKind::Failure,
        transcript::KnowledgeKind::RootCauseHypothesis => graph::KnowledgeKind::RootCauseHypothesis,
        transcript::KnowledgeKind::Repair => graph::KnowledgeKind::Repair,
        transcript::KnowledgeKind::Verification => graph::KnowledgeKind::Verification,
        transcript::KnowledgeKind::Risk => graph::KnowledgeKind::Risk,
        transcript::KnowledgeKind::Lesson => graph::KnowledgeKind::Lesson,
        transcript::KnowledgeKind::OpenQuestion => graph::KnowledgeKind::OpenQuestion,
    }
}

const fn map_entity_kind(value: transcript::KnowledgeEntityKind) -> graph::KnowledgeEntityKind {
    match value {
        transcript::KnowledgeEntityKind::Project => graph::KnowledgeEntityKind::Project,
        transcript::KnowledgeEntityKind::Repository => graph::KnowledgeEntityKind::Repository,
        transcript::KnowledgeEntityKind::Module => graph::KnowledgeEntityKind::Module,
        transcript::KnowledgeEntityKind::File => graph::KnowledgeEntityKind::File,
        transcript::KnowledgeEntityKind::Command => graph::KnowledgeEntityKind::Command,
        transcript::KnowledgeEntityKind::Tool => graph::KnowledgeEntityKind::Tool,
        transcript::KnowledgeEntityKind::Dependency => graph::KnowledgeEntityKind::Dependency,
        transcript::KnowledgeEntityKind::Configuration => graph::KnowledgeEntityKind::Configuration,
        transcript::KnowledgeEntityKind::Environment => graph::KnowledgeEntityKind::Environment,
        transcript::KnowledgeEntityKind::Error => graph::KnowledgeEntityKind::Error,
        transcript::KnowledgeEntityKind::Concept => graph::KnowledgeEntityKind::Concept,
        transcript::KnowledgeEntityKind::Artifact => graph::KnowledgeEntityKind::Artifact,
        transcript::KnowledgeEntityKind::Other => graph::KnowledgeEntityKind::Other,
    }
}

const fn map_confidence(value: transcript::KnowledgeConfidence) -> graph::KnowledgeConfidence {
    match value {
        transcript::KnowledgeConfidence::Low => graph::KnowledgeConfidence::Low,
        transcript::KnowledgeConfidence::Medium => graph::KnowledgeConfidence::Medium,
        transcript::KnowledgeConfidence::High => graph::KnowledgeConfidence::High,
    }
}

const fn map_epistemic_status(value: transcript::EpistemicStatus) -> graph::EpistemicStatus {
    match value {
        transcript::EpistemicStatus::Explicit => graph::EpistemicStatus::Explicit,
        transcript::EpistemicStatus::Inferred => graph::EpistemicStatus::Inferred,
        transcript::EpistemicStatus::Hypothesis => graph::EpistemicStatus::Hypothesis,
    }
}

const fn map_predicate(value: transcript::KnowledgePredicate) -> graph::KnowledgePredicate {
    match value {
        transcript::KnowledgePredicate::Uses => graph::KnowledgePredicate::Uses,
        transcript::KnowledgePredicate::Modifies => graph::KnowledgePredicate::Modifies,
        transcript::KnowledgePredicate::DependsOn => graph::KnowledgePredicate::DependsOn,
        transcript::KnowledgePredicate::Causes => graph::KnowledgePredicate::Causes,
        transcript::KnowledgePredicate::BlockedBy => graph::KnowledgePredicate::BlockedBy,
        transcript::KnowledgePredicate::Resolves => graph::KnowledgePredicate::Resolves,
        transcript::KnowledgePredicate::Verifies => graph::KnowledgePredicate::Verifies,
        transcript::KnowledgePredicate::Produces => graph::KnowledgePredicate::Produces,
        transcript::KnowledgePredicate::ContributesTo => graph::KnowledgePredicate::ContributesTo,
        transcript::KnowledgePredicate::Contradicts => graph::KnowledgePredicate::Contradicts,
        transcript::KnowledgePredicate::RelatedTo => graph::KnowledgePredicate::RelatedTo,
    }
}

#[cfg(test)]
mod tests {
    use harness_graph_graph_port as graph;
    use harness_graph_protocol as protocol;
    use harness_graph_transcript_enrichment as transcript;

    use super::{
        convert_claim_subjects, map_confidence, map_entity_kind, map_epistemic_status, map_field,
        map_knowledge_kind, map_predicate, map_role,
    };

    #[test]
    fn transcript_enum_mappings_are_structure_preserving() {
        assert_eq!(
            map_role(protocol::TranscriptRole::User),
            graph::TranscriptRole::User
        );
        assert_eq!(
            map_role(protocol::TranscriptRole::Agent),
            graph::TranscriptRole::Agent
        );
        assert_eq!(
            map_role(protocol::TranscriptRole::Collaborator),
            graph::TranscriptRole::Collaborator
        );
        assert_eq!(
            map_role(protocol::TranscriptRole::Tool),
            graph::TranscriptRole::Tool
        );

        let fields = [
            protocol::TranscriptField::Message,
            protocol::TranscriptField::ContentText,
            protocol::TranscriptField::Arguments,
            protocol::TranscriptField::Input,
            protocol::TranscriptField::Output,
            protocol::TranscriptField::Stdout,
            protocol::TranscriptField::Stderr,
            protocol::TranscriptField::AggregatedOutput,
            protocol::TranscriptField::LastAgentMessage,
            protocol::TranscriptField::Query,
            protocol::TranscriptField::Action,
            protocol::TranscriptField::Invocation,
            protocol::TranscriptField::Result,
            protocol::TranscriptField::Changes,
            protocol::TranscriptField::Execution,
            protocol::TranscriptField::Tools,
        ];
        for field in fields {
            assert_eq!(field.as_str(), map_field(field).as_str());
        }
    }

    #[test]
    fn semantic_enum_mappings_preserve_every_closed_value() {
        let kinds = [
            transcript::KnowledgeKind::Goal,
            transcript::KnowledgeKind::Decision,
            transcript::KnowledgeKind::Constraint,
            transcript::KnowledgeKind::Artifact,
            transcript::KnowledgeKind::Dependency,
            transcript::KnowledgeKind::Failure,
            transcript::KnowledgeKind::RootCauseHypothesis,
            transcript::KnowledgeKind::Repair,
            transcript::KnowledgeKind::Verification,
            transcript::KnowledgeKind::Risk,
            transcript::KnowledgeKind::Lesson,
            transcript::KnowledgeKind::OpenQuestion,
        ];
        for kind in kinds {
            assert_eq!(
                format!("{kind:?}"),
                format!("{:?}", map_knowledge_kind(kind))
            );
        }

        let entities = [
            transcript::KnowledgeEntityKind::Project,
            transcript::KnowledgeEntityKind::Repository,
            transcript::KnowledgeEntityKind::Module,
            transcript::KnowledgeEntityKind::File,
            transcript::KnowledgeEntityKind::Command,
            transcript::KnowledgeEntityKind::Tool,
            transcript::KnowledgeEntityKind::Dependency,
            transcript::KnowledgeEntityKind::Configuration,
            transcript::KnowledgeEntityKind::Environment,
            transcript::KnowledgeEntityKind::Error,
            transcript::KnowledgeEntityKind::Concept,
            transcript::KnowledgeEntityKind::Artifact,
            transcript::KnowledgeEntityKind::Other,
        ];
        for kind in entities {
            assert_eq!(format!("{kind:?}"), format!("{:?}", map_entity_kind(kind)));
        }
    }

    #[test]
    fn epistemic_and_causal_mappings_do_not_strengthen_assertions() {
        assert_eq!(
            map_confidence(transcript::KnowledgeConfidence::Low),
            graph::KnowledgeConfidence::Low
        );
        assert_eq!(
            map_confidence(transcript::KnowledgeConfidence::Medium),
            graph::KnowledgeConfidence::Medium
        );
        assert_eq!(
            map_confidence(transcript::KnowledgeConfidence::High),
            graph::KnowledgeConfidence::High
        );
        assert_eq!(
            map_epistemic_status(transcript::EpistemicStatus::Explicit),
            graph::EpistemicStatus::Explicit
        );
        assert_eq!(
            map_epistemic_status(transcript::EpistemicStatus::Inferred),
            graph::EpistemicStatus::Inferred
        );
        assert_eq!(
            map_epistemic_status(transcript::EpistemicStatus::Hypothesis),
            graph::EpistemicStatus::Hypothesis
        );

        let predicates = [
            transcript::KnowledgePredicate::Uses,
            transcript::KnowledgePredicate::Modifies,
            transcript::KnowledgePredicate::DependsOn,
            transcript::KnowledgePredicate::Causes,
            transcript::KnowledgePredicate::BlockedBy,
            transcript::KnowledgePredicate::Resolves,
            transcript::KnowledgePredicate::Verifies,
            transcript::KnowledgePredicate::Produces,
            transcript::KnowledgePredicate::ContributesTo,
            transcript::KnowledgePredicate::Contradicts,
            transcript::KnowledgePredicate::RelatedTo,
        ];
        for predicate in predicates {
            assert_eq!(
                format!("{predicate:?}"),
                format!("{:?}", map_predicate(predicate))
            );
        }
    }

    #[test]
    fn claim_subject_mapping_preserves_session_and_multiple_entity_scope()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(matches!(
            convert_claim_subjects(&transcript::ClaimSubjects::SessionWide)?,
            graph::KnowledgeClaimSubjects::SessionWide
        ));

        let first = transcript::KnowledgeEntity::new(
            transcript::KnowledgeEntityKind::Project,
            transcript::KnowledgeEntityLabel::new("HarnessGraph")?,
        );
        let second = transcript::KnowledgeEntity::new(
            transcript::KnowledgeEntityKind::Repository,
            transcript::KnowledgeEntityLabel::new("harness-graph")?,
        );
        let source = transcript::ClaimSubjects::entities([first.id(), second.id()])?;
        let converted = convert_claim_subjects(&source)?;
        let actual = converted.iter().map(|id| id.to_hex()).collect::<Vec<_>>();
        let mut expected = vec![first.id().to_hex(), second.id().to_hex()];
        expected.sort();

        assert_eq!(actual, expected);
        Ok(())
    }
}
