//! Source-safe application failure classification for the Neo4j boundary.

use std::io::ErrorKind;

use harness_graph_enrichment_application::ClassifiedEnrichmentFailure;
use harness_graph_graph_port::EnrichmentFailureClass;
use neo4rs::{
    Error as Neo4jDriverError, Neo4jClientErrorKind, Neo4jErrorKind, Neo4jSecurityErrorKind,
};

use crate::Neo4jAdapterError;

impl ClassifiedEnrichmentFailure for Neo4jAdapterError {
    fn enrichment_failure_class(&self) -> EnrichmentFailureClass {
        match self {
            Self::Connection { source } | Self::Operation { source, .. } => {
                classify_driver_error(source)
            }
            Self::IntegerRange { .. }
            | Self::TimestampFormat { .. }
            | Self::InvalidReadResult { .. }
            | Self::InvalidSemanticProperty { .. }
            | Self::Planning(_)
            | Self::Domain(_)
            | Self::Enrichment(_)
            | Self::Experience(_)
            | Self::ConflictingEnrichment { .. }
            | Self::EnrichmentTransition { .. } => EnrichmentFailureClass::Projection,
        }
    }
}

fn classify_driver_error(source: &Neo4jDriverError) -> EnrichmentFailureClass {
    match source {
        Neo4jDriverError::IOError { detail } if detail.kind() == ErrorKind::TimedOut => {
            EnrichmentFailureClass::Timeout
        }
        Neo4jDriverError::IOError { .. } | Neo4jDriverError::ConnectionError => {
            EnrichmentFailureClass::Transport
        }
        Neo4jDriverError::AuthenticationError(_) => EnrichmentFailureClass::Authentication,
        Neo4jDriverError::Neo4j(error) => classify_server_error(error.kind()),
        // neo4rs deliberately marks its error enum non-exhaustive. Unknown
        // structural and future variants must remain terminal until their
        // retry semantics are understood and represented by an explicit typed
        // branch. The pure test below enumerates every current structural
        // variant so a dependency upgrade remains reviewable.
        _ => EnrichmentFailureClass::Projection,
    }
}

const fn classify_server_error(kind: Neo4jErrorKind) -> EnrichmentFailureClass {
    match kind {
        Neo4jErrorKind::Transient => EnrichmentFailureClass::TemporarilyUnavailable,
        Neo4jErrorKind::Client(client) => classify_client_error(client),
        Neo4jErrorKind::Database | Neo4jErrorKind::Unknown => EnrichmentFailureClass::Projection,
    }
}

const fn classify_client_error(kind: Neo4jClientErrorKind) -> EnrichmentFailureClass {
    match kind {
        Neo4jClientErrorKind::Security(security) => classify_security_error(security),
        Neo4jClientErrorKind::SessionExpired => EnrichmentFailureClass::TemporarilyUnavailable,
        Neo4jClientErrorKind::FatalDiscovery
        | Neo4jClientErrorKind::TransactionTerminated
        | Neo4jClientErrorKind::ProtocolViolation
        | Neo4jClientErrorKind::Other
        | Neo4jClientErrorKind::Unknown => EnrichmentFailureClass::Projection,
    }
}

const fn classify_security_error(kind: Neo4jSecurityErrorKind) -> EnrichmentFailureClass {
    match kind {
        Neo4jSecurityErrorKind::AuthorizationExpired => {
            EnrichmentFailureClass::TemporarilyUnavailable
        }
        Neo4jSecurityErrorKind::Authentication
        | Neo4jSecurityErrorKind::TokenExpired
        | Neo4jSecurityErrorKind::Other
        | Neo4jSecurityErrorKind::Unknown => EnrichmentFailureClass::Authentication,
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use harness_graph_domain::DomainError;
    use harness_graph_enrichment_application::ClassifiedEnrichmentFailure;
    use harness_graph_graph_port::{
        EnrichmentFailureClass, EnrichmentGraphError, ExperienceGraphError,
    };
    use harness_graph_planning::PlanningError;
    use neo4rs::{
        DeError, Error as Neo4jDriverError, Neo4jClientErrorKind, Neo4jErrorKind,
        Neo4jSecurityErrorKind,
    };

    use super::{Neo4jAdapterError, classify_driver_error, classify_server_error};

    #[test]
    fn adapter_errors_exhaustively_fail_closed_except_typed_driver_failures() {
        let terminal_errors = [
            Neo4jAdapterError::IntegerRange { field: "count" },
            Neo4jAdapterError::TimestampFormat {
                source: time::error::Format::InvalidComponent("timestamp"),
            },
            Neo4jAdapterError::InvalidReadResult { field: "run" },
            Neo4jAdapterError::InvalidSemanticProperty { field: "status" },
            Neo4jAdapterError::Planning(PlanningError::InvalidPrecedentLimit),
            Neo4jAdapterError::Domain(DomainError::InvalidGraphNamespace),
            Neo4jAdapterError::Enrichment(EnrichmentGraphError::InvalidChunkCount),
            Neo4jAdapterError::Experience(ExperienceGraphError::MissingNarrativeEpisode),
            Neo4jAdapterError::ConflictingEnrichment { object: "run" },
            Neo4jAdapterError::EnrichmentTransition {
                transition: "complete run",
            },
        ];

        for error in terminal_errors {
            assert_eq!(
                error.enrichment_failure_class(),
                EnrichmentFailureClass::Projection
            );
        }

        let connection = Neo4jAdapterError::Connection {
            source: Neo4jDriverError::ConnectionError,
        };
        assert_eq!(
            connection.enrichment_failure_class(),
            EnrichmentFailureClass::Transport
        );

        let operation = Neo4jAdapterError::Operation {
            operation: "project chunk",
            source: Neo4jDriverError::AuthenticationError("not retained".to_owned()),
        };
        assert_eq!(
            operation.enrichment_failure_class(),
            EnrichmentFailureClass::Authentication
        );
    }

    #[test]
    fn current_driver_error_variants_have_closed_source_safe_classes() {
        let timeout = Neo4jDriverError::IOError {
            detail: io::Error::new(io::ErrorKind::TimedOut, "private transport detail"),
        };
        assert_eq!(
            classify_driver_error(&timeout),
            EnrichmentFailureClass::Timeout
        );

        let transport = [
            Neo4jDriverError::IOError {
                detail: io::Error::new(io::ErrorKind::ConnectionReset, "private transport detail"),
            },
            Neo4jDriverError::ConnectionError,
        ];
        for error in transport {
            assert_eq!(
                classify_driver_error(&error),
                EnrichmentFailureClass::Transport
            );
        }

        let authentication = Neo4jDriverError::AuthenticationError("private auth detail".into());
        assert_eq!(
            classify_driver_error(&authentication),
            EnrichmentFailureClass::Authentication
        );

        let terminal = [
            Neo4jDriverError::UrlParseError(url::ParseError::EmptyHost),
            Neo4jDriverError::UnsupportedScheme("unsupported".into()),
            Neo4jDriverError::InvalidDnsName("invalid".into()),
            Neo4jDriverError::StringTooLong,
            Neo4jDriverError::MapTooBig,
            Neo4jDriverError::BytesTooBig,
            Neo4jDriverError::ListTooLong,
            Neo4jDriverError::InvalidConfig,
            Neo4jDriverError::UnsupportedVersion("unsupported".into()),
            Neo4jDriverError::UnexpectedMessage("unexpected".into()),
            Neo4jDriverError::UnknownType("unknown".into()),
            Neo4jDriverError::UnknownMessage("unknown".into()),
            Neo4jDriverError::ConversionError,
            Neo4jDriverError::InvalidTypeMarker("invalid".into()),
            Neo4jDriverError::DeserializationError(DeError::Other("invalid".into())),
        ];
        for error in terminal {
            assert_eq!(
                classify_driver_error(&error),
                EnrichmentFailureClass::Projection
            );
        }
    }

    #[test]
    fn every_server_error_kind_has_conservative_retry_semantics() {
        let retryable = [
            Neo4jErrorKind::Transient,
            Neo4jErrorKind::Client(Neo4jClientErrorKind::SessionExpired),
            Neo4jErrorKind::Client(Neo4jClientErrorKind::Security(
                Neo4jSecurityErrorKind::AuthorizationExpired,
            )),
        ];
        for kind in retryable {
            assert_eq!(
                classify_server_error(kind),
                EnrichmentFailureClass::TemporarilyUnavailable
            );
        }

        let authentication = [
            Neo4jSecurityErrorKind::Authentication,
            Neo4jSecurityErrorKind::TokenExpired,
            Neo4jSecurityErrorKind::Other,
            Neo4jSecurityErrorKind::Unknown,
        ];
        for security in authentication {
            assert_eq!(
                classify_server_error(Neo4jErrorKind::Client(Neo4jClientErrorKind::Security(
                    security
                ),)),
                EnrichmentFailureClass::Authentication
            );
        }

        let terminal = [
            Neo4jErrorKind::Client(Neo4jClientErrorKind::FatalDiscovery),
            Neo4jErrorKind::Client(Neo4jClientErrorKind::TransactionTerminated),
            Neo4jErrorKind::Client(Neo4jClientErrorKind::ProtocolViolation),
            Neo4jErrorKind::Client(Neo4jClientErrorKind::Other),
            Neo4jErrorKind::Client(Neo4jClientErrorKind::Unknown),
            Neo4jErrorKind::Database,
            Neo4jErrorKind::Unknown,
        ];
        for kind in terminal {
            assert_eq!(
                classify_server_error(kind),
                EnrichmentFailureClass::Projection
            );
        }
    }
}
