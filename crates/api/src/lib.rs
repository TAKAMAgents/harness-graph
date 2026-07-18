//! Axum live-event ingestion, server-sent events, and source-safe experience reads.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response, Sse, sse::Event},
    routing::{get, post},
};
use futures_util::{Stream, StreamExt, stream};
use harness_graph_event_journal::{
    AppendOnlyJournal, AppendOutcome, JournalEntry, JournalError, JournalSequence, LiveEvent,
    ReplayCursor,
};
use harness_graph_graph_port::{
    ExperienceReader, ExperienceScope, ExperienceSessionLookup, ExperienceSessionQuery,
    ExperienceSessionSummaries,
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};

const MAX_EVENT_BODY_BYTES: usize = 16 * 1_024;
const BROADCAST_CAPACITY: usize = 1_024;

/// API service construction or durable journal access failure.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// Journal validation or durability failure.
    #[error(transparent)]
    Journal(#[from] JournalError),

    /// A worker thread failed while accessing the blocking durable file.
    #[error("journal worker failed: {source}")]
    Worker {
        /// Tokio worker join failure.
        #[source]
        source: tokio::task::JoinError,
    },

    /// The journal mutex was poisoned by a failed filesystem operation.
    #[error("journal synchronization state is poisoned")]
    PoisonedJournal,

    /// A session path parameter failed typed identity validation.
    #[error("invalid experience session identifier")]
    InvalidExperienceSessionId,

    /// The read-only experience store could not satisfy the request.
    #[error("experience graph is unavailable")]
    ExperienceUnavailable,

    /// No verified deterministic experience exists for the requested session.
    #[error("experience session was not found")]
    ExperienceSessionNotFound,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::Journal(JournalError::InvalidSequence) => (
                StatusCode::BAD_REQUEST,
                "invalid_replay_cursor",
                "replay cursor must be greater than zero",
            ),
            Self::Journal(JournalError::EventIdentityConflict) => (
                StatusCode::CONFLICT,
                "event_identity_conflict",
                "event identity already exists with different content",
            ),
            Self::Journal(
                JournalError::InvalidSessionId | JournalError::InvalidTimestamp { .. },
            ) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_live_event",
                "live event failed typed validation",
            ),
            Self::Journal(_) | Self::Worker { .. } | Self::PoisonedJournal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "journal_unavailable",
                "durable live journal is unavailable",
            ),
            Self::InvalidExperienceSessionId => (
                StatusCode::BAD_REQUEST,
                "invalid_experience_session_id",
                "session identifier must be a UUID",
            ),
            Self::ExperienceUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "experience_unavailable",
                "the source-safe experience graph is unavailable",
            ),
            Self::ExperienceSessionNotFound => (
                StatusCode::NOT_FOUND,
                "experience_session_not_found",
                "no verified experience exists for this session",
            ),
        };
        (status, Json(ErrorOutput { code, message })).into_response()
    }
}

#[derive(Debug, Serialize)]
struct ErrorOutput {
    code: &'static str,
    message: &'static str,
}

#[derive(Clone)]
struct JournalService {
    journal: Arc<Mutex<AppendOnlyJournal>>,
}

impl JournalService {
    fn new(journal: AppendOnlyJournal) -> Self {
        Self {
            journal: Arc::new(Mutex::new(journal)),
        }
    }

    async fn append(&self, event: LiveEvent) -> Result<AppendOutcome, ApiError> {
        let journal = Arc::clone(&self.journal);
        tokio::task::spawn_blocking(move || {
            journal
                .lock()
                .map_err(|_| ApiError::PoisonedJournal)?
                .append(event)
                .map_err(ApiError::from)
        })
        .await
        .map_err(|source| ApiError::Worker { source })?
    }

    async fn replay(&self, cursor: ReplayCursor) -> Result<Vec<JournalEntry>, ApiError> {
        let journal = Arc::clone(&self.journal);
        tokio::task::spawn_blocking(move || {
            journal
                .lock()
                .map_err(|_| ApiError::PoisonedJournal)
                .map(|journal| journal.replay(cursor))
        })
        .await
        .map_err(|source| ApiError::Worker { source })?
    }
}

#[derive(Clone)]
struct ApiState {
    journal: JournalService,
    sender: broadcast::Sender<JournalEntry>,
}

/// Build the production router over a verified append-only journal.
pub fn router(journal: AppendOnlyJournal) -> Router {
    let (sender, _) = broadcast::channel(BROADCAST_CAPACITY);
    let state = ApiState {
        journal: JournalService::new(journal),
        sender,
    };
    Router::new()
        .route("/health", get(health))
        .route("/v1/live/events", post(append_event).get(replay_events))
        .route("/v1/live/events/stream", get(stream_events))
        .layer(DefaultBodyLimit::max(MAX_EVENT_BODY_BYTES))
        .with_state(state)
}

/// Build the journal router plus typed read-only experience routes.
///
/// The reader error is intentionally erased at the HTTP boundary so driver,
/// topology, credential, and query details can never enter a response body.
pub fn router_with_experience<R>(
    journal: AppendOnlyJournal,
    reader: R,
    scope: ExperienceScope,
) -> Router
where
    R: ExperienceReader + 'static,
{
    router(journal).merge(
        Router::new()
            .route(
                "/v1/experience/sessions",
                get(list_experience_sessions::<R>),
            )
            .route(
                "/v1/experience/sessions/{session_id}",
                get(read_experience_session::<R>),
            )
            .with_state(ExperienceState::new(reader, scope)),
    )
}

struct ExperienceState<R> {
    reader: Arc<R>,
    scope: ExperienceScope,
}

impl<R> ExperienceState<R> {
    fn new(reader: R, scope: ExperienceScope) -> Self {
        Self {
            reader: Arc::new(reader),
            scope,
        }
    }
}

impl<R> Clone for ExperienceState<R> {
    fn clone(&self) -> Self {
        Self {
            reader: Arc::clone(&self.reader),
            scope: self.scope.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
struct ExperienceSessionListOutput {
    sessions: ExperienceSessionSummaries,
}

async fn list_experience_sessions<R>(
    State(state): State<ExperienceState<R>>,
) -> Result<Json<ExperienceSessionListOutput>, ApiError>
where
    R: ExperienceReader + 'static,
{
    let sessions = state
        .reader
        .experience_sessions(&state.scope)
        .await
        .map_err(|_| ApiError::ExperienceUnavailable)?;
    Ok(Json(ExperienceSessionListOutput { sessions }))
}

async fn read_experience_session<R>(
    State(state): State<ExperienceState<R>>,
    Path(session_id): Path<String>,
) -> Result<Response, ApiError>
where
    R: ExperienceReader + 'static,
{
    let session_id = harness_graph_domain::SessionId::parse(&session_id)
        .map_err(|_| ApiError::InvalidExperienceSessionId)?;
    let lookup = state
        .reader
        .experience_session(&ExperienceSessionQuery::new(
            state.scope.clone(),
            session_id,
        ))
        .await
        .map_err(|_| ApiError::ExperienceUnavailable)?;
    match lookup {
        ExperienceSessionLookup::Found(detail) => Ok(Json(detail).into_response()),
        ExperienceSessionLookup::NotFound => Err(ApiError::ExperienceSessionNotFound),
    }
}

#[derive(Debug, Serialize)]
struct HealthOutput {
    status: &'static str,
    journal: &'static str,
}

async fn health() -> Json<HealthOutput> {
    Json(HealthOutput {
        status: "ready",
        journal: "append_only",
    })
}

#[derive(Debug, Serialize)]
struct AppendOutput {
    status: &'static str,
    entry: JournalEntry,
}

async fn append_event(
    State(state): State<ApiState>,
    Json(event): Json<LiveEvent>,
) -> Result<(StatusCode, Json<AppendOutput>), ApiError> {
    let outcome = state.journal.append(event).await?;
    let status = if outcome.is_appended() {
        let entry = outcome.entry().clone();
        drop(state.sender.send(entry));
        "appended"
    } else {
        "duplicate"
    };
    let http_status = if outcome.is_appended() {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((
        http_status,
        Json(AppendOutput {
            status,
            entry: outcome.entry().clone(),
        }),
    ))
}

#[derive(Debug, Deserialize)]
struct ReplayQuery {
    after: Option<u64>,
}

impl ReplayQuery {
    fn cursor(&self) -> Result<ReplayCursor, JournalError> {
        self.after.map_or(Ok(ReplayCursor::Beginning), |value| {
            JournalSequence::new(value).map(ReplayCursor::After)
        })
    }
}

#[derive(Debug, Serialize)]
struct ReplayOutput {
    entries: Vec<JournalEntry>,
}

async fn replay_events(
    State(state): State<ApiState>,
    Query(query): Query<ReplayQuery>,
) -> Result<Json<ReplayOutput>, ApiError> {
    let entries = state.journal.replay(query.cursor()?).await?;
    Ok(Json(ReplayOutput { entries }))
}

async fn stream_events(
    State(state): State<ApiState>,
    Query(query): Query<ReplayQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, axum::Error>>>, ApiError> {
    let receiver = state.sender.subscribe();
    let history = state.journal.replay(query.cursor()?).await?;
    let watermark = history
        .last()
        .map_or(query.after.unwrap_or(0), |entry| entry.sequence().value());
    let historical = stream::iter(history.into_iter().map(sse_entry));
    let live = BroadcastStream::new(receiver).filter_map(move |result| {
        let event = match result {
            Ok(entry) if entry.sequence().value() > watermark => Some(sse_entry(entry)),
            Ok(_) => None,
            Err(BroadcastStreamRecvError::Lagged(skipped)) => Some(Ok(Event::default()
                .event("stream_lagged")
                .data(skipped.to_string()))),
        };
        std::future::ready(event)
    });
    Ok(Sse::new(historical.chain(live)).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

fn sse_entry(entry: JournalEntry) -> Result<Event, axum::Error> {
    Event::default()
        .id(entry.sequence().value().to_string())
        .event("live_event")
        .json_data(entry)
}
