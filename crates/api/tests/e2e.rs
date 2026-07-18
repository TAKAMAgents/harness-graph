//! Real-socket E2E coverage for journal ingestion, idempotency, replay, and SSE.

use std::{path::PathBuf, time::Duration};

use futures_util::StreamExt;
use harness_graph_event_journal::{AppendOnlyJournal, JournalPath};
use harness_graph_graph_port::{ExperienceEnrichmentVisibility, ExperienceReader, ExperienceScope};
use harness_graph_neo4j_adapter::Neo4jAdapter;
use secrecy::SecretString;
use serde_json::json;
use url::Url;

#[tokio::test]
async fn durable_http_ingestion_replay_and_sse_are_end_to_end()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let journal_path = JournalPath::new(directory.path().join("live.jsonl"))?;
    let journal = AppendOnlyJournal::open(&journal_path)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server =
        tokio::spawn(
            async move { axum::serve(listener, harness_graph_api::router(journal)).await },
        );
    let client = reqwest::Client::new();
    let event = json!({
        "event_id": "019d2a40-7324-77a2-832c-f5f9f84473b0",
        "session_id": "ses_api_e2e",
        "occurred_at": "2026-07-18T12:00:00Z",
        "payload": { "type": "session_started" }
    });

    let first = client
        .post(format!("http://{address}/v1/live/events"))
        .json(&event)
        .send()
        .await?;
    assert_eq!(first.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        first.json::<serde_json::Value>().await?["status"],
        "appended"
    );

    let duplicate = client
        .post(format!("http://{address}/v1/live/events"))
        .json(&event)
        .send()
        .await?;
    assert_eq!(duplicate.status(), reqwest::StatusCode::OK);
    assert_eq!(
        duplicate.json::<serde_json::Value>().await?["status"],
        "duplicate"
    );

    let replay = client
        .get(format!("http://{address}/v1/live/events"))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;
    assert_eq!(replay["entries"].as_array().map(Vec::len), Some(1));
    assert_eq!(replay["entries"][0]["sequence"], 1);

    let response = client
        .get(format!("http://{address}/v1/live/events/stream"))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.headers().get(reqwest::header::CONTENT_TYPE),
        Some(&reqwest::header::HeaderValue::from_static(
            "text/event-stream"
        ))
    );
    let mut stream = response.bytes_stream();
    let chunk = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await?
        .ok_or("SSE stream ended before replay")??;
    let event_text = String::from_utf8(chunk.to_vec())?;
    assert!(event_text.contains("event: live_event"));
    assert!(event_text.contains("id: 1"));

    server.abort();
    let _cancelled = server.await;
    drop(client);
    let reopened =
        AppendOnlyJournal::open(&JournalPath::new(PathBuf::from(journal_path.as_path()))?)?;
    assert_eq!(
        reopened
            .replay(harness_graph_event_journal::ReplayCursor::Beginning)
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires configured real Neo4j with at least one verified session"]
async fn real_socket_experience_routes_read_real_neo4j_without_sensitive_fields()
-> Result<(), Box<dyn std::error::Error>> {
    let connection_url = required_setting("NEO4J_CONNECTION_URL", "NEO4J_CONECTION_URL")?;
    let password = required_setting("NEO4J_PASSWORD", "NEO4J_INATANSE_PASSWORD")?;
    let username = optional_setting("NEO4J_USERNAME").unwrap_or_else(|| "neo4j".to_owned());
    let namespace = harness_graph_domain::GraphNamespace::new(
        optional_setting("HARNESS_GRAPH_NAMESPACE").unwrap_or_else(|| "default".to_owned()),
    )?;
    let url = Url::parse(&connection_url)?;
    let host = url.host_str().ok_or("Neo4j URL requires a host")?;
    let address = format!("{host}:{}", url.port().unwrap_or(7687));
    let adapter = Neo4jAdapter::connect(&address, &username, SecretString::from(password)).await?;

    let directory = tempfile::tempdir()?;
    let journal = AppendOnlyJournal::open(&JournalPath::new(
        directory.path().join("experience-live.jsonl"),
    )?)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let socket = listener.local_addr()?;
    let scope = ExperienceScope::new(namespace, ExperienceEnrichmentVisibility::Enabled);
    let direct_sessions = adapter.experience_sessions(&scope).await?;
    if direct_sessions.iter().next().is_none() {
        return Err("real Neo4j namespace has no verified experience session".into());
    }
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            harness_graph_api::router_with_experience(journal, adapter, scope),
        )
        .await
    });
    let client = reqwest::Client::new();

    let list_response = client
        .get(format!("http://{socket}/v1/experience/sessions"))
        .send()
        .await?;
    assert_eq!(list_response.status(), reqwest::StatusCode::OK);
    let list = list_response.json::<serde_json::Value>().await?;
    assert_source_safe_json(&list)?;
    let first = list["sessions"]
        .as_array()
        .and_then(|sessions| sessions.first())
        .ok_or("real Neo4j namespace has no verified experience session")?;
    let session_id = first["session_id"]
        .as_str()
        .ok_or("experience list session identity is missing")?;

    let detail_response = client
        .get(format!(
            "http://{socket}/v1/experience/sessions/{session_id}"
        ))
        .send()
        .await?;
    assert_eq!(detail_response.status(), reqwest::StatusCode::OK);
    let detail = detail_response.json::<serde_json::Value>().await?;
    assert_eq!(detail["session_id"], session_id);
    assert_source_safe_json(&detail)?;
    assert!(detail["activities"].is_array());
    assert!(detail["source_anchors"].is_array());

    server.abort();
    let _cancelled = server.await;
    Ok(())
}

fn assert_source_safe_json(value: &serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
    const FORBIDDEN_KEYS: &[&str] = &[
        "key",
        "field_path",
        "raw_transcript",
        "local_path",
        "provider_body",
        "request_body",
        "excerpt",
    ];
    match value {
        serde_json::Value::Object(values) => {
            for (key, child) in values {
                if FORBIDDEN_KEYS.contains(&key.as_str()) {
                    return Err(format!("experience response exposed forbidden field {key}").into());
                }
                assert_source_safe_json(child)?;
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                assert_source_safe_json(child)?;
            }
        }
        serde_json::Value::String(text) => {
            for forbidden in [
                "MISTRAL_API_KEY",
                "-----BEGIN PRIVATE KEY-----",
                "/Users/",
                "file://",
            ] {
                if text.contains(forbidden) {
                    return Err("experience response exposed sensitive content".into());
                }
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
    Ok(())
}

fn required_setting(
    canonical: &'static str,
    alias: &'static str,
) -> Result<String, Box<dyn std::error::Error>> {
    file_setting(canonical)
        .or_else(|| file_setting(alias))
        .or_else(|| process_setting(canonical))
        .or_else(|| process_setting(alias))
        .ok_or_else(|| format!("missing {canonical}").into())
}

fn optional_setting(name: &str) -> Option<String> {
    file_setting(name).or_else(|| process_setting(name))
}

fn file_setting(name: &str) -> Option<String> {
    dotenvy::dotenv_iter().ok().and_then(|iterator| {
        iterator
            .filter_map(Result::ok)
            .find_map(|(key, value)| (key == name).then_some(value))
    })
}

fn process_setting(name: &str) -> Option<String> {
    std::env::var(name).ok()
}
