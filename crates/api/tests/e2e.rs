//! Real-socket E2E coverage for journal ingestion, idempotency, replay, and SSE.

use std::{path::PathBuf, time::Duration};

use futures_util::StreamExt;
use harness_graph_event_journal::{AppendOnlyJournal, JournalPath};
use serde_json::json;

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
