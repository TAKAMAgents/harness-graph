//! Full-process E2E coverage for the foundation ingestion slice.

use std::{
    io::Write,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const SESSION_ID: &str = "019c63db-2995-74c3-b898-c1b92a8e1317";

fn fixture_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    Ok(manifest.join("../../fixtures/source-safe").canonicalize()?)
}

fn command() -> Result<Command, Box<dyn std::error::Error>> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_harness-graph"));
    command
        .current_dir(std::env::temp_dir())
        .env("CODEX_SESSION_RAW_DATA_PATH", fixture_root()?)
        .env("NEO4J_CONNECTION_URL", "neo4j://127.0.0.1:7687")
        .env("NEO4J_USERNAME", "neo4j")
        .env("NEO4J_PASSWORD", "source-safe-test-password")
        .env("MISTRAL_API_KEY", "source-safe-test-key")
        .env("MISTRAL_MODEL", "mistral-small-latest")
        .env_remove("NEO4J_CONECTION_URL")
        .env_remove("NEO4J_INATANSE_PASSWORD")
        .env_remove("MISTARL_API_KEY");
    Ok(command)
}

fn run_json(arguments: &[&str]) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let output = command()?.args(arguments).output()?;
    if !output.status.success() {
        return Err(format!(
            "command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

#[test]
fn doctor_reports_mistral_without_exposing_secrets() -> Result<(), Box<dyn std::error::Error>> {
    let output = command()?.arg("doctor").output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("\"foundation_model_provider\": \"mistral\""));
    assert!(!stdout.contains("source-safe-test-key"));
    assert!(!stdout.contains("source-safe-test-password"));
    Ok(())
}

#[test]
fn archive_discovery_verification_and_streaming_are_end_to_end()
-> Result<(), Box<dyn std::error::Error>> {
    let discovery = run_json(&["discover", "--scope", "all"])?;
    assert_eq!(discovery["unique_sessions"], 1);
    assert_eq!(discovery["sessions"][0]["session_id"], SESSION_ID);

    let verification = run_json(&["verify", "--session-id", SESSION_ID])?;
    assert_eq!(verification["status"], "verified");
    assert_eq!(verification["expected_records"], 12);

    let inspection = run_json(&["inspect", "--session-id", SESSION_ID])?;
    assert_eq!(inspection["status"], "inspected");
    assert_eq!(inspection["known_records"], 11);
    assert_eq!(inspection["quarantined_records"], 1);
    assert_eq!(inspection["total_records"], 12);
    Ok(())
}

#[test]
fn deterministic_analysis_preserves_partial_calls_and_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let output = run_json(&["analyze", "--session-id", SESSION_ID])?;
    assert_eq!(output["status"], "analyzed");
    assert_eq!(output["analysis"]["tool_calls"], 2);
    assert_eq!(output["analysis"]["completed_tool_calls"], 1);
    assert_eq!(output["analysis"]["pending_tool_calls"], 0);
    assert_eq!(output["analysis"]["interrupted_tool_calls"], 0);
    assert_eq!(output["analysis"]["orphaned_tool_results"], 1);
    assert_eq!(output["analysis"]["outcome_class"], "unverified_completion");
    assert_eq!(output["analysis"]["verification_status"], "missing");
    assert_eq!(output["analysis"]["risk_exposures"], 2);
    assert_eq!(output["analysis"]["semantic_activities"], 4);
    assert_eq!(output["analysis"]["path_steps"], 4);
    assert_eq!(
        output["analysis"]["path_signature"].as_str().map(str::len),
        Some(64)
    );
    Ok(())
}

#[test]
fn tampered_bundle_fails_before_semantic_parsing() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("active/2026-02-16").join(SESSION_ID);
    std::fs::create_dir_all(destination.join("raw"))?;
    let source = fixture_root()?.join("active/2026-02-16").join(SESSION_ID);
    for file in ["README.md", "metadata.json", "checksums.sha256"] {
        std::fs::copy(source.join(file), destination.join(file))?;
    }
    std::fs::copy(
        source.join("raw/rollout.jsonl"),
        destination.join("raw/rollout.jsonl"),
    )?;
    std::fs::OpenOptions::new()
        .append(true)
        .open(destination.join("raw/rollout.jsonl"))?
        .write_all(b"\n")?;

    let output = command()?
        .env("CODEX_SESSION_RAW_DATA_PATH", temporary.path())
        .args(["verify", "--session-id", SESSION_ID])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)?.contains("checksum verification failed"));
    Ok(())
}

#[tokio::test]
async fn serve_process_durably_accepts_and_replays_live_events()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = reservation.local_addr()?;
    drop(reservation);
    let child = command()?
        .env("HARNESS_GRAPH_BIND_ADDRESS", address.to_string())
        .env(
            "HARNESS_GRAPH_JOURNAL_PATH",
            temporary.path().join("live.jsonl"),
        )
        .arg("serve")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let mut server = ServerProcess::new(child);
    let client = reqwest::Client::new();
    let base = format!("http://{address}");
    wait_until_ready(&client, &base).await?;
    let event_id = uuid::Uuid::now_v7().to_string();
    let event = serde_json::json!({
        "event_id": event_id,
        "session_id": "ses_cli_e2e",
        "occurred_at": "2026-07-18T12:00:00Z",
        "payload": {
            "type": "activity_observed",
            "kind": "verify",
            "status": "succeeded"
        }
    });

    let appended = client
        .post(format!("{base}/v1/live/events"))
        .json(&event)
        .send()
        .await?;
    assert_eq!(appended.status(), reqwest::StatusCode::CREATED);
    let replay = client
        .get(format!("{base}/v1/live/events"))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;
    assert_eq!(replay["entries"].as_array().map(Vec::len), Some(1));
    assert_eq!(replay["entries"][0]["event"]["event_id"], event_id);

    server.stop()?;
    Ok(())
}

async fn wait_until_ready(
    client: &reqwest::Client,
    base: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match client.get(format!("{base}/health")).send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(_) | Err(_) if Instant::now() < deadline => tokio::task::yield_now().await,
            Ok(response) => {
                return Err(format!("server health returned {}", response.status()).into());
            }
            Err(error) => return Err(format!("server did not become ready: {error}").into()),
        }
    }
}

struct ServerProcess {
    child: Option<Child>,
}

impl ServerProcess {
    const fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn stop(&mut self) -> Result<(), std::io::Error> {
        if let Some(mut child) = self.child.take() {
            child.kill()?;
            child.wait()?;
        }
        Ok(())
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
