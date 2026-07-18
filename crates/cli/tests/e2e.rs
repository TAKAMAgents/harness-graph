//! Full-process E2E coverage for the foundation ingestion slice.

use std::{
    collections::BTreeMap,
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    time::{Duration, Instant},
};

use neo4rs::{Graph, query};
use sha2::{Digest, Sha256};

const SESSION_ID: &str = "019c63db-2995-74c3-b898-c1b92a8e1317";
const SECOND_SESSION_ID: &str = "019c63db-2995-74c3-b898-c1b92a8e1318";
const METADATA_ONLY_SESSION_ID: &str = "019c63db-2995-74c3-b898-c1b92a8e1319";

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

struct RepositoryEnvironment {
    neo4j_url: String,
    neo4j_username: String,
    neo4j_password: String,
    mistral_api_key: String,
    mistral_model: String,
}

impl RepositoryEnvironment {
    fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let repository_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let values = dotenvy::from_path_iter(repository_root.join(".env"))?
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        Ok(Self {
            neo4j_url: required_environment_value(
                &values,
                "NEO4J_CONNECTION_URL",
                "NEO4J_CONECTION_URL",
            )?,
            neo4j_username: values
                .get("NEO4J_USERNAME")
                .cloned()
                .unwrap_or_else(|| "neo4j".to_owned()),
            neo4j_password: required_environment_value(
                &values,
                "NEO4J_PASSWORD",
                "NEO4J_INATANSE_PASSWORD",
            )?,
            mistral_api_key: values
                .get("MISTRAL_API_KEY")
                .filter(|value| !value.trim().is_empty())
                .cloned()
                .ok_or("repository .env is missing MISTRAL_API_KEY")?,
            mistral_model: values
                .get("MISTRAL_MODEL")
                .cloned()
                .unwrap_or_else(|| "mistral-small-latest".to_owned()),
        })
    }

    fn command(&self, archive_root: &Path, namespace: &str) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_harness-graph"));
        command
            .current_dir(std::env::temp_dir())
            .env("CODEX_SESSION_RAW_DATA_PATH", archive_root)
            .env("NEO4J_CONNECTION_URL", &self.neo4j_url)
            .env("NEO4J_USERNAME", &self.neo4j_username)
            .env("NEO4J_PASSWORD", &self.neo4j_password)
            .env("HARNESS_GRAPH_NAMESPACE", namespace)
            .env("MISTRAL_API_KEY", &self.mistral_api_key)
            .env("MISTRAL_MODEL", &self.mistral_model)
            .env_remove("NEO4J_CONECTION_URL")
            .env_remove("NEO4J_INATANSE_PASSWORD")
            .env_remove("MISTARL_API_KEY");
        command
    }

    async fn graph(&self) -> Result<Graph, Box<dyn std::error::Error>> {
        let url = url::Url::parse(&self.neo4j_url)?;
        let host = url.host_str().ok_or("Neo4j URL is missing a host")?;
        let bolt_address = format!("{host}:{}", url.port().unwrap_or(7687));
        Ok(Graph::new(&bolt_address, &self.neo4j_username, &self.neo4j_password).await?)
    }
}

fn required_environment_value(
    values: &BTreeMap<String, String>,
    canonical_name: &'static str,
    legacy_name: &'static str,
) -> Result<String, Box<dyn std::error::Error>> {
    values
        .get(canonical_name)
        .or_else(|| values.get(legacy_name))
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(|| format!("repository .env is missing {canonical_name}").into())
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
fn unrelated_working_directory_dotenv_is_ignored_and_process_configuration_is_preserved()
-> Result<(), Box<dyn std::error::Error>> {
    let working_directory = tempfile::tempdir()?;
    std::fs::write(
        working_directory.path().join(".env"),
        "CODEX_SESSION_RAW_DATA_PATH=/deliberately/invalid\nMISTRAL_API_KEY=file-only-canary\n",
    )?;
    let output = command()?
        .current_dir(working_directory.path())
        .args(["discover", "--scope", "all"])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "environment override failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    let rendered = String::from_utf8(output.stdout)?;
    let result: serde_json::Value = serde_json::from_str(&rendered)?;
    assert_eq!(result["unique_sessions"], 1);
    assert!(!rendered.contains("file-only-canary"));
    Ok(())
}

#[test]
#[ignore = "requires the real Mistral credential from the repository .env"]
fn repository_canonical_mistral_key_wins_over_inherited_and_cwd_values_end_to_end()
-> Result<(), Box<dyn std::error::Error>> {
    let environment = RepositoryEnvironment::load()?;
    let working_directory = tempfile::tempdir()?;
    std::fs::write(
        working_directory.path().join(".env"),
        "MISTRAL_API_KEY=deliberately-invalid-cwd-key\n",
    )?;
    let output = environment
        .command(&fixture_root()?, "cli_mistral_config_e2e")
        .current_dir(working_directory.path())
        .env("MISTRAL_API_KEY", "deliberately-invalid-inherited-key")
        .arg("mistral-health")
        .output()?;

    ensure(
        output.status.success(),
        "repository Mistral credential was not selected",
    )?;
    ensure_secret_safe(&environment, &output)?;
    let rendered = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!rendered.contains("deliberately-invalid"));
    Ok(())
}

#[tokio::test]
#[ignore = "requires the real Neo4j credentials from the repository .env"]
async fn repository_neo4j_credentials_win_over_unrelated_inherited_values_end_to_end()
-> Result<(), Box<dyn std::error::Error>> {
    let environment = RepositoryEnvironment::load()?;
    let temporary = tempfile::tempdir()?;
    let reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = reservation.local_addr()?;
    drop(reservation);
    let child = environment
        .command(&fixture_root()?, "default")
        .env(
            "NEO4J_CONNECTION_URL",
            "neo4j://unrelated-inherited.invalid:7687",
        )
        .env("NEO4J_PASSWORD", "unrelated-inherited-password")
        .env("HARNESS_GRAPH_BIND_ADDRESS", address.to_string())
        .env(
            "HARNESS_GRAPH_JOURNAL_PATH",
            temporary.path().join("neo4j-precedence.jsonl"),
        )
        .env("HARNESS_GRAPH_TRANSCRIPT_ENRICHMENT_MODE", "disabled")
        .arg("serve")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let mut server = ServerProcess::new(child);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let base = format!("http://{address}");
    wait_until_ready(&client, &base).await?;

    let response = client
        .get(format!("{base}/v1/experience/sessions"))
        .send()
        .await?;
    ensure(
        response.status().is_success(),
        "repository Neo4j credential was not selected",
    )?;
    let body = response.text().await?;
    assert!(!body.contains("unrelated-inherited"));
    assert!(!body.contains(&environment.neo4j_password));
    server.stop()?;
    Ok(())
}

#[test]
fn bulk_import_exposes_bounded_concurrency_contract() -> Result<(), Box<dyn std::error::Error>> {
    let help = command()?.args(["import-all", "--help"]).output()?;
    assert!(help.status.success());
    let stdout = String::from_utf8(help.stdout)?;
    assert!(stdout.contains("--scope"));
    assert!(stdout.contains("--concurrency"));

    for invalid in ["0", "9"] {
        let output = command()?
            .args(["import-all", "--concurrency", invalid])
            .output()?;
        assert!(!output.status.success());
        assert!(String::from_utf8(output.stderr)?.contains("expected an integer between 1 and 8"));
    }
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
fn transcript_dry_run_scans_real_verified_fixture_without_external_clients()
-> Result<(), Box<dyn std::error::Error>> {
    let mut single = command()?;
    single
        .env("NEO4J_CONNECTION_URL", "deliberately-invalid")
        .env("MISTRAL_MODEL", "not-a-mistral-model")
        .env("MISTRAL_MAX_CONCURRENCY", "99")
        .env("HARNESS_GRAPH_TRANSCRIPT_ENRICHMENT_MODE", "disabled")
        .env("HARNESS_GRAPH_MISTRAL_PRIVACY_CONTROL", "unverified")
        .env_remove("HARNESS_GRAPH_REDACTION_HMAC_KEY")
        .args([
            "enrich-transcripts",
            "--session-id",
            SESSION_ID,
            "--authorization",
            "operator-e2e",
            "--dry-run",
        ]);
    let output = single.output()?;
    if !output.status.success() {
        return Err(format!(
            "single transcript dry run failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    let rendered = String::from_utf8(output.stdout)?;
    let single_json: serde_json::Value = serde_json::from_str(&rendered)?;
    assert_eq!(single_json["status"], "eligible");
    assert_eq!(single_json["session_id"], SESSION_ID);
    assert!(single_json["verified_records"].as_u64().unwrap_or_default() > 0);
    assert!(single_json["expected_chunks"].as_u64().unwrap_or_default() > 0);
    assert!(!rendered.contains("source-safe-test-key"));
    assert!(!rendered.contains("source-safe-test-password"));
    assert!(!rendered.contains("rollout.jsonl"));

    let mut all = command()?;
    all.env("NEO4J_CONNECTION_URL", "deliberately-invalid")
        .env("MISTRAL_MODEL", "not-a-mistral-model")
        .env("MISTRAL_MAX_CONCURRENCY", "99")
        .env("HARNESS_GRAPH_TRANSCRIPT_ENRICHMENT_MODE", "disabled")
        .env("HARNESS_GRAPH_MISTRAL_PRIVACY_CONTROL", "unverified")
        .env_remove("HARNESS_GRAPH_REDACTION_HMAC_KEY")
        .args([
            "enrich-all-transcripts",
            "--scope",
            "all",
            "--authorization",
            "operator-e2e",
            "--dry-run",
        ]);
    let output = all.output()?;
    if !output.status.success() {
        return Err(format!(
            "all transcript dry run failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    let rendered = String::from_utf8(output.stdout)?;
    let all_json: serde_json::Value = serde_json::from_str(&rendered)?;
    assert_eq!(all_json["status"], "dry_run_complete");
    assert_eq!(all_json["discovered_sessions"], 1);
    assert_eq!(all_json["eligible_sessions"], 1);
    assert_eq!(all_json["blocks"]["scanner"], 0);
    assert_eq!(
        all_json["blocks"]["scanner_reasons"]["non_text_control_data"],
        0
    );
    assert_eq!(
        all_json["blocks"]["scanner_reasons"]["asset_or_binary_data"],
        0
    );
    assert_eq!(
        all_json["blocks"]["scanner_reasons"]["suspicious_encoded_blob"],
        0
    );
    assert_eq!(all_json["external_provider_calls"], 0);
    assert_eq!(all_json["neo4j_writes"], 0);
    assert!(!rendered.contains("source-safe-test-key"));
    assert!(!rendered.contains("source-safe-test-password"));
    assert!(!rendered.contains("rollout.jsonl"));
    Ok(())
}

#[test]
fn transcript_apply_contract_and_privacy_gates_fail_closed_before_external_clients()
-> Result<(), Box<dyn std::error::Error>> {
    let help = command()?
        .args(["enrich-all-transcripts", "--help"])
        .output()?;
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout)?;
    for required in [
        "--dry-run",
        "--apply",
        "--concurrency",
        "--limit",
        "--authorization",
    ] {
        assert!(help.contains(required));
    }

    let disabled = command()?
        .env("NEO4J_CONNECTION_URL", "deliberately-invalid")
        .env("HARNESS_GRAPH_TRANSCRIPT_ENRICHMENT_MODE", "disabled")
        .env(
            "HARNESS_GRAPH_MISTRAL_PRIVACY_CONTROL",
            "training_opt_out_verified",
        )
        .args([
            "enrich-transcripts",
            "--session-id",
            SESSION_ID,
            "--authorization",
            "operator-e2e",
            "--apply",
        ])
        .output()?;
    assert!(!disabled.status.success());
    let disabled = String::from_utf8(disabled.stderr)?;
    assert!(disabled.contains("enrichment_enabled"));
    assert!(!disabled.contains("Neo4j connection failed"));
    assert!(!disabled.contains("source-safe-test-key"));
    assert!(!disabled.contains("source-safe-test-password"));

    let unverified = command()?
        .env("NEO4J_CONNECTION_URL", "deliberately-invalid")
        .env("HARNESS_GRAPH_TRANSCRIPT_ENRICHMENT_MODE", "enabled")
        .env("HARNESS_GRAPH_MISTRAL_PRIVACY_CONTROL", "unverified")
        .args([
            "enrich-transcripts",
            "--session-id",
            SESSION_ID,
            "--authorization",
            "operator-e2e",
            "--apply",
        ])
        .output()?;
    assert!(!unverified.status.success());
    let unverified = String::from_utf8(unverified.stderr)?;
    assert!(unverified.contains("training_opt_out_verified"));
    assert!(!unverified.contains("Neo4j connection failed"));
    assert!(!unverified.contains("source-safe-test-key"));
    assert!(!unverified.contains("source-safe-test-password"));
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
#[ignore = "requires the real Neo4j credentials from the repository .env"]
async fn bulk_import_settles_repairs_and_preserves_shared_source_provenance()
-> Result<(), Box<dyn std::error::Error>> {
    let environment = RepositoryEnvironment::load()?;
    let temporary = tempfile::tempdir()?;
    let first_bundle = temporary.path().join("active/2026-02-16").join(SESSION_ID);
    let second_bundle = temporary
        .path()
        .join("active/2026-02-16")
        .join(SECOND_SESSION_ID);
    let metadata_only_bundle = temporary
        .path()
        .join("active/2026-02-16")
        .join(METADATA_ONLY_SESSION_ID);
    create_source_safe_bundle(&first_bundle, SESSION_ID)?;
    create_source_safe_bundle(&second_bundle, SECOND_SESSION_ID)?;
    create_source_safe_bundle(&metadata_only_bundle, METADATA_ONLY_SESSION_ID)?;
    reduce_to_metadata_only(&metadata_only_bundle)?;
    std::fs::OpenOptions::new()
        .append(true)
        .open(second_bundle.join("raw/rollout.jsonl"))?
        .write_all(b"\n")?;

    let namespace = format!("cli_bulk_e2e_{}", uuid::Uuid::now_v7().simple());
    let graph = environment.graph().await?;
    let scenario = run_bulk_import_scenario(
        &environment,
        &graph,
        temporary.path(),
        &second_bundle,
        &namespace,
    )
    .await;
    let cleanup = purge_namespace(&graph, &namespace).await;
    scenario?;
    cleanup?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires real Neo4j and Mistral; performs a paid call on source-safe fixture data"]
async fn transcript_apply_projects_additively_and_identical_rerun_submits_no_chunks()
-> Result<(), Box<dyn std::error::Error>> {
    let environment = RepositoryEnvironment::load()?;
    let namespace = format!("cli_enrichment_e2e_{}", uuid::Uuid::now_v7().simple());
    let graph = environment.graph().await?;
    let scenario = run_transcript_apply_rerun_scenario(&environment, &namespace);
    let cleanup = purge_namespace(&graph, &namespace).await;
    scenario?;
    cleanup?;
    Ok(())
}

fn run_transcript_apply_rerun_scenario(
    environment: &RepositoryEnvironment,
    namespace: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let first = transcript_apply_command(environment, namespace)?.output()?;
    ensure(first.status.success(), "first transcript apply failed")?;
    ensure_secret_safe(environment, &first)?;
    let first: serde_json::Value = serde_json::from_slice(&first.stdout)?;
    ensure(
        first["status"] == "completed"
            && first["submitted_chunks"]
                .as_u64()
                .is_some_and(|count| count > 0)
            && first["run_input_tokens"]
                .as_u64()
                .is_some_and(|count| count > 0),
        "first transcript apply did not commit a provider-backed run",
    )?;

    let second = transcript_apply_command(environment, namespace)?.output()?;
    ensure(second.status.success(), "identical transcript rerun failed")?;
    ensure_secret_safe(environment, &second)?;
    let second: serde_json::Value = serde_json::from_slice(&second.stdout)?;
    ensure(
        second["status"] == "exact_fingerprint_unchanged"
            && second["submitted_chunks"] == 0
            && second["new_cost_microusd"] == 0,
        "identical transcript rerun did not preserve the zero-submission identity",
    )
}

fn transcript_apply_command(
    environment: &RepositoryEnvironment,
    namespace: &str,
) -> Result<Command, Box<dyn std::error::Error>> {
    let mut command = environment.command(&fixture_root()?, namespace);
    command
        .env("HARNESS_GRAPH_TRANSCRIPT_ENRICHMENT_MODE", "enabled")
        .env(
            "HARNESS_GRAPH_MISTRAL_PRIVACY_CONTROL",
            "training_opt_out_verified",
        )
        .env(
            "HARNESS_GRAPH_REDACTION_HMAC_KEY",
            "source-safe-e2e-stable-redaction-key-00000000000000000000",
        )
        .env("MISTRAL_TRANSCRIPT_MODEL", "mistral-small-2603")
        .args([
            "enrich-transcripts",
            "--session-id",
            SESSION_ID,
            "--authorization",
            "operator-live-e2e",
            "--apply",
        ]);
    Ok(command)
}

async fn run_bulk_import_scenario(
    environment: &RepositoryEnvironment,
    graph: &Graph,
    archive_root: &Path,
    second_bundle: &Path,
    namespace: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let first = run_bulk_import(environment, archive_root, namespace)?;
    ensure(
        !first.status.success(),
        "tampered sweep unexpectedly succeeded",
    )?;
    ensure_secret_safe(environment, &first)?;
    let first_summary: serde_json::Value = serde_json::from_slice(&first.stdout)?;
    ensure(
        first_summary["status"] == "completed_with_failures",
        "tampered sweep did not settle with failures",
    )?;
    ensure(
        first_summary["discovered_sessions"] == 3
            && first_summary["imported_sessions"] == 2
            && first_summary["failed_sessions"] == 1,
        "tampered sweep session counts were incorrect",
    )?;
    ensure(
        first_summary["sessions"]
            .as_array()
            .is_some_and(|sessions| {
                sessions.iter().any(|session| {
                    session["session_id"] == SECOND_SESSION_ID
                        && session["status"] == "failed"
                        && session["failure_class"] == "archive_integrity"
                })
            }),
        "tampered session did not report an archive-integrity failure",
    )?;
    ensure(
        first_summary["sessions"]
            .as_array()
            .is_some_and(|sessions| {
                sessions.iter().any(|session| {
                    session["session_id"] == METADATA_ONLY_SESSION_ID
                        && session["status"] == "imported"
                        && session["analysis"]["status"] == "insufficient_semantic_evidence"
                        && session["analysis"]["semantic_activities"] == 0
                })
            }),
        "metadata-only session did not preserve typed analysis unavailability",
    )?;

    std::fs::copy(
        fixture_root()?
            .join("active/2026-02-16")
            .join(SESSION_ID)
            .join("raw/rollout.jsonl"),
        second_bundle.join("raw/rollout.jsonl"),
    )?;
    let second = run_bulk_import(environment, archive_root, namespace)?;
    ensure(second.status.success(), "repaired sweep failed")?;
    ensure_secret_safe(environment, &second)?;
    let second_summary: serde_json::Value = serde_json::from_slice(&second.stdout)?;
    ensure(
        second_summary["status"] == "completed"
            && second_summary["already_complete_sessions"] == 3
            && second_summary["imported_sessions"] == 0
            && second_summary["failed_sessions"] == 0,
        "repaired sweep did not skip all completed source snapshots",
    )?;
    let sessions = second_summary["sessions"]
        .as_array()
        .ok_or("repaired sweep omitted session settlements")?;
    ensure(
        sessions.len() == 3
            && sessions
                .iter()
                .all(|session| session["status"] == "already_complete")
            && sessions[0]["source_digest"] == sessions[1]["source_digest"],
        "repaired sessions did not share one completed raw digest",
    )?;

    let counts = graph_counts(graph, namespace).await?;
    ensure(
        counts
            == GraphCounts {
                sessions: 3,
                provenance_edges: 3,
                sources: 2,
                observations: 13,
                receipts: 2,
                activities: 4,
                outcomes: 1,
                paths: 1,
            },
        "Neo4j counts did not preserve provenance or typed analysis absence",
    )
}

fn run_bulk_import(
    environment: &RepositoryEnvironment,
    archive_root: &Path,
    namespace: &str,
) -> Result<Output, std::io::Error> {
    environment
        .command(archive_root, namespace)
        .args(["import-all", "--scope", "active", "--concurrency", "2"])
        .output()
}

fn ensure_secret_safe(
    environment: &RepositoryEnvironment,
    output: &Output,
) -> Result<(), Box<dyn std::error::Error>> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    ensure(
        !stdout.contains(&environment.neo4j_password)
            && !stderr.contains(&environment.neo4j_password)
            && !stdout.contains(&environment.mistral_api_key)
            && !stderr.contains(&environment.mistral_api_key),
        "CLI output exposed a configured secret",
    )
}

fn create_source_safe_bundle(
    destination: &Path,
    session_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = fixture_root()?.join("active/2026-02-16").join(SESSION_ID);
    std::fs::create_dir_all(destination.join("raw"))?;
    std::fs::copy(source.join("README.md"), destination.join("README.md"))?;
    std::fs::copy(
        source.join("raw/rollout.jsonl"),
        destination.join("raw/rollout.jsonl"),
    )?;
    let mut metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(source.join("metadata.json"))?)?;
    metadata["session_id"] = serde_json::Value::String(session_id.to_owned());
    let metadata_bytes = serde_json::to_vec_pretty(&metadata)?;
    std::fs::write(destination.join("metadata.json"), &metadata_bytes)?;
    write_checksum_manifest(destination, &metadata_bytes)
}

fn reduce_to_metadata_only(destination: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let raw_path = destination.join("raw/rollout.jsonl");
    let raw = std::fs::read_to_string(&raw_path)?;
    let first_record = raw.lines().next().ok_or("fixture raw stream is empty")?;
    let raw_bytes = format!("{first_record}\n").into_bytes();
    std::fs::write(&raw_path, &raw_bytes)?;

    let metadata_path = destination.join("metadata.json");
    let mut metadata: serde_json::Value = serde_json::from_slice(&std::fs::read(&metadata_path)?)?;
    let raw_digest = sha256_bytes(&raw_bytes);
    let raw_size = u64::try_from(raw_bytes.len())?;
    metadata["source_size_bytes"] = serde_json::Value::from(raw_size);
    metadata["source_sha256"] = serde_json::Value::String(raw_digest.clone());
    metadata["raw_size_bytes"] = serde_json::Value::from(raw_size);
    metadata["raw_sha256"] = serde_json::Value::String(raw_digest);
    metadata["record_count"] = serde_json::Value::from(1_u64);
    metadata["parse_error_count"] = serde_json::Value::from(0_u64);
    let metadata_bytes = serde_json::to_vec_pretty(&metadata)?;
    std::fs::write(metadata_path, &metadata_bytes)?;
    write_checksum_manifest(destination, &metadata_bytes)
}

fn write_checksum_manifest(
    destination: &Path,
    metadata_bytes: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = format!(
        "{}  README.md\n{}  metadata.json\n{}  raw/rollout.jsonl\n",
        sha256_file(&destination.join("README.md"))?,
        sha256_bytes(metadata_bytes),
        sha256_file(&destination.join("raw/rollout.jsonl"))?,
    );
    std::fs::write(destination.join("checksums.sha256"), manifest)?;
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, std::io::Error> {
    std::fs::read(path).map(|bytes| sha256_bytes(&bytes))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Debug, PartialEq, Eq)]
struct GraphCounts {
    sessions: i64,
    provenance_edges: i64,
    sources: i64,
    observations: i64,
    receipts: i64,
    activities: i64,
    outcomes: i64,
    paths: i64,
}

async fn graph_counts(
    graph: &Graph,
    namespace: &str,
) -> Result<GraphCounts, Box<dyn std::error::Error>> {
    let mut rows = graph
        .execute(
            query(
                "MATCH (session:HGSession {hg_namespace: $namespace}) \
                 OPTIONAL MATCH (session)-[edge:IMPORTED_FROM]->\
                     (source:HGSourceSnapshot {hg_namespace: $namespace}) \
                 WITH count(DISTINCT session) AS sessions, \
                      count(DISTINCT edge) AS provenance_edges, \
                      count(DISTINCT source) AS sources \
                 MATCH (observation:HGObservation {hg_namespace: $namespace}) \
                 WITH sessions, provenance_edges, sources, \
                      count(observation) AS observations \
                 OPTIONAL MATCH (receipt:HGIngestionReceipt {hg_namespace: $namespace}) \
                 WITH sessions, provenance_edges, sources, observations, \
                      count(DISTINCT receipt) AS receipts \
                 OPTIONAL MATCH (activity:HGActivity {hg_namespace: $namespace}) \
                 WITH sessions, provenance_edges, sources, observations, receipts, \
                      count(DISTINCT activity) AS activities \
                 OPTIONAL MATCH (outcome:HGOutcome {hg_namespace: $namespace}) \
                 WITH sessions, provenance_edges, sources, observations, receipts, activities, \
                      count(DISTINCT outcome) AS outcomes \
                 OPTIONAL MATCH (path:HGPathPattern {hg_namespace: $namespace}) \
                 RETURN sessions, provenance_edges, sources, observations, receipts, activities, \
                        outcomes, count(DISTINCT path) AS paths",
            )
            .param("namespace", namespace),
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or("Neo4j count query returned no row")?;
    Ok(GraphCounts {
        sessions: row.get("sessions")?,
        provenance_edges: row.get("provenance_edges")?,
        sources: row.get("sources")?,
        observations: row.get("observations")?,
        receipts: row.get("receipts")?,
        activities: row.get("activities")?,
        outcomes: row.get("outcomes")?,
        paths: row.get("paths")?,
    })
}

async fn purge_namespace(graph: &Graph, namespace: &str) -> Result<(), Box<dyn std::error::Error>> {
    graph
        .run(
            query("MATCH (node {hg_namespace: $namespace}) DETACH DELETE node")
                .param("namespace", namespace),
        )
        .await?;
    Ok(())
}

fn ensure(condition: bool, message: &'static str) -> Result<(), Box<dyn std::error::Error>> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

#[tokio::test]
#[ignore = "requires the real Neo4j credentials from the repository .env"]
async fn serve_process_exposes_real_experience_reads_and_preserves_journal_routes()
-> Result<(), Box<dyn std::error::Error>> {
    let environment = RepositoryEnvironment::load()?;
    let temporary = tempfile::tempdir()?;
    let namespace = format!("cli_serve_e2e_{}", uuid::Uuid::now_v7().simple());
    let graph = environment.graph().await?;
    let imported = environment
        .command(&fixture_root()?, &namespace)
        .args(["import", "--session-id", SESSION_ID])
        .output()?;
    ensure(
        imported.status.success(),
        "source-safe fixture import failed",
    )?;
    ensure_secret_safe(&environment, &imported)?;

    let reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = reservation.local_addr()?;
    drop(reservation);
    let child = environment
        .command(&fixture_root()?, &namespace)
        .env("HARNESS_GRAPH_BIND_ADDRESS", address.to_string())
        .env(
            "HARNESS_GRAPH_JOURNAL_PATH",
            temporary.path().join("live.jsonl"),
        )
        .env("HARNESS_GRAPH_TRANSCRIPT_ENRICHMENT_MODE", "disabled")
        .arg("serve")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let mut server = ServerProcess::new(child);
    let scenario = exercise_serve_routes(&environment, address, &mut server).await;
    let cleanup = purge_namespace(&graph, &namespace).await;
    scenario?;
    cleanup?;
    Ok(())
}

async fn exercise_serve_routes(
    environment: &RepositoryEnvironment,
    address: std::net::SocketAddr,
    server: &mut ServerProcess,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let base = format!("http://{address}");
    wait_until_ready(&client, &base).await?;

    let sessions = client
        .get(format!("{base}/v1/experience/sessions"))
        .send()
        .await?;
    ensure(
        sessions.status().is_success(),
        "experience session list was unavailable",
    )?;
    let sessions = sessions.json::<serde_json::Value>().await?;
    ensure(
        sessions["sessions"]
            .as_array()
            .is_some_and(|values| values.iter().any(|value| value["session_id"] == SESSION_ID)),
        "imported session was absent from the experience list",
    )?;

    let detail = client
        .get(format!("{base}/v1/experience/sessions/{SESSION_ID}"))
        .send()
        .await?;
    ensure(
        detail.status().is_success(),
        "experience session detail was unavailable",
    )?;
    let detail = detail.json::<serde_json::Value>().await?;
    ensure(
        detail["display"]["source"] == "deterministic_fallback"
            && detail["enrichment"]["state"] == "unavailable"
            && detail["enrichment"]["reason"] == "disabled",
        "disabled enrichment visibility was not preserved",
    )?;

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

    let rendered = format!("{sessions}{detail}{replay}");
    assert!(!rendered.contains(&environment.neo4j_password));
    assert!(!rendered.contains(&environment.mistral_api_key));
    assert!(!rendered.contains("/Users/"));

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
