//! Real-provider full-process E2E for synchronized Mistral interpretation.

use std::{collections::BTreeSet, path::PathBuf, process::Command};

const SESSION_ID: &str = "019f5854-4c2d-77d3-9c03-dce55770e6d7";
const SOURCE_SAFE_TASK: &str =
    "Investigate and improve an agent workflow with incomplete verification evidence.";
const PATHFINDER_TASK: &str =
    "Repair and verify a deprecated configuration under restricted sandboxing.";

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn canonical_project_key() -> Result<String, Box<dyn std::error::Error>> {
    let env_path = repository_root().join(".env");
    let credential = dotenvy::from_path_iter(env_path)?.find_map(|entry| match entry {
        Ok((name, value)) if name == "MISTRAL_API_KEY" && !value.trim().is_empty() => {
            Some(Ok(value))
        }
        Ok(_) => None,
        Err(error) => Some(Err(error.into())),
    });
    match credential {
        Some(result) => result,
        None => Err("repository .env is missing canonical MISTRAL_API_KEY".into()),
    }
}

#[test]
#[ignore = "requires real archive data and the canonical MISTRAL_API_KEY in repository .env"]
fn live_interpret_classifies_and_extracts_concurrently() -> Result<(), Box<dyn std::error::Error>> {
    let credential = canonical_project_key()?;
    let output = Command::new(env!("CARGO_BIN_EXE_harness-graph"))
        .current_dir(repository_root())
        .env_remove("MISTARL_API_KEY")
        .args([
            "interpret",
            "--session-id",
            SESSION_ID,
            "--task",
            SOURCE_SAFE_TASK,
        ])
        .output()?;
    let stdout = String::from_utf8(output.stdout)?;
    let stderr = String::from_utf8(output.stderr)?;
    assert!(!stdout.contains(&credential));
    assert!(!stderr.contains(&credential));
    if !output.status.success() {
        return Err(format!("live interpret failed: {stderr}").into());
    }

    let document: serde_json::Value = serde_json::from_str(&stdout)?;
    assert_eq!(document["status"], "interpreted");
    assert_eq!(document["provider"], "mistral");
    assert_eq!(document["execution_mode"], "concurrent");
    assert_eq!(document["synchronization"], "all_results_settle");
    assert_eq!(document["max_concurrency"], 2);
    assert_eq!(document["session_id"], SESSION_ID);
    assert!(
        document["model"]
            .as_str()
            .is_some_and(|model| !model.is_empty())
    );

    let category = document["classification"]["category"]
        .as_str()
        .ok_or("classification category must be a string")?;
    assert!(
        [
            "bug_fix",
            "feature",
            "refactor",
            "research",
            "operations",
            "documentation",
            "testing",
            "data_analysis",
            "other",
        ]
        .contains(&category)
    );
    let confidence = document["classification"]["confidence"]
        .as_str()
        .ok_or("classification confidence must be a string")?;
    assert!(["low", "medium", "high"].contains(&confidence));
    let explanation = document["classification"]["explanation"]
        .as_str()
        .ok_or("classification explanation must be a string")?;
    assert!((1..=300).contains(&explanation.chars().count()));

    let extraction = &document["extraction"];
    assert_eq!(extraction["deterministic_activities"], 50);
    assert_eq!(extraction["narrative_activity_count"], 17);
    let narratives = extraction["narrative_activities"]
        .as_array()
        .ok_or("narrative_activities must be an array")?;
    assert_eq!(narratives.len(), 17);
    let citations: Vec<_> = narratives
        .iter()
        .flat_map(|activity| {
            activity["cited_activity_ids"]
                .as_array()
                .into_iter()
                .flatten()
        })
        .filter_map(serde_json::Value::as_str)
        .collect();
    assert_eq!(citations.len(), 50);
    assert_eq!(citations.iter().copied().collect::<BTreeSet<_>>().len(), 50);
    assert!(citations.iter().all(|citation| citation.len() == 64));

    assert_valid_usage(&document["classification"]["usage"])?;
    assert_valid_usage(&extraction["usage"])?;
    Ok(())
}

#[test]
#[ignore = "requires real Neo4j and the canonical MISTRAL_API_KEY in repository .env"]
fn live_pathfinder_preserves_typed_session_and_activity_citations()
-> Result<(), Box<dyn std::error::Error>> {
    let credential = canonical_project_key()?;
    let output = Command::new(env!("CARGO_BIN_EXE_harness-graph"))
        .current_dir(repository_root())
        .env_remove("MISTARL_API_KEY")
        .args(["pathfinder", "--task", PATHFINDER_TASK, "--precedents", "1"])
        .output()?;
    let stdout = String::from_utf8(output.stdout)?;
    let stderr = String::from_utf8(output.stderr)?;
    assert!(!stdout.contains(&credential));
    assert!(!stderr.contains(&credential));
    if !output.status.success() {
        return Err(format!("live Pathfinder failed: {stderr}").into());
    }

    let document: serde_json::Value = serde_json::from_str(&stdout)?;
    assert_eq!(document["status"], "planned");
    assert_eq!(document["provider"], "mistral");
    assert_eq!(document["retrieved_precedents"], 1);
    let cited_sessions = document["cited_session_ids"]
        .as_array()
        .ok_or("cited_session_ids must be an array")?;
    assert_eq!(cited_sessions.len(), 1);
    for session in cited_sessions {
        uuid::Uuid::parse_str(
            session
                .as_str()
                .ok_or("cited session identity must be a string")?,
        )?;
    }
    let steps = document["steps"]
        .as_array()
        .ok_or("steps must be an array")?;
    assert!((3..=10).contains(&steps.len()));
    for step in steps {
        let citations = step["cited_activity_ids"]
            .as_array()
            .ok_or("cited_activity_ids must be an array")?;
        assert!(!citations.is_empty());
        assert!(citations.iter().all(|citation| {
            citation.as_str().is_some_and(|value| {
                value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        }));
    }
    assert_valid_usage(&document["usage"])?;
    Ok(())
}

fn assert_valid_usage(usage: &serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
    let input = usage["input_tokens"]
        .as_u64()
        .ok_or("input_tokens must be an unsigned integer")?;
    let output = usage["output_tokens"]
        .as_u64()
        .ok_or("output_tokens must be an unsigned integer")?;
    let total = usage["total_tokens"]
        .as_u64()
        .ok_or("total_tokens must be an unsigned integer")?;
    assert!(input > 0);
    assert!(output > 0);
    assert_eq!(total, input + output);
    Ok(())
}
