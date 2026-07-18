//! `HarnessGraph` binary entrypoint.

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match harness_graph_cli::run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = %error, "harness-graph command failed");
            eprintln!("error: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
