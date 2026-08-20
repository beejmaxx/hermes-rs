//! Multi-process CLI proof for durable session resume.

use std::{
    fs,
    process::{Command, Output},
};

use hermesd::adapters::{AgentTools, SqliteSessionStore};
use ports::SessionStore;
use serde_json::json;
use tempfile::tempdir;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers};

#[tokio::test(flavor = "multi_thread")]
async fn separate_cli_processes_resume_one_frozen_session() -> Result<(), Box<dyn std::error::Error>>
{
    let state_dir = tempdir()?;
    let workspace = tempdir()?;
    fs::write(workspace.path().join("README.md"), "durable\n")?;
    let root = fs::canonicalize(workspace.path())?;
    let database = state_dir.path().join("state.db");
    let server = MockServer::start().await;
    let base_url = format!("{}/v1", server.uri());
    let system = format!(
        "You are Hermes RS, a precise and helpful agent. You may inspect the workspace at {} using read_file and search_files. These tools are read-only. You may delegate focused independent subtasks to isolated leaf agents. Never claim to have modified files or run commands.",
        root.display()
    );
    let tools = AgentTools::catalog();

    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_json(json!({
            "model": "test-model",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": "first"}
            ],
            "tools": tools,
            "stream": true,
            "stream_options": {"include_usage": true}
        })))
        .respond_with(streaming_text("First answer."))
        .mount(&server)
        .await;

    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_json(json!({
            "model": "test-model",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": "first"},
                {"role": "assistant", "content": "First answer."},
                {"role": "user", "content": "second"}
            ],
            "tools": AgentTools::catalog(),
            "stream": true,
            "stream_options": {"include_usage": true}
        })))
        .respond_with(streaming_text("Second answer."))
        .mount(&server)
        .await;

    let first = run_cli(&[
        "--state",
        path_text(&database)?,
        "chat",
        "--session",
        "durable",
        "--provider",
        "custom",
        "--base-url",
        &base_url,
        "--model",
        "test-model",
        "--root",
        path_text(&root)?,
        "first",
    ])?;
    assert_success(&first)?;
    assert_eq!(String::from_utf8(first.stdout)?, "First answer.\n");

    let second =
        run_cli(&["--state", path_text(&database)?, "chat", "--session", "durable", "second"])?;
    assert_success(&second)?;
    assert_eq!(String::from_utf8(second.stdout)?, "Second answer.\n");

    let listed = run_cli(&["--state", path_text(&database)?, "session", "list"])?;
    assert_success(&listed)?;
    let listing = String::from_utf8(listed.stdout)?;
    assert!(listing.contains("durable\tcustom\ttest-model\tgeneration=3\tmessages=4"));

    let mut store = SqliteSessionStore::open(&database)?;
    let snapshot = store.load(&domain::SessionId::new("durable")?)?;
    assert_eq!(snapshot.owner_generation.get(), 3);
    assert_eq!(snapshot.conversation.len(), 4);
    let requests = server
        .received_requests()
        .await
        .ok_or("request recording is disabled on the mock server")?;
    assert_eq!(requests.len(), 2);
    Ok(())
}

fn run_cli(arguments: &[&str]) -> Result<Output, std::io::Error> {
    Command::new(env!("CARGO_BIN_EXE_hermesd")).args(arguments).output()
}

fn assert_success(output: &Output) -> Result<(), Box<dyn std::error::Error>> {
    if output.status.success() {
        return Ok(());
    }
    Err(format!("CLI failed with {}:\n{}", output.status, String::from_utf8_lossy(&output.stderr))
        .into())
}

fn path_text(path: &std::path::Path) -> Result<&str, Box<dyn std::error::Error>> {
    path.to_str().ok_or_else(|| "test path is not valid UTF-8".into())
}

fn streaming_text(content: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).insert_header("content-type", "text/event-stream").set_body_string(
        format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({
                "choices": [{
                    "delta": {"role": "assistant", "content": content},
                    "finish_reason": "stop"
                }]
            })
        ),
    )
}
