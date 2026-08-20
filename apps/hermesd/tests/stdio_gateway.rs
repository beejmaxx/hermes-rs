//! Child-process proof that the Rust gateway speaks the existing TUI protocol.

use std::{
    fs,
    io::{BufRead, BufReader, Write},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    time::Duration,
};

use domain::{ForegroundTurnState, ForegroundTurnTerminal};
use hermesd::adapters::{AgentTools, SqliteForegroundTurnStore, SqliteSessionStore};
use ports::{ForegroundTurnStore, SessionStore};
use serde_json::{Value, json};
use tempfile::tempdir;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers};

#[tokio::test(flavor = "multi_thread")]
async fn stdio_client_can_create_prompt_and_resume_a_durable_session()
-> Result<(), Box<dyn std::error::Error>> {
    let state_dir = tempdir()?;
    let workspace = tempdir()?;
    fs::write(workspace.path().join("README.md"), "gateway\n")?;
    let root = fs::canonicalize(workspace.path())?;
    let database = state_dir.path().join("state.db");
    let server = MockServer::start().await;
    let system = "Keep this test response concise.";

    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_json(json!({
            "model": "test-model",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": "hello gateway"}
            ],
            "tools": AgentTools::catalog(),
            "stream": true,
            "stream_options": {"include_usage": true}
        })))
        .respond_with(streaming_text("Hello from the Rust gateway."))
        .mount(&server)
        .await;

    let mut gateway = GatewayProcess::spawn(&[
        "--state",
        path_text(&database)?,
        "gateway",
        "--provider",
        "custom",
        "--base-url",
        &format!("{}/v1", server.uri()),
        "--model",
        "test-model",
        "--root",
        path_text(&root)?,
        "--system",
        system,
    ])?;

    let ready = gateway.read_frame()?;
    assert_eq!(ready["method"], "event");
    assert_eq!(ready["params"]["type"], "gateway.ready");
    assert_eq!(ready["params"]["payload"]["backend"], "hermes-rs");

    gateway.request("setup", "setup.status", json!({}))?;
    let setup = gateway.read_response("setup")?;
    assert_eq!(setup["result"]["provider_configured"], true);

    gateway.request("create", "session.create", json!({"cols": 100}))?;
    let created = gateway.read_response("create")?;
    let session_id = created["result"]["session_id"]
        .as_str()
        .ok_or("session.create omitted session_id")?
        .to_owned();
    assert_eq!(created["result"]["info"]["model"], "test-model");

    gateway.request(
        "detect",
        "input.detect_drop",
        json!({"session_id": session_id, "text": "hello gateway"}),
    )?;
    let detected = gateway.read_response("detect")?;
    assert_eq!(detected["result"]["matched"], false);

    gateway.request(
        "prompt",
        "prompt.submit",
        json!({"session_id": session_id, "text": "hello gateway"}),
    )?;
    let accepted = gateway.read_response("prompt")?;
    assert_eq!(accepted["result"]["status"], "streaming");

    let mut event_types = Vec::new();
    let mut streamed = String::new();
    loop {
        let frame = gateway.read_frame()?;
        if frame["method"] != "event" || frame["params"]["session_id"] != session_id {
            continue;
        }
        let kind = frame["params"]["type"].as_str().unwrap_or_default();
        event_types.push(kind.to_owned());
        if kind == "message.delta" {
            streamed.push_str(frame["params"]["payload"]["text"].as_str().unwrap_or_default());
        }
        if kind == "message.complete" {
            assert_eq!(frame["params"]["payload"]["text"], "Hello from the Rust gateway.");
        }
        if kind == "session.info" {
            break;
        }
    }
    assert_eq!(event_types, ["message.start", "message.delta", "message.complete", "session.info"]);
    assert_eq!(streamed, "Hello from the Rust gateway.");

    gateway.request("resume", "session.resume", json!({"cols": 100, "session_id": session_id}))?;
    let resumed = gateway.read_response("resume")?;
    assert_eq!(resumed["result"]["message_count"], 2);
    assert_eq!(
        resumed["result"]["messages"],
        json!([
            {"role": "user", "text": "hello gateway"},
            {"role": "assistant", "text": "Hello from the Rust gateway."}
        ])
    );

    gateway.shutdown()?;

    let mut store = SqliteSessionStore::open(&database)?;
    let snapshot = store.load(&domain::SessionId::new(&session_id)?)?;
    assert_eq!(snapshot.owner_generation.get(), 2);
    assert_eq!(snapshot.conversation.len(), 2);
    let requests = server
        .received_requests()
        .await
        .ok_or("request recording is disabled on the mock server")?;
    assert_eq!(requests.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn stdio_client_can_interrupt_a_turn_without_leaving_the_session_busy()
-> Result<(), Box<dyn std::error::Error>> {
    let state_dir = tempdir()?;
    let workspace = tempdir()?;
    let root = fs::canonicalize(workspace.path())?;
    let database = state_dir.path().join("state.db");
    let server = MockServer::start().await;

    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .respond_with(
            streaming_text("This response should be cancelled.").set_delay(Duration::from_secs(2)),
        )
        .mount(&server)
        .await;

    let mut gateway = GatewayProcess::spawn(&[
        "--state",
        path_text(&database)?,
        "gateway",
        "--provider",
        "custom",
        "--base-url",
        &format!("{}/v1", server.uri()),
        "--model",
        "test-model",
        "--root",
        path_text(&root)?,
    ])?;
    let _ready = gateway.read_frame()?;

    gateway.request("create", "session.create", json!({}))?;
    let created = gateway.read_response("create")?;
    let session_id = created["result"]["session_id"]
        .as_str()
        .ok_or("session.create omitted session_id")?
        .to_owned();

    gateway.request(
        "prompt",
        "prompt.submit",
        json!({"session_id": session_id, "text": "cancel this turn"}),
    )?;
    let accepted = gateway.read_response("prompt")?;
    assert_eq!(accepted["result"]["status"], "streaming");
    let started = gateway.read_frame()?;
    assert_eq!(started["params"]["type"], "message.start");

    gateway.request("interrupt", "session.interrupt", json!({"session_id": session_id}))?;
    let mut acknowledged = false;
    let mut interrupted = false;
    while !acknowledged || !interrupted {
        let frame = gateway.read_frame()?;
        if frame.get("id").and_then(Value::as_str) == Some("interrupt") {
            assert_eq!(frame["result"]["status"], "interrupted");
            acknowledged = true;
        }
        if frame["method"] == "event"
            && frame["params"]["session_id"] == session_id
            && frame["params"]["type"] == "message.complete"
        {
            assert_eq!(frame["params"]["payload"]["status"], "interrupted");
            interrupted = true;
        }
    }

    gateway.request("resume", "session.resume", json!({"session_id": session_id}))?;
    let resumed = gateway.read_response("resume")?;
    assert_eq!(resumed["result"]["running"], false);
    assert_eq!(resumed["result"]["message_count"], 0);
    assert_eq!(
        resumed["result"]["messages"],
        json!([
            {"role": "user", "text": "cancel this turn"},
            {
                "role": "system",
                "text": "Foreground turn interrupted; it was not committed or replayed."
            }
        ])
    );
    assert_eq!(resumed["result"]["recovery"]["status"], "interrupted");
    assert_eq!(resumed["result"]["recovery"]["auto_replayed"], false);

    gateway.shutdown()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn process_death_is_reconciled_without_replaying_the_prompt()
-> Result<(), Box<dyn std::error::Error>> {
    let state_dir = tempdir()?;
    let workspace = tempdir()?;
    let root = fs::canonicalize(workspace.path())?;
    let database = state_dir.path().join("state.db");
    let server = MockServer::start().await;

    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .respond_with(
            streaming_text("This response must never commit.").set_delay(Duration::from_secs(10)),
        )
        .mount(&server)
        .await;

    let base_url = format!("{}/v1", server.uri());
    let arguments = [
        "--state",
        path_text(&database)?,
        "gateway",
        "--provider",
        "custom",
        "--base-url",
        &base_url,
        "--model",
        "test-model",
        "--root",
        path_text(&root)?,
    ];
    let mut gateway = GatewayProcess::spawn(&arguments)?;
    let ready = gateway.read_frame()?;
    assert_eq!(ready["params"]["payload"]["reconciled_foreground_turns"], 0);
    let mut contender = GatewayProcess::spawn(&arguments)?;
    assert!(contender.read_frame().is_err(), "a second gateway acquired the live state database");
    drop(contender);

    gateway.request("create", "session.create", json!({}))?;
    let created = gateway.read_response("create")?;
    let session_id = created["result"]["session_id"]
        .as_str()
        .ok_or("session.create omitted session_id")?
        .to_owned();
    gateway.request(
        "prompt",
        "prompt.submit",
        json!({"session_id": session_id, "text": "survive this crash"}),
    )?;
    assert_eq!(gateway.read_response("prompt")?["result"]["status"], "streaming");
    assert_eq!(gateway.read_frame()?["params"]["type"], "message.start");

    let mut provider_started = false;
    for _ in 0..100 {
        let requests = server
            .received_requests()
            .await
            .ok_or("request recording is disabled on the mock server")?;
        if !requests.is_empty() {
            provider_started = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(provider_started, "provider request did not start before the crash");
    gateway.request("live-resume", "session.resume", json!({"session_id": session_id}))?;
    let live = gateway.read_response("live-resume")?;
    assert_eq!(live["result"]["running"], true);
    assert_eq!(live["result"]["messages"], json!([]));
    assert_eq!(live["result"]["inflight"]["user"], "survive this crash");
    assert_eq!(live["result"]["inflight"]["streaming"], true);
    gateway.kill()?;

    let mut recovered = GatewayProcess::spawn(&arguments)?;
    let ready = recovered.read_frame()?;
    assert_eq!(ready["params"]["payload"]["reconciled_foreground_turns"], 1);
    recovered.request("resume", "session.resume", json!({"session_id": session_id}))?;
    let resumed = recovered.read_response("resume")?;
    assert_eq!(resumed["result"]["running"], false);
    assert_eq!(resumed["result"]["message_count"], 0);
    assert_eq!(
        resumed["result"]["messages"],
        json!([
            {"role": "user", "text": "survive this crash"},
            {
                "role": "system",
                "text": "Foreground turn outcome is unknown after restart; it was not replayed."
            }
        ])
    );
    assert_eq!(resumed["result"]["recovery"]["status"], "outcome_unknown");
    assert_eq!(resumed["result"]["recovery"]["prompt"], "survive this crash");
    assert_eq!(resumed["result"]["recovery"]["auto_replayed"], false);
    recovered.shutdown()?;

    let mut turns = SqliteForegroundTurnStore::open(&database)?;
    let latest = turns
        .latest(&domain::SessionId::new(&session_id)?)?
        .ok_or("reconciled foreground turn is missing")?;
    assert!(matches!(
        latest.state,
        ForegroundTurnState::Terminal {
            outcome: ForegroundTurnTerminal::OutcomeUnknown { .. },
            ..
        }
    ));
    Ok(())
}

struct GatewayProcess {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
}

impl GatewayProcess {
    fn spawn(arguments: &[&str]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut child = Command::new(env!("CARGO_BIN_EXE_hermesd"))
            .args(arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let input = child.stdin.take().ok_or("gateway stdin was not piped")?;
        let output = child.stdout.take().ok_or("gateway stdout was not piped")?;
        Ok(Self { child, input: Some(input), output: BufReader::new(output) })
    }

    fn request(
        &mut self,
        id: &str,
        method: &str,
        params: Value,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let input = self.input.as_mut().ok_or("gateway stdin is closed")?;
        serde_json::to_writer(
            &mut *input,
            &json!({"id": id, "jsonrpc": "2.0", "method": method, "params": params}),
        )?;
        input.write_all(b"\n")?;
        input.flush()?;
        Ok(())
    }

    fn read_response(&mut self, id: &str) -> Result<Value, Box<dyn std::error::Error>> {
        loop {
            let frame = self.read_frame()?;
            if frame.get("id").and_then(Value::as_str) == Some(id) {
                return Ok(frame);
            }
        }
    }

    fn read_frame(&mut self) -> Result<Value, Box<dyn std::error::Error>> {
        let mut line = String::new();
        let read = self.output.read_line(&mut line)?;
        if read == 0 {
            let status = self.child.try_wait()?;
            return Err(format!("gateway stdout closed unexpectedly (status {status:?})").into());
        }
        serde_json::from_str(&line).map_err(Into::into)
    }

    fn shutdown(mut self) -> Result<(), Box<dyn std::error::Error>> {
        drop(self.input.take());
        let status = self.child.wait()?;
        if status.success() { Ok(()) } else { Err(format!("gateway exited with {status}").into()) }
    }

    fn kill(mut self) -> Result<(), Box<dyn std::error::Error>> {
        drop(self.input.take());
        self.child.kill()?;
        let _status = self.child.wait()?;
        Ok(())
    }
}

impl Drop for GatewayProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
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
