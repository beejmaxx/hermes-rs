//! Child-process proof that the Rust gateway speaks the existing TUI protocol.

use std::{
    fs,
    io::{BufRead, BufReader, Write},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    time::Duration,
};

use domain::{DelegationState, DelegationTerminal, ForegroundTurnState, ForegroundTurnTerminal};
use hermesd::adapters::{
    AgentTools, ReadOnlyLocalTools, SqliteDelegationStore, SqliteForegroundTurnStore,
    SqliteSessionStore,
};
use ports::{DelegationStore, ForegroundTurnStore, SessionStore};
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
            "tools": AgentTools::background_catalog(),
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

#[tokio::test(flavor = "multi_thread")]
async fn background_delegation_runs_and_is_delivered_with_the_next_user_turn()
-> Result<(), Box<dyn std::error::Error>> {
    let state_dir = tempdir()?;
    let workspace = tempdir()?;
    let root = fs::canonicalize(workspace.path())?;
    let database = state_dir.path().join("state.db");
    let server = MockServer::start().await;
    let system = "Use durable background delegation when asked.";
    let goal = "Inspect the architecture independently.";
    let child_system = format!(
        "You are a focused leaf subagent. You cannot delegate, interact with the user, or modify files. Inspect the workspace at {} with read-only tools when useful.\n\nTASK:\n{goal}",
        root.display()
    );

    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_json(json!({
            "model": "test-model",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": "start background work"}
            ],
            "tools": AgentTools::background_catalog(),
            "stream": true,
            "stream_options": {"include_usage": true}
        })))
        .respond_with(tool_call("delegate-background", "delegate_task", json!({"goal": goal})))
        .mount(&server)
        .await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_string_contains("Queued background delegation"))
        .respond_with(streaming_text("The background task is queued."))
        .mount(&server)
        .await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_json(json!({
            "model": "test-model",
            "messages": [
                {"role": "system", "content": child_system},
                {
                    "role": "user",
                    "content": "Complete the assigned task and return a concise result for the parent agent."
                }
            ],
            "tools": ReadOnlyLocalTools::catalog(),
            "stream": true,
            "stream_options": {"include_usage": true}
        })))
        .respond_with(streaming_text("The architecture has a typed Rust kernel."))
        .mount(&server)
        .await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_string_contains("<background_completion"))
        .and(matchers::body_string_contains("what did it find?"))
        .respond_with(streaming_text("The child found a typed Rust kernel."))
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
    assert_eq!(ready["params"]["payload"]["reconciled_delegations"], 0);
    gateway.request("create", "session.create", json!({}))?;
    let created = gateway.read_response("create")?;
    let parent_id = created["result"]["session_id"]
        .as_str()
        .ok_or("session.create omitted session_id")?
        .to_owned();

    gateway.request(
        "dispatch",
        "prompt.submit",
        json!({"session_id": parent_id, "text": "start background work"}),
    )?;
    assert_eq!(gateway.read_response("dispatch")?["result"]["delivered_background_completions"], 0);
    read_until_session_info(&mut gateway, &parent_id)?;

    let parent_session_id = domain::SessionId::new(&parent_id)?;
    let completion = wait_for_completion(&database, &parent_session_id).await?;
    assert!(matches!(
        &completion.outcome,
        DelegationTerminal::Completed { summary }
            if summary == "The architecture has a typed Rust kernel."
    ));
    let snapshot = SqliteDelegationStore::open(&database)?.load(&completion.delegation_id)?;
    assert!(matches!(snapshot.state, DelegationState::Terminal { .. }));
    assert_eq!(
        SqliteSessionStore::open(&database)?
            .load(&snapshot.spec.child_session_id)?
            .conversation
            .len(),
        2
    );

    gateway.request(
        "delivery",
        "prompt.submit",
        json!({"session_id": parent_id, "text": "what did it find?"}),
    )?;
    assert_eq!(gateway.read_response("delivery")?["result"]["delivered_background_completions"], 1);
    read_until_session_info(&mut gateway, &parent_id)?;
    assert!(
        SqliteDelegationStore::open(&database)?
            .available_completions_for(&parent_session_id, i64::MAX as u64, 10)?
            .is_empty()
    );
    let parent = SqliteSessionStore::open(&database)?.load(&parent_session_id)?;
    assert_eq!(parent.conversation.len(), 6);
    gateway.request("resume", "session.resume", json!({"session_id": parent_id}))?;
    let resumed = gateway.read_response("resume")?;
    assert_eq!(resumed["result"]["messages"][4]["text"], "what did it find?");
    gateway.shutdown()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn running_background_delegation_can_be_listed_and_cancelled_durably()
-> Result<(), Box<dyn std::error::Error>> {
    let state_dir = tempdir()?;
    let workspace = tempdir()?;
    let root = fs::canonicalize(workspace.path())?;
    let database = state_dir.path().join("state.db");
    let server = MockServer::start().await;
    let system = "Use durable background delegation when asked.";
    let goal = "Perform an inspection that the operator may cancel.";
    let child_system = format!(
        "You are a focused leaf subagent. You cannot delegate, interact with the user, or modify files. Inspect the workspace at {} with read-only tools when useful.\n\nTASK:\n{goal}",
        root.display()
    );

    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_string_contains("start cancellable background work"))
        .and(matchers::body_string_contains("durable leaf-agent session"))
        .respond_with(tool_call("delegate-cancellable", "delegate_task", json!({"goal": goal})))
        .mount(&server)
        .await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_string_contains("Queued background delegation"))
        .respond_with(streaming_text("The cancellable task is queued."))
        .mount(&server)
        .await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_json(json!({
            "model": "test-model",
            "messages": [
                {"role": "system", "content": child_system},
                {
                    "role": "user",
                    "content": "Complete the assigned task and return a concise result for the parent agent."
                }
            ],
            "tools": ReadOnlyLocalTools::catalog(),
            "stream": true,
            "stream_options": {"include_usage": true}
        })))
        .respond_with(
            streaming_text("This cancelled result must never be committed.")
                .set_delay(Duration::from_secs(10)),
        )
        .mount(&server)
        .await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_string_contains("<background_completion"))
        .and(matchers::body_string_contains("\"status\": \"cancelled\""))
        .and(matchers::body_string_contains("confirm the cancellation"))
        .respond_with(streaming_text("The background task was cancelled."))
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
    let _ready = gateway.read_frame()?;
    gateway.request("create", "session.create", json!({}))?;
    let created = gateway.read_response("create")?;
    let parent_id = created["result"]["session_id"]
        .as_str()
        .ok_or("session.create omitted session_id")?
        .to_owned();
    gateway.request(
        "dispatch",
        "prompt.submit",
        json!({"session_id": parent_id, "text": "start cancellable background work"}),
    )?;
    let _accepted = gateway.read_response("dispatch")?;
    read_until_session_info(&mut gateway, &parent_id)?;

    let mut running = None;
    for attempt in 0..200 {
        let request_id = format!("list-{attempt}");
        gateway.request(
            &request_id,
            "delegation.list",
            json!({"session_id": parent_id, "limit": 10}),
        )?;
        let response = gateway.read_response(&request_id)?;
        if response["result"]["delegations"][0]["state"]["state"] == "running" {
            running = Some(response["result"]["delegations"][0].clone());
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let running = running.ok_or("delegation did not enter running state")?;
    let delegation_id = running["spec"]["delegation_id"]
        .as_str()
        .ok_or("delegation.list omitted delegation_id")?
        .to_owned();

    gateway.request(
        "status-running",
        "delegation.status",
        json!({"session_id": parent_id, "delegation_id": delegation_id}),
    )?;
    assert_eq!(
        gateway.read_response("status-running")?["result"]["delegation"]["state"]["state"],
        "running"
    );
    let mut child_request_started = false;
    for _ in 0..200 {
        let requests = server
            .received_requests()
            .await
            .ok_or("request recording is disabled on the mock server")?;
        if requests.iter().any(|request| {
            serde_json::from_slice::<Value>(&request.body).is_ok_and(|body| {
                body["messages"][0]["content"].as_str() == Some(child_system.as_str())
            })
        }) {
            child_request_started = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(child_request_started, "child provider request did not start before cancellation");
    gateway.request(
        "cancel",
        "delegation.cancel",
        json!({
            "session_id": parent_id,
            "delegation_id": delegation_id,
            "reason": "operator ended the experiment"
        }),
    )?;
    let cancelled = gateway.read_response("cancel")?;
    assert_eq!(cancelled["result"]["accepted"], true);
    assert_eq!(
        cancelled["result"]["delegation"]["state"]["cancellation"]["reason"],
        "operator ended the experiment"
    );

    let parent_session_id = domain::SessionId::new(&parent_id)?;
    let completion = wait_for_completion(&database, &parent_session_id).await?;
    assert_eq!(completion.delegation_id.as_str(), delegation_id);
    assert!(matches!(
        &completion.outcome,
        DelegationTerminal::Cancelled { reason } if reason == "operator ended the experiment"
    ));
    let delegation = SqliteDelegationStore::open(&database)?.load(&completion.delegation_id)?;
    assert!(matches!(
        delegation.state,
        DelegationState::Terminal { outcome: DelegationTerminal::Cancelled { .. }, .. }
    ));
    assert!(
        SqliteSessionStore::open(&database)?
            .load(&delegation.spec.child_session_id)?
            .conversation
            .is_empty()
    );

    gateway.request(
        "status-cancelled",
        "delegation.status",
        json!({"session_id": parent_id, "delegation_id": delegation_id}),
    )?;
    assert_eq!(
        gateway.read_response("status-cancelled")?["result"]["delegation"]["state"]["outcome"]["status"],
        "cancelled"
    );
    gateway.request(
        "delivery",
        "prompt.submit",
        json!({"session_id": parent_id, "text": "confirm the cancellation"}),
    )?;
    assert_eq!(gateway.read_response("delivery")?["result"]["delivered_background_completions"], 1);
    read_until_session_info(&mut gateway, &parent_id)?;
    assert!(
        SqliteDelegationStore::open(&database)?
            .available_completions_for(&parent_session_id, i64::MAX as u64, 10)?
            .is_empty()
    );
    let child_request_count = server
        .received_requests()
        .await
        .ok_or("request recording is disabled on the mock server")?
        .iter()
        .filter(|request| {
            serde_json::from_slice::<Value>(&request.body).is_ok_and(|body| {
                body["messages"][0]["content"].as_str() == Some(child_system.as_str())
            })
        })
        .count();
    assert_eq!(child_request_count, 1);
    gateway.shutdown()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn killed_background_worker_reconciles_unknown_without_replay()
-> Result<(), Box<dyn std::error::Error>> {
    let state_dir = tempdir()?;
    let workspace = tempdir()?;
    let root = fs::canonicalize(workspace.path())?;
    let database = state_dir.path().join("state.db");
    let server = MockServer::start().await;
    let system = "Use background delegation.";
    let goal = "Perform a slow independent inspection.";
    let child_system = format!(
        "You are a focused leaf subagent. You cannot delegate, interact with the user, or modify files. Inspect the workspace at {} with read-only tools when useful.\n\nTASK:\n{goal}",
        root.display()
    );

    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_string_contains("start slow background work"))
        .and(matchers::body_string_contains("durable leaf-agent session"))
        .respond_with(tool_call("delegate-slow", "delegate_task", json!({"goal": goal})))
        .mount(&server)
        .await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_string_contains("Queued background delegation"))
        .respond_with(streaming_text("The slow task is queued."))
        .mount(&server)
        .await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_json(json!({
            "model": "test-model",
            "messages": [
                {"role": "system", "content": child_system},
                {
                    "role": "user",
                    "content": "Complete the assigned task and return a concise result for the parent agent."
                }
            ],
            "tools": ReadOnlyLocalTools::catalog(),
            "stream": true,
            "stream_options": {"include_usage": true}
        })))
        .respond_with(
            streaming_text("This result must not survive the process death.")
                .set_delay(Duration::from_secs(10)),
        )
        .mount(&server)
        .await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_string_contains("outcome_unknown"))
        .and(matchers::body_string_contains("report the recovered state"))
        .respond_with(streaming_text("The background outcome is unknown and was not replayed."))
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
        "--system",
        system,
    ];
    let mut gateway = GatewayProcess::spawn(&arguments)?;
    let _ready = gateway.read_frame()?;
    gateway.request("create", "session.create", json!({}))?;
    let created = gateway.read_response("create")?;
    let parent_id = created["result"]["session_id"]
        .as_str()
        .ok_or("session.create omitted session_id")?
        .to_owned();
    gateway.request(
        "dispatch",
        "prompt.submit",
        json!({"session_id": parent_id, "text": "start slow background work"}),
    )?;
    let _accepted = gateway.read_response("dispatch")?;
    read_until_session_info(&mut gateway, &parent_id)?;

    let mut child_request_started = false;
    for _ in 0..200 {
        let requests = server
            .received_requests()
            .await
            .ok_or("request recording is disabled on the mock server")?;
        if requests.iter().any(|request| {
            serde_json::from_slice::<Value>(&request.body).is_ok_and(|body| {
                body["messages"][0]["content"].as_str() == Some(child_system.as_str())
            })
        }) {
            child_request_started = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(child_request_started, "child provider request did not start before process death");
    gateway.kill()?;

    let mut recovered = GatewayProcess::spawn(&arguments)?;
    let ready = recovered.read_frame()?;
    assert_eq!(ready["params"]["payload"]["reconciled_delegations"], 1);
    let parent_session_id = domain::SessionId::new(&parent_id)?;
    let completion = wait_for_completion(&database, &parent_session_id).await?;
    assert!(matches!(
        &completion.outcome,
        DelegationTerminal::OutcomeUnknown { reason }
            if reason.contains("owning gateway exited")
    ));
    let delegation = SqliteDelegationStore::open(&database)?.load(&completion.delegation_id)?;
    assert!(
        SqliteSessionStore::open(&database)?
            .load(&delegation.spec.child_session_id)?
            .conversation
            .is_empty()
    );

    recovered.request(
        "delivery",
        "prompt.submit",
        json!({"session_id": parent_id, "text": "report the recovered state"}),
    )?;
    assert_eq!(
        recovered.read_response("delivery")?["result"]["delivered_background_completions"],
        1
    );
    read_until_session_info(&mut recovered, &parent_id)?;
    assert!(
        SqliteDelegationStore::open(&database)?
            .available_completions_for(&parent_session_id, i64::MAX as u64, 10)?
            .is_empty()
    );
    recovered.shutdown()?;
    Ok(())
}

fn read_until_session_info(
    gateway: &mut GatewayProcess,
    session_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let frame = gateway.read_frame()?;
        if frame["method"] == "event"
            && frame["params"]["session_id"] == session_id
            && frame["params"]["type"] == "session.info"
        {
            return Ok(());
        }
    }
}

async fn wait_for_completion(
    database: &std::path::Path,
    parent_session_id: &domain::SessionId,
) -> Result<protocol::DelegationCompletion, Box<dyn std::error::Error>> {
    for _ in 0..200 {
        let available = SqliteDelegationStore::open(database)?.available_completions_for(
            parent_session_id,
            i64::MAX as u64,
            10,
        )?;
        if let Some(completion) = available.into_iter().next() {
            return Ok(completion);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err("background completion was not durably enqueued".into())
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

fn tool_call(id: &str, name: &str, arguments: Value) -> ResponseTemplate {
    ResponseTemplate::new(200).insert_header("content-type", "text/event-stream").set_body_string(
        format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({
                "choices": [{
                    "delta": {
                        "role": "assistant",
                        "tool_calls": [{
                            "index": 0,
                            "id": id,
                            "type": "function",
                            "function": {"name": name, "arguments": arguments.to_string()}
                        }]
                    },
                    "finish_reason": "tool_calls"
                }]
            })
        ),
    )
}
