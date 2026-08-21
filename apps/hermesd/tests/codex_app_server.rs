//! Child-process integration proof for the typed Codex app-server client.

#![cfg(unix)]

use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};

use hermesd::adapters::{
    CodexAppServer, CodexAppServerCommand, CodexAppServerError, CodexAppServerEvent,
    CodexApprovalPolicy, CodexInitializeParams, CodexNotification, CodexSandboxMode,
    CodexThreadStartParams, CodexTurnInterruptParams, CodexTurnStartParams, CodexTurnStatus,
};
use serde_json::json;
use tempfile::{TempDir, tempdir};

#[tokio::test]
async fn client_supervises_full_duplex_thread_and_turn_lifecycle()
-> Result<(), Box<dyn std::error::Error>> {
    let (_fixture, executable) = fake_app_server(
        r#"
require_arg "$1" "app-server"
require_arg "$2" "--stdio"
require_arg "$3" "--fake-flag"
require_arg "$4" 'literal argument;$(not-a-command)'

read_frame
require_text '"id":1'
require_text '"method":"initialize"'
require_text '"experimentalApi":true'
emit '{"id":"server-1","method":"item/tool/requestUserInput","params":{"question":"continue?"}}'
emit '{"id":1,"result":{"userAgent":"fake-codex/1","codexHome":"/tmp/fake-codex-home","platformFamily":"unix","platformOs":"macos"}}'

read_frame
require_text '"id":"server-1"'
require_text '"result":{"ok":true}'

read_frame
require_text '"method":"initialized"'

read_frame
require_text '"id":2'
require_text '"method":"thread/start"'
require_text '"model":"gpt-5.6-luna"'
require_text '"approvalPolicy":"never"'
require_text '"sandbox":"read-only"'
emit '{"method":"thread/started","params":{"thread":{"id":"thread-1"}}}'
emit '{"id":2,"result":{"thread":{"id":"thread-1"},"model":"gpt-5.6-luna","modelProvider":"openai_http","cwd":"/tmp/fake-workspace","approvalPolicy":"never","sandbox":{"type":"readOnly"}}}'

read_frame
require_text '"id":3'
require_text '"method":"turn/start"'
require_text '"threadId":"thread-1"'
require_text '"text":"first prompt"'
emit '{"method":"turn/started","params":{"threadId":"thread-1","turn":{"id":"turn-1","status":"inProgress","items":[]}}}'
emit '{"id":3,"result":{"turn":{"id":"turn-1","status":"inProgress","items":[]}}}'
emit '{"method":"item/agentMessage/delta","params":{"threadId":"thread-1","turnId":"turn-1","itemId":"message-1","delta":"hello "}}'
emit '{"method":"item/agentMessage/delta","params":{"threadId":"thread-1","turnId":"turn-1","itemId":"message-1","delta":"world"}}'
emit '{"method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed","items":[]}}}'

read_frame
require_text '"id":4'
require_text '"method":"turn/start"'
require_text '"text":"interrupt me"'
emit '{"id":4,"result":{"turn":{"id":"turn-2","status":"inProgress","items":[]}}}'
emit '{"method":"item/agentMessage/delta","params":{"threadId":"thread-1","turnId":"turn-2","itemId":"message-2","delta":"partial"}}'

read_frame
require_text '"id":5'
require_text '"method":"turn/interrupt"'
require_text '"turnId":"turn-2"'
emit '{"id":5,"result":{}}'
emit '{"method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"turn-2","status":"interrupted","items":[]}}}'

if IFS= read -r line; then
  echo "unexpected frame after lifecycle: $line" >&2
  exit 97
fi
"#,
    )?;
    let specification = CodexAppServerCommand::new(&executable)
        .arg("--fake-flag")
        .arg("literal argument;$(not-a-command)")
        .current_dir("/tmp");
    assert_eq!(specification.executable(), executable.as_path());
    assert_eq!(specification.arguments(), ["--fake-flag", "literal argument;$(not-a-command)"]);

    let mut client = CodexAppServer::spawn(&specification)?;
    let initialized = client
        .initialize(&CodexInitializeParams::hermes("0.1.0").with_experimental_api(true))
        .await?;
    assert_eq!(initialized.user_agent(), "fake-codex/1");
    assert_eq!(initialized.codex_home(), std::path::Path::new("/tmp/fake-codex-home"));
    assert_eq!(initialized.platform_family(), "unix");
    assert_eq!(initialized.platform_os(), "macos");

    let CodexAppServerEvent::Request(server_request) = client.next_event().await? else {
        return Err("initialize did not preserve the interleaved server request".into());
    };
    assert_eq!(server_request.id(), &hermesd::adapters::CodexRequestId::String("server-1".into()));
    assert_eq!(server_request.method(), "item/tool/requestUserInput");
    assert_eq!(server_request.params(), &json!({"question": "continue?"}));
    client.respond(server_request.id(), &json!({"ok": true})).await?;
    client.initialized().await?;

    let opened = client
        .start_thread(
            &CodexThreadStartParams::new()
                .with_model("gpt-5.6-luna")
                .with_cwd("/tmp/fake-workspace")
                .with_approval_policy(CodexApprovalPolicy::Never)
                .with_sandbox(CodexSandboxMode::ReadOnly)
                .with_base_instructions("Stay within Hermes authority.")
                .with_developer_instructions("Use only client-hosted capabilities.")
                .with_ephemeral(false),
        )
        .await?;
    assert_eq!(opened.thread().id(), "thread-1");
    assert_eq!(opened.model(), "gpt-5.6-luna");
    assert_eq!(opened.model_provider(), "openai_http");
    assert_eq!(opened.cwd(), std::path::Path::new("/tmp/fake-workspace"));
    assert_eq!(opened.approval_policy(), &json!("never"));
    assert_eq!(opened.sandbox(), &json!({"type": "readOnly"}));

    let first = client
        .start_turn(
            &CodexTurnStartParams::text("thread-1", "first prompt")
                .with_model("gpt-5.6-luna")
                .with_effort("low")
                .with_client_user_message_id("foreground-1"),
        )
        .await?;
    assert_eq!(first.turn().id(), "turn-1");
    assert_eq!(first.turn().status(), CodexTurnStatus::InProgress);

    let CodexAppServerEvent::Notification(CodexNotification::ThreadStarted(thread)) =
        client.next_event().await?
    else {
        return Err("thread/start notification was not buffered in order".into());
    };
    assert_eq!(thread.id(), "thread-1");
    let CodexAppServerEvent::Notification(CodexNotification::TurnStarted(started)) =
        client.next_event().await?
    else {
        return Err("turn/start notification was not buffered in order".into());
    };
    assert_eq!(started.thread_id(), "thread-1");
    assert_eq!(started.turn().id(), "turn-1");

    let mut text = String::new();
    for _ in 0..2 {
        let CodexAppServerEvent::Notification(CodexNotification::AgentMessageDelta(delta)) =
            client.next_event().await?
        else {
            return Err("expected assistant message delta".into());
        };
        assert_eq!(delta.thread_id(), "thread-1");
        assert_eq!(delta.turn_id(), "turn-1");
        assert_eq!(delta.item_id(), "message-1");
        text.push_str(delta.delta());
    }
    assert_eq!(text, "hello world");
    let CodexAppServerEvent::Notification(CodexNotification::TurnCompleted(completed)) =
        client.next_event().await?
    else {
        return Err("expected successful turn completion".into());
    };
    assert_eq!(completed.thread_id(), "thread-1");
    assert_eq!(completed.turn().status(), CodexTurnStatus::Completed);
    assert!(completed.turn().items().is_empty());
    assert_eq!(completed.turn().error(), None);

    let second = client.start_turn(&CodexTurnStartParams::text("thread-1", "interrupt me")).await?;
    assert_eq!(second.turn().id(), "turn-2");
    let CodexAppServerEvent::Notification(CodexNotification::AgentMessageDelta(partial)) =
        client.next_event().await?
    else {
        return Err("expected partial assistant output before interruption".into());
    };
    assert_eq!(partial.delta(), "partial");
    client.interrupt_turn(&CodexTurnInterruptParams::new("thread-1", "turn-2")).await?;
    let CodexAppServerEvent::Notification(CodexNotification::TurnCompleted(interrupted)) =
        client.next_event().await?
    else {
        return Err("expected interrupted turn completion".into());
    };
    assert_eq!(interrupted.turn().status(), CodexTurnStatus::Interrupted);
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn malformed_worker_frame_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let (_fixture, executable) = fake_app_server(
        r#"
read_frame
emit 'this is not json'
"#,
    )?;
    let mut client = CodexAppServer::spawn(&CodexAppServerCommand::new(executable))?;
    let error = match client.initialize(&CodexInitializeParams::hermes("0.1.0")).await {
        Ok(_) => return Err("malformed frame unexpectedly initialized the worker".into()),
        Err(error) => error,
    };
    assert!(matches!(error, CodexAppServerError::MalformedFrame(_)));
    Ok(())
}

#[tokio::test]
async fn child_exit_reports_status_and_bounded_stderr() -> Result<(), Box<dyn std::error::Error>> {
    let (_fixture, executable) = fake_app_server(
        r#"
read_frame
echo 'fake worker crash' >&2
exit 17
"#,
    )?;
    let mut client = CodexAppServer::spawn(&CodexAppServerCommand::new(executable))?;
    let error = match client.initialize(&CodexInitializeParams::hermes("0.1.0")).await {
        Ok(_) => return Err("exited worker unexpectedly initialized".into()),
        Err(error) => error,
    };
    let CodexAppServerError::ProcessExited { code, stderr } = error else {
        return Err(format!("expected process exit, got {error}").into());
    };
    assert_eq!(code, Some(17));
    assert!(stderr.contains("fake worker crash"));
    Ok(())
}

fn fake_app_server(body: &str) -> Result<(TempDir, PathBuf), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let executable = directory.path().join("fake-codex");
    let script = format!(
        r#"#!/bin/sh
set -eu

emit() {{
  printf '%s\n' "$1"
}}

read_frame() {{
  if ! IFS= read -r line; then
    echo 'expected protocol frame before EOF' >&2
    exit 90
  fi
}}

require_text() {{
  case "$line" in
    *"$1"*) ;;
    *) echo "frame missing $1: $line" >&2; exit 91 ;;
  esac
}}

require_arg() {{
  if [ "$1" != "$2" ]; then
    echo "expected argument $2, received $1" >&2
    exit 92
  fi
}}

{body}
"#,
    );
    fs::write(&executable, script)?;
    let mut permissions = fs::metadata(&executable)?.permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(&executable, permissions)?;
    Ok((directory, executable))
}
