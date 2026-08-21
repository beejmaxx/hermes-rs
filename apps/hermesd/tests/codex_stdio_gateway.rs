//! Existing-client proof for a Codex-backed Rust gateway session.

#![cfg(unix)]

use std::{
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
};

use hermesd::adapters::{SqliteEffectLedger, SqliteSessionStore};
use ports::{EffectLedger, SessionStore};
use protocol::{CodexAuthorityProfile, EngineConfig, ModelReasoningEffort, TransportKind};
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};

#[tokio::test(flavor = "multi_thread")]
async fn codex_gateway_uses_terminal_approval_persists_and_projects_history()
-> Result<(), Box<dyn std::error::Error>> {
    let state_dir = tempdir()?;
    let workspace = tempdir()?;
    let root = fs::canonicalize(workspace.path())?;
    let database = state_dir.path().join("state.db");
    let counter = state_dir.path().join("worker-count");
    let (_fixture, codex) = fake_codex(&root, &counter)?;
    let command = "printf approved > codex-approved.txt";
    let mut gateway = GatewayProcess::spawn(&[
        "--state",
        path_text(&database)?,
        "gateway",
        "--engine",
        "codex",
        "--codex-command",
        path_text(&codex)?,
        "--model",
        "gpt-5.6-luna",
        "--root",
        path_text(&root)?,
    ])?;

    let ready = gateway.read_frame()?;
    assert_eq!(ready["params"]["payload"]["backend"], "hermes-rs");
    gateway.request("setup", "setup.status", json!({}))?;
    assert_eq!(gateway.read_response("setup")?["result"]["provider_configured"], true);
    gateway.request("create", "session.create", json!({}))?;
    let created = gateway.read_response("create")?;
    let session_id = created["result"]["session_id"]
        .as_str()
        .ok_or("session.create omitted session_id")?
        .to_owned();
    assert_eq!(created["result"]["info"]["engine"], "codex");
    assert_eq!(created["result"]["info"]["tools"]["delegation"], json!([]));
    assert_eq!(created["result"]["info"]["tools"]["terminal"], json!(["terminal"]));

    gateway.request(
        "first",
        "prompt.submit",
        json!({"session_id": session_id, "text": "create the approved marker"}),
    )?;
    assert_eq!(gateway.read_response("first")?["result"]["status"], "streaming");
    let approval = read_until_approval(&mut gateway, &session_id)?;
    assert_eq!(approval["params"]["payload"]["command"], command);
    assert!(!root.join("codex-approved.txt").exists());
    gateway.request(
        "approve",
        "approval.respond",
        json!({"session_id": session_id, "choice": "once"}),
    )?;
    let approved = read_response_and_session_info(&mut gateway, "approve", &session_id)?;
    assert_eq!(approved["result"]["resolved"], true);
    assert_eq!(fs::read_to_string(root.join("codex-approved.txt"))?, "approved");

    gateway.request("resume-one", "session.resume", json!({"session_id": session_id}))?;
    let resumed = gateway.read_response("resume-one")?;
    assert_eq!(resumed["result"]["message_count"], 4);
    assert_eq!(resumed["result"]["messages"][0]["text"], "create the approved marker");
    assert_eq!(resumed["result"]["messages"][3]["text"], "The approved marker was created.");

    gateway.request(
        "second",
        "prompt.submit",
        json!({"session_id": session_id, "text": "what happened?"}),
    )?;
    assert_eq!(gateway.read_response("second")?["result"]["status"], "streaming");
    let completion = read_until_message_complete(&mut gateway, &session_id)?;
    assert_eq!(
        completion["params"]["payload"]["text"],
        "The prior approved terminal action created the marker."
    );
    read_until_session_info(&mut gateway, &session_id)?;
    gateway.request("resume-two", "session.resume", json!({"session_id": session_id}))?;
    let resumed = gateway.read_response("resume-two")?;
    assert_eq!(resumed["result"]["message_count"], 6);
    assert_eq!(resumed["result"]["messages"][4]["text"], "what happened?");
    gateway.shutdown()?;

    let changed_effort = Command::new(env!("CARGO_BIN_EXE_hermesd"))
        .args([
            "--state",
            path_text(&database)?,
            "chat",
            "--session",
            &session_id,
            "--reasoning",
            "high",
            "try to change the frozen effort",
        ])
        .output()?;
    assert!(!changed_effort.status.success());
    assert!(
        String::from_utf8_lossy(&changed_effort.stderr)
            .contains("--reasoning cannot change for an existing session")
    );

    let session_id = domain::SessionId::new(session_id)?;
    let snapshot = SqliteSessionStore::open(&database)?.load(&session_id)?;
    assert_eq!(snapshot.config.transport, TransportKind::CodexAppServer);
    assert_eq!(snapshot.config.provider_adapter, "codex-app-server");
    assert_eq!(
        snapshot.config.engine_config,
        EngineConfig::CodexAppServer {
            reasoning_effort: ModelReasoningEffort::Low,
            authority_profile: CodexAuthorityProfile::HermesOwnedEffectsV1,
        }
    );
    assert_eq!(snapshot.owner_generation.get(), 3);
    assert_eq!(snapshot.conversation.len(), 6);
    assert!(SqliteEffectLedger::open(&database)?.pending()?.is_empty());
    assert_eq!(fs::read_to_string(counter)?, "2\n");
    Ok(())
}

fn read_until_approval(
    gateway: &mut GatewayProcess,
    session_id: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
    loop {
        let frame = gateway.read_frame()?;
        if frame["method"] == "event"
            && frame["params"]["session_id"] == session_id
            && frame["params"]["type"] == "tool.start"
        {
            return Err("terminal tool started before approval resolved".into());
        }
        if frame["method"] == "event"
            && frame["params"]["session_id"] == session_id
            && frame["params"]["type"] == "approval.request"
        {
            return Ok(frame);
        }
    }
}

fn read_response_and_session_info(
    gateway: &mut GatewayProcess,
    response_id: &str,
    session_id: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut response = None;
    let mut complete = false;
    while response.is_none() || !complete {
        let frame = gateway.read_frame()?;
        if frame.get("id").and_then(Value::as_str) == Some(response_id) {
            response = Some(frame);
        } else if frame["method"] == "event"
            && frame["params"]["session_id"] == session_id
            && frame["params"]["type"] == "session.info"
        {
            complete = true;
        }
    }
    response.ok_or_else(|| "approval response was not received".into())
}

fn read_until_message_complete(
    gateway: &mut GatewayProcess,
    session_id: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
    loop {
        let frame = gateway.read_frame()?;
        if frame["method"] == "event"
            && frame["params"]["session_id"] == session_id
            && frame["params"]["type"] == "message.complete"
        {
            return Ok(frame);
        }
    }
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
}

impl Drop for GatewayProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn fake_codex(
    workspace: &Path,
    counter: &Path,
) -> Result<(TempDir, PathBuf), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let executable = directory.path().join("fake-codex");
    let cwd = path_text(workspace)?;
    let counter = path_text(counter)?;
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

reject_text() {{
  case "$line" in
    *"$1"*) echo "frame unexpectedly contained $1: $line" >&2; exit 92 ;;
    *) ;;
  esac
}}

require_arg() {{
  if [ "$1" != "$2" ]; then
    echo "expected argument $2, received $1" >&2
    exit 93
  fi
}}

require_arg "$1" "app-server"
require_arg "$2" "--stdio"
count=0
if [ -f '{counter}' ]; then
  IFS= read -r count < '{counter}'
fi
count=$((count + 1))
printf '%s\n' "$count" > '{counter}'
thread_id="thread-$count"
turn_id="turn-$count"

read_frame
require_text '"method":"initialize"'
require_text '"experimentalApi":true'
emit '{{"id":1,"result":{{"userAgent":"fake-codex/gateway","codexHome":"/tmp/fake-codex-home","platformFamily":"unix","platformOs":"macos"}}}}'

read_frame
require_text '"method":"initialized"'
read_frame
require_text '"method":"config/read"'
emit '{{"id":2,"result":{{"config":{{"mcp_servers":{{"ambient.docs":{{"command":"docs"}}}}}},"origins":{{}},"layers":null}}}}'

read_frame
require_text '"method":"thread/start"'
require_text '"environments":[]'
require_text '"mcp_servers":{{"ambient.docs":{{"enabled":false}}}}'
require_text '"name":"terminal"'
reject_text '"name":"delegate_task"'
emit "{{\"method\":\"thread/started\",\"params\":{{\"thread\":{{\"id\":\"$thread_id\"}}}}}}"
emit "{{\"id\":3,\"result\":{{\"thread\":{{\"id\":\"$thread_id\"}},\"model\":\"gpt-5.6-luna\",\"modelProvider\":\"openai_http\",\"cwd\":\"{cwd}\",\"approvalPolicy\":\"never\",\"sandbox\":{{\"type\":\"readOnly\"}}}}}}"

read_frame
require_text '"method":"turn/start"'
require_text '"environments":[]'
require_text '"approvalPolicy":"never"'
require_text '"sandboxPolicy":{{"type":"readOnly","networkAccess":false}}'
require_text '"effort":"low"'
if [ "$count" -eq 1 ]; then
  require_text 'create the approved marker'
else
  require_text 'Hermes is starting a fresh cognitive worker'
  require_text 'what happened?'
  require_text 'The approved marker was created.'
fi
emit "{{\"method\":\"turn/started\",\"params\":{{\"threadId\":\"$thread_id\",\"turn\":{{\"id\":\"$turn_id\",\"status\":\"inProgress\",\"items\":[]}}}}}}"
emit "{{\"id\":4,\"result\":{{\"turn\":{{\"id\":\"$turn_id\",\"status\":\"inProgress\",\"items\":[]}}}}}}"

if [ "$count" -eq 1 ]; then
  emit "{{\"id\":\"dynamic-terminal\",\"method\":\"item/tool/call\",\"params\":{{\"threadId\":\"$thread_id\",\"turnId\":\"$turn_id\",\"callId\":\"worker-terminal\",\"namespace\":null,\"tool\":\"terminal\",\"arguments\":{{\"command\":\"printf approved > codex-approved.txt\"}}}}}}"
  read_frame
  require_text '"id":"dynamic-terminal"'
  require_text 'Command exited with status 0'
  require_text '"success":true'
  answer='The approved marker was created.'
else
  answer='The prior approved terminal action created the marker.'
fi
emit "{{\"method\":\"item/agentMessage/delta\",\"params\":{{\"threadId\":\"$thread_id\",\"turnId\":\"$turn_id\",\"itemId\":\"message-$count\",\"delta\":\"$answer\"}}}}"
emit "{{\"method\":\"turn/completed\",\"params\":{{\"threadId\":\"$thread_id\",\"turn\":{{\"id\":\"$turn_id\",\"status\":\"completed\",\"items\":[]}}}}}}"

if IFS= read -r line; then
  echo "unexpected frame after turn: $line" >&2
  exit 94
fi
"#,
    );
    fs::write(&executable, script)?;
    let mut permissions = fs::metadata(&executable)?.permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(&executable, permissions)?;
    Ok((directory, executable))
}

fn path_text(path: &Path) -> Result<&str, Box<dyn std::error::Error>> {
    path.to_str().ok_or_else(|| "test path is not valid UTF-8".into())
}
