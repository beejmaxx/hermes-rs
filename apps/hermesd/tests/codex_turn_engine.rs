//! Process-level proof that Codex can reason while Hermes remains the tool authority.

#![cfg(unix)]

use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};

use domain::ToolResultStatus;
use hermesd::adapters::{
    CodexAppServerCommand, CodexEngineTurnRequest, CodexTurnEngine, ReadOnlyLocalTools,
    SqliteEffectLedger,
};
use ports::EffectLedger;
use protocol::TerminalStatus;
use runtime::{JournaledToolBroker, RuntimeEventObserver, RuntimeEventObserverError};
use serde_json::Value;
use tempfile::{TempDir, tempdir};

#[derive(Default)]
struct RecordingObserver {
    events: Vec<Value>,
}

impl RuntimeEventObserver for RecordingObserver {
    fn observe(&mut self, event: &Value) -> Result<(), RuntimeEventObserverError> {
        self.events.push(event.clone());
        Ok(())
    }
}

#[tokio::test]
async fn codex_turn_routes_its_only_effect_through_the_hermes_ledger()
-> Result<(), Box<dyn std::error::Error>> {
    let workspace = tempdir()?;
    fs::write(workspace.path().join("README.md"), "Hermes authority\n")?;
    let canonical_workspace = fs::canonicalize(workspace.path())?;
    let cwd_json = serde_json::to_string(
        canonical_workspace.to_str().ok_or("temporary workspace path is not UTF-8")?,
    )?;
    let (_fixture, executable) = fake_app_server(
        r#"
require_arg "$1" "app-server"
require_arg "$2" "--stdio"

read_frame
require_text '"id":1'
require_text '"method":"initialize"'
require_text '"experimentalApi":true'
emit '{"id":1,"result":{"userAgent":"fake-codex/engine-test","codexHome":"/tmp/fake-codex-home","platformFamily":"unix","platformOs":"macos"}}'

read_frame
require_text '"method":"initialized"'

read_frame
require_text '"id":2'
require_text '"method":"config/read"'
require_text '"cwd":__CWD_JSON__'
emit '{"id":2,"result":{"config":{"mcp_servers":{"ambient.docs":{"command":"docs"}}},"origins":{},"layers":null}}'

read_frame
require_text '"id":3'
require_text '"method":"thread/start"'
require_text '"model":"gpt-5.6-luna"'
require_text '"cwd":__CWD_JSON__'
require_text '"approvalPolicy":"never"'
require_text '"sandbox":"read-only"'
require_text '"ephemeral":true'
require_text '"environments":[]'
require_text '"shell_tool":false'
require_text '"mcp_servers":{"ambient.docs":{"enabled":false}}'
require_text '"dynamicTools":[{"type":"function","name":"read_file"'
emit '{"method":"thread/started","params":{"thread":{"id":"thread-authority"}}}'
emit '{"id":3,"result":{"thread":{"id":"thread-authority"},"model":"gpt-5.6-luna","modelProvider":"openai_http","cwd":__CWD_JSON__,"approvalPolicy":"never","sandbox":{"type":"readOnly"}}}'

read_frame
require_text '"id":4'
require_text '"method":"turn/start"'
require_text '"threadId":"thread-authority"'
require_text '"text":"Read README.md and report its contents."'
emit '{"method":"turn/started","params":{"threadId":"thread-authority","turn":{"id":"turn-authority","status":"inProgress","items":[]}}}'
emit '{"id":4,"result":{"turn":{"id":"turn-authority","status":"inProgress","items":[]}}}'
emit '{"id":"dynamic-read","method":"item/tool/call","params":{"threadId":"thread-authority","turnId":"turn-authority","callId":"worker-call-read","namespace":null,"tool":"read_file","arguments":{"path":"README.md"}}}'

read_frame
require_text '"id":"dynamic-read"'
require_text '"type":"inputText"'
require_text 'Hermes authority'
require_text '"success":true'
emit '{"method":"item/agentMessage/delta","params":{"threadId":"thread-authority","turnId":"turn-authority","itemId":"message-1","delta":"README says Hermes authority."}}'
emit '{"method":"turn/completed","params":{"threadId":"thread-authority","turn":{"id":"turn-authority","status":"completed","items":[]}}}'

if IFS= read -r line; then
  echo "unexpected frame after turn: $line" >&2
  exit 97
fi
"#,
        &cwd_json,
    )?;
    let engine = CodexTurnEngine::new(
        CodexAppServerCommand::new(executable),
        "gpt-5.6-luna",
        &canonical_workspace,
        "Hermes owns durable state and tool authority.",
        "Use only the dynamic tools supplied by Hermes.",
    )?
    .with_effort("low");
    let scope = "codex-authority-turn";
    let tools = ReadOnlyLocalTools::new(&canonical_workspace, scope)?;
    let ledger = SqliteEffectLedger::in_memory()?;
    let mut tools = JournaledToolBroker::new(tools, ledger, scope)?;
    let mut observer = RecordingObserver::default();

    let outcome = engine
        .run_new(
            CodexEngineTurnRequest {
                execution_scope: scope.into(),
                semantic_history: Vec::new(),
                prompt: "Read README.md and report its contents.".into(),
                client_user_message_id: Some("foreground-turn-1".into()),
            },
            &ReadOnlyLocalTools::catalog(),
            &mut tools,
            &mut observer,
        )
        .await?;

    assert_eq!(outcome.binding.thread_id, "thread-authority");
    assert_eq!(outcome.binding.worker_user_agent, "fake-codex/engine-test");
    assert_eq!(outcome.binding.model_provider, "openai_http");
    assert_eq!(outcome.binding.authority.dynamic_tools(), ["read_file", "search_files"]);
    assert_eq!(outcome.binding.authority.disabled_mcp_servers(), ["ambient.docs"]);
    assert_eq!(outcome.contract.terminal_outcome.status, TerminalStatus::Completed);
    assert_eq!(
        outcome.contract.terminal_outcome.final_response.as_deref(),
        Some("README says Hermes authority.")
    );
    assert_eq!(outcome.contract.semantic_conversation.len(), 4);
    assert!(matches!(
        &outcome.contract.semantic_conversation[2],
        domain::SemanticMessage::ToolResultBatch { results }
            if results.len() == 1
                && results[0].status == ToolResultStatus::Succeeded
                && results[0].content.contains("Hermes authority")
                && results[0].execution_key.as_deref()
                    == Some("codex-authority-turn:worker-call-read")
    ));
    assert_eq!(observer.events, outcome.contract.public_events);
    assert!(outcome.contract.public_events.iter().any(|event| {
        event["type"] == "tool.complete"
            && event["execution_key"] == "codex-authority-turn:worker-call-read"
    }));
    let (_, mut ledger) = tools.into_parts();
    assert!(ledger.pending()?.is_empty());
    Ok(())
}

fn fake_app_server(
    body: &str,
    cwd_json: &str,
) -> Result<(TempDir, PathBuf), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let executable = directory.path().join("fake-codex");
    let body = body.replace("__CWD_JSON__", cwd_json);
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
