//! End-to-end parent/child delegation through the live runtime and effect ledger.

use std::fs;

use hermesd::adapters::{
    AgentTools, AgentToolsConfig, OpenAiCompatibleProvider, ReadOnlyLocalTools, SqliteEffectLedger,
};
use ports::EffectLedger;
use protocol::{AgentTurnRequest, ProviderMessage, TerminalStatus, TransportKind};
use runtime::JournaledToolBroker;
use rusqlite::Connection;
use serde_json::{Value, json};
use tempfile::tempdir;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers};

#[tokio::test]
async fn parent_delegates_an_isolated_child_and_receives_only_its_summary()
-> Result<(), Box<dyn std::error::Error>> {
    let state_dir = tempdir()?;
    let workspace = tempdir()?;
    fs::write(workspace.path().join("README.md"), "delegated\n")?;
    let root = fs::canonicalize(workspace.path())?;
    let database = state_dir.path().join("state.db");
    let server = MockServer::start().await;
    let base_url = format!("{}/v1", server.uri());
    let parent_system = "Coordinate focused work.";
    let parent_prompt = "Ask a child to inspect README.md, then report its answer.";
    let goal = "Read README.md and report its contents.";
    let context = "Focus on the first line.";
    let child_system = format!(
        "You are a focused leaf subagent. You cannot delegate, interact with the user, or modify files. Inspect the workspace at {} with read-only tools when useful.\n\nTASK:\n{goal}\n\nCONTEXT:\n{context}",
        root.display()
    );

    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_json(json!({
            "model": "test-model",
            "messages": [
                {"role": "system", "content": parent_system},
                {"role": "user", "content": parent_prompt}
            ],
            "tools": AgentTools::catalog(),
            "stream": true,
            "stream_options": {"include_usage": true}
        })))
        .respond_with(tool_call(
            "parent-delegate",
            "delegate_task",
            json!({"goal": goal, "context": context}),
        ))
        .mount(&server)
        .await;

    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_json(json!({
            "model": "test-model",
            "messages": [
                {"role": "system", "content": child_system},
                {"role": "user", "content": "Complete the assigned task and return a concise result for the parent agent."}
            ],
            "tools": ReadOnlyLocalTools::catalog(),
            "stream": true,
            "stream_options": {"include_usage": true}
        })))
        .respond_with(tool_call("child-read", "read_file", json!({"path": "README.md"})))
        .mount(&server)
        .await;

    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_json(json!({
            "model": "test-model",
            "messages": [
                {"role": "system", "content": child_system},
                {"role": "user", "content": "Complete the assigned task and return a concise result for the parent agent."},
                {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [{
                        "id": "child-read",
                        "type": "function",
                        "function": {"name": "read_file", "arguments": "{\"path\":\"README.md\"}"}
                    }]
                },
                {"role": "tool", "tool_call_id": "child-read", "content": "1|delegated\n"}
            ],
            "tools": ReadOnlyLocalTools::catalog(),
            "stream": true,
            "stream_options": {"include_usage": true}
        })))
        .respond_with(text("The file says delegated."))
        .mount(&server)
        .await;

    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_json(json!({
            "model": "test-model",
            "messages": [
                {"role": "system", "content": parent_system},
                {"role": "user", "content": parent_prompt},
                {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [{
                        "id": "parent-delegate",
                        "type": "function",
                        "function": {
                            "name": "delegate_task",
                            "arguments": "{\"context\":\"Focus on the first line.\",\"goal\":\"Read README.md and report its contents.\"}"
                        }
                    }]
                },
                {
                    "role": "tool",
                    "tool_call_id": "parent-delegate",
                    "content": "The file says delegated."
                }
            ],
            "tools": AgentTools::catalog(),
            "stream": true,
            "stream_options": {"include_usage": true}
        })))
        .respond_with(text("The child reports that README says delegated."))
        .mount(&server)
        .await;

    let scope = "parent-turn";
    let mut provider = OpenAiCompatibleProvider::new(&base_url, Some("test-key".into()))?;
    let tools_config = AgentToolsConfig::new(
        root.clone(),
        database.clone(),
        base_url,
        Some("test-key".into()),
        "test-model",
        true,
    )?;
    let inner = AgentTools::new(tools_config, scope)?;
    let ledger = SqliteEffectLedger::open(&database)?;
    let mut tools = JournaledToolBroker::new(inner, ledger, scope)?;
    let outcome = runtime::run_turn(
        AgentTurnRequest {
            execution_scope: scope.into(),
            transport: TransportKind::ChatCompletions,
            model: "test-model".into(),
            system_prompt: Some(parent_system.into()),
            conversation: vec![ProviderMessage::User { content: parent_prompt.into() }],
            tools: AgentTools::catalog(),
        },
        &mut provider,
        &mut tools,
    )
    .await?;

    assert_eq!(outcome.terminal_outcome.status, TerminalStatus::Completed);
    assert_eq!(
        outcome.terminal_outcome.final_response.as_deref(),
        Some("The child reports that README says delegated.")
    );
    assert_eq!(outcome.provider_requests.len(), 2);
    assert_eq!(outcome.semantic_conversation.len(), 4);
    let (_, mut ledger) = tools.into_parts();
    assert!(ledger.pending()?.is_empty());
    drop(ledger);

    let connection = Connection::open(&database)?;
    let effects = connection.query_row(
        "SELECT
             count(*),
             sum(effect_json = '\"model_inference\"'),
             sum(effect_json = '\"read_only\"'),
             sum(status = 'planned')
         FROM tool_effects",
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        },
    )?;
    assert_eq!(effects, (2, 1, 1, 0));
    let requests = server
        .received_requests()
        .await
        .ok_or("request recording is disabled on the mock server")?;
    assert_eq!(requests.len(), 4);
    Ok(())
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

fn text(content: &str) -> ResponseTemplate {
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
