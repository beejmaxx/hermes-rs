//! End-to-end live-edge proof using local HTTP and filesystem adapters.

use std::fs;

use hermesd::adapters::{OpenAiCompatibleProvider, ReadOnlyLocalTools, SqliteEffectLedger};
use ports::EffectLedger;
use protocol::{AgentTurnRequest, ProviderMessage, TerminalStatus, TransportKind};
use runtime::JournaledToolBroker;
use serde_json::json;
use tempfile::tempdir;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers};

#[tokio::test]
async fn streamed_provider_tool_round_trip_uses_the_real_runtime()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempdir()?;
    fs::write(root.path().join("README.md"), "Rust boundary\n")?;
    let server = MockServer::start().await;
    let catalog = ReadOnlyLocalTools::catalog();
    let system = "Inspect the workspace when needed.";
    let prompt = "What does README.md say?";

    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_json(json!({
            "model": "test-model",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt}
            ],
            "tools": catalog,
            "stream": true,
            "stream_options": {"include_usage": true}
        })))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(concat!(
                    "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call-read\",\"type\":\"function\",\"function\":{\"name\":\"read_file\",\"arguments\":\"{\\\"path\\\":\\\"README.md\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
                    "data: [DONE]\n\n",
                )),
        )
        .mount(&server)
        .await;

    Mock::given(matchers::method("POST"))
        .and(matchers::path("/v1/chat/completions"))
        .and(matchers::body_json(json!({
            "model": "test-model",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
                {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [{
                        "id": "call-read",
                        "type": "function",
                        "function": {
                            "name": "read_file",
                            "arguments": "{\"path\":\"README.md\"}"
                        }
                    }]
                },
                {
                    "role": "tool",
                    "tool_call_id": "call-read",
                    "content": "1|Rust boundary\n"
                }
            ],
            "tools": ReadOnlyLocalTools::catalog(),
            "stream": true,
            "stream_options": {"include_usage": true}
        })))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(concat!(
                    "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"The file says Rust boundary.\"},\"finish_reason\":\"stop\"}]}\n\n",
                    "data: [DONE]\n\n",
                )),
        )
        .mount(&server)
        .await;

    let mut provider =
        OpenAiCompatibleProvider::new(&format!("{}/v1", server.uri()), Some("test-key".into()))?;
    let tools = ReadOnlyLocalTools::new(root.path(), "integration-turn")?;
    let ledger = SqliteEffectLedger::in_memory()?;
    let mut tools = JournaledToolBroker::new(tools, ledger, "integration-turn")?;
    let outcome = runtime::run_turn(
        AgentTurnRequest {
            execution_scope: "integration-turn".into(),
            transport: TransportKind::ChatCompletions,
            model: "test-model".into(),
            system_prompt: Some(system.into()),
            conversation: vec![ProviderMessage::User { content: prompt.into() }],
            tools: ReadOnlyLocalTools::catalog(),
        },
        &mut provider,
        &mut tools,
    )
    .await?;

    assert_eq!(outcome.terminal_outcome.status, TerminalStatus::Completed);
    assert_eq!(
        outcome.terminal_outcome.final_response.as_deref(),
        Some("The file says Rust boundary.")
    );
    assert_eq!(outcome.provider_requests.len(), 2);
    assert_eq!(outcome.semantic_conversation.len(), 4);
    let (_, mut ledger) = tools.into_parts();
    assert!(ledger.pending()?.is_empty());
    let received = server
        .received_requests()
        .await
        .ok_or("request recording is disabled on the mock server")?;
    assert_eq!(received.len(), 2);
    Ok(())
}
