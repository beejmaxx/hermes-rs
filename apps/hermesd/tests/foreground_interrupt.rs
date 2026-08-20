//! Cancellation proof for write-ahead tool-effect durability.

use std::{collections::BTreeMap, time::Duration};

use domain::{PlannedToolCall, ToolEffect, ToolTerminal};
use futures_util::{FutureExt, future::BoxFuture, stream};
use hermesd::adapters::SqliteEffectLedger;
use ports::{
    AttemptErrorPolicy, EffectLedger, Provider, ProviderAttempt, ProviderError, ToolBroker,
    ToolBrokerError,
};
use protocol::{
    AgentTurnRequest, ChatCompletionsRequest, ProviderEvent, ProviderMessage, TransportKind,
};
use runtime::JournaledToolBroker;
use serde_json::json;
use tempfile::tempdir;
use tokio::sync::oneshot;

struct ToolCallingProvider;

impl Provider for ToolCallingProvider {
    fn stream<'a>(
        &'a mut self,
        _request: ChatCompletionsRequest,
    ) -> BoxFuture<'a, Result<ProviderAttempt, ProviderError>> {
        async {
            Ok(ProviderAttempt {
                attempt_id: "effect-attempt".into(),
                error_policy: AttemptErrorPolicy::Stop,
                events: Box::pin(stream::iter(
                    [
                        ProviderEvent::MessageStart,
                        ProviderEvent::ToolCallDelta {
                            index: 0,
                            id: Some("call-external".into()),
                            name: Some("external_mutation".into()),
                            arguments_delta: "{}".into(),
                        },
                        ProviderEvent::Completed {
                            finish_reason: Some("tool_calls".into()),
                            provider_data: None,
                        },
                    ]
                    .into_iter()
                    .map(Ok),
                )),
            })
        }
        .boxed()
    }
}

struct GatedExternalTool {
    started: Option<oneshot::Sender<()>>,
}

impl ToolBroker for GatedExternalTool {
    fn plan(
        &mut self,
        calls: &[domain::ToolCall],
    ) -> Result<Vec<PlannedToolCall>, ToolBrokerError> {
        let call = calls
            .first()
            .filter(|_| calls.len() == 1)
            .ok_or_else(|| ToolBrokerError::new("expected exactly one tool call"))?;
        Ok(vec![PlannedToolCall {
            call_id: call.id.clone(),
            name: call.name.clone(),
            arguments: call.arguments.clone(),
            execution_key: "interrupt-scope:call-external".into(),
            effect: ToolEffect::ExternalMutation,
            approval: None,
        }])
    }

    fn execute<'a>(
        &'a mut self,
        _calls: &'a [PlannedToolCall],
    ) -> BoxFuture<'a, Result<Vec<ToolTerminal>, ToolBrokerError>> {
        let started = self.started.take();
        async move {
            let started = started.ok_or_else(|| ToolBrokerError::new("tool reused"))?;
            let _ = started.send(());
            std::future::pending().await
        }
        .boxed()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cancellation_preserves_a_dispatched_effect_as_durably_pending()
-> Result<(), Box<dyn std::error::Error>> {
    let state_dir = tempdir()?;
    let state = state_dir.path().join("state.db");
    let (started_sender, started_receiver) = oneshot::channel();
    let (cancel_sender, cancel_receiver) = oneshot::channel();
    let turn = tokio::spawn({
        let state = state.clone();
        async move {
            let mut provider = ToolCallingProvider;
            let tools = GatedExternalTool { started: Some(started_sender) };
            let ledger = SqliteEffectLedger::open(&state)?;
            let mut tools = JournaledToolBroker::new(tools, ledger, "interrupt-scope")?;
            let request = AgentTurnRequest {
                execution_scope: "interrupt-scope".into(),
                transport: TransportKind::ChatCompletions,
                model: "test-model".into(),
                system_prompt: None,
                conversation: vec![ProviderMessage::User { content: "mutate it".into() }],
                tools: vec![json!({
                    "type": "function",
                    "function": {
                        "name": "external_mutation",
                        "parameters": {"type": "object", "properties": {}}
                    }
                })],
            };
            tokio::select! {
                outcome = runtime::run_turn(request, &mut provider, &mut tools) => Ok(Some(outcome?)),
                _ = cancel_receiver => Ok::<_, anyhow::Error>(None),
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(2), started_receiver).await??;
    cancel_sender.send(()).map_err(|()| "turn cancellation receiver dropped")?;
    let outcome = tokio::time::timeout(Duration::from_secs(2), turn).await???;
    assert!(outcome.is_none());

    let mut ledger = SqliteEffectLedger::open(&state)?;
    let pending = ledger.pending()?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].execution_scope, "interrupt-scope");
    assert_eq!(pending[0].plan.call_id.as_str(), "call-external");
    assert_eq!(pending[0].plan.effect, ToolEffect::ExternalMutation);
    assert_eq!(pending[0].plan.arguments.0, BTreeMap::new());
    Ok(())
}
