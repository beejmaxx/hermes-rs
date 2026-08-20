//! Proof that runtime public events are observable before a provider stream ends.

use std::time::Duration;

use futures_util::{FutureExt, future::BoxFuture};
use ports::{
    AttemptErrorPolicy, Provider, ProviderAttempt, ProviderError, ToolBroker, ToolBrokerError,
};
use protocol::{
    AgentTurnRequest, ChatCompletionsRequest, ProviderEvent, ProviderMessage, TransportKind,
};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

struct GatedProvider {
    release: Option<oneshot::Receiver<()>>,
}

impl Provider for GatedProvider {
    fn stream<'a>(
        &'a mut self,
        _request: ChatCompletionsRequest,
    ) -> BoxFuture<'a, Result<ProviderAttempt, ProviderError>> {
        let release = self.release.take();
        async move {
            let release = release.ok_or_else(|| ProviderError::new("provider reused"))?;
            let events = async_stream::try_stream! {
                yield ProviderEvent::MessageStart;
                yield ProviderEvent::TextDelta { text: "visible now".into() };
                release
                    .await
                    .map_err(|_| ProviderError::new("release sender dropped"))?;
                yield ProviderEvent::Completed {
                    finish_reason: Some("stop".into()),
                    provider_data: None,
                };
            };
            Ok(ProviderAttempt {
                attempt_id: "gated-attempt".into(),
                error_policy: AttemptErrorPolicy::Stop,
                events: Box::pin(events),
            })
        }
        .boxed()
    }
}

struct EmptyTools;

impl ToolBroker for EmptyTools {
    fn plan(
        &mut self,
        calls: &[domain::ToolCall],
    ) -> Result<Vec<domain::PlannedToolCall>, ToolBrokerError> {
        if calls.is_empty() {
            Ok(Vec::new())
        } else {
            Err(ToolBrokerError::new("unexpected tool call"))
        }
    }

    fn execute<'a>(
        &'a mut self,
        _calls: &'a [domain::PlannedToolCall],
    ) -> BoxFuture<'a, Result<Vec<domain::ToolTerminal>, ToolBrokerError>> {
        async { Ok(Vec::new()) }.boxed()
    }
}

struct ChannelObserver {
    events: mpsc::UnboundedSender<Value>,
}

impl runtime::RuntimeEventObserver for ChannelObserver {
    fn observe(&mut self, event: &Value) -> Result<(), runtime::RuntimeEventObserverError> {
        self.events
            .send(event.clone())
            .map_err(|_| runtime::RuntimeEventObserverError::new("event receiver dropped"))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn delta_is_observed_before_the_provider_is_released()
-> Result<(), Box<dyn std::error::Error>> {
    let (release_sender, release_receiver) = oneshot::channel();
    let (event_sender, mut event_receiver) = mpsc::unbounded_channel();
    let request = AgentTurnRequest {
        execution_scope: "live-observer-test".into(),
        transport: TransportKind::ChatCompletions,
        model: "test-model".into(),
        system_prompt: None,
        conversation: vec![ProviderMessage::User { content: "hello".into() }],
        tools: Vec::new(),
    };
    let turn = tokio::spawn(async move {
        let mut provider = GatedProvider { release: Some(release_receiver) };
        let mut tools = EmptyTools;
        let mut observer = ChannelObserver { events: event_sender };
        runtime::run_turn_observed(request, &mut provider, &mut tools, &mut observer).await
    });

    let mut observed = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), event_receiver.recv())
            .await?
            .ok_or("event channel closed before message.delta")?;
        let is_delta = event.get("type").and_then(Value::as_str) == Some("message.delta");
        observed.push(event);
        if is_delta {
            break;
        }
    }
    assert!(!turn.is_finished(), "turn completed before the provider release");
    release_sender.send(()).map_err(|()| "provider release receiver dropped")?;

    let outcome = tokio::time::timeout(Duration::from_secs(2), turn).await???;
    while let Ok(event) = event_receiver.try_recv() {
        observed.push(event);
    }
    assert_eq!(observed, outcome.public_events);
    assert_eq!(outcome.terminal_outcome.final_response.as_deref(), Some("visible now"));
    Ok(())
}
