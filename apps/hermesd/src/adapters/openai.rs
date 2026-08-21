//! OpenAI-compatible Chat Completions transport.

use std::time::Duration;

use async_stream::stream;
use eventsource_stream::Eventsource;
use futures_util::{FutureExt, StreamExt, future::BoxFuture};
use ports::{AttemptErrorPolicy, Provider, ProviderAttempt, ProviderError};
use protocol::{ChatCompletionsRequest, ProviderEvent, ProviderMessage};
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use thiserror::Error;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_ERROR_BODY_BYTES: usize = 4_096;

/// Invalid configuration for an OpenAI-compatible provider endpoint.
#[derive(Debug, Error)]
pub enum OpenAiProviderConfigError {
    /// The supplied base URL was not a valid URL.
    #[error("invalid provider base URL: {0}")]
    InvalidUrl(#[from] url::ParseError),
    /// Only encrypted HTTP endpoints are accepted, except loopback development servers.
    #[error("provider URL must use HTTPS unless it targets a loopback host")]
    InsecureUrl,
    /// URLs containing embedded credentials are forbidden.
    #[error("provider URL must not contain embedded credentials")]
    EmbeddedCredentials,
    /// The HTTP client could not be constructed.
    #[error("could not construct provider HTTP client: {0}")]
    Client(#[from] reqwest::Error),
}

/// A streaming OpenAI-compatible Chat Completions provider.
pub struct OpenAiCompatibleProvider {
    client: Client,
    endpoint: Url,
    api_key: Option<String>,
    next_attempt: u64,
}

impl OpenAiCompatibleProvider {
    /// Validate and normalize a base URL without constructing a client or sending a request.
    pub fn validate_base_url(base_url: &str) -> Result<(), OpenAiProviderConfigError> {
        chat_completions_endpoint(base_url).map(drop)
    }

    /// Create a provider using the default HTTP client and timeout.
    pub fn new(base_url: &str, api_key: Option<String>) -> Result<Self, OpenAiProviderConfigError> {
        let client = Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .user_agent(concat!("hermes-rs/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Self::with_client(base_url, api_key, client)
    }

    /// Create a provider with a caller-supplied HTTP client.
    pub fn with_client(
        base_url: &str,
        api_key: Option<String>,
        client: Client,
    ) -> Result<Self, OpenAiProviderConfigError> {
        let endpoint = chat_completions_endpoint(base_url)?;
        Ok(Self { client, endpoint, api_key, next_attempt: 1 })
    }
}

impl Provider for OpenAiCompatibleProvider {
    fn stream<'a>(
        &'a mut self,
        request: ChatCompletionsRequest,
    ) -> BoxFuture<'a, Result<ProviderAttempt, ProviderError>> {
        async move {
            let attempt_id = format!("openai-{}", self.next_attempt);
            self.next_attempt = self.next_attempt.saturating_add(1);

            let mut outgoing =
                self.client.post(self.endpoint.clone()).json(&wire_request(&request));
            if let Some(api_key) = &self.api_key {
                outgoing = outgoing.bearer_auth(api_key);
            }
            let response = outgoing
                .send()
                .await
                .map_err(|error| ProviderError::new(format!("request failed: {error}")))?;
            let status = response.status();
            if !status.is_success() {
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|error| format!("could not read error response: {error}"));
                return Err(ProviderError::new(format!(
                    "HTTP {status}: {}",
                    truncate_utf8(&body, MAX_ERROR_BODY_BYTES)
                )));
            }

            let events = normalized_events(response);
            Ok(ProviderAttempt {
                attempt_id,
                error_policy: AttemptErrorPolicy::Stop,
                events: Box::pin(events),
            })
        }
        .boxed()
    }
}

fn chat_completions_endpoint(base_url: &str) -> Result<Url, OpenAiProviderConfigError> {
    let normalized = format!("{}/", base_url.trim_end_matches('/'));
    let base = Url::parse(&normalized)?;
    if !base.username().is_empty() || base.password().is_some() {
        return Err(OpenAiProviderConfigError::EmbeddedCredentials);
    }
    let loopback = base.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host.parse::<std::net::IpAddr>().is_ok_and(|address| address.is_loopback())
    });
    if base.scheme() != "https" && !(base.scheme() == "http" && loopback) {
        return Err(OpenAiProviderConfigError::InsecureUrl);
    }
    Ok(base.join("chat/completions")?)
}

fn wire_request(request: &ChatCompletionsRequest) -> Value {
    let mut body = Map::from_iter([
        ("model".into(), json!(request.model)),
        ("messages".into(), Value::Array(request.messages.iter().map(wire_message).collect())),
        ("stream".into(), Value::Bool(true)),
        ("stream_options".into(), json!({"include_usage": true})),
    ]);
    if !request.tools.is_empty() {
        body.insert("tools".into(), Value::Array(request.tools.clone()));
    }
    Value::Object(body)
}

fn wire_message(message: &ProviderMessage) -> Value {
    match message {
        ProviderMessage::System { content } => json!({"role": "system", "content": content}),
        ProviderMessage::User { content } => json!({"role": "user", "content": content}),
        ProviderMessage::Assistant { content, tool_calls, .. } => {
            let mut message = Map::from_iter([
                ("role".into(), json!("assistant")),
                ("content".into(), json!(content)),
            ]);
            if !tool_calls.is_empty() {
                message.insert("tool_calls".into(), json!(tool_calls));
            }
            Value::Object(message)
        }
        ProviderMessage::Tool { tool_call_id, content, .. } => json!({
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": content,
        }),
    }
}

fn normalized_events(
    response: reqwest::Response,
) -> impl futures_util::Stream<Item = Result<ProviderEvent, ProviderError>> + Send {
    stream! {
        let mut source = response.bytes_stream().eventsource();
        let mut started = false;
        let mut finish_reason = None;
        let mut emitted_terminal = false;
        while let Some(event) = source.next().await {
            let event = match event {
                Ok(event) => event,
                Err(error) => {
                    yield Err(ProviderError::new(format!("invalid SSE stream: {error}")));
                    emitted_terminal = true;
                    break;
                }
            };
            let data = event.data.trim();
            if data == "[DONE]" {
                if !started {
                    yield Ok(ProviderEvent::MessageStart);
                }
                yield Ok(ProviderEvent::Completed {
                    finish_reason: Some(finish_reason.take().unwrap_or_else(|| "stop".into())),
                    provider_data: None,
                });
                emitted_terminal = true;
                break;
            }
            if data.is_empty() {
                continue;
            }

            let chunk = match serde_json::from_str::<ChatCompletionChunk>(data) {
                Ok(chunk) => chunk,
                Err(error) => {
                    yield Ok(ProviderEvent::Malformed {
                        reason: Some(format!("invalid_chat_completion_chunk: {error}")),
                    });
                    emitted_terminal = true;
                    break;
                }
            };
            if let Some(error) = chunk.error {
                yield Ok(ProviderEvent::Error { reason: Some(error.message) });
                emitted_terminal = true;
                break;
            }
            if chunk.choices.len() > 1 {
                yield Ok(ProviderEvent::Malformed {
                    reason: Some("multiple_chat_completion_choices_are_not_supported".into()),
                });
                emitted_terminal = true;
                break;
            }
            if let Some(completed_reason) = finish_reason.as_deref()
                && let Some(choice) = chunk.choices.first()
                && (!choice.delta.is_semantically_empty()
                    || choice
                        .finish_reason
                        .as_deref()
                        .is_some_and(|reason| reason != completed_reason))
            {
                yield Ok(ProviderEvent::Malformed {
                    reason: Some("provider_emitted_data_after_finish_reason".into()),
                });
                emitted_terminal = true;
                break;
            }
            if !started && !chunk.choices.is_empty() {
                started = true;
                yield Ok(ProviderEvent::MessageStart);
            }
            if let Some(choice) = chunk.choices.into_iter().next() {
                if let Some(content) = choice.delta.content
                    && !content.is_empty()
                {
                    yield Ok(ProviderEvent::TextDelta { text: content });
                }
                if let Some(reasoning) = choice.delta.reasoning_content.or(choice.delta.reasoning)
                    && !reasoning.is_empty()
                {
                    yield Ok(ProviderEvent::ReasoningDelta { text: reasoning });
                }
                for call in choice.delta.tool_calls {
                    let function = call.function.unwrap_or_default();
                    yield Ok(ProviderEvent::ToolCallDelta {
                        index: call.index,
                        id: call.id,
                        name: function.name,
                        arguments_delta: function.arguments.unwrap_or_default(),
                    });
                }
                if let Some(reason) = choice.finish_reason {
                    finish_reason = Some(reason);
                }
            }
            if let Some(usage) = chunk.usage {
                yield Ok(ProviderEvent::Usage {
                    prompt_tokens: usage.prompt_tokens,
                    completion_tokens: usage.completion_tokens,
                    total_tokens: usage.total_tokens,
                    cached_tokens: usage
                        .prompt_tokens_details
                        .map_or(0, |details| details.cached_tokens),
                });
            }
        }
        if !emitted_terminal
            && let Some(reason) = finish_reason
        {
            yield Ok(ProviderEvent::Completed {
                finish_reason: Some(reason),
                provider_data: None,
            });
        }
    }
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

#[derive(Debug, Deserialize)]
struct ChatCompletionChunk {
    #[serde(default)]
    choices: Vec<ChunkChoice>,
    #[serde(default)]
    usage: Option<ChunkUsage>,
    #[serde(default)]
    error: Option<ChunkError>,
}

#[derive(Debug, Deserialize)]
struct ChunkChoice {
    #[serde(default)]
    delta: ChunkDelta,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ChunkDelta {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    reasoning_content: Option<String>,
    #[serde(default)]
    reasoning: Option<String>,
    #[serde(default)]
    tool_calls: Vec<ChunkToolCall>,
}

impl ChunkDelta {
    fn is_semantically_empty(&self) -> bool {
        self.content.as_deref().is_none_or(str::is_empty)
            && self.reasoning_content.as_deref().is_none_or(str::is_empty)
            && self.reasoning.as_deref().is_none_or(str::is_empty)
            && self.tool_calls.is_empty()
    }
}

#[derive(Debug, Deserialize)]
struct ChunkToolCall {
    index: usize,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    function: Option<ChunkFunction>,
}

#[derive(Debug, Default, Deserialize)]
struct ChunkFunction {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChunkUsage {
    #[serde(default)]
    prompt_tokens: u64,
    #[serde(default)]
    completion_tokens: u64,
    #[serde(default)]
    total_tokens: u64,
    #[serde(default)]
    prompt_tokens_details: Option<PromptTokenDetails>,
}

#[derive(Debug, Deserialize)]
struct PromptTokenDetails {
    #[serde(default)]
    cached_tokens: u64,
}

#[derive(Debug, Deserialize)]
struct ChunkError {
    message: String,
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use ports::Provider;
    use protocol::{ChatCompletionsRequest, ProviderEvent, ProviderMessage};
    use serde_json::json;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers};

    use super::{OpenAiCompatibleProvider, OpenAiProviderConfigError};

    #[test]
    fn validates_endpoint_security_before_requests() {
        assert!(OpenAiCompatibleProvider::validate_base_url("https://example.com/v1").is_ok());
        assert!(OpenAiCompatibleProvider::validate_base_url("http://127.0.0.1:11434/v1").is_ok());
        assert!(matches!(
            OpenAiCompatibleProvider::validate_base_url("http://example.com/v1"),
            Err(OpenAiProviderConfigError::InsecureUrl)
        ));
        assert!(matches!(
            OpenAiCompatibleProvider::validate_base_url("https://user:secret@example.com/v1"),
            Err(OpenAiProviderConfigError::EmbeddedCredentials)
        ));
    }

    #[tokio::test]
    async fn streams_text_usage_and_a_terminal() -> Result<(), Box<dyn std::error::Error>> {
        let server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/v1/chat/completions"))
            .and(matchers::header("authorization", "Bearer test-key"))
            .and(matchers::body_json(json!({
                "model": "test-model",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": true,
                "stream_options": {"include_usage": true}
            })))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(concat!(
                        "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"hel\"},\"finish_reason\":null}]}\n\n",
                        "data: {\"choices\":[{\"delta\":{\"content\":\"lo\"},\"finish_reason\":\"stop\"}]}\n\n",
                        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5,\"prompt_tokens_details\":{\"cached_tokens\":1}}}\n\n",
                        "data: [DONE]\n\n",
                    )),
            )
            .mount(&server)
            .await;

        let mut provider = OpenAiCompatibleProvider::new(
            &format!("{}/v1", server.uri()),
            Some("test-key".into()),
        )?;
        let mut attempt = provider
            .stream(ChatCompletionsRequest {
                model: "test-model".into(),
                messages: vec![ProviderMessage::User { content: "hello".into() }],
                tools: Vec::new(),
            })
            .await?;
        let events = attempt.events.by_ref().collect::<Vec<_>>().await;

        assert_eq!(
            events,
            vec![
                Ok(ProviderEvent::MessageStart),
                Ok(ProviderEvent::TextDelta { text: "hel".into() }),
                Ok(ProviderEvent::TextDelta { text: "lo".into() }),
                Ok(ProviderEvent::Usage {
                    prompt_tokens: 3,
                    completion_tokens: 2,
                    total_tokens: 5,
                    cached_tokens: 1,
                }),
                Ok(ProviderEvent::Completed {
                    finish_reason: Some("stop".into()),
                    provider_data: None,
                }),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn accepts_repeated_terminal_choice_when_it_only_carries_usage()
    -> Result<(), Box<dyn std::error::Error>> {
        let server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(concat!(
                        "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\n",
                        "data: {\"choices\":[{\"delta\":{\"content\":\"\",\"role\":\"assistant\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1,\"total_tokens\":4}}\n\n",
                    )),
            )
            .mount(&server)
            .await;

        let mut provider = OpenAiCompatibleProvider::new(&server.uri(), None)?;
        let attempt = provider
            .stream(ChatCompletionsRequest {
                model: "test-model".into(),
                messages: vec![ProviderMessage::User { content: "hello".into() }],
                tools: Vec::new(),
            })
            .await?;
        let events = attempt.events.collect::<Vec<_>>().await;

        assert_eq!(
            events,
            vec![
                Ok(ProviderEvent::MessageStart),
                Ok(ProviderEvent::TextDelta { text: "hello".into() }),
                Ok(ProviderEvent::Usage {
                    prompt_tokens: 3,
                    completion_tokens: 1,
                    total_tokens: 4,
                    cached_tokens: 0,
                }),
                Ok(ProviderEvent::Completed {
                    finish_reason: Some("stop".into()),
                    provider_data: None,
                }),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn rejects_semantic_data_after_finish_reason() -> Result<(), Box<dyn std::error::Error>> {
        let server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(concat!(
                        "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\n",
                        "data: {\"choices\":[{\"delta\":{\"content\":\"late mutation\"},\"finish_reason\":null}]}\n\n",
                    )),
            )
            .mount(&server)
            .await;

        let mut provider = OpenAiCompatibleProvider::new(&server.uri(), None)?;
        let attempt = provider
            .stream(ChatCompletionsRequest {
                model: "test-model".into(),
                messages: vec![ProviderMessage::User { content: "hello".into() }],
                tools: Vec::new(),
            })
            .await?;
        let events = attempt.events.collect::<Vec<_>>().await;

        assert_eq!(
            events,
            vec![
                Ok(ProviderEvent::MessageStart),
                Ok(ProviderEvent::TextDelta { text: "hello".into() }),
                Ok(ProviderEvent::Malformed {
                    reason: Some("provider_emitted_data_after_finish_reason".into()),
                }),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn strips_kernel_only_tool_result_fields_from_wire_request()
    -> Result<(), Box<dyn std::error::Error>> {
        let server = MockServer::start().await;
        Mock::given(matchers::method("POST"))
            .and(matchers::path("/chat/completions"))
            .and(matchers::body_json(json!({
                "model": "test-model",
                "messages": [{
                    "role": "tool",
                    "tool_call_id": "call-1",
                    "content": "contents"
                }],
                "stream": true,
                "stream_options": {"include_usage": true}
            })))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string("data: [DONE]\n\n"),
            )
            .mount(&server)
            .await;

        let mut provider = OpenAiCompatibleProvider::new(&server.uri(), None)?;
        let attempt = provider
            .stream(ChatCompletionsRequest {
                model: "test-model".into(),
                messages: vec![ProviderMessage::Tool {
                    tool_call_id: "call-1".into(),
                    status: "succeeded".into(),
                    content: "contents".into(),
                    execution_key: "secret-internal-key".into(),
                }],
                tools: Vec::new(),
            })
            .await?;
        let events = attempt.events.collect::<Vec<_>>().await;
        assert_eq!(
            events,
            vec![
                Ok(ProviderEvent::MessageStart),
                Ok(ProviderEvent::Completed {
                    finish_reason: Some("stop".into()),
                    provider_data: None,
                }),
            ]
        );
        Ok(())
    }
}
